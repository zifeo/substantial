import asyncio
from datetime import timedelta
import inspect

import orjson as json

import time
from typing import Any, Callable, Union, Optional
from pydantic.dataclasses import dataclass

from substantial.protos import events


class Interrupt(BaseException):
    hint: str

    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""


class RetryMode(BaseException):
    def __init__(self, delta: timedelta, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""
        self.delta = delta


class RetryFail(BaseException):
    def __init__(self, error_message: str) -> None:
        self.error_message = error_message


class DelayMode(BaseException):
    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""


class CancelWorkflow(BaseException):
    hint: str

    def __init__(self, hint: Union[str, None] = None) -> None:
        self.hint = hint or ""


class AppError(BaseException):
    pass


@dataclass
class RetryStrategy:
    max_retries: int
    initial_backoff_interval: Union[int, None]
    max_backoff_interval: Union[int, None]
    compensate_on_first_fail: bool = False

    def __post_init__(self):
        if self.max_retries < 1:
            raise AppError("max_retries < 1")
        low = self.initial_backoff_interval
        high = self.max_backoff_interval
        if low is not None and high is not None:
            if low >= high:
                raise AppError("initial_backoff_interval >= max_backoff_interval")
            if low < 0:
                raise AppError("initial_backoff_interval < 0")
        elif low is not None and high is None:
            self.max_backoff_interval = low + 10
        elif low is None and high is not None:
            self.initial_backoff_interval = max(
                0,
                self.max_backoff_interval - 10,
            )

    def linear(self, retries_left: int) -> timedelta:
        """Scaled timeout in seconds"""
        if retries_left <= 0:
            raise Exception("retries_left <= 0")
        dt = self.max_backoff_interval - self.initial_backoff_interval
        return timedelta(
            int(((self.max_retries - retries_left) * dt) / self.max_retries)
        )


@dataclass
class CompensationStrategy:
    compensate_fn: Optional[Callable[[], Any]] = None
    max_compensation_attemps: int = 3


class CompensationFailed(BaseException):
    def __init__(
        self,
        original_error: Exception,
        compensation_error: Optional[Exception] = None,
    ):
        self.original_error = original_error
        self.compensation_error = compensation_error


@dataclass
class ValueEval:
    lambda_fn: Callable[[], Any]
    timeout: Union[float, None]
    retry_strategy: Union[RetryStrategy, None]
    compensate_fn: Optional[Callable[[], Any]]
    max_compensation_attempts: int = 3

    async def exec(
        self,
        ctx,
        save_id,
        counter,
    ) -> Any:
        strategy = self.retry_strategy or RetryStrategy(
            max_retries=3, initial_backoff_interval=0, max_backoff_interval=10
        )

        try:
            # if there is compensation, we need to add it to the stack
            if self.compensate_fn:
                ctx.compensation_stack.append(self.compensate_fn)

            # ctx.source(LogKind.Meta, inspect.getsource(self.lambda_fn))
            before_spawn = time.time()
            op = self.lambda_fn()

            ret = None
            if inspect.iscoroutine(op):
                after_spawn = time.time()
                elapsed_after_spawn = after_spawn - before_spawn
                timeout = (
                    None
                    if self.timeout is None
                    else max(0.0001, self.timeout - elapsed_after_spawn)
                )
                ret = await asyncio.wait_for(op, timeout)
            elif not inspect.isfunction(op):
                after_spawn = time.time()
                elapsed_after_spawn = after_spawn - before_spawn
                if self.timeout is not None and elapsed_after_spawn > self.timeout:
                    pass  # raise? in a way, op has been resolved at this stage
                ret = op
            else:
                raise Exception(
                    f"Expected value or coroutine object, got {type(op)} instead"
                )

            save = events.Save(save_id, json.dumps(ret), -1)
            ctx.source(events.Event(save=save))
            return ret
        except Exception as e:
            if strategy.compensate_on_first_fail:
                await self._trigger_compensation(ctx, save_id, e)
                # should be same as save fn without compensate fn ?
                message = f"{type(e).__name__}: {e}"
                raise RetryFail(message)

            counter = counter or 1
            retries_left = strategy.max_retries - counter
            if retries_left > 0:
                save = events.Save(
                    save_id,
                    json.dumps(None),
                    counter + 1,
                )
                ctx.source(events.Event(save=save))

                delta = strategy.linear(retries_left)
                raise RetryMode(delta)
            else:
                await self._trigger_compensation(ctx, save_id, e)
                message = f"{type(e).__name__}: {e}"
                raise RetryFail(message)

    async def _trigger_compensation(self, ctx, save_id, error):
        compensation_stack = ctx.compensation_stack.copy()
        if compensation_stack and len(compensation_stack):
            compensation_stack.reverse()
            for compensation_fn in compensation_stack:
                try:
                    compensation_result = compensation_fn()
                    if inspect.iscoroutine(compensation_result):
                        compensation_result = await compensation_result
                    ctx.source(
                        events.Event(
                            compensation=events.Compensation(
                                save_id=save_id,
                                error=str(error),
                                compensation_result=json.dumps(
                                    compensation_result,
                                ),
                            )
                        )
                    )
                except Exception as compensation_error:
                    raise CompensationFailed(error, compensation_error)


Empty: Any = object()
