import queue
from typing import Union
from uuid import uuid4
import logging

import asyncio
import aioprocessing

import logging.handlers

std_log_handler = logging.StreamHandler()
std_log_handler.setFormatter(logging.Formatter(fmt=" %(name)s :: %(message)s"))

batch_log_handler = logging.handlers.MemoryHandler(
    capacity=5,
    flushLevel=logging.ERROR,
    flushOnClose=True,
    target=std_log_handler,
)


def process_worker(send_queue, receive_queue, i):
    logger = logging.getLogger(f"worker-{i}")
    logger.propagate = False
    logger.addHandler(batch_log_handler)

    while True:
        id, f = send_queue.get()
        logger.info("Processing")
        res = f()
        receive_queue.put([id, res])
        send_queue.task_done()


class MultithreadedQueue:
    """ Process functions accross different threads """

    def __init__(self, num_workers=2):
        self.num_workers = num_workers
        self.active = False

    async def __aenter__(self):
        assert not self.active
        self.active = True

        # wait_on

        self.send_queue = aioprocessing.AioJoinableQueue()
        self.receive_queue = aioprocessing.AioJoinableQueue()
        self.fs = {}
        self.ps = [
            aioprocessing.AioProcess(
                target=process_worker, args=(self.send_queue, self.receive_queue, i)
            )
            for i in range(self.num_workers)
        ]
        for p in self.ps:
            p.start()

        self.receiver = asyncio.create_task(self.receive())
        return self.send

    async def send(self, f, id: Union[str, None] = None):
        id = id or uuid4()
        self.fs[id] = asyncio.Future()
        await self.send_queue.coro_put([id, f])
        return await self.fs[id]

    async def receive(self):
        while True:
            try:
                # timeout enforces the threadpoolexecutor to refresh regularly and accept cancellation
                id, res = await self.receive_queue.coro_get(timeout=0.5)
            except queue.Empty:
                continue
            except asyncio.CancelledError:
                break

            self.fs[id].set_result(res)
            del self.fs[id]
            self.receive_queue.task_done()

    async def __aexit__(self, exc_type, exc, tb):
        assert self.active

        await self.send_queue.coro_join()
        self.send_queue.close()

        await self.receive_queue.coro_join()
        self.receive_queue.close()

        for p in self.ps:
            p.terminate()

        self.receiver.cancel()
        await self.receiver

        self.active = False
