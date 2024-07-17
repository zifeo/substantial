# Generated by the protocol buffer compiler.  DO NOT EDIT!
# sources: protocol/events.proto
# plugin: python-betterproto
# This file has been @generated

from dataclasses import dataclass
from datetime import datetime
from typing import List

import betterproto
import betterproto.lib.google.protobuf as betterproto_lib_google_protobuf


@dataclass(eq=False, repr=False)
class Start(betterproto.Message):
    kwargs: "betterproto_lib_google_protobuf.Struct" = betterproto.message_field(1)


@dataclass(eq=False, repr=False)
class Save(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    value: str = betterproto.string_field(2)
    counter: int = betterproto.int32_field(3)


@dataclass(eq=False, repr=False)
class Sleep(betterproto.Message):
    id: int = betterproto.uint32_field(1)
    start: datetime = betterproto.message_field(2)
    end: datetime = betterproto.message_field(3)


@dataclass(eq=False, repr=False)
class Send(betterproto.Message):
    name: str = betterproto.string_field(1)
    value: str = betterproto.string_field(2)


@dataclass(eq=False, repr=False)
class Stop(betterproto.Message):
    ok: str = betterproto.string_field(1, group="result")
    err: str = betterproto.string_field(2, group="result")


@dataclass(eq=False, repr=False)
class Event(betterproto.Message):
    at: datetime = betterproto.message_field(1)
    start: "Start" = betterproto.message_field(10, group="of")
    save: "Save" = betterproto.message_field(11, group="of")
    sleep: "Sleep" = betterproto.message_field(12, group="of")
    send: "Send" = betterproto.message_field(13, group="of")
    stop: "Stop" = betterproto.message_field(14, group="of")


@dataclass(eq=False, repr=False)
class Records(betterproto.Message):
    run_id: str = betterproto.string_field(1)
    events: List["Event"] = betterproto.message_field(2)
