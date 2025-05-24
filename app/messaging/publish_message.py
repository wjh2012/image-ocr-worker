from dataclasses import dataclass
from typing import Literal


@dataclass
class PublishMessageHeader:
    event_id: str
    event_type: Literal["image.thumbnail.result"]
    trace_id: str
    timestamp: str
    source_service: str


@dataclass
class OcrServiceData:
    text: list[str]


@dataclass
class PublishMessageBody:
    gid: str
    status: Literal["success", "fail"]
    completed_at: str
    payload: OcrServiceData
