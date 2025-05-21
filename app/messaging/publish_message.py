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
class PublishMessagePayload:
    gid: str
    status: str
    completed_at: str
    ocr_result: any
