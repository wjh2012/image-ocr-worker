import json
import logging
from dataclasses import dataclass
from typing import Literal

from aio_pika.abc import AbstractIncomingMessage
from typing_extensions import Optional


@dataclass
class ConsumeMessageHeader:
    event_id: str
    event_type: Literal["image.thumbnail.requested",]
    trace_id: str
    timestamp: str
    source_service: str


@dataclass
class ConsumeMessagePayload:
    gid: str
    bucket: str
    original_object_key: str


# ✅ 파싱 유틸 함수
def parse_message(
    message: AbstractIncomingMessage,
) -> tuple[Optional[ConsumeMessageHeader], Optional[ConsumeMessagePayload]]:
    try:
        body_dict = json.loads(message.body.decode())
        payload = ConsumeMessagePayload(**body_dict)

        headers_dict = message.headers
        header = ConsumeMessageHeader(**headers_dict)

        return header, payload

    except (json.JSONDecodeError, KeyError, TypeError, ValueError) as e:
        logging.error(f"❌ 메시지 파싱 실패: {e}")
        return None, None
