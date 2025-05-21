import io
import json
from dataclasses import asdict
from datetime import datetime, timezone

import aio_pika
import logging

from PIL import Image
from aio_pika.abc import AbstractIncomingMessage
from pymongo.errors import PyMongoError
from uuid_extensions import uuid7str

from app.config.env_config import get_settings
from app.db.mongo import mongo_collection
from app.messaging.consume_message import parse_message
from app.messaging.publish_message import PublishMessageHeader, PublishMessagePayload
from app.ocr.ocr_service import OcrService
from app.storage.aio_boto import AioBoto

config = get_settings()

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AioConsumer:
    def __init__(
        self,
        minio_manager: AioBoto,
        ocr_service: OcrService,
    ):
        self.minio_manager = minio_manager
        self.ocr_service = ocr_service

        self.amqp_url = f"amqp://{config.rabbitmq_user}:{config.rabbitmq_password}@{config.rabbitmq_host}:{config.rabbitmq_port}"

        self.consume_exchange_name = config.rabbitmq_image_ocr_consume_exchange
        self.consume_queue_name = config.rabbitmq_image_ocr_consume_queue
        self.consume_routing_key = config.rabbitmq_image_ocr_consume_routing_key

        self.publish_exchange_name = config.rabbitmq_image_ocr_publish_exchange
        self.publish_routing_key = config.rabbitmq_image_ocr_publish_routing_key

        self.prefetch_count = 1

        self.dlx_name = config.rabbitmq_image_ocr_dlx
        self.dlx_routing_key = config.rabbitmq_image_ocr_dlx_routing_key

        self._connection = None
        self._channel = None

        self._consume_exchange = None
        self._consume_queue = None

        self._publish_exchange = None

        self._dlx = None
        self._dlq = None

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url)
        self._channel = await self._connection.channel()

        await self._channel.set_qos(prefetch_count=self.prefetch_count)

        self._publish_exchange = await self._channel.declare_exchange(
            self.publish_exchange_name, aio_pika.ExchangeType.DIRECT, durable=True
        )
        self._consume_exchange = await self._channel.get_exchange(
            self.consume_exchange_name
        )
        self._dlx = await self._channel.declare_exchange(
            self.dlx_name, aio_pika.ExchangeType.DIRECT
        )

        args = {
            "x-dead-letter-exchange": self.dlx_name,
            "x-dead-letter-routing-key": self.dlx_routing_key,
            "x-message-ttl": 10000,  # 10초
        }

        self._consume_queue = await self._channel.declare_queue(
            self.consume_queue_name, durable=True, arguments=args
        )
        await self._consume_queue.bind(
            self._consume_exchange, routing_key=self.consume_routing_key
        )
        logging.info(
            f"✅ RabbitMQ 연결 성공: {self.amqp_url}, 큐: {self.consume_queue_name}"
        )

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            message_received_time = datetime.now(timezone.utc).isoformat()
            logging.info("📩 메시지 수신!")

            header, payload = parse_message(message)
            if not header or not payload:
                logging.warning("⚠️ 메시지 파싱 실패로 인해 처리를 중단합니다.")
                return

            gid = payload.gid
            original_object_key = payload.original_object_key
            download_bucket_name = payload.bucket

            logging.info(
                f"✅ 메시지 파싱 완료 - GID: {gid}, Bucket: {download_bucket_name}"
            )

            file_obj = io.BytesIO()

            await self.minio_manager.download_image_with_client(
                bucket_name=download_bucket_name,
                key=original_object_key,
                file_obj=file_obj,
            )
            file_length = file_obj.getbuffer().nbytes
            logging.info(f"✅ MinIO 파일 다운로드 성공: Size: {file_length} bytes")

            file_obj.seek(0)

            try:
                image = Image.open(file_obj)
            except Exception as e:
                logging.error(f"이미지 변환 실패: {e}")
                file_obj.close()
                await message.reject(requeue=False)
                return

            file_received_time = datetime.now(timezone.utc).isoformat()

            ocr_result = self.ocr_service.ocr(image)
            ocr_completed_time = datetime.now(timezone.utc).isoformat()
            file_obj.close()

            try:
                result = await mongo_collection.insert_one(
                    {
                        "gid": gid,
                        "ocr_result": ocr_result.txts,
                        "message_received_time": message_received_time,
                        "file_received_time": file_received_time,
                        "ocr_completed_time": ocr_completed_time,
                    }
                )

                if result.inserted_id:
                    logging.info(f"✅ nosql에 정보 저장 완료, ID: {result.inserted_id}")
                    body = PublishMessagePayload(
                        gid=gid,
                        status="success",
                        completed_at=ocr_completed_time,
                        ocr_result=ocr_result.txts,
                    )
                    await self.publish_message(trace_id=header.trace_id, body=body)

                else:
                    logging.warning("⚠️ nosql에 정보가 저장되지 않았습니다.")

            except PyMongoError as e:
                logging.error(f"❌ nosql 저장 실패: {e}")

    async def publish_message(self, trace_id: str, body: PublishMessagePayload):
        event_id = uuid7str()

        headers = PublishMessageHeader(
            event_id=event_id,
            event_type=self.publish_routing_key,
            trace_id=trace_id,
            timestamp=datetime.now(timezone.utc).isoformat(),
            source_service="image-mock-producer",
        )

        message = aio_pika.Message(
            body=json.dumps(asdict(body)).encode("utf-8"),
            headers=asdict(headers),
            content_type="application/json",
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        )

        await self._publish_exchange.publish(
            message=message,
            routing_key=self.publish_routing_key,
        )
        logging.info(f"📤 메시지 발행 완료: {self.publish_routing_key}")

    async def consume(self):
        if not self._consume_queue:
            await self.connect()

        logging.info(f"📡 큐({self.consume_queue_name})에서 메시지 소비 시작...")
        await self._consume_queue.consume(self.on_message, no_ack=False)

    async def close(self):
        if self._connection:
            await self._connection.close()
            logging.info("🔴 RabbitMQ 연결 종료")
