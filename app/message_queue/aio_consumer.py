import io
import json
import uuid

import aio_pika
import logging

import numpy as np
from PIL import Image
from aio_pika.abc import AbstractIncomingMessage

from app.db.mongo import mongo_client, mongo_collection
from app.service.blank_detector import BlankDetector
from app.storage.aio_boto import AioBoto
from app.db.database import AsyncSessionLocal
from app.db.models import ImageValidationResult

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class AioConsumer:
    def __init__(
        self,
        minio_manager: AioBoto,
        amqp_url: str,
        queue_name: str,
        prefetch_count: int = 1,
    ):
        self.amqp_url = amqp_url
        self.queue_name = queue_name
        self.prefetch_count = prefetch_count
        self.minio_manager = minio_manager
        self._connection = None
        self._channel = None
        self._queue = None
        self._dlx = None
        self._dlq = None
        self.blank_detector = BlankDetector(
            bin_threshold=150, blank_threshold_ratio=0.99999
        )

    async def connect(self):
        self._connection = await aio_pika.connect_robust(self.amqp_url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self.prefetch_count)

        self._dlx = await self._channel.declare_exchange(
            "dead_letter_exchange", aio_pika.ExchangeType.DIRECT
        )
        self._dlq = await self._channel.declare_queue("dead_letter_queue")
        await self._dlq.bind(self._dlx, routing_key="dead_letter")

        args = {
            "x-dead-letter-exchange": "dead_letter_exchange",
            "x-dead-letter-routing-key": "dead_letter",
            "x-message-ttl": 10000,  # 10초
        }

        self._queue = await self._channel.declare_queue(
            self.queue_name, durable=True, arguments=args
        )
        logging.info(f"✅ RabbitMQ 연결 성공: {self.amqp_url}, 큐: {self.queue_name}")

    async def on_message(self, message: AbstractIncomingMessage) -> None:
        async with message.process(requeue=True):
            logging.info("📩 메시지 수신!")

            data = json.loads(message.body)
            gid = data["gid"]
            file_name = data["file_name"]
            bucket_name = data["bucket"]

            try:
                gid = uuid.UUID(gid)
            except ValueError:
                logging.error(f"❌ 유효하지 않은 UUID 형식: {gid}")
                return

            file_obj = io.BytesIO()
            await self.minio_manager.download_image_with_client(
                bucket_name=bucket_name, key=file_name, file_obj=file_obj
            )
            file_length = file_obj.getbuffer().nbytes
            logging.info(f"✅ MinIO 파일 다운로드 성공: Size: {file_length} bytes")

            file_obj.seek(0)

            try:
                image = Image.open(file_obj)
                image_np = np.array(image)
            except Exception as e:
                logging.error(f"이미지 변환 실패: {e}")
                file_obj.close()
                return

            is_blank = self.blank_detector.is_blank_image(image_np)
            file_obj.close()

            async with AsyncSessionLocal() as session:
                validation_result = ImageValidationResult(
                    gid=gid, is_blank=is_blank, is_folded=False, tilt_angle=0.1
                )
                session.add(validation_result)
                await session.commit()
                logging.info("✅ DB에 정보 저장 완료")

            session = await mongo_client.start_session()
            async with session:
                async with session.start_transaction():  # 트랜잭션 시작
                    await mongo_collection.insert_one(
                        {"name": "Alice", "age": 25}, session=session
                    )
                    await mongo_collection.insert_one(
                        {"name": "Bob", "age": 30}, session=session
                    )
                    logging.info("✅ nosql에 정보 저장 완료")

    async def consume(self):
        if not self._queue:
            await self.connect()

        logging.info(f"📡 큐({self.queue_name})에서 메시지 소비 시작...")
        await self._queue.consume(self.on_message, no_ack=False)

    async def close(self):
        if self._connection:
            await self._connection.close()
            logging.info("🔴 RabbitMQ 연결 종료")
