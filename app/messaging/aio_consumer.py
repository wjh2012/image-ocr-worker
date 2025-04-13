import io
import json
from datetime import datetime

import aio_pika
import logging

from PIL import Image
from aio_pika.abc import AbstractIncomingMessage
from pymongo.errors import PyMongoError

from app.db.mongo import mongo_collection
from app.service.ocr_service import OcrService
from app.storage.aio_boto import AioBoto

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
        self.ocr_service = OcrService()

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
            message_received_time = datetime.now()
            logging.info("📩 메시지 수신!")

            data = json.loads(message.body)
            gid = data["gid"]
            file_name = data["file_name"]
            bucket_name = data["bucket"]

            file_obj = io.BytesIO()
            await self.minio_manager.download_image_with_client(
                bucket_name=bucket_name, key=file_name, file_obj=file_obj
            )
            file_length = file_obj.getbuffer().nbytes
            logging.info(f"✅ MinIO 파일 다운로드 성공: Size: {file_length} bytes")

            file_obj.seek(0)

            try:
                image = Image.open(file_obj)
            except Exception as e:
                logging.error(f"이미지 변환 실패: {e}")
                file_obj.close()
                return
            file_received_time = datetime.now()

            ocr_result = self.ocr_service.ocr(image)
            created_time = datetime.now()
            file_obj.close()

            try:
                result = await mongo_collection.insert_one(
                    {
                        "gid": gid,
                        "ocr_result": ocr_result.txts,
                        "message_received_time": message_received_time,
                        "file_received_time": file_received_time,
                        "created_time": created_time,
                    }
                )

                if result.inserted_id:
                    logging.info(f"✅ nosql에 정보 저장 완료, ID: {result.inserted_id}")
                else:
                    logging.warning("⚠️ nosql에 정보가 저장되지 않았습니다.")

            except PyMongoError as e:
                logging.error(f"❌ nosql 저장 실패: {e}")

    async def consume(self):
        if not self._queue:
            await self.connect()

        logging.info(f"📡 큐({self.queue_name})에서 메시지 소비 시작...")
        await self._queue.consume(self.on_message, no_ack=False)

    async def close(self):
        if self._connection:
            await self._connection.close()
            logging.info("🔴 RabbitMQ 연결 종료")
