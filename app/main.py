import os
import sys

from app.ocr.ocr_service import OcrService
from app.ocr.ocr_service_impl import OcrServiceImpl

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))


import asyncio
import pytz
from dotenv import load_dotenv

from app.storage.aio_boto import AioBoto
from app.messaging.consume.aio_consumer import AioConsumer

KST = pytz.timezone("Asia/Seoul")
load_dotenv()

minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_user = os.getenv("RABBITMQ_USER")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_consume_queue = os.getenv("RABBITMQ_CONSUME_QUEUE")
rabbitmq_produce_queue = os.getenv("RABBITMQ_PRODUCE_QUEUE")

if not minio_host or not minio_port:
    raise ValueError("MINIO_HOST 또는 MINIO_PORT 환경 변수가 설정되지 않았습니다.")

if not rabbitmq_host or not rabbitmq_port or not rabbitmq_user or not rabbitmq_password:
    raise ValueError("RabbitMQ 환경 변수가 올바르게 설정되지 않았습니다.")


async def main():
    minio = AioBoto(f"http://{minio_host}:{minio_port}")
    await minio.connect()

    ocr_service = OcrServiceImpl()

    consumer = AioConsumer(
        minio_manager=minio,
        amqp_url=f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/",
        consume_queue=rabbitmq_consume_queue,
        produce_queue=rabbitmq_produce_queue,
        ocr_service=ocr_service,
    )
    await consumer.connect()

    consume_task = asyncio.create_task(consumer.consume())

    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("KeyboardInterrupt가 감지되었습니다. 종료 중...")
    finally:
        consume_task.cancel()
        try:
            await consume_task
        except asyncio.CancelledError:
            print("consume_task 취소됨")
        await consumer.close()
        if hasattr(minio, "close"):
            await minio.close()

        current_task = asyncio.current_task()
        pending = [task for task in asyncio.all_tasks() if task is not current_task]
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

        loop = asyncio.get_running_loop()
        await loop.shutdown_asyncgens()

        print("자원 정리 완료. 프로그램 종료.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("프로그램이 종료되었습니다.")
