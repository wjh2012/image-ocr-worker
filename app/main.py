import os

import asyncio

from app.storage.aio_boto import AioBoto
from app.message_queue.aio_consumer import AioConsumer


from dotenv import load_dotenv

load_dotenv()

minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
rabbitmq_host = os.getenv("RABBITMQ_HOST")
rabbitmq_port = os.getenv("RABBITMQ_PORT")
rabbitmq_user = os.getenv("RABBITMQ_USER")
rabbitmq_password = os.getenv("RABBITMQ_PASSWORD")
rabbitmq_consume_queue = os.getenv("RABBITMQ_CONSUME_QUEUE")

if not minio_host or not minio_port:
    raise ValueError("MINIO_HOST 또는 MINIO_PORT 환경 변수가 설정되지 않았습니다.")

if not rabbitmq_host or not rabbitmq_port or not rabbitmq_user or not rabbitmq_password:
    raise ValueError("RabbitMQ 환경 변수가 올바르게 설정되지 않았습니다.")


async def main():
    minio = AioBoto(f"http://{minio_host}:{minio_port}")
    await minio.connect()
    consumer = AioConsumer(
        minio_manager=minio,
        amqp_url=f"amqp://{rabbitmq_user}:{rabbitmq_password}@{rabbitmq_host}:{rabbitmq_port}/",
        queue_name=rabbitmq_consume_queue,
    )

    await consumer.connect()
    await consumer.consume()

    try:
        await asyncio.Future()
    except KeyboardInterrupt:
        await consumer.close()


if __name__ == "__main__":
    asyncio.run(main())
