import os
import sys


sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from app.ocr.ocr_service_impl import OcrServiceImpl

import asyncio
import pytz

from app.config.env_config import get_settings
from app.storage.aio_boto import AioBoto
from app.messaging.consume.aio_consumer import AioConsumer

config = get_settings()

KST = pytz.timezone("Asia/Seoul")


async def main():
    minio = AioBoto()
    await minio.connect()

    ocr_service = OcrServiceImpl()

    consumer = AioConsumer(
        minio_manager=minio,
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
