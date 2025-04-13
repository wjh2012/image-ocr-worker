import io
import os
from datetime import datetime

import aioboto3
from PIL import Image

from app.config.custom_logger import time_logger


class AioBoto:
    def __init__(self, minio_url: str):
        self.minio_url = minio_url
        self._session = None
        self.s3_client_cm = None
        self.s3_client = None

    async def connect(self):
        self._session = aioboto3.Session()
        self.s3_client_cm = self._session.client(
            "s3",
            endpoint_url=self.minio_url,
            aws_access_key_id=os.getenv("MINIO_USER", "admin"),
            aws_secret_access_key=os.getenv("MINIO_PASSWORD", "adminadmin"),
        )
        self.s3_client = await self.s3_client_cm.__aenter__()

    @time_logger
    async def upload_image_with_client(self, file, bucket_name: str, key: str):
        await self.s3_client.upload_fileobj(file, Bucket=bucket_name, Key=key)
        print(f"✅ MinIO client 파일 업로드 성공: {key} (Bucket: {bucket_name})")

    @time_logger
    async def download_image_with_client(
        self, bucket_name: str, key: str, file_obj: io.BytesIO
    ):
        await self.s3_client.download_fileobj(
            Bucket=bucket_name, Key=key, Fileobj=file_obj
        )

    async def close(self):
        if self.s3_client_cm:
            await self.s3_client_cm.__aexit__(None, None, None)
        print("❌ Minio 연결 종료")


minio_host = os.getenv("MINIO_HOST")
minio_port = os.getenv("MINIO_PORT")
minio_manager = AioBoto(f"http://{minio_host}:{minio_port}")


async def load_image_from_minio(bucket_name, file_name):
    file_obj = io.BytesIO()
    await minio_manager.download_image_with_client(bucket_name, file_name, file_obj)
    file_obj.seek(0)
    image = Image.open(file_obj)
    return image, datetime.now()
