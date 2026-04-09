import asyncio
import json
import logging
from typing import Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

from validator.config import S3Config

logger = logging.getLogger(__name__)


class S3Repository:
    def __init__(self, s3_config: S3Config):
        self.s3_config = s3_config
        self._s3_client = None
        self._bucket_name: Optional[str] = None
        self._endpoint_url: Optional[str] = None
        self._s3_prefix = ""
        self._parse_bucket_url()

    def _parse_bucket_url(self):
        if self.s3_config.bucket_name:
            self._bucket_name = self.s3_config.bucket_name
            self._endpoint_url = self.s3_config.endpoint_url
        else:
            from urllib.parse import urlparse

            parsed = urlparse(self.s3_config.bucket_url)
            path_parts = parsed.path.strip("/").split("/")
            if path_parts and path_parts[0]:
                self._bucket_name = path_parts[0]
                self._endpoint_url = f"{parsed.scheme}://{parsed.netloc}"
            else:
                self._endpoint_url = self.s3_config.bucket_url.rstrip("/")
                self._bucket_name = "blockmachine-gateway-logs"

        self._s3_prefix = getattr(self.s3_config, "prefix", "") or ""
        logger.info(
            f"S3: endpoint={self._endpoint_url}, "
            f"bucket={self._bucket_name}, "
            f"prefix={self._s3_prefix or '(none)'}"
        )

    def _get_s3(self):
        if self._s3_client is None:
            self._s3_client = boto3.client(
                "s3",
                endpoint_url=self._endpoint_url,
                region_name=self.s3_config.region,
                aws_access_key_id=self.s3_config.access_key_id,
                aws_secret_access_key=self.s3_config.secret_access_key,
                config=Config(
                    signature_version="s3v4",
                    s3={
                        "addressing_style": getattr(
                            self.s3_config, "addressing_style", "auto"
                        )
                    },
                ),
            )
        return self._s3_client

    @property
    def prefix(self) -> str:
        return self._s3_prefix

    def key(self, *parts: str) -> str:
        k = "/".join(parts)
        return f"{self._s3_prefix}/{k}" if self._s3_prefix else k

    async def get_object_json(self, key: str) -> Optional[dict]:
        s3 = self._get_s3()
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: s3.get_object(Bucket=self._bucket_name, Key=key),
            )
            return json.loads(resp["Body"].read().decode("utf-8"))
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise

    async def get_object_text(self, key: str) -> Optional[str]:
        s3 = self._get_s3()
        try:
            resp = await asyncio.get_running_loop().run_in_executor(
                None,
                lambda: s3.get_object(Bucket=self._bucket_name, Key=key),
            )
            return resp["Body"].read().decode("utf-8")
        except ClientError as e:
            if e.response["Error"]["Code"] in ("404", "NoSuchKey"):
                return None
            raise

    async def list_prefixes(self, prefix: str) -> list[str]:
        s3 = self._get_s3()
        resp = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: s3.list_objects_v2(
                Bucket=self._bucket_name,
                Prefix=prefix,
                Delimiter="/",
            ),
        )
        return [cp.get("Prefix", "") for cp in resp.get("CommonPrefixes", [])]

    async def list_objects(self, prefix: str) -> list[dict]:
        s3 = self._get_s3()
        resp = await asyncio.get_running_loop().run_in_executor(
            None,
            lambda: s3.list_objects_v2(Bucket=self._bucket_name, Prefix=prefix),
        )
        return resp.get("Contents", [])
