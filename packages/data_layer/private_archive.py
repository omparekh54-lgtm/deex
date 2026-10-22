"""Private, bounded source evidence. Never emits credentials or deletes objects."""
from __future__ import annotations

import hashlib
import json
import os
import urllib.error
import urllib.request

from packages.data_layer.cloud_sources import MAX_BYTES, SourceError

ENDPOINT = "https://br-twilight-sea-azumjkpc.storage.c-3.ap-southeast-1.aws.neon.tech"
BUCKET = "pms-private-archive"
LIMIT = 4_000_000_000
PROBE_KEY = "verification/collector-private-probe.txt"
PROBE_BODY = b"PMS private archive transport verification\n"


def anonymous_denied(url):
    # This public probe contains no market, portfolio or credential data.
    try:
        with urllib.request.urlopen(url, timeout=15):
            return False
    except urllib.error.HTTPError as exc:
        return exc.code == 403
    except Exception:
        return False


class PrivateArchive:
    def __init__(self, client, *, privacy_check=anonymous_denied, limit=LIMIT):
        self.client = client
        self.limit = limit
        self.receipts = {}
        self.uploaded = 0
        self.verified = 0
        self.private = False
        try:
            client.put_object(Bucket=BUCKET, Key=PROBE_KEY, Body=PROBE_BODY)
            self._verify(PROBE_KEY, hashlib.sha256(PROBE_BODY).hexdigest(), len(PROBE_BODY))
            if not privacy_check(ENDPOINT + "/" + BUCKET + "/" + PROBE_KEY):
                raise SourceError("archive_privacy_not_verified")
            self.private = True
            self.used = 0
            objects = 0
            for page in client.get_paginator("list_objects_v2").paginate(Bucket=BUCKET):
                for item in page.get("Contents", []):
                    objects += 1
                    self.used += item["Size"]
                    if objects > 200_000 or self.used >= limit:
                        raise SourceError("archive_capacity_limit")
        except SourceError:
            raise
        except Exception:
            raise SourceError("archive_connection_or_credentials_failed") from None

    @classmethod
    def from_env(cls):
        try:
            config = json.loads(os.environ.get("PMS_NEON_ARCHIVE_JSON", ""))
            if not isinstance(config, dict) or set(config) != {"access_key_id", "secret_access_key"}:
                raise ValueError()
            if not all(isinstance(v, str) and v.strip() for v in config.values()):
                raise ValueError()
        except (ValueError, TypeError):
            raise SourceError("archive_credential_configuration_invalid") from None
        import boto3
        from botocore.config import Config
        client = boto3.client(
            "s3", endpoint_url=ENDPOINT, region_name="ap-southeast-1",
            aws_access_key_id=config["access_key_id"], aws_secret_access_key=config["secret_access_key"],
            config=Config(signature_version="s3v4", s3={"addressing_style": "path"},
                          connect_timeout=15, read_timeout=30,
                          retries={"max_attempts": 3, "mode": "standard"}),
        )
        return cls(client)

    def _verify(self, key, expected, size):
        body = self.client.get_object(Bucket=BUCKET, Key=key)["Body"]
        digest = hashlib.sha256()
        total = 0
        try:
            for chunk in iter(lambda: body.read(1024 * 1024), b""):
                total += len(chunk)
                if total > size:
                    raise SourceError("archive_checksum_mismatch")
                digest.update(chunk)
        finally:
            body.close()
        if total != size or digest.hexdigest() != expected:
            raise SourceError("archive_checksum_mismatch")

    def put(self, doc):
        if not self.private:
            raise SourceError("archive_privacy_not_verified")
        if not doc.body or len(doc.body) > MAX_BYTES or len(doc.body) != doc.size:
            raise SourceError("archive_document_size_invalid")
        if hashlib.sha256(doc.body).hexdigest() != doc.sha256:
            raise SourceError("archive_document_checksum_invalid")
        key = "raw-sources/" + doc.observed_at.date().isoformat() + "/" + doc.sha256 + ".bin"
        if key in self.receipts:
            return key
        try:
            try:
                head = self.client.head_object(Bucket=BUCKET, Key=key)
            except Exception as exc:
                code = getattr(exc, "response", {}).get("Error", {}).get("Code")
                if code not in {"NoSuchKey", "NotFound", "404"}:
                    raise
                head = None
            if head is None:
                if self.used + doc.size >= self.limit:
                    raise SourceError("archive_capacity_limit")
                self.client.put_object(
                    Bucket=BUCKET, Key=key, Body=doc.body, ContentType="application/octet-stream",
                    Metadata={"sha256": doc.sha256, "observed_at": doc.observed_at.isoformat()},
                )
                self.used += doc.size
                self.uploaded += 1
            elif head["ContentLength"] != doc.size or head.get("Metadata", {}).get("sha256") != doc.sha256:
                raise SourceError("archive_existing_object_mismatch")
            # ETags are not checksums for multipart files. Verify actual downloaded bytes.
            self._verify(key, doc.sha256, doc.size)
            self.receipts[key] = {"sha256": doc.sha256, "bytes": doc.size}
            self.verified += 1
            return key
        except SourceError:
            raise
        except Exception:
            raise SourceError("archive_write_or_verify_failed") from None

    def summary(self):
        return {"status": "VERIFIED" if self.private else "BLOCKED", "bucket": BUCKET,
                "objects_verified": self.verified, "objects_uploaded": self.uploaded,
                "bucket_bytes": self.used, "budget_bytes": self.limit,
                "anonymous_read_denied": self.private}
