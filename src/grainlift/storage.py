# Copyright (c) 2026 Query Farm LLC
# SPDX-License-Identifier: Apache-2.0
"""Large requests and results through S3-compatible object storage (VGI-RPC external locations)."""

from __future__ import annotations

import hashlib
import hmac
import importlib.util
import os
import urllib.request
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING
from urllib.parse import quote, urlsplit

from vgi_rpc.external import ServerExternalConfig, UploadUrl
from vgi_rpc.external_fetch import FetchConfig

if TYPE_CHECKING:
    from collections.abc import Callable

    import pyarrow as pa

__all__ = ["ExternalStorageConfig"]

_MAX_URL_TTL_SECONDS = 604_800  # SigV4 presigned URLs are valid for at most seven days.


@dataclass(frozen=True)
class ExternalStorageConfig:
    """An S3-compatible bucket (AWS S3, Cloudflare R2, MinIO, ...) for large requests and results.

    The HTTP host hands clients presigned URLs: a request over the request
    limit is uploaded to the bucket (``POST /__upload_url__/init``) and sent
    as a pointer, and a result batch of at least ``threshold_bytes`` is stored
    there for the client to fetch. URLs are signed here (AWS Signature
    Version 4), so clients need no storage credentials. The host fetches only
    objects in its own bucket. Objects are never deleted; give the bucket a
    lifecycle rule that expires them, and a CORS rule allowing ``PUT`` and
    ``GET`` for browser clients.

    Attributes:
        endpoint: The S3 API endpoint, e.g. ``https://<account>.r2.cloudflarestorage.com``.
        bucket: Bucket name.
        region: Signing region (``auto`` for R2).
        prefix: Key prefix for the host's objects.
        access_key_id: Access key; defaults to ``AWS_ACCESS_KEY_ID``.
        secret_access_key: Secret key; defaults to ``AWS_SECRET_ACCESS_KEY``. Never shown in ``repr``.
        virtual_hosted_style: Use ``https://<bucket>.<endpoint host>/`` instead of ``<endpoint>/<bucket>/``.
        url_ttl_seconds: How long presigned URLs stay valid.
        threshold_bytes: Result batches at least this large go to the bucket.
        max_upload_bytes: Largest request a client may upload (advertised; enforced on fetch).
    """

    endpoint: str
    bucket: str
    region: str = "auto"
    prefix: str = ""
    access_key_id: str | None = None
    secret_access_key: str | None = field(default=None, repr=False)
    virtual_hosted_style: bool = False
    url_ttl_seconds: int = 900
    threshold_bytes: int = 1024 * 1024
    max_upload_bytes: int = 256 * 1024 * 1024

    def __post_init__(self) -> None:
        """Validate the configuration.

        Raises:
            ValueError: If a field is out of range.
        """
        parts = urlsplit(self.endpoint)
        if parts.scheme not in ("http", "https") or not parts.hostname:
            raise ValueError("external storage endpoint must be an absolute http(s) URL")
        if parts.query or parts.fragment:
            raise ValueError("external storage endpoint must not have a query or fragment")
        if not self.bucket.strip():
            raise ValueError("external storage bucket must not be empty")
        if not self.region.strip():
            raise ValueError("external storage region must not be empty")
        for name, value in (
            ("url_ttl_seconds", self.url_ttl_seconds),
            ("threshold_bytes", self.threshold_bytes),
            ("max_upload_bytes", self.max_upload_bytes),
        ):
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                raise ValueError(f"external storage {name} must be a positive integer")
        if self.url_ttl_seconds > _MAX_URL_TTL_SECONDS:
            raise ValueError(f"external storage url_ttl_seconds must not exceed {_MAX_URL_TTL_SECONDS}")

    def credentials(self) -> tuple[str, str]:
        """Return the configured credentials, or the AWS environment variables.

        Returns:
            The access key ID and secret access key.

        Raises:
            ValueError: If either is missing.
        """
        access_key_id = self.access_key_id or os.environ.get("AWS_ACCESS_KEY_ID")
        secret_access_key = self.secret_access_key or os.environ.get("AWS_SECRET_ACCESS_KEY")
        if not access_key_id or not secret_access_key:
            raise ValueError(
                "external storage needs credentials: set access_key_id and secret_access_key, "
                "or AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY"
            )
        return access_key_id, secret_access_key

    def presigner(self) -> Presigner:
        """Build the URL signer for this bucket.

        Returns:
            The presigner.
        """
        access_key_id, secret_access_key = self.credentials()
        return Presigner(
            self.endpoint, self.bucket, self.region, access_key_id, secret_access_key, self.virtual_hosted_style
        )

    def server_config(self) -> tuple[ServerExternalConfig, PresignedStorage]:
        """Build the VGI-RPC result externalization config and the upload URL provider.

        Returns:
            The server config and the storage, which is also the upload URL provider.

        Raises:
            ImportError: If the ``storage`` extra is not installed.
        """
        # VGI-RPC fetches client uploads with aiohttp, from vgi-rpc[external].
        if importlib.util.find_spec("aiohttp") is None:
            raise ImportError("Install grainlift[storage] to use external storage")
        presigner = self.presigner()
        storage = PresignedStorage(presigner, self.prefix, self.url_ttl_seconds)
        fetch = FetchConfig(max_fetch_bytes=self.max_upload_bytes, max_decompressed_bytes=self.max_upload_bytes)
        config = ServerExternalConfig(
            storage=storage,
            externalize_threshold_bytes=self.threshold_bytes,
            fetch_config=fetch,
            url_validator=presigner.validator(),
        )
        return config, storage


class Presigner:
    """Presigns S3 object URLs with AWS Signature Version 4 (query-string authentication)."""

    def __init__(
        self,
        endpoint: str,
        bucket: str,
        region: str,
        access_key_id: str,
        secret_access_key: str,
        virtual_hosted_style: bool = False,
    ) -> None:
        """Create a presigner.

        Args:
            endpoint: The S3 API endpoint.
            bucket: Bucket name.
            region: Signing region.
            access_key_id: Access key ID.
            secret_access_key: Secret access key.
            virtual_hosted_style: Name the bucket in the host rather than the path.
        """
        parts = urlsplit(endpoint)
        path = parts.path.rstrip("/")
        host = parts.netloc
        if virtual_hosted_style:
            host = f"{bucket}.{host}"
            base_path = f"{path}/"
        else:
            base_path = f"{path}/{_uri_encode(bucket, encode_slash=True)}/"
        self._scheme = parts.scheme
        self._host = host
        self._base_path = base_path
        self._region = region
        self._access_key_id = access_key_id
        self._secret_access_key = secret_access_key

    def __repr__(self) -> str:
        """Describe the bucket without credentials.

        Returns:
            The representation.
        """
        return f"Presigner({self._scheme}://{self._host}{self._base_path})"

    def presign(self, method: str, key: str, now: datetime, expires_seconds: int) -> str:
        """Return a URL valid for ``method`` on ``key`` for ``expires_seconds`` from ``now``.

        Args:
            method: HTTP method the URL is signed for.
            key: Object key.
            now: Signing time.
            expires_seconds: Validity in seconds.

        Returns:
            The presigned URL.
        """
        path = self._base_path + _uri_encode(key, encode_slash=False)
        timestamp = now.astimezone(UTC).strftime("%Y%m%dT%H%M%SZ")
        date = timestamp[:8]
        scope = f"{date}/{self._region}/s3/aws4_request"
        query = "&".join(
            sorted(
                f"{_uri_encode(name, encode_slash=True)}={_uri_encode(value, encode_slash=True)}"
                for name, value in (
                    ("X-Amz-Algorithm", "AWS4-HMAC-SHA256"),
                    ("X-Amz-Credential", f"{self._access_key_id}/{scope}"),
                    ("X-Amz-Date", timestamp),
                    ("X-Amz-Expires", str(expires_seconds)),
                    ("X-Amz-SignedHeaders", "host"),
                )
            )
        )
        canonical = f"{method}\n{path}\n{query}\nhost:{self._host}\n\nhost\nUNSIGNED-PAYLOAD"
        to_sign = f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{hashlib.sha256(canonical.encode()).hexdigest()}"
        key_bytes = _hmac(f"AWS4{self._secret_access_key}".encode(), date)
        for part in (self._region, "s3", "aws4_request"):
            key_bytes = _hmac(key_bytes, part)
        signature = hmac.new(key_bytes, to_sign.encode(), hashlib.sha256).hexdigest()
        return f"{self._scheme}://{self._host}{path}?{query}&X-Amz-Signature={signature}"

    def validator(self) -> Callable[[str], None]:
        """Accept only this bucket's objects, so a client cannot point the host at another address.

        Returns:
            A URL validator raising ``ValueError`` for any other URL.
        """
        scheme, host, base_path = self._scheme, self._host.lower(), self._base_path

        def validate(url: str) -> None:
            parts = urlsplit(url)
            if parts.scheme != scheme or parts.netloc.lower() != host or not parts.path.startswith(base_path):
                raise ValueError("external location URL is not in this host's storage bucket")

        return validate


class PresignedStorage:
    """VGI-RPC ``ExternalStorage`` and ``UploadUrlProvider`` over presigned PUT/GET URLs."""

    def __init__(self, presigner: Presigner, prefix: str, ttl_seconds: int) -> None:
        """Create the storage.

        Args:
            presigner: Signs the bucket's URLs.
            prefix: Key prefix for new objects.
            ttl_seconds: Validity of each presigned URL.
        """
        self._presigner = presigner
        self._prefix = prefix
        self._ttl_seconds = ttl_seconds

    def _pair(self) -> UploadUrl:
        key = f"{self._prefix}{uuid.uuid4()}.arrow"
        now = datetime.now(UTC)
        return UploadUrl(
            upload_url=self._presigner.presign("PUT", key, now, self._ttl_seconds),
            download_url=self._presigner.presign("GET", key, now, self._ttl_seconds),
            expires_at=now + timedelta(seconds=self._ttl_seconds),
        )

    def generate_upload_url(self, schema: pa.Schema) -> UploadUrl:
        """Vend a PUT/GET pair for a new object.

        Args:
            schema: Schema of the data to be uploaded (unused).

        Returns:
            The presigned URLs.
        """
        return self._pair()

    def upload(self, data: bytes, schema: pa.Schema, *, content_encoding: str | None = None) -> str:
        """Store a result batch and return its presigned GET URL.

        Args:
            data: Arrow IPC stream bytes.
            schema: Schema of the data (unused).
            content_encoding: Encoding applied to ``data``, if any.

        Returns:
            The presigned GET URL.

        Raises:
            RuntimeError: If the bucket rejects the upload.
        """
        urls = self._pair()
        headers = {"Content-Type": "application/vnd.apache.arrow.stream"}
        if content_encoding:
            headers["Content-Encoding"] = content_encoding
        request = urllib.request.Request(urls.upload_url, data=data, headers=headers, method="PUT")
        try:
            with urllib.request.urlopen(request, timeout=60) as response:  # noqa: S310 - presigned bucket URL
                status = response.status
        except OSError as exc:
            # Never echo the URL: its query string carries the signature.
            raise RuntimeError(f"external storage upload failed ({type(exc).__name__})") from None
        if not 200 <= status < 300:
            raise RuntimeError(f"external storage upload returned HTTP {status}")
        return urls.download_url


def _hmac(key: bytes, message: str) -> bytes:
    return hmac.new(key, message.encode(), hashlib.sha256).digest()


def _uri_encode(value: str, *, encode_slash: bool) -> str:
    """SigV4 URI encoding: everything but unreserved characters, and ``/`` too unless it separates segments.

    Args:
        value: Text to encode.
        encode_slash: Encode ``/`` as well.

    Returns:
        The encoded text.
    """
    return quote(value, safe="-_.~" if encode_slash else "-_.~/")
