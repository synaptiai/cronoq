"""Codecs that turn payloads and results into bytes for a durable store."""

from __future__ import annotations

import json
from typing import Any, Protocol


class Codec(Protocol):
    """Encodes payloads and results for a durable store, and decodes them on recovery."""

    def encode(self, obj: Any, /) -> bytes: ...

    def decode(self, data: bytes, /) -> Any: ...


class JsonCodec:
    """JSON (UTF-8). The default codec: decoding never runs code, whatever the file holds."""

    def encode(self, obj: Any, /) -> bytes:
        return json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode()

    def decode(self, data: bytes, /) -> Any:
        return json.loads(data)
