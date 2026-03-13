"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware


class IdempotencyMiddleware(BaseHTTPMiddleware):
    """
    Middleware для идемпотентности POST-запросов оплаты.

    Идея:
    - Клиент отправляет `Idempotency-Key` в header.
    - Если запрос с таким ключом уже выполнялся для того же endpoint и payload,
      middleware возвращает кэшированный ответ (без повторного списания).
    """

    def __init__(self, app, ttl_seconds: int = 24 * 60 * 60):
        super().__init__(app)
        self.ttl_seconds = ttl_seconds

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """
        TODO: Реализовать алгоритм.

        Рекомендуемая логика:
        1) Пропускать только целевые запросы:
           - method == POST
           - path в whitelist для платежей
        2) Читать Idempotency-Key из headers.
           Если ключа нет -> обычный call_next(request)
        3) Считать request_hash (например sha256 от body).
        4) В транзакции:
           - проверить запись в idempotency_keys
           - если completed и hash совпадает -> вернуть кэш (status_code + body)
           - если key есть, но hash другой -> вернуть 409 Conflict
           - если ключа нет -> создать запись processing
        5) Выполнить downstream request через call_next.
        6) Сохранить response в idempotency_keys со статусом completed.
        7) Вернуть response клиенту.

        Дополнительно:
        - обработайте кейс конкурентных одинаковых ключей
          (уникальный индекс + retry/select existing).
        """

        # Текущая заглушка: middleware ничего не меняет.
        # TODO: заменить на полноценную реализацию с БД.
        return await call_next(request)

    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса для проверки reuse ключа с другим payload."""
        return hashlib.sha256(raw_body).hexdigest()

    @staticmethod
    def encode_response_payload(body_obj) -> str:
        """Сериализация response body для сохранения в idempotency_keys."""
        return json.dumps(body_obj, ensure_ascii=False)
