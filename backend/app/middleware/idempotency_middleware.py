"""Idempotency middleware template for LAB 04."""

import hashlib
import json
from typing import Callable

from fastapi import Request, Response, HTTPException
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import text

from app.infrastructure.db import SessionLocal


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
        if request.method != "POST" or not request.url.path.startswith("/api/payments/"):
            return await call_next(request)
        
        idempotency_key = request.headers.get("Idempotency-Key")
        if not idempotency_key:
            return await call_next(request)
        
        request_body = await request.body()
        request_hash = self.build_request_hash(request_body)

        async with SessionLocal() as session:
            idempotency_record = await session.execute(text("""
                SELECT status, request_hash, status_code, response_body
                FROM idempotency_keys
                WHERE idempotency_key = :key
                AND request_method = :method
                AND request_path = :path
                FOR UPDATE
            """),
            {"key": idempotency_key, "method": request.method, "path": request.url.path})

            idempotency_record = idempotency_record.fetchone()
                        
            # если запись найдена - проверяем хеши и возвращаем результат
            if idempotency_record:
                status, record_hash, status_code, response_body = idempotency_record

                if request_hash != record_hash:
                    return Response(
                        content=json.dumps({"error": "Повторное использование Idempotency-Key с другим содержимым"}),
                        status_code=409,
                        media_type="application/json"
                    )
                else:
                    if status == 'processing':
                        return Response(
                            content=json.dumps({"error": "Данный запрос уже в обработке!"}),
                            status_code=400,
                            media_type="application/json"
                        )
                    else:
                        return Response(
                            content=json.dumps(response_body),
                            status_code=status_code,
                            media_type="application/json",
                            headers={"X-Idempotency-Replayed": "true"}
                        )
                        
            # если запись не найдена заводим её
            else:
                await session.execute(text("""
                    INSERT INTO idempotency_keys 
                    (idempotency_key, request_method, request_path, request_hash, status, expires_at)
                    VALUES 
                    (:key, :method, :path, :hash, 'processing', NOW() + INTERVAL '1 day')
                """),
                {'key': idempotency_key, 'method': request.method, 'path': request.url.path, 'hash': request_hash})

        response: Response = await call_next(request)

        response_body = b""
        async for chunk in response.body_iterator:
            response_body += chunk

        body_json = None
        if response_body:
            try:
                body_json = json.loads(response_body.decode())
            except:
                body_json = {"raw": response_body.decode()}

        async with SessionLocal() as update_session:
            async with update_session.begin():
                await update_session.execute(text("""
                    UPDATE idempotency_keys
                    SET 
                        status='completed',
                        status_code=:code,
                        response_body=CAST(:body AS jsonb) 
                    WHERE idempotency_key=:key AND request_method=:method AND request_path=:path
                """), 
                {'key': idempotency_key, 'method': request.method, 'path': request.url.path, 'code': response.status_code, 'body': json.dumps(body_json) if body_json else None})

        return Response(
            content=response_body,
            status_code=response.status_code,
            media_type="application/json",
            headers=dict(response.headers)
        )


    @staticmethod
    def build_request_hash(raw_body: bytes) -> str:
        """Стабильный хэш тела запроса для проверки reuse ключа с другим payload."""
        return hashlib.sha256(raw_body).hexdigest()


    @staticmethod
    def encode_response_payload(body_obj) -> str:
        """Сериализация response body для сохранения в idempotency_keys."""
        return json.dumps(body_obj, ensure_ascii=False)