"""
LAB 04: Проверка идемпотентного повтора запроса.

Цель:
При повторном запросе с тем же Idempotency-Key вернуть
кэшированный результат без повторного списания.
"""

import asyncio
import pytest
import uuid
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text
from httpx import AsyncClient

from app.application.payment_service import PaymentService
from app.main import app


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def db_session():
    engine = create_async_engine(DATABASE_URL, echo=False)
    Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)
    async with Session() as session:
        try:
            yield session
            await session.commit()
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()



@pytest.fixture
async def db_engine():
   engine = create_async_engine(DATABASE_URL, echo=True)
   yield engine
   


@pytest.fixture
async def test_order(db_session, db_engine):
    """
    Создать тестовый заказ со статусом 'created'.
    """
    user_id = uuid.uuid4()
    test_user = {
        "id": user_id,
        "email": f"test_user_{str(user_id)[:5]}@gmail.com",
        "name": "Test User"
    }

    order_id = uuid.uuid4()
    test_order = {
        "id": order_id,
        "user_id": user_id,
        "status": "created",
        "total_amount": 23.0
    }

    async with db_session.begin():
        await db_session.execute(
                text("""
                    INSERT INTO users (id, email, name, created_at)
                    VALUES (:id, :email, :name, NOW())
                    ON CONFLICT (id) DO NOTHING
                """),
                test_user
            )
        
        await db_session.execute(
                text("""
                    INSERT INTO orders (id, user_id, status, total_amount, created_at)
                    VALUES (:id, :user_id, :status, :total_amount, NOW())
                """),
                test_order
            )
        
        await db_session.execute(
                text("""
                    INSERT INTO order_status_history (id, order_id, status, changed_at)
                    VALUES (gen_random_uuid(), :order_id, 'created', NOW())
                """),
                {"order_id": order_id}
            )
        
    yield order_id 

    async with AsyncSession(db_engine) as delete_session:
        async with delete_session.begin():
            await delete_session.execute(
                text("DELETE FROM order_status_history WHERE order_id = :order_id"),
                {"order_id": order_id}
            )
            await delete_session.execute(
                text("DELETE FROM orders WHERE id = :order_id"),
                {"order_id": order_id}
            )
            await delete_session.execute(
                text("DELETE FROM users WHERE id = :user_id"),
                {"user_id": user_id}
            )
            await delete_session.commit()



@pytest.mark.asyncio
async def test_retry_with_same_key_returns_cached_response(db_engine, test_order):
   """
   TODO: Реализовать тест.
   Рекомендуемые шаги:
   1) Создать заказ в статусе created.
   2) Сделать первый POST /api/payments/retry-demo (mode='unsafe')
      с заголовком Idempotency-Key: fixed-key-123.
   3) Повторить тот же POST с тем же ключом и тем же payload.
   4) Проверить:
      - второй ответ пришёл из кэша (через признак, который вы добавите,
        например header X-Idempotency-Replayed=true),
      - в order_status_history только одно событие paid,
      - в idempotency_keys есть запись completed с response_body/status_code.
   """
   order_id = test_order

   async with AsyncClient(app=app, base_url="http://test") as client:
      headers = {"Idempotency-Key": "fixed-key-123"}

      payload = {
         "order_id": str(order_id),
         "mode": "unsafe"
      }

      response_1 = await client.post(
         "/api/payments/retry-demo",
         json=payload,
         headers=headers)
      
      print(f"Результат первого запроса: {json.loads(response_1.content)}")
      
      assert response_1.status_code == 200, f"Статус ответа должен быть 200, получено: {response_1.status_code}"
      
      response_2 = await client.post(
         "/api/payments/retry-demo",
         json=payload,
         headers=headers)
      
      print(f"Результат повторного запроса: {json.loads(response_2.content)}")

      assert response_2.status_code == 200, "Статус ответа должен быть 200"
      
      assert response_2.headers.get("X-Idempotency-Replayed") == "true", f"После повторного использования ключа заголовок X-Idempotency-Replayed должен быть равен true. Полученное значение: {response_2.headers.get('X-Idempotency-Replayed')}"

      async with AsyncSession(db_engine) as check_session:
         payment_service = PaymentService(check_session)
         history = await payment_service.get_payment_history(order_id)
      
      assert len(history) == 1, "Оплата должна была пройти ровно один раз"

      async with AsyncSession(db_engine) as check_session:
         idempotency_record = await check_session.execute(text("""
            SELECT status
            FROM idempotency_keys 
            WHERE idempotency_key='fixed-key-123'
         """))
         idempotency_record = idempotency_record.fetchone()
         assert idempotency_record is not None, "Запись в idempotency_keys для ключа fixed-key-123 не найдена!"
         assert idempotency_record.status == "completed", "Задача должна была перейти в статус completed!"

   async with AsyncSession(db_engine) as cleanup_session:
      await cleanup_session.execute(text("""
         DELETE FROM idempotency_keys WHERE idempotency_key='fixed-key-123'
      """))
      await cleanup_session.commit()



@pytest.mark.asyncio
async def test_same_key_different_payload_returns_conflict(db_engine, test_order):
   """
   Негативный тест.

   Один и тот же Idempotency-Key нельзя использовать с другим payload.
   Ожидается 409 Conflict (или эквивалентная бизнес-ошибка).
   """
   order_id = test_order

   async with AsyncClient(app=app, base_url="http://test") as client:
      headers = {"Idempotency-Key": "fixed-key-123"}

      payload_1 = {
         "order_id": str(order_id),
         "mode": "unsafe"
      }

      response_1 = await client.post(
         "/api/payments/retry-demo",
         json=payload_1,
         headers=headers)
      
      assert response_1.status_code == 200, "Статус ответа должен быть 200"
      
      payload_2 = {
         "order_id": str(order_id),
         "mode": "safe"
      }
      response_2 = await client.post(
         "/api/payments/retry-demo",
         json=payload_2,
         headers=headers)

      assert response_2.status_code == 409, "Ожидался статус 409, невалидное повторное использование idempotency key"
      

   async with AsyncSession(db_engine) as cleanup_session:
      await cleanup_session.execute(text("""
         DELETE FROM idempotency_keys WHERE idempotency_key='fixed-key-123'
      """))
      await cleanup_session.commit()