"""
LAB 04: Демонстрация проблемы retry без идемпотентности.

Сценарий:
1) Клиент отправил запрос на оплату.
2) До получения ответа \"сеть оборвалась\" (моделируем повтором запроса).
3) Клиент повторил запрос БЕЗ Idempotency-Key.
4) В unsafe-режиме возможна двойная оплата.
"""

import asyncio
import pytest
import uuid
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import sessionmaker
from sqlalchemy import text

from app.application.payment_service import PaymentService


DATABASE_URL = "postgresql+asyncpg://postgres:postgres@localhost:5432/marketplace"


@pytest.fixture
async def db_session():
    engine = create_async_engine(DATABASE_URL, echo=True)
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



@pytest.mark.asyncio
async def test_retry_without_idempotency_can_double_pay(db_engine, test_order):
   """
    TODO: Реализовать тест.

    Рекомендуемые шаги:
    1) Создать заказ в статусе created.
    2) Выполнить две параллельные попытки POST /api/payments/retry-demo
       с mode='unsafe' и БЕЗ заголовка Idempotency-Key.
    3) Проверить историю order_status_history:
       - paid-событий больше 1 (или иная метрика двойного списания).
    4) Вывести понятный отчёт в stdout:
       - сколько попыток
       - сколько paid в истории
       - почему это проблема.
   """
   order_id = test_order

   async def payment_attempt_1():
      async with AsyncSession(db_engine) as session1:
         service1 = PaymentService(session1)
         return await service1.pay_order_unsafe(order_id)
           
   async def payment_attempt_2():
      async with AsyncSession(db_engine) as session2:
         service2 = PaymentService(session2)
         return await service2.pay_order_unsafe(order_id)
           
   await asyncio.gather(
      payment_attempt_1(),
      payment_attempt_2(),
      return_exceptions=True
   )

   await asyncio.sleep(1)

   async with AsyncSession(db_engine) as check_session:
      service = PaymentService(check_session)
      history = await service.get_payment_history(order_id)
   assert len(history) == 2, "Ожидалось 2 записи об оплате (RACE CONDITION!)"

   print(f"⚠️ RACE CONDITION DETECTED!")
   print(f"Заказ {order_id} был оплачен ДВАЖДЫ:")
   for record in history:
       print(f"  - {record['changed_at']}: status = {record['status']}")