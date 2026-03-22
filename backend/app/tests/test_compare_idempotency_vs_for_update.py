"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)
"""

import asyncio
import pytest
import uuid
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
    engine = create_async_engine(DATABASE_URL, echo=False)
    yield engine
   

async def create_test_order(session):
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

   async with session.begin():
       await session.execute(
               text("""
                   INSERT INTO users (id, email, name, created_at)
                   VALUES (:id, :email, :name, NOW())
                   ON CONFLICT (id) DO NOTHING
               """),
               test_user
           )
       
       await session.execute(
               text("""
                   INSERT INTO orders (id, user_id, status, total_amount, created_at)
                   VALUES (:id, :user_id, :status, :total_amount, NOW())
               """),
               test_order
           )
       
       await session.execute(
               text("""
                   INSERT INTO order_status_history (id, order_id, status, changed_at)
                   VALUES (gen_random_uuid(), :order_id, 'created', NOW())
               """),
               {"order_id": order_id}
           )
       
   return order_id, user_id


async def delete_test_order(session, order_id, user_id):
   async with session.begin():
       await session.execute(
           text("DELETE FROM order_status_history WHERE order_id = :order_id"),
           {"order_id": order_id}
       )
       await session.execute(
           text("DELETE FROM orders WHERE id = :order_id"),
           {"order_id": order_id}
       )
       await session.execute(
           text("DELETE FROM users WHERE id = :user_id"),
           {"user_id": user_id}
       )


@pytest.fixture
async def two_test_orders(db_session):
    order_id_1, user_id_1 = await create_test_order(db_session)
    order_id_2, user_id_2 = await create_test_order(db_session)
    
    yield order_id_1, user_id_1, order_id_2, user_id_2



@pytest.mark.asyncio
async def test_compare_for_update_and_idempotency_behaviour(db_engine, two_test_orders):
   """
   Cравнительный тест/сценарий.

   Минимум сравнения:
   1) Повтор запроса с mode='for_update':
      - защита от гонки на уровне БД,
      - повтор может вернуть бизнес-ошибку \"already paid\".
   2) Повтор запроса с mode='unsafe' + Idempotency-Key:
      - второй вызов возвращает тот же кэшированный успешный ответ,
        без повторного списания.
   В конце добавьте вывод:
   - чем отличаются цели и UX двух подходов,
   - почему они не взаимоисключающие и могут использоваться вместе.
   """
   order_id_1, user_id_1, order_id_2, user_id_2 = two_test_orders

   async with AsyncClient(app=app, base_url="http://test") as client:
      print("===== Тестирование FOR UPDATE =====")
      payload_for_update = {
         "order_id": str(order_id_1),
         "mode": "for_update"
      }
      response_for_update = await client.post(
         "/api/payments/retry-demo", 
         json=payload_for_update
      )
      assert response_for_update.status_code == 200, "Статус ответа должен быть 200"
      assert response_for_update.json().get("success") == True, "Запрос должен был обработаться успешно"

      response_for_update = await client.post(
         "/api/payments/retry-demo", 
         json=payload_for_update
      )
      assert response_for_update.status_code == 200, "Статус ответа должен быть 200"
      assert response_for_update.json().get("success") == False, "Ожидался статус False"
      assert "already paid" in response_for_update.json().get("message").lower() 

      print("===== Тестирование идемпотентности =====")
      payload_idempotency = {
         "order_id": str(order_id_2),
         "mode": "unsafe"
      }
      headers = {"Idempotency-Key": "test-key-0912"}

      response_idempotency = await client.post(
         "/api/payments/retry-demo",
         json=payload_idempotency,
         headers=headers
      )
        
      assert response_idempotency.status_code == 200, "Статус ответа должен быть 200"
      assert response_idempotency.json().get("success") == True, "Ожидался статус True"

      response_idempotency = await client.post(
         "/api/payments/retry-demo",
         json=payload_idempotency,
         headers=headers
      )
        
      assert response_idempotency.status_code == 200, "Статус ответа должен быть 200"
      assert response_idempotency.json().get("success") == True, "Ожидался статус True"
      assert response_idempotency.headers.get("X-Idempotency-Replayed") == "true", "Значение X-Idempotency-Replayed должно быть true"

      async with AsyncSession(db_engine) as check_session:
         payment_service = PaymentService(check_session)
         history_for_update = await payment_service.get_payment_history(order_id_1)
         history_idempotency = await payment_service.get_payment_history(order_id_2)
      
      assert len(history_for_update) == 1, f"FOR UPDATE - некорректное количество оплат: {len(history_for_update)}"
      assert len(history_idempotency) == 1, f"Иденпотентность - некорректное количество оплат: {len(history_idempotency)}"

   async with AsyncSession(db_engine) as cleanup_session:
      await delete_test_order(cleanup_session, order_id_1, user_id_1)
      await delete_test_order(cleanup_session, order_id_2, user_id_2)
      await cleanup_session.execute(text("""
         DELETE FROM idempotency_keys 
      """))
      await cleanup_session.commit()