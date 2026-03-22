"""Сервис для демонстрации конкурентных оплат.

Этот модуль содержит два метода оплаты:
1. pay_order_unsafe() - небезопасная реализация (READ COMMITTED без блокировок)
2. pay_order_safe() - безопасная реализация (REPEATABLE READ + FOR UPDATE)
"""

import uuid
from typing import Optional

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.domain.exceptions import OrderAlreadyPaidError, OrderNotFoundError


class PaymentService:
    """Сервис для обработки платежей с разными уровнями изоляции."""

    def __init__(self, session: AsyncSession):
        self.session = session

    async def pay_order_unsafe(self, order_id: uuid.UUID) -> dict:
        """
        НЕБЕЗОПАСНАЯ реализация оплаты заказа.
        
        Использует READ COMMITTED (по умолчанию) без блокировок.
        ЛОМАЕТСЯ при конкурентных запросах - может привести к двойной оплате!
        
        TODO: Реализовать метод следующим образом:
        
        1. Прочитать текущий статус заказа:
           SELECT status FROM orders WHERE id = :order_id
           
        2. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError
           
        3. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid' 
           WHERE id = :order_id AND status = 'created'
           
        4. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
           
        5. Сделать commit
        
        ВАЖНО: НЕ используйте FOR UPDATE!
        ВАЖНО: НЕ меняйте уровень изоляции (оставьте READ COMMITTED по умолчанию)!
        
        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        async with self.session.begin():
            get_status_query = text("""
                SELECT status FROM orders WHERE id = :order_id
            """)

            status = await self.session.execute(get_status_query, {"order_id": order_id})
            status = status.first()

            if not status:
                raise OrderNotFoundError(order_id=order_id)

            if status[0] != 'created':
                raise OrderAlreadyPaidError(order_id=order_id)
            
            update_status_query = text("""
                UPDATE orders SET status = 'paid' WHERE id= :order_id AND status = 'created'
            """)

            await self.session.execute(update_status_query, {"order_id": order_id})

            update_history_query = text("""
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
            """)

            await self.session.execute(update_history_query, {"order_id": order_id})
        
        return {
            "order_id": order_id,
            "status": "paid"
        }




    async def pay_order_safe(self, order_id: uuid.UUID) -> dict:
        """
        БЕЗОПАСНАЯ реализация оплаты заказа.
        
        Использует REPEATABLE READ + FOR UPDATE для предотвращения race condition.
        Корректно работает при конкурентных запросах.
        
        TODO: Реализовать метод следующим образом:
        
        1. Установить уровень изоляции REPEATABLE READ:
           await self.session.execute(
               text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
           )
           
        2. Заблокировать строку заказа для обновления:
           SELECT status FROM orders WHERE id = :order_id FOR UPDATE
           
           ВАЖНО: FOR UPDATE гарантирует, что другие транзакции будут ЖДАТЬ
           освобождения блокировки. Это предотвращает race condition.
           
        3. Проверить, что статус = 'created'
           Если нет - выбросить OrderAlreadyPaidError
           
        4. Изменить статус на 'paid':
           UPDATE orders SET status = 'paid' 
           WHERE id = :order_id AND status = 'created'
           
        5. Записать изменение в историю:
           INSERT INTO order_status_history (id, order_id, status, changed_at)
           VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
           
        6. Сделать commit
        
        ВАЖНО: Обязательно используйте FOR UPDATE!
        ВАЖНО: Обязательно установите REPEATABLE READ!
        
        Args:
            order_id: ID заказа для оплаты
            
        Returns:
            dict с информацией о заказе после оплаты
            
        Raises:
            OrderNotFoundError: если заказ не найден
            OrderAlreadyPaidError: если заказ уже оплачен
        """
        async with self.session.begin():
            await self.session.execute(
               text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
            )

            status = await self.session.execute(text("""SELECT status FROM orders WHERE id = :order_id FOR UPDATE"""),
                {"order_id": order_id}
            )
            status = status.first()

            if not status:
                raise OrderNotFoundError(order_id=order_id)

            if status[0] != 'created':
                raise OrderAlreadyPaidError(order_id=order_id)
            
            update_status_query = text("""
                UPDATE orders SET status = 'paid' WHERE id= :order_id AND status = 'created'
            """)

            await self.session.execute(update_status_query, {"order_id": order_id})

            update_history_query = text("""
                INSERT INTO order_status_history (id, order_id, status, changed_at)
                VALUES (gen_random_uuid(), :order_id, 'paid', NOW())
            """)

            await self.session.execute(update_history_query, {"order_id": order_id})
        
        return {
            "order_id": order_id,
            "status": "paid"
        }

            

    async def get_payment_history(self, order_id: uuid.UUID) -> list[dict]:
        """
        Получить историю оплат для заказа.
        
        Используется для проверки, сколько раз был оплачен заказ.
        
        TODO: Реализовать метод:
        
        SELECT id, order_id, status, changed_at
        FROM order_status_history
        WHERE order_id = :order_id AND status = 'paid'
        ORDER BY changed_at
        
        Args:
            order_id: ID заказа
            
        Returns:
            Список словарей с записями об оплате
        """
        raw_history = await self.session.execute(
            text("""
                SELECT id, order_id, status, changed_at
                FROM order_status_history
                WHERE order_id = :order_id AND status = 'paid'
                ORDER BY changed_at
            """),
            {"order_id": order_id}
        )
        raw_history = raw_history.fetchall()

        history = []
        
        for row in raw_history:
            history.append({
                "id": str(row[0]),
                "order_id": str(row[1]),
                "status": row[2],
                "changed_at": row[3]
            })
        
        return history
