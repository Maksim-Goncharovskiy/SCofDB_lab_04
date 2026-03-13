"""API endpoints для тестирования конкурентных оплат."""

import uuid
from typing import Literal
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession

from app.infrastructure.db import get_db
from app.application.payment_service import PaymentService


router = APIRouter(prefix="/api/payments", tags=["payments"])


class PaymentRequest(BaseModel):
    """Запрос на оплату заказа."""
    order_id: uuid.UUID
    mode: Literal["safe", "unsafe"] = "safe"


class PaymentResponse(BaseModel):
    """Ответ на запрос оплаты."""
    success: bool
    message: str
    order_id: uuid.UUID
    status: str | None = None


class PaymentHistoryResponse(BaseModel):
    """История оплат для заказа."""
    order_id: uuid.UUID
    payment_count: int
    payments: list[dict]


class RetryPaymentRequest(BaseModel):
    """
    Запрос для сценария retry в LAB 04.

    mode:
    - unsafe: без защиты от повторного запроса
    - for_update: защита из lab_02 (REPEATABLE READ + FOR UPDATE)
    """

    order_id: uuid.UUID
    mode: Literal["unsafe", "for_update"] = "unsafe"


@router.post("/pay", response_model=PaymentResponse)
async def pay_order(
    request: PaymentRequest,
    session: AsyncSession = Depends(get_db)
):
    """
    Оплатить заказ.
    
    Параметры:
    - order_id: ID заказа для оплаты
    - mode: "safe" (с блокировками) или "unsafe" (без блокировок)
    
    Возвращает:
    - success: true если оплата прошла успешно
    - message: описание результата
    - order_id: ID заказа
    - status: текущий статус заказа
    """
    try:
        service = PaymentService(session)
        
        if request.mode == "safe":
            result = await service.pay_order_safe(request.order_id)
        else:
            result = await service.pay_order_unsafe(request.order_id)
        
        return PaymentResponse(
            success=True,
            message=f"Order paid successfully using {request.mode} mode",
            order_id=request.order_id,
            status=result.get("status", "paid")
        )
    
    except Exception as e:
        return PaymentResponse(
            success=False,
            message=str(e),
            order_id=request.order_id,
            status=None
        )


@router.get("/history/{order_id}", response_model=PaymentHistoryResponse)
async def get_payment_history(
    order_id: uuid.UUID,
    session: AsyncSession = Depends(get_db)
):
    """
    Получить историю оплат для заказа.
    
    Возвращает:
    - order_id: ID заказа
    - payment_count: количество попыток оплаты
    - payments: список записей об оплате с временными метками
    """
    try:
        service = PaymentService(session)
        history = await service.get_payment_history(order_id)
        
        return PaymentHistoryResponse(
            order_id=order_id,
            payment_count=len(history),
            payments=history
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/retry-demo", response_model=PaymentResponse)
async def retry_demo_payment(
    request: RetryPaymentRequest,
    session: AsyncSession = Depends(get_db)
):
    """
    LAB 04: Endpoint для сценария \"запрос на оплату -> обрыв сети -> повторный запрос\".

    Как использовать:
    1) Вызвать endpoint 2 раза для одного order_id.
    2) Во втором вызове:
       - с тем же Idempotency-Key ожидается кэшированный ответ (после реализации middleware);
       - без ключа (или с новым ключом) возможна повторная оплата в режиме unsafe.

    mode:
    - unsafe: вызывает pay_order_unsafe()
    - for_update: вызывает pay_order_safe() для сравнения с решением lab_02
    """
    service = PaymentService(session)
    try:
        if request.mode == "for_update":
            result = await service.pay_order_safe(request.order_id)
            mode_name = "for_update"
        else:
            result = await service.pay_order_unsafe(request.order_id)
            mode_name = "unsafe"

        return PaymentResponse(
            success=True,
            message=f"Retry demo payment succeeded ({mode_name})",
            order_id=request.order_id,
            status=result.get("status", "paid")
        )
    except Exception as e:
        return PaymentResponse(
            success=False,
            message=str(e),
            order_id=request.order_id,
            status=None
        )


@router.post("/test-concurrent")
async def test_concurrent_payment(
    request: PaymentRequest,
    session: AsyncSession = Depends(get_db)
):
    """
    ДЕМОНСТРАЦИОННЫЙ endpoint
    
    ⚠️ Этот endpoint СПЕЦИАЛЬНО создан для демонстрации race condition!
    В реальном приложении такого быть не должно.
    
    Параметры:
    - order_id: ID заказа
    - mode: "safe" или "unsafe"
    
    Возвращает результаты обеих попыток оплаты.
    """
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession as AS
    from sqlalchemy.orm import sessionmaker
    
    # Получить DATABASE_URL из настроек
    from app.infrastructure.db import DATABASE_URL
    
    # Создать два независимых соединения
    engine1 = create_async_engine(DATABASE_URL)
    engine2 = create_async_engine(DATABASE_URL)
    
    SessionLocal1 = sessionmaker(engine1, class_=AS, expire_on_commit=False)
    SessionLocal2 = sessionmaker(engine2, class_=AS, expire_on_commit=False)
    
    async def attempt_payment_1():
        """Первая попытка оплаты."""
        async with SessionLocal1() as session1:
            try:
                service = PaymentService(session1)
                if request.mode == "safe":
                    result = await service.pay_order_safe(request.order_id)
                else:
                    result = await service.pay_order_unsafe(request.order_id)
                return {"success": True, "result": result, "attempt": 1}
            except Exception as e:
                return {"success": False, "error": str(e), "attempt": 1}
    
    async def attempt_payment_2():
        """Вторая попытка оплаты."""
        async with SessionLocal2() as session2:
            try:
                service = PaymentService(session2)
                if request.mode == "safe":
                    result = await service.pay_order_safe(request.order_id)
                else:
                    result = await service.pay_order_unsafe(request.order_id)
                return {"success": True, "result": result, "attempt": 2}
            except Exception as e:
                return {"success": False, "error": str(e), "attempt": 2}
    
    # Запустить две попытки ПАРАЛЛЕЛЬНО
    results = await asyncio.gather(
        attempt_payment_1(),
        attempt_payment_2(),
        return_exceptions=True
    )
    
    # Закрыть engines
    await engine1.dispose()
    await engine2.dispose()
    
    # Получить историю оплат
    service = PaymentService(session)
    history = await service.get_payment_history(request.order_id)
    
    # Подсчитать успешные и неудачные попытки
    success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
    error_count = sum(1 for r in results if isinstance(r, dict) and not r.get("success"))
    
    return {
        "mode": request.mode,
        "order_id": str(request.order_id),
        "results": results,
        "summary": {
            "total_attempts": 2,
            "successful": success_count,
            "failed": error_count,
            "payment_count_in_history": len(history),
            "race_condition_detected": len(history) > 1
        },
        "history": history,
        "explanation": (
            f"⚠️ RACE CONDITION! Order was paid {len(history)} times!" 
            if len(history) > 1 
            else f"✅ No race condition. Order was paid {len(history)} time(s)."
        )
    }
