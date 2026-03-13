"""
LAB 04: Сравнение подходов
1) FOR UPDATE (решение из lab_02)
2) Idempotency-Key + middleware (lab_04)
"""

import pytest


@pytest.mark.asyncio
async def test_compare_for_update_and_idempotency_behaviour():
    """
    TODO: Реализовать сравнительный тест/сценарий.

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
    raise NotImplementedError("TODO: Реализовать сравнение FOR UPDATE vs Idempotency-Key")
