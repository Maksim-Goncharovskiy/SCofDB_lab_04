# Статус лабораторной работы №4

## Что уже готово
- ✅ Основа проекта из предыдущей лабораторной (`backend`, `frontend`, docker)
- ✅ Endpoint `POST /api/payments/retry-demo` для retry-сценария
- ✅ Подключён `IdempotencyMiddleware` (пока как заглушка)
- ✅ Добавлена миграция-шаблон `backend/migrations/002_idempotency_keys.sql`
- ✅ Добавлены шаблоны тестов LAB 04
- ✅ Добавлены SQL-утилиты ручной проверки в `sql/`
- ✅ Шаблон отчёта `REPORT.md`

## Что делает студент

### Backend
- [ ] Реализует таблицу `idempotency_keys` в `002_idempotency_keys.sql`
- [ ] Реализует логику middleware в `idempotency_middleware.py`
- [ ] (при необходимости) дорабатывает payment flow в `payment_routes.py`

### Тесты/демо
- [ ] Реализует `test_retry_without_idempotency.py`
- [ ] Реализует `test_retry_with_idempotency_key.py`
- [ ] Реализует `test_compare_idempotency_vs_for_update.py`

### Отчёт
- [ ] Заполняет все TODO в `REPORT.md`
- [ ] Доказывает, что повтор с тем же ключом возвращает кэш
- [ ] Сравнивает подходы: idempotency key vs FOR UPDATE

## Минимальные требования к сдаче
1. Таблица `idempotency_keys` создана и используется.
2. Повтор с тем же `Idempotency-Key` не вызывает повторного списания.
3. Второй ответ возвращается из кэша (или эквивалентно подтверждено).
4. Проведено сравнение с решением ЛР2 (`FOR UPDATE`).
5. Отчёт заполнен и содержит технические выводы.
