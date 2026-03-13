# Лабораторная работа №4
## Идемпотентность платежных запросов в FastAPI

## Важное уточнение
ЛР4 является **продолжением ЛР3/ЛР2** и выполняется на том же проекте.

В `lab_04` уже лежит кодовая база из предыдущей лабораторной:
- `backend/`
- `frontend/`
- `Dockerfile.backend`
- `Dockerfile.frontend`
- `.github/`

Если у студента есть доработки в предыдущей лабе, их нужно перенести в `lab_04`.

## Цель работы
Смоделировать и исправить сценарий:
1. Клиент отправил запрос на оплату.
2. Сеть оборвалась до получения ответа.
3. Клиент повторил тот же запрос.
4. Без защиты возможна двойная оплата.

Реализовать:
- таблицу `idempotency_keys`;
- middleware идемпотентности в FastAPI;
- возврат кэшированного ответа при повторе с тем же ключом;
- сравнение с подходом из ЛР2 (`REPEATABLE READ + FOR UPDATE`).

## Что дано готовым
1. Код проекта из предыдущей лабораторной.
2. Endpoint для retry-сценария:
   - `POST /api/payments/retry-demo`  
   режимы:
   - `unsafe` (без FOR UPDATE),
   - `for_update` (решение из ЛР2).
3. SQL-утилиты для ручной проверки:
   - `sql/01_prepare_demo_order.sql`
   - `sql/02_check_order_paid_history.sql`
   - `sql/03_check_idempotency_keys.sql`
4. Шаблон отчёта `REPORT.md`.

## Что нужно реализовать (TODO)

### 1) Миграция таблицы идемпотентности
Файл: `backend/migrations/002_idempotency_keys.sql`

Нужно:
- создать таблицу `idempotency_keys`;
- хранить ключ, endpoint, hash запроса, статус обработки, кэш ответа;
- добавить уникальный constraint для защиты от дубликатов;
- добавить индексы для lookup и cleanup.

### 2) Middleware идемпотентности
Файл: `backend/app/middleware/idempotency_middleware.py`

Нужно:
- читать `Idempotency-Key` из заголовка;
- для повторного запроса с тем же ключом и тем же payload возвращать кэшированный ответ;
- не вызывать повторно бизнес-логику списания;
- при reuse ключа с другим payload возвращать `409 Conflict`.

### 3) Демонстрация сценария без защиты
Файл: `backend/app/tests/test_retry_without_idempotency.py`

Нужно показать, что в `unsafe` сценарии повтор запроса может привести к двойной оплате.

### 4) Демонстрация сценария с Idempotency-Key
Файл: `backend/app/tests/test_retry_with_idempotency_key.py`

Нужно показать:
- повтор с тем же ключом -> кэшированный ответ;
- нет повторного списания;
- запись в `idempotency_keys` содержит сохранённый ответ.

### 5) Сравнение с решением из ЛР2
Файл: `backend/app/tests/test_compare_idempotency_vs_for_update.py`

Нужно объяснить и показать различия:
- `FOR UPDATE`: защита от race condition на уровне БД;
- idempotency key: защита от повторов на уровне API-контракта.

## Запуск
```bash
cd lab_04
docker compose down -v
docker compose up -d --build
```

Проверка:
- Backend: `http://localhost:8082/health`
- Frontend: `http://localhost:5174`
- PostgreSQL: `localhost:5434`

## Рекомендуемый порядок выполнения
```bash
# 1) Применить базовую схему (если не применена через init)
docker compose exec -T db psql -U postgres -d marketplace -f /docker-entrypoint-initdb.d/001_init.sql

# 2) Реализовать и применить миграцию идемпотентности
docker compose exec -T db psql -U postgres -d marketplace -f /docker-entrypoint-initdb.d/002_idempotency_keys.sql

# 3) Подготовить demo-order (опционально)
docker compose exec -T db psql -U postgres -d marketplace -f /sql/01_prepare_demo_order.sql

# 4) Запустить тесты LAB 04 (после реализации TODO)
docker compose exec -T backend pytest app/tests/test_retry_without_idempotency.py -v -s
docker compose exec -T backend pytest app/tests/test_retry_with_idempotency_key.py -v -s
docker compose exec -T backend pytest app/tests/test_compare_idempotency_vs_for_update.py -v -s
```

## Структура LAB 04
```
lab_04/
├── backend/
│   ├── app/
│   │   ├── middleware/
│   │   │   └── idempotency_middleware.py      # TODO
│   │   ├── api/
│   │   │   └── payment_routes.py              # retry endpoint уже добавлен
│   │   └── tests/
│   │       ├── test_retry_without_idempotency.py
│   │       ├── test_retry_with_idempotency_key.py
│   │       └── test_compare_idempotency_vs_for_update.py
│   └── migrations/
│       └── 002_idempotency_keys.sql           # TODO
├── sql/
│   ├── 01_prepare_demo_order.sql
│   ├── 02_check_order_paid_history.sql
│   └── 03_check_idempotency_keys.sql
├── REPORT.md
└── README.md
```

## Критерии оценки
- Корректность реализации `idempotency_keys` + middleware — 35%
- Демонстрация retry-сценария без защиты и с защитой — 25%
- Сравнение с подходом FOR UPDATE из ЛР2 — 20%
- Качество отчёта и обоснований — 20%
