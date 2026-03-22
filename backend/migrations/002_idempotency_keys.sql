-- ============================================
-- LAB 04: Идемпотентность платежных запросов
-- ============================================

-- TODO:
-- Создайте таблицу idempotency_keys, которая хранит:
-- 1) сам idempotency key
-- 2) идентификатор запроса (method + path)
-- 3) хэш/сигнатуру тела запроса (чтобы обнаруживать reuse ключа с другим payload)
-- 4) статус обработки (processing / completed / failed)
-- 5) кэш ответа (status code + body)
-- 6) timestamp'ы (created_at, updated_at, expires_at)

-- Рекомендуемый каркас (можно изменить при обосновании):
--
CREATE TABLE idempotency_keys (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    idempotency_key VARCHAR(255) NOT NULL,
    request_method VARCHAR(16) NOT NULL,
    request_path TEXT NOT NULL,
    request_hash TEXT NOT NULL,
    status VARCHAR(32) NOT NULL DEFAULT 'processing',
    status_code INTEGER,
    response_body JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMP NOT NULL,
    CONSTRAINT idempotency_status_check CHECK (status IN ('processing', 'completed', 'failed')),
    CONSTRAINT key_endpoint_unique_check UNIQUE (idempotency_key, request_method, request_path)
);

-- TODO:
-- Добавьте индексы:
-- 1) для очистки просроченных ключей (expires_at)
CREATE INDEX idx_delete_expired_keys ON idempotency_keys USING BTREE (expires_at);

-- 2) для быстрых lookup по ключу/пути/методу
CREATE INDEX idx_lookup_key ON idempotency_keys USING BTREE (idempotency_key, request_method, request_path);

-- TODO (опционально):
-- триггер автообновления updated_at
CREATE FUNCTION update_updated_at_func()
RETURNS TRIGGER AS $$
BEGIN 
    NEW.updated_at = NOW();
    return NEW;
END;
$$ LANGUAGE plpgsql
