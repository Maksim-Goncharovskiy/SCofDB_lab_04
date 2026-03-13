\timing on

-- ============================================
-- LAB 04: Проверка "двойной оплаты"
-- ============================================
--
-- TODO:
-- Замените {{order_id}} на реальный UUID заказа.

-- Сколько раз заказ был переведён в paid
SELECT
    order_id,
    count(*) AS paid_events
FROM order_status_history
WHERE order_id = '{{order_id}}'::uuid
  AND status = 'paid'
GROUP BY order_id;

-- Детальная история статусов заказа
SELECT
    id,
    order_id,
    status,
    changed_at
FROM order_status_history
WHERE order_id = '{{order_id}}'::uuid
ORDER BY changed_at;
