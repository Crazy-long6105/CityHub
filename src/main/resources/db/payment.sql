-- Apply once after seckill.sql, with old order writers paused.
-- Audit legacy pending orders and purchase qualifications before enabling lifecycle jobs.
-- Unverified legacy orders intentionally retain a NULL deadline and cannot be paid/closed automatically.
ALTER TABLE tb_voucher_order
    ADD COLUMN pay_expire_time DATETIME(3) NULL,
    ADD COLUMN failure_reason VARCHAR(255) NULL,
    MODIFY COLUMN pay_time TIMESTAMP(3) NULL DEFAULT NULL,
    ADD INDEX idx_order_expiry (status, pay_expire_time, id);

CREATE TABLE IF NOT EXISTS tb_order_compensation_task (
    id BIGINT NOT NULL AUTO_INCREMENT,
    order_id BIGINT NOT NULL,
    user_id BIGINT UNSIGNED NOT NULL,
    voucher_id BIGINT UNSIGNED NOT NULL,
    task_type VARCHAR(32) NOT NULL,
    status TINYINT NOT NULL DEFAULT 0,
    retry_count INT NOT NULL DEFAULT 0,
    next_execute_time DATETIME(3) NOT NULL,
    claim_token VARCHAR(64) NULL,
    lease_until DATETIME(3) NULL,
    last_error VARCHAR(500) NULL,
    create_time DATETIME(3) NOT NULL,
    update_time DATETIME(3) NOT NULL,
    completed_time DATETIME(3) NULL,
    PRIMARY KEY (id),
    UNIQUE KEY uk_order_task (order_id, task_type),
    KEY idx_task_due (status, next_execute_time, id),
    KEY idx_task_lease (status, lease_until, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
