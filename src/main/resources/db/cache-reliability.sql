-- Apply once; do not reimport hmdp.sql. Retain DONE records for at least 7 days.
CREATE TABLE IF NOT EXISTS tb_cache_invalidation_task (
  id varchar(36) NOT NULL,
  business_type varchar(32) NOT NULL DEFAULT 'SHOP',
  business_id bigint NOT NULL,
  status varchar(16) NOT NULL DEFAULT 'PENDING',
  retry_count int NOT NULL DEFAULT 0,
  dispatch_retry_count int NOT NULL DEFAULT 0,
  next_execute_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  next_dispatch_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  dispatch_token varchar(36) DEFAULT NULL,
  claim_token varchar(36) DEFAULT NULL,
  lease_until datetime(3) DEFAULT NULL,
  broadcast_done tinyint NOT NULL DEFAULT 0,
  broadcast_token varchar(36) DEFAULT NULL,
  next_broadcast_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  last_error varchar(500) DEFAULT NULL,
  create_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  update_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  completed_time datetime(3) DEFAULT NULL,
  PRIMARY KEY (id),
  KEY idx_dispatch (status, next_dispatch_time, id),
  KEY idx_lease (status, lease_until, id),
  KEY idx_broadcast (status, broadcast_done, next_broadcast_time, id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS tb_cache_message_quarantine (
  topic varchar(128) NOT NULL,
  partition_id int NOT NULL,
  offset_id bigint NOT NULL,
  payload mediumtext,
  last_error varchar(500),
  create_time datetime(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  PRIMARY KEY (topic, partition_id, offset_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
