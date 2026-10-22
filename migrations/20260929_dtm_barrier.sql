-- ============================================================
-- DTM 分布式事务子事务屏障表 (Sub-transaction Barrier Table)
-- 用于借助本地 ACID 事务自动杜绝空补偿 (Null-Compensate)、
-- 防悬挂 (Dangling) 以及重试幂等去重 (Idempotency)
-- ============================================================

-- 在 mall_inventory 与 mall_order 等涉及分布式事务的业务数据库中执行

CREATE TABLE IF NOT EXISTS dtm_barrier (
  id          BIGINT(22)   NOT NULL AUTO_INCREMENT,
  trans_type  VARCHAR(45)  DEFAULT '' COMMENT '事务模式: saga, tcc, msg, xa',
  gid         VARCHAR(128) NOT NULL   COMMENT '全局事务唯一标识 ID',
  branch_id   VARCHAR(128) NOT NULL   COMMENT '子事务分支唯一标识 ID',
  op          VARCHAR(45)  NOT NULL   COMMENT '分支操作类型: action, compensate, try, confirm, cancel',
  barrier_id  VARCHAR(45)  DEFAULT '' COMMENT '屏障阶段序号',
  reason      VARCHAR(45)  DEFAULT '' COMMENT '回滚或拦截原因',
  create_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  update_time DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uniq_barrier (gid, branch_id, op, barrier_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='DTM 分布式事务子事务屏障防悬挂与幂等记录表';
