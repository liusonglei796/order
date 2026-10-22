-- 结算令牌幂等体系第二道防线迁移 (Database Unique Index Fallback)
-- 背景：(1) parent_orders 未落库 settlement_token，Redis 状态机是唯一的防重手段；
--       (2) Redis 集群脑裂切主丢数据、或租约自愈重入时，缺少数据库层的最终兜底；
--       (3) inventory_reservations 缺少 (tenant_id, order_no, sku_id) 唯一约束，
--           同一订单号可被重复预占库存。
-- 目标：让"同一结算令牌 -> 唯一一张主订单"、"同一订单号同一 SKU -> 唯一一条预占流水"
--       成为数据库层面的强约束，把重复订单与重复扣库存的概率彻底压到 0。
-- GORM AutoMigrate 只对新增表结构生效，存量库必须手工执行本脚本。执行前请先备份。

-- ============================================================
-- Step 1: parent_orders 增加结算令牌列
-- 语义：允许 NULL（历史数据与非结算链路建单），MySQL 唯一索引允许多行 NULL
-- ============================================================

ALTER TABLE parent_orders
  ADD COLUMN settlement_token VARCHAR(64) NULL COMMENT '结算幂等令牌 (Redis 状态机令牌)' AFTER order_no;

-- ============================================================
-- Step 2: 清理历史重复令牌 (加唯一索引前必须先处理)
-- 同一租户下同一令牌只保留最早创建的一张主订单，其余令牌置空以免索引创建失败
-- ============================================================

UPDATE parent_orders po
  JOIN (
    SELECT tenant_id, settlement_token, MIN(id) AS keep_id
    FROM parent_orders
    WHERE settlement_token IS NOT NULL AND settlement_token <> ''
    GROUP BY tenant_id, settlement_token
    HAVING COUNT(*) > 1
  ) dup ON dup.keep_id = po.id
SET po.settlement_token = NULL
WHERE po.settlement_token IS NOT NULL
  AND po.settlement_token <> ''
  AND po.id <> dup.keep_id;

-- ============================================================
-- Step 3: 建立结算令牌复合唯一索引 (第二道防线)
-- 语义：UNIQUE (tenant_id, settlement_token)
-- 效果：同一租户下同一结算令牌第二次 INSERT 必然抛出唯一键冲突并回滚事务
-- ============================================================

CREATE UNIQUE INDEX uniq_parent_settlement_token
  ON parent_orders (tenant_id, settlement_token);

-- ============================================================
-- Step 4: 库存预占流水唯一索引 (杜绝重复预占)
-- 语义：UNIQUE (tenant_id, order_no, sku_id)
-- 效果：主订单号由结算令牌确定性推导，租约自愈重入时订单号不变，
--       第二次预占必然命中唯一键冲突并被应用层幂等短路
-- ============================================================

ALTER TABLE inventory_reservations
  ADD UNIQUE INDEX uniq_tenant_order_sku (tenant_id, order_no, sku_id);
