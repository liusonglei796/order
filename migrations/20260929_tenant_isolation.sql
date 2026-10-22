-- 多租户隔离修复迁移
-- 背景：Audit 发现 (1) 6 个业务编号的唯一索引漏了 tenant_id，实际建成单列唯一；
--       (2) parent_orders / escrow_journal_entries / after_sales_items 三张表没有 tenant_id；
--       (3) 多个查询方法未按 tenant_id 过滤，存在跨租户读写。
-- GORM AutoMigrate 只会新增索引，不会修改或删除已存在的索引，因此存量库必须手工执行本脚本。
-- 执行前请先备份。

-- ============================================================
-- Step 1: 补齐缺失的 tenant_id 列
-- ============================================================

ALTER TABLE parent_orders
  ADD COLUMN tenant_id VARCHAR(64) NOT NULL DEFAULT 'tenant-default' AFTER id,
  ADD KEY idx_parent_tenant (tenant_id);

ALTER TABLE escrow_journal_entries
  ADD COLUMN tenant_id VARCHAR(64) NOT NULL DEFAULT 'tenant-default' AFTER id;

ALTER TABLE after_sales_items
  ADD COLUMN tenant_id VARCHAR(64) NOT NULL DEFAULT 'tenant-default' AFTER id,
  ADD KEY idx_as_id (tenant_id, after_sales_id);

-- ============================================================
-- Step 2: 回填历史数据
-- 存量 parent_orders 可通过 child_orders 推断租户（父单归属 = 其子单归属）
-- ============================================================

UPDATE parent_orders po
  JOIN (
    SELECT parent_id, MIN(tenant_id) AS tenant_id
    FROM child_orders
    GROUP BY parent_id
  ) c ON c.parent_id = po.id
SET po.tenant_id = c.tenant_id
WHERE po.tenant_id = 'tenant-default';

-- 记账凭证归属 = 其分录行归属
UPDATE escrow_journal_entries je
  JOIN (
    SELECT entry_id, MIN(tenant_id) AS tenant_id
    FROM escrow_journal_lines
    GROUP BY entry_id
  ) l ON l.entry_id = je.id
SET je.tenant_id = l.tenant_id
WHERE je.tenant_id = 'tenant-default';

-- 售后明细归属 = 其售后单归属
UPDATE after_sales_items asi
  JOIN after_sales_orders aso ON aso.id = asi.after_sales_id
SET asi.tenant_id = aso.tenant_id
WHERE asi.tenant_id = 'tenant-default';

-- ============================================================
-- Step 3: 重建唯一索引（补上 tenant_id 组成复合索引）
-- 原索引名为单列唯一，业务编号实际是全局唯一，会导致 A 租户占用后 B 租户无法使用
-- ============================================================

ALTER TABLE users DROP INDEX uk_tenant_username;
ALTER TABLE users ADD UNIQUE KEY uk_tenant_username (tenant_id, username);

ALTER TABLE orders DROP INDEX uniq_tenant_order_no;
ALTER TABLE orders ADD UNIQUE KEY uniq_tenant_order_no (tenant_id, order_no);

ALTER TABLE parent_orders DROP INDEX uniq_parent_order_no;
ALTER TABLE parent_orders ADD UNIQUE KEY uniq_parent_order_no (tenant_id, order_no);

ALTER TABLE child_orders DROP INDEX uniq_tenant_child_order_no;
ALTER TABLE child_orders ADD UNIQUE KEY uniq_tenant_child_order_no (tenant_id, order_no);

ALTER TABLE after_sales_orders DROP INDEX uniq_after_sales_no;
ALTER TABLE after_sales_orders ADD UNIQUE KEY uniq_after_sales_no (tenant_id, after_sales_no);

ALTER TABLE escrow_journal_entries DROP INDEX uniq_entry_no;
ALTER TABLE escrow_journal_entries ADD UNIQUE KEY uniq_entry_no (tenant_id, entry_no);

-- ============================================================
-- Step 4: 建立租户注册表
-- ============================================================

CREATE TABLE IF NOT EXISTS tenants (
  id          VARCHAR(64)  NOT NULL,
  name        VARCHAR(128) NOT NULL,
  type        VARCHAR(16)  NOT NULL DEFAULT 'MERCHANT',
  status      VARCHAR(16)  NOT NULL DEFAULT 'ACTIVE',
  contact     VARCHAR(128) NULL,
  created_at  DATETIME     NULL,
  updated_at  DATETIME     NULL,
  PRIMARY KEY (id),
  KEY idx_tenants_type_status (type, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT IGNORE INTO tenants (id, name, type, status) VALUES
  ('platform',       '平台',     'PLATFORM', 'ACTIVE'),
  ('tenant-default', '默认商户', 'MERCHANT', 'ACTIVE');

-- ============================================================
-- Step 5: 存量租户回填
-- 从各业务表收集实际出现过的 tenant_id，登记为已存在租户
-- ============================================================

INSERT IGNORE INTO tenants (id, name, type, status)
SELECT t.tid, CONCAT('迁移租户 ', t.tid), 'MERCHANT', 'ACTIVE'
FROM (
  SELECT tenant_id AS tid FROM users
  UNION SELECT tenant_id FROM orders
  UNION SELECT tenant_id FROM child_orders
  UNION SELECT tenant_id FROM after_sales_orders
  UNION SELECT tenant_id FROM merchant_wallets
) t
WHERE t.tid IS NOT NULL AND t.tid <> '';

-- ============================================================
-- 验证
-- ============================================================

-- 确认 6 个唯一索引均为两列
SELECT TABLE_NAME, INDEX_NAME, GROUP_CONCAT(COLUMN_NAME ORDER BY SEQ_IN_INDEX) AS cols
FROM information_schema.STATISTICS
WHERE TABLE_SCHEMA = DATABASE()
  AND NON_UNIQUE = 0
  AND INDEX_NAME IN ('uk_tenant_username','uniq_tenant_order_no','uniq_parent_order_no',
                     'uniq_tenant_child_order_no','uniq_after_sales_no','uniq_entry_no')
GROUP BY TABLE_NAME, INDEX_NAME;

-- 确认无残留空租户
SELECT 'parent_orders' AS tbl, COUNT(*) AS orphan FROM parent_orders WHERE tenant_id IS NULL OR tenant_id = ''
UNION ALL SELECT 'escrow_journal_entries', COUNT(*) FROM escrow_journal_entries WHERE tenant_id IS NULL OR tenant_id = ''
UNION ALL SELECT 'after_sales_items', COUNT(*) FROM after_sales_items WHERE tenant_id IS NULL OR tenant_id = '';
