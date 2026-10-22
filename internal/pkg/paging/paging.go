// Package paging 集中收敛全仓库的分页参数归一化规则。
//
// 存在的理由：分页边界此前在 7 个 service 与多个 repository 中各抄一份，
// 且上界夹取并不一致——product 与 wallet 夹到 100，order 与 user 完全没有上界。
// 缺失上界使 page_size 可达 int32 上限，直落 order 仓储的三层预加载循环，
// 形成无需凭据即可触发的查询放大。归一化必须是单一 owner，不能由调用方各自负责。
package paging

import "math"

const (
	// DefaultPage 为 page 非法时回落的首页页码。
	DefaultPage = 1
	// DefaultPageSize 为 page_size 非法时回落的默认每页条数。
	DefaultPageSize = 10
	// MaxPageSize 为单次查询允许读取的最大行数。
	//
	// 该上界同时是安全边界：仓储层的聚合读取会对每一行再发出子查询，
	// 未设上界时单次请求的 SQL 条数随 page_size 线性膨胀。
	MaxPageSize = 100
)

// Step 0.1: 将调用方传入的分页参数收敛到安全区间
// 来源: service 层从 proto 请求体读取的 page / page_size
// 用于: 统一夹取下界与上界，保证所有下游查询的偏移量与行数有确定上限
func Normalize(page, pageSize int) (int, int) {
	if page < DefaultPage {
		page = DefaultPage
	}
	switch {
	case pageSize < 1:
		pageSize = DefaultPageSize
	case pageSize > MaxPageSize:
		pageSize = MaxPageSize
	}
	return page, pageSize
}

// Step 0.2: 在 proto 的 int32 分页参数边界上完成归一化
// 来源: 生成的 *GetXxxRequest 协议字段（int32）
// 用于: 在服务层一次性吸收 proto 与仓储层之间的整数类型差异，向下只传 int
func FromProto(page, pageSize int32) (int, int) {
	return Normalize(int(page), int(pageSize))
}

// Step 0.3: 计算 LIMIT 偏移量
// 来源: Step 0.1 归一化后的 page 与 pageSize
// 用于: 仓储层构造 Offset，保证调用方即便跳过归一化也不会算出负偏移
func Offset(page, pageSize int) int {
	page, pageSize = Normalize(page, pageSize)
	return (page - 1) * pageSize
}

// Step 0.4: 将仓储层返回的 int64 总数收敛到 proto 的 int32 表示范围
// 来源: 仓储层 COUNT(*) 返回的 int64
// 用于: 服务层填充响应体 total 字段，避免 int64 到 int32 的静默截断溢出
func ClampTotal(total int64) int32 {
	if total < 0 {
		return 0
	}
	if total > math.MaxInt32 {
		return math.MaxInt32
	}
	return int32(total)
}
