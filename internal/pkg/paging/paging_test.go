package paging

import "testing"

func TestNormalize(t *testing.T) {
	cases := []struct {
		name           string
		page, pageSize int
		wantPage       int
		wantPageSize   int
	}{
		{"负数与零回落默认值", 0, 0, DefaultPage, DefaultPageSize},
		{"负偏移不产生负数", -5, -1, DefaultPage, DefaultPageSize},
		{"上界被夹取到 MaxPageSize", 1, 100000, DefaultPage, MaxPageSize},
		{"int32 上界被夹取", 1, 2147483647, DefaultPage, MaxPageSize},
		{"合法值原样透传", 3, 20, 3, 20},
		{"恰好等于上界", 2, MaxPageSize, 2, MaxPageSize},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			page, pageSize := Normalize(tc.page, tc.pageSize)
			if page != tc.wantPage || pageSize != tc.wantPageSize {
				t.Fatalf("Normalize(%d, %d) = (%d, %d), want (%d, %d)",
					tc.page, tc.pageSize, page, pageSize, tc.wantPage, tc.wantPageSize)
			}
			if pageSize > MaxPageSize {
				t.Fatalf("pageSize %d 越过安全上界 %d", pageSize, MaxPageSize)
			}
		})
	}
}

func TestFromProto(t *testing.T) {
	page, pageSize := FromProto(0, 9999)
	if page != DefaultPage || pageSize != MaxPageSize {
		t.Fatalf("FromProto(0, 9999) = (%d, %d), want (%d, %d)", page, pageSize, DefaultPage, MaxPageSize)
	}
}

func TestOffsetNeverNegative(t *testing.T) {
	if got := Offset(-1, -1); got != 0 {
		t.Fatalf("Offset(-1, -1) = %d, want 0", got)
	}
	if got := Offset(3, 20); got != 40 {
		t.Fatalf("Offset(3, 20) = %d, want 40", got)
	}
}

func TestClampTotal(t *testing.T) {
	cases := []struct {
		in   int64
		want int32
	}{
		{-1, 0},
		{0, 0},
		{42, 42},
		{2147483647, 2147483647},
		{2147483648, 2147483647},
		{1 << 40, 2147483647},
	}
	for _, tc := range cases {
		if got := ClampTotal(tc.in); got != tc.want {
			t.Fatalf("ClampTotal(%d) = %d, want %d", tc.in, got, tc.want)
		}
	}
}
