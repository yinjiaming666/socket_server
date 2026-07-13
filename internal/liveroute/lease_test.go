package liveroute

import (
	"testing"
	"time"
)

// TestDefaultLeaseTTLIsThirtySeconds 防止故障节点残留窗口被意外改回较长时间。
func TestDefaultLeaseTTLIsThirtySeconds(t *testing.T) {
	if DefaultLeaseTTL != 30*time.Second {
		t.Fatalf("expected 30s live route lease, got %s", DefaultLeaseTTL)
	}
}
