package consistenthash

import (
	"testing"
)

func TestConsistenthash(t *testing.T) {
	t.Run("test", func(t *testing.T) {
		h := NewConsistenHash(3, nil)
		addr1 := "192.168.1.10"
		addr2 := "192.168.1.200"

		h.AddNode(addr1, addr2)

		got1, _ := h.SearchNode("192.168.1.20")
		if got1 != addr1 {
			t.Errorf("want %v, got %v", addr1, got1)
		}
		got2, _ := h.SearchNode("192.168.1.150")
		if got2 != addr2 {
			t.Errorf("want %v, got %v", addr2, got2)
		}
	})
}
