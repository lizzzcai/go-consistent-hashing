package consistenthash

import (
	"errors"
	"fmt"
	"hash/crc32"
	"sort"
	"sync"
)

// for sort.Sort
type slots []uint32

func (s slots) Len() int {
	return len(s)
}

func (s slots) Less(i, j int) bool {
	return s[i] < s[j]
}

func (s slots) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type Hash func(data []byte) uint32

type ConsistentHash struct {
	sync.Mutex
	hash       Hash              // hash function
	nodesMap   map[uint32]string // map hash slot to node name
	nodesSlots slots             // slots for virtual nodes
	replicas   int               // number of virtual node for each machine
}

func NewConsistenHash(replicas int, fn Hash) *ConsistentHash {
	h := &ConsistentHash{
		hash:       fn,
		nodesMap:   make(map[uint32]string),
		nodesSlots: slots{},
		replicas:   replicas,
	}
	if h.hash == nil {
		h.hash = crc32.ChecksumIEEE
	}
	return h
}

// hash function by crc32
func (h *ConsistentHash) dohash(key string) uint32 {
	return h.hash([]byte(key))
}

func (h *ConsistentHash) IsEmpty() bool {
	return len(h.nodesSlots) == 0
}

// add machine
func (h *ConsistentHash) AddNode(addrs ...string) {
	h.Lock()
	defer h.Unlock()
	// iterate the address
	for _, addr := range addrs {
		// generate virtual nodes and calculate the hash slot
		for i := 0; i < h.replicas; i++ {
			slot := h.dohash(fmt.Sprintf("%s%d", addr, i))
			h.nodesMap[slot] = addr
		}
	}

	// sort all the virtual nodes
	h.sortNodesSlots()
}

func (h *ConsistentHash) sortNodesSlots() {

	slots := slots{}
	for key := range h.nodesMap {
		slots = append(slots, key)
	}
	sort.Sort(slots)
	h.nodesSlots = slots
}

func (h *ConsistentHash) DeleteNode(addrs ...string) {
	h.Lock()
	defer h.Lock()
	for _, addr := range addrs {
		// delete virtual nodes of the given address
		for i := 0; i < h.replicas; i++ {
			slot := h.dohash(fmt.Sprintf("%s%d", addr, i))
			delete(h.nodesMap, slot)
		}
	}
	h.sortNodesSlots()
}

func (h *ConsistentHash) SearchNode(key string) (string, error) {
	if h.IsEmpty() {
		return "", errors.New("Empty")
	}

	slot := h.dohash(key)
	// use binary search
	index := sort.Search(len(h.nodesSlots), func(i int) bool { return h.nodesSlots[i] >= slot })
	if index >= len(h.nodesSlots) {
		index = 0
	}

	if addr, ok := h.nodesMap[h.nodesSlots[index]]; ok {
		return addr, nil
	}

	return "", errors.New("Not found")
}
