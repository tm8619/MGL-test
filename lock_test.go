package main

import (
	"sync"
	"testing"
)

type LockType int

const (
	Shared LockType = iota
	Exclusive
)

type LockNode struct {
	mu        sync.RWMutex
	shared    int
	exclusive bool
	children  map[string]*LockNode
}

type LockManager struct {
	root *LockNode
}

func NewLockManager() *LockManager {
	return &LockManager{
		root: &LockNode{children: make(map[string]*LockNode)},
	}
}

// path: [User, Account, Resource]
func (lm *LockManager) Lock(path []string, lockType LockType) func() {
	nodes := []*LockNode{}
	n := lm.root
	for i, name := range path {
		n.mu.RLock()
		child, ok := n.children[name]
		n.mu.RUnlock()
		if !ok {
			n.mu.Lock()
			if child, ok = n.children[name]; !ok {
				child = &LockNode{children: make(map[string]*LockNode)}
				n.children[name] = child
			}
			n.mu.Unlock()
		}
		n = child
		if i < len(path)-1 {
			n.mu.RLock() // 上位は共有ロック
			n.shared++
			n.mu.RUnlock()
			nodes = append(nodes, n)
		}
	}
	// 最下層は排他ロック
	n.mu.Lock()
	n.exclusive = true
	n.mu.Unlock()
	nodes = append(nodes, n)

	return func() {
		for i := len(nodes) - 1; i >= 0; i-- {
			n := nodes[i]
			if i == len(nodes)-1 {
				n.mu.Lock()
				n.exclusive = false
				n.mu.Unlock()
			} else {
				n.mu.Lock()
				n.shared--
				n.mu.Unlock()
			}
		}
	}
}

func TestLockHierarchy(t *testing.T) {
	lm := NewLockManager()
	path := []string{"user1", "accountA", "resourceX"}
	unlock := lm.Lock(path, Exclusive)
	// resourceXに排他ロック、上位は共有ロック
	// resourceX
	node := lm.root.children["user1"].children["accountA"].children["resourceX"]
	if !node.exclusive {
		t.Error("resourceX should be exclusively locked")
	}
	// accountA
	account := lm.root.children["user1"].children["accountA"]
	if account.shared != 1 {
		t.Errorf("accountA should have 1 shared lock, got %d", account.shared)
	}
	// user1
	user := lm.root.children["user1"]
	if user.shared != 1 {
		t.Errorf("user1 should have 1 shared lock, got %d", user.shared)
	}
	unlock()
	if node.exclusive {
		t.Error("resourceX should be unlocked after unlock()")
	}
	if account.shared != 0 {
		t.Errorf("accountA shared lock should be 0 after unlock, got %d", account.shared)
	}
	if user.shared != 0 {
		t.Errorf("user1 shared lock should be 0 after unlock, got %d", user.shared)
	}
}

func TestMultipleResourceLock(t *testing.T) {
	lm := NewLockManager()
	unlock1 := lm.Lock([]string{"user1", "accountA", "resourceX"}, Exclusive)
	unlock2 := lm.Lock([]string{"user1", "accountA", "resourceY"}, Exclusive)
	// accountAのsharedは2になる
	account := lm.root.children["user1"].children["accountA"]
	if account.shared != 2 {
		t.Errorf("accountA should have 2 shared locks, got %d", account.shared)
	}
	unlock1()
	unlock2()
	if account.shared != 0 {
		t.Errorf("accountA shared lock should be 0 after unlocks, got %d", account.shared)
	}
}
