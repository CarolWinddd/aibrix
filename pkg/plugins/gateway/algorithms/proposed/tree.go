/*
Copyright 2024 The Aibrix Team.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proposed

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultNumTimeBuckets    = 20
	defaultWindowSize        = 1200 * time.Second
	defaultEvictionInterval  = 1200 * time.Second
	defaultMaxNodes          = 200000
	defaultHotRatio          = 0.001
	defaultLowWatermarkRatio = 0.8
)

type OnNodesEvictedFunc func(nodeIDs []int64)

type Config struct {
	NumTimeBuckets    int
	WindowSize        time.Duration
	EvictionInterval  time.Duration
	MaxNodes          int
	HotRatio          float64
	LowWatermarkRatio float64
}

func DefaultConfig() *Config {
	return &Config{
		NumTimeBuckets:    defaultNumTimeBuckets,
		WindowSize:        defaultWindowSize,
		EvictionInterval:  defaultEvictionInterval,
		MaxNodes:          defaultMaxNodes,
		HotRatio:          defaultHotRatio,
		LowWatermarkRatio: defaultLowWatermarkRatio,
	}
}

type BlockNode struct {
	id        int64
	blockHash uint64
	parent    *BlockNode
	children  map[uint64]*BlockNode

	bucketHits  []int64
	lastRotated time.Time
	lastAccess  atomic.Int64
	totalHits   atomic.Int64
	depth       int
	childCount  int32

	hitsCache atomic.Int64
}

func (n *BlockNode) ID() int64            { return n.id }
func (n *BlockNode) Depth() int           { return n.depth }
func (n *BlockNode) BlockHash() uint64    { return n.blockHash }
func (n *BlockNode) Parent() *BlockNode   { return n.parent }
func (n *BlockNode) ChildCount() int32    { return atomic.LoadInt32(&n.childCount) }

func (n *BlockNode) LastAccess() time.Time {
	return time.Unix(0, n.lastAccess.Load())
}

func (n *BlockNode) TotalHits() int64 {
	return n.totalHits.Load()
}

func (n *BlockNode) Children() map[uint64]*BlockNode {
	return n.children
}

func (n *BlockNode) windowHits() int64 {
	var sum int64
	for _, v := range n.bucketHits {
		sum += v
	}
	return sum
}

func (n *BlockNode) WindowHits() int64 {
	return n.hitsCache.Load()
}

func (n *BlockNode) isLeaf() bool {
	return n.ChildCount() == 0
}

func (n *BlockNode) rotateBuckets(now time.Time, numBuckets int, bucketDuration time.Duration) {
	elapsed := now.Sub(n.lastRotated)
	shifts := int(elapsed / bucketDuration)
	if shifts <= 0 {
		return
	}
	if shifts >= numBuckets {
		for i := range n.bucketHits {
			n.bucketHits[i] = 0
		}
	} else {
		copy(n.bucketHits[shifts:], n.bucketHits[:numBuckets-shifts])
		for i := 0; i < shifts; i++ {
			n.bucketHits[i] = 0
		}
	}
	n.lastRotated = now
}

func (n *BlockNode) refreshHitsCache() {
	n.hitsCache.Store(n.windowHits())
}

func (n *BlockNode) recordAccess(now time.Time, numBuckets int, bucketDuration time.Duration) {
	n.rotateBuckets(now, numBuckets, bucketDuration)
	n.bucketHits[0]++
	n.totalHits.Add(1)
	n.lastAccess.Store(now.UnixNano())
	n.refreshHitsCache()
}

type MatchResult struct {
	MatchedBlocks int
	MatchNode     *BlockNode
}

type BlockTree struct {
	mu sync.RWMutex

	root     *BlockNode
	allNodes map[int64]*BlockNode

	nextNodeID atomic.Int64
	nodeCount  atomic.Int64
	evicting   atomic.Bool

	config *Config

	bucketDuration time.Duration
	lowWatermark   int64
	evictCh        chan struct{}
	stopCh         chan struct{}
	stopped        atomic.Bool

	onNodesEvicted OnNodesEvictedFunc
}

func NewBlockTree(cfg *Config) *BlockTree {
	if cfg == nil {
		cfg = DefaultConfig()
	}
	if cfg.NumTimeBuckets <= 0 {
		cfg.NumTimeBuckets = defaultNumTimeBuckets
	}
	if cfg.WindowSize <= 0 {
		cfg.WindowSize = defaultWindowSize
	}
	if cfg.EvictionInterval <= 0 {
		cfg.EvictionInterval = defaultEvictionInterval
	}
	if cfg.MaxNodes <= 0 {
		cfg.MaxNodes = defaultMaxNodes
	}
	if cfg.HotRatio <= 0 {
		cfg.HotRatio = defaultHotRatio
	}
	if cfg.LowWatermarkRatio <= 0 || cfg.LowWatermarkRatio >= 1.0 {
		cfg.LowWatermarkRatio = defaultLowWatermarkRatio
	}

	now := time.Now()
	t := &BlockTree{
		config:         cfg,
		allNodes:       make(map[int64]*BlockNode),
		bucketDuration: cfg.WindowSize / time.Duration(cfg.NumTimeBuckets),
		lowWatermark:   int64(float64(cfg.MaxNodes) * cfg.LowWatermarkRatio),
		evictCh:        make(chan struct{}, 1),
		stopCh:         make(chan struct{}),
	}

	t.root = t.newNode(0, nil, now)
	t.root.lastRotated = now
	t.root.lastAccess.Store(now.UnixNano())
	t.root.refreshHitsCache()

	go t.evictionLoop()

	klog.InfoS("block_tree_initialized",
		"num_buckets", cfg.NumTimeBuckets,
		"window_size", cfg.WindowSize,
		"eviction_interval", cfg.EvictionInterval,
		"max_nodes", cfg.MaxNodes,
		"hot_ratio", cfg.HotRatio,
		"low_watermark", t.lowWatermark,
		"bucket_duration", t.bucketDuration,
	)
	return t
}

func (t *BlockTree) SetOnNodesEvicted(cb OnNodesEvictedFunc) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onNodesEvicted = cb
}

func (t *BlockTree) newNode(blockHash uint64, parent *BlockNode, now time.Time) *BlockNode {
	id := t.nextNodeID.Add(1)
	depth := 0
	if parent != nil {
		depth = parent.depth + 1
	}
	node := &BlockNode{
		id:          id,
		blockHash:   blockHash,
		parent:      parent,
		children:    make(map[uint64]*BlockNode),
		bucketHits:  make([]int64, t.config.NumTimeBuckets),
		lastRotated: now,
		depth:       depth,
	}
	node.lastAccess.Store(now.UnixNano())
	t.allNodes[id] = node
	t.nodeCount.Add(1)
	return node
}

func (t *BlockTree) NodeCount() int64 {
	return t.nodeCount.Load()
}

func (t *BlockTree) Root() *BlockNode {
	return t.root
}

func (t *BlockTree) IsHot(node *BlockNode) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	now := time.Now()
	return t.isHotLocked(node, now)
}

func (t *BlockTree) isHotLocked(node *BlockNode, now time.Time) bool {
	if node == nil {
		return false
	}
	if node == t.root {
		return true
	}

	node.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)
	node.refreshHitsCache()
	nodeHits := node.windowHits()
	if nodeHits == 0 {
		return false
	}

	t.root.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)
	t.root.refreshHitsCache()
	totalHits := t.root.windowHits()
	if totalHits == 0 {
		return false
	}

	ratio := float64(nodeHits) / float64(totalHits)
	return ratio >= t.config.HotRatio
}

func (t *BlockTree) GetPrefix(blockHashes []uint64) MatchResult {
	now := time.Now()

	t.mu.Lock()

	if len(blockHashes) == 0 {
		t.root.recordAccess(now, t.config.NumTimeBuckets, t.bucketDuration)
		t.mu.Unlock()
		t.triggerEvictionIfNeeded()
		return MatchResult{MatchedBlocks: 0, MatchNode: t.root}
	}

	t.root.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)

	path := make([]*BlockNode, 0, len(blockHashes)+1)
	path = append(path, t.root)
	current := t.root
	pathEnd := 0

	for i := 0; i < len(blockHashes); i++ {
		child, exists := current.children[blockHashes[i]]
		if !exists {
			break
		}
		child.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)
		path = append(path, child)
		pathEnd = i + 1
		current = child
	}

	bestNode := t.root
	bestLen := 0
	totalHits := t.root.windowHits()
	for i := 1; i < len(path); i++ {
		node := path[i]
		nodeHits := node.windowHits()
		if totalHits > 0 && float64(nodeHits)/float64(totalHits) >= t.config.HotRatio {
			bestNode = node
			bestLen = i
		}
	}

	for _, node := range path {
		node.bucketHits[0]++
		node.totalHits.Add(1)
		node.lastAccess.Store(now.UnixNano())
	}
	for _, node := range path {
		node.refreshHitsCache()
	}

	t.insertPathLocked(current, blockHashes, pathEnd, now)

	overCapacity := t.nodeCount.Load() > int64(t.config.MaxNodes)

	t.mu.Unlock()

	if overCapacity {
		t.triggerEvictionIfNeeded()
	}

	return MatchResult{
		MatchedBlocks: bestLen,
		MatchNode:     bestNode,
	}
}

func (t *BlockTree) triggerEvictionIfNeeded() {
	if t.nodeCount.Load() <= int64(t.config.MaxNodes) {
		return
	}
	if t.evicting.Load() {
		return
	}
	select {
	case t.evictCh <- struct{}{}:
	default:
	}
}

func (t *BlockTree) insertPathLocked(current *BlockNode, blockHashes []uint64, from int, now time.Time) {
	for i := from; i < len(blockHashes); i++ {
		child, exists := current.children[blockHashes[i]]
		if exists {
			current = child
			continue
		}
		newNode := t.newNode(blockHashes[i], current, now)
		newNode.bucketHits[0] = 1
		newNode.totalHits.Store(1)
		newNode.refreshHitsCache()
		current.children[blockHashes[i]] = newNode
		atomic.AddInt32(&current.childCount, 1)
		current = newNode
	}
}

func (t *BlockTree) evictionLoop() {
	ticker := time.NewTicker(t.config.EvictionInterval)
	defer ticker.Stop()

	for {
		select {
		case <-t.stopCh:
			return
		case <-ticker.C:
			t.runEviction(time.Now(), t.lowWatermark)
		case <-t.evictCh:
			t.runEviction(time.Now(), t.lowWatermark)
		}
	}
}

func (t *BlockTree) runEviction(now time.Time, targetCount int64) {
	if !t.evicting.CompareAndSwap(false, true) {
		return
	}
	defer t.evicting.Store(false)

	t.mu.Lock()
	rootCount, nodeCount, nodeIDs := t.evictColdNodesLocked(now, targetCount)

	if rootCount > 0 {
		klog.V(4).InfoS("block_tree_eviction",
			"cold_subtree_root_count", rootCount,
			"evicted_node_count", nodeCount,
			"node_count", t.nodeCount.Load(),
			"target", targetCount,
		)
	}

	needsReEvict := t.nodeCount.Load() > int64(t.config.MaxNodes)
	cb := t.onNodesEvicted
	t.mu.Unlock()

	if len(nodeIDs) > 0 && cb != nil {
		cb(nodeIDs)
	}

	if needsReEvict {
		select {
		case t.evictCh <- struct{}{}:
		default:
		}
	}
}

type evictCandidate struct {
	node      *BlockNode
	hits      int64
	lastAccess time.Time
	depth     int
}

func (t *BlockTree) evictColdNodesLocked(now time.Time, targetCount int64) (evictedRootCount int, evictedNodeCount int, evictedNodeIDs []int64) {
	evictedRootCount = 0
	evictedNodeCount = 0

	queue := make([]*BlockNode, 0, len(t.root.children))
	for _, child := range t.root.children {
		queue = append(queue, child)
	}

	var coldCandidates []evictCandidate

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		node.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)
		hits := node.windowHits()
		node.refreshHitsCache()
		if hits == 0 {
			subCount, subIDs := t.evictSubtreeLocked(node)
			evictedNodeCount += subCount
			evictedNodeIDs = append(evictedNodeIDs, subIDs...)
			evictedRootCount++
			if t.nodeCount.Load() <= targetCount {
				return evictedRootCount, evictedNodeCount, evictedNodeIDs
			}
			continue
		}

		coldCandidates = append(coldCandidates, evictCandidate{
			node:       node,
			hits:       hits,
			lastAccess: time.Unix(0, node.lastAccess.Load()),
			depth:      node.depth,
		})

		for _, child := range node.children {
			queue = append(queue, child)
		}
	}

	if t.nodeCount.Load() <= targetCount {
		return evictedRootCount, evictedNodeCount, evictedNodeIDs
	}

	sort.Slice(coldCandidates, func(i, j int) bool {
		if coldCandidates[i].hits != coldCandidates[j].hits {
			return coldCandidates[i].hits < coldCandidates[j].hits
		}
		return coldCandidates[i].lastAccess.Before(coldCandidates[j].lastAccess)
	})

	for _, c := range coldCandidates {
		if c.node.parent == nil {
			continue
		}
		if _, exists := t.allNodes[c.node.id]; !exists {
			continue
		}
		subCount, subIDs := t.evictSubtreeLocked(c.node)
		if subCount > 0 {
			evictedNodeCount += subCount
			evictedNodeIDs = append(evictedNodeIDs, subIDs...)
			evictedRootCount++
			if t.nodeCount.Load() <= targetCount {
				break
			}
		}
	}

	return evictedRootCount, evictedNodeCount, evictedNodeIDs
}

func (t *BlockTree) evictSubtreeLocked(node *BlockNode) (evictedCount int, evictedNodeIDs []int64) {
	if node == t.root || node.parent == nil {
		return 0, nil
	}

	parent := node.parent

	evictedCount = 0
	stack := []*BlockNode{node}
	for len(stack) > 0 {
		n := stack[len(stack)-1]
		stack = stack[:len(stack)-1]

		for _, child := range n.children {
			stack = append(stack, child)
		}

		delete(t.allNodes, n.id)
		t.nodeCount.Add(-1)
		evictedNodeIDs = append(evictedNodeIDs, n.id)
		n.parent = nil
		n.children = nil
		n.bucketHits = nil
		evictedCount++

		klog.V(6).InfoS("block_tree_evict_node", "node_id", n.id, "depth", n.depth, "hash", n.blockHash)
	}

	delete(parent.children, node.blockHash)
	atomic.AddInt32(&parent.childCount, -1)

	return evictedCount, evictedNodeIDs
}

func (t *BlockTree) Close() {
	if t.stopped.CompareAndSwap(false, true) {
		close(t.stopCh)
	}
}

func (t *BlockTree) SnapshotNodeCounts() (total int64, leaves int64, hotLeaves int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := time.Now()
	total = t.nodeCount.Load()
	for _, node := range t.allNodes {
		if node.isLeaf() {
			leaves++
			node.rotateBuckets(now, t.config.NumTimeBuckets, t.bucketDuration)
			node.refreshHitsCache()
			if t.isHotLocked(node, now) {
				hotLeaves++
			}
		}
	}
	return
}
