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
	"math"
	"math/rand"
	"sort"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

const (
	defaultCooldownInterval     = 3 * time.Second
	defaultWeightStep           = 0.8
	defaultMinWeight            = 0.1
	defaultMinTrafficForRemap   = 1
	defaultMaxTargets           = 10000000
	defaultOverloadStddevFactor = 1
	defaultOverloadAbsCount     = 8
	defaultLoadSensitivity      = 1.0
)

type RemapConfig struct {
	CooldownInterval     time.Duration
	WeightStep           float64
	MinWeight            float64
	MinTrafficForRemap   int64
	MaxTargets           int
	OverloadStddevFactor float64
	OverloadAbsCount     float64
	LoadSensitivity      float64
	RandSeed             int64
}

func DefaultRemapConfig() *RemapConfig {
	return &RemapConfig{
		CooldownInterval:     defaultCooldownInterval,
		WeightStep:           defaultWeightStep,
		MinWeight:            defaultMinWeight,
		MinTrafficForRemap:   defaultMinTrafficForRemap,
		MaxTargets:           defaultMaxTargets,
		OverloadStddevFactor: defaultOverloadStddevFactor,
		OverloadAbsCount:     defaultOverloadAbsCount,
		LoadSensitivity:      defaultLoadSensitivity,
		RandSeed:             time.Now().UnixNano(),
	}
}

type WeightedTarget struct {
	InstanceID int
	Weight     float64
}

type TriggerAction int

const (
	NoAction TriggerAction = iota
	NeedsExpand
	NeedsContract
	NeedsRebalance
)

type ExpandDecider func(node *BlockNode, targets []WeightedTarget, allInstances []int, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (shouldExpand bool, expandTo int, donorIdx int)
type ContractDecider func(node *BlockNode, targets []WeightedTarget, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (shouldContract bool, contractIdx int)
type RebalanceDecider func(node *BlockNode, targets []WeightedTarget, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (shouldRebalance bool, fromIdx int, toIdx int)

type instanceCooldown struct {
	expiresAt time.Time
}

type loadStats struct {
	mean   float64
	stddev float64
	min    float64
	max    float64
}

func computeLoadStats(loads []float64) loadStats {
	if len(loads) == 0 {
		return loadStats{}
	}
	var sum float64
	minV := math.MaxFloat64
	maxV := -math.MaxFloat64
	for _, v := range loads {
		sum += v
		if v < minV {
			minV = v
		}
		if v > maxV {
			maxV = v
		}
	}
	mean := sum / float64(len(loads))
	var varianceSum float64
	for _, v := range loads {
		diff := v - mean
		varianceSum += diff * diff
	}
	stddev := math.Sqrt(varianceSum / float64(len(loads)))
	return loadStats{mean: mean, stddev: stddev, min: minV, max: maxV}
}

func isOverloaded(load float64, stats loadStats, cfg *RemapConfig) bool {
	if load > stats.mean+cfg.OverloadStddevFactor*stats.stddev {
		return true
	}
	if load > stats.min*float64(cfg.OverloadAbsCount) {
		return true
	}
	return false
}

func isUnderloaded(load float64, stats loadStats, cfg *RemapConfig) bool {
	if load < stats.mean-cfg.OverloadStddevFactor*stats.stddev {
		return true
	}
	if load < stats.max-float64(cfg.OverloadAbsCount) {
		return true
	}
	return false
}

func collectActiveLoads(targets []WeightedTarget, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time) ([]float64, []int) {
	activeLoads := make([]float64, 0, len(targets))
	activeIdx := make([]int, 0, len(targets))
	for i, t := range targets {
		if cd, ok := cooldowns[t.InstanceID]; ok && now.Before(cd.expiresAt) {
			continue
		}
		activeLoads = append(activeLoads, loads[t.InstanceID])
		activeIdx = append(activeIdx, i)
	}
	return activeLoads, activeIdx
}

type nodeMappingState struct {
	targets    []WeightedTarget
	remapCount int
	cooldowns  map[int]*instanceCooldown
}

type NodeMapping struct {
	mu     sync.RWMutex
	tree   *BlockTree
	config *RemapConfig
	rng    *rand.Rand

	states map[int64]*nodeMappingState

	ExpandDecider    ExpandDecider
	ContractDecider  ContractDecider
	RebalanceDecider RebalanceDecider
}

func NewNodeMapping(tree *BlockTree, cfg *RemapConfig) *NodeMapping {
	if cfg == nil {
		cfg = DefaultRemapConfig()
	}
	if cfg.CooldownInterval <= 0 {
		cfg.CooldownInterval = defaultCooldownInterval
	}
	if cfg.WeightStep <= 0 || cfg.WeightStep >= 1.0 {
		cfg.WeightStep = defaultWeightStep
	}
	if cfg.MinWeight <= 0 || cfg.MinWeight >= 0.5 {
		cfg.MinWeight = defaultMinWeight
	}
	if cfg.MinTrafficForRemap <= 0 {
		cfg.MinTrafficForRemap = defaultMinTrafficForRemap
	}
	if cfg.MaxTargets <= 1 {
		cfg.MaxTargets = defaultMaxTargets
	}
	if cfg.OverloadStddevFactor <= 0 {
		cfg.OverloadStddevFactor = defaultOverloadStddevFactor
	}
	if cfg.OverloadAbsCount <= 0 {
		cfg.OverloadAbsCount = defaultOverloadAbsCount
	}
	if cfg.LoadSensitivity < 0 {
		cfg.LoadSensitivity = defaultLoadSensitivity
	}

	nm := &NodeMapping{
		tree:             tree,
		config:           cfg,
		rng:              rand.New(rand.NewSource(cfg.RandSeed)),
		states:           make(map[int64]*nodeMappingState),
		ExpandDecider:    defaultExpandDecider,
		ContractDecider:  defaultContractDecider,
		RebalanceDecider: defaultRebalanceDecider,
	}

	rootState := &nodeMappingState{
		targets:   []WeightedTarget{{InstanceID: 0, Weight: 1.0}},
		cooldowns: make(map[int]*instanceCooldown),
	}
	nm.states[tree.Root().ID()] = rootState

	tree.SetOnNodesEvicted(nm.OnNodesEvicted)

	klog.InfoS("node_mapping_initialized",
		"cooldown", cfg.CooldownInterval,
		"overload_stddev_factor", cfg.OverloadStddevFactor,
		"overload_abs_count", cfg.OverloadAbsCount,
		"weight_step", cfg.WeightStep,
		"min_weight", cfg.MinWeight,
		"min_traffic", cfg.MinTrafficForRemap,
		"max_targets", cfg.MaxTargets,
		"load_sensitivity", cfg.LoadSensitivity,
	)
	return nm
}

func (m *NodeMapping) ensureMappingForPath(node *BlockNode) {
	if node == nil {
		return
	}
	path := make([]*BlockNode, 0, 8)
	for n := node; n != nil; n = n.Parent() {
		path = append(path, n)
	}
	for i := len(path) - 1; i >= 0; i-- {
		n := path[i]
		if _, ok := m.states[n.ID()]; ok {
			continue
		}
		var parentTargets []WeightedTarget
		if n.Parent() != nil {
			if pState, ok := m.states[n.Parent().ID()]; ok {
				parentTargets = copyTargets(pState.targets)
			}
		}
		if parentTargets == nil {
			parentTargets = []WeightedTarget{{InstanceID: 0, Weight: 1.0}}
		}
		m.states[n.ID()] = &nodeMappingState{
			targets:   parentTargets,
			cooldowns: make(map[int]*instanceCooldown),
		}
	}
}

func (m *NodeMapping) cleanupExpiredCooldowns(state *nodeMappingState, now time.Time) {
	for id, cd := range state.cooldowns {
		if now.After(cd.expiresAt) {
			delete(state.cooldowns, id)
		}
	}
}

func (m *NodeMapping) OnNodesEvicted(nodeIDs []int64) {
	if len(nodeIDs) == 0 {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, id := range nodeIDs {
		delete(m.states, id)
	}
	klog.V(5).InfoS("node_mapping_cleanup_evicted",
		"evicted_count", len(nodeIDs),
		"remaining_state_count", len(m.states),
	)
}

func (m *NodeMapping) isCoolingDown(state *nodeMappingState, instanceID int, now time.Time) bool {
	cd, ok := state.cooldowns[instanceID]
	if !ok {
		return false
	}
	if now.After(cd.expiresAt) {
		delete(state.cooldowns, instanceID)
		return false
	}
	return true
}

func copyTargets(src []WeightedTarget) []WeightedTarget {
	dst := make([]WeightedTarget, len(src))
	copy(dst, src)
	return dst
}

func (m *NodeMapping) GetTargets(node *BlockNode) []WeightedTarget {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.states[node.ID()]
	if !ok {
		return nil
	}
	return copyTargets(state.targets)
}

func (m *NodeMapping) SelectInstance(node *BlockNode, availableInstances []int, loads map[int]float64) int {
	if len(availableInstances) == 0 {
		return 0
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.ensureMappingForPath(node)
	state, ok := m.states[node.ID()]
	if !ok || len(state.targets) == 0 {
		state = &nodeMappingState{
			targets:   []WeightedTarget{{InstanceID: availableInstances[0], Weight: 1.0}},
			cooldowns: make(map[int]*instanceCooldown),
		}
		m.states[node.ID()] = state
	}

	targets := filterAvailable(state.targets, availableInstances)
	if len(targets) == 0 {
		targets = []WeightedTarget{{InstanceID: availableInstances[m.rng.Intn(len(availableInstances))], Weight: 1.0}}
		state.targets = targets
	}
	state.targets = normalizeWeights(targets)

	now := time.Now()
	nodeHits := node.WindowHits()
	hasEnoughTraffic := nodeHits >= m.config.MinTrafficForRemap

	m.cleanupExpiredCooldowns(state, now)

	if hasEnoughTraffic && len(availableInstances) > 1 {
		// action := m.evaluateRemap(node, state, availableInstances, loads, now)
		// if action == NeedsExpand {
		// 	m.doExpand(node, state, availableInstances, loads, now)
		// } else if action == NeedsContract {
		// 	m.doContract(node, state, loads, now)
		// } else if action == NeedsRebalance {
		// 	m.doRebalance(node, state, loads, now)
		// }
		m.doExpand(node, state, availableInstances, loads, now)
		m.doContract(node, state, loads, now)
	}
	selected := m.pickWeighted(state.targets, loads)
	klog.InfoS("node_mapping_select_instance",
		"node_id", node.ID(),
		"available_instances", availableInstances,
		"targets", state.targets,
		"loads", loads,
		"selected_instance", selected,
	)
	return selected
}

func (m *NodeMapping) evaluateRemap(node *BlockNode, state *nodeMappingState, allInstances []int, loads map[int]float64, now time.Time) TriggerAction {
	if m.RebalanceDecider != nil {
		if shouldRebalance, _, _ := m.RebalanceDecider(node, state.targets, loads, state.cooldowns, now, m.config); shouldRebalance {
			return NeedsRebalance
		}
	}
	if m.ExpandDecider != nil {
		if shouldExpand, _, _ := m.ExpandDecider(node, state.targets, allInstances, loads, state.cooldowns, now, m.config); shouldExpand {
			if len(state.targets) < m.config.MaxTargets {
				return NeedsExpand
			}
		}
	}
	if m.ContractDecider != nil {
		if shouldContract, _ := m.ContractDecider(node, state.targets, loads, state.cooldowns, now, m.config); shouldContract {
			if len(state.targets) > 1 {
				return NeedsContract
			}
		}
	}
	return NoAction
}

func defaultExpandDecider(node *BlockNode, targets []WeightedTarget, allInstances []int, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (bool, int, int) {
	if len(targets) == 0 || len(allInstances) <= 1 {
		return false, -1, -1
	}

	_, targetActiveIdx := collectActiveLoads(targets, loads, cooldowns, now)
	if len(targetActiveIdx) == 0 {
		return false, -1, -1
	}

	globalLoads := make([]float64, 0, len(allInstances))
	for _, id := range allInstances {
		if cd, ok := cooldowns[id]; ok && now.Before(cd.expiresAt) {
			continue
		}
		globalLoads = append(globalLoads, loads[id])
	}
	if len(globalLoads) < 2 {
		return false, -1, -1
	}
	stats := computeLoadStats(globalLoads)

	overloadedIdx := -1
	maxLoad := -math.MaxFloat64
	for _, i := range targetActiveIdx {
		l := loads[targets[i].InstanceID]
		if isOverloaded(l, stats, cfg) && l > maxLoad {
			maxLoad = l
			overloadedIdx = i
		}
	}

	if overloadedIdx < 0 {
		return false, -1, -1
	}

	targetSet := make(map[int]struct{}, len(targets))
	for _, t := range targets {
		targetSet[t.InstanceID] = struct{}{}
	}

	bestExpandTo := -1
	bestLoad := math.MaxFloat64
	for _, id := range allInstances {
		if _, ok := targetSet[id]; ok {
			continue
		}
		if cd, ok := cooldowns[id]; ok && now.Before(cd.expiresAt) {
			continue
		}
		l := loads[id]
		if l < bestLoad {
			bestLoad = l
			bestExpandTo = id
		}
	}

	if bestExpandTo < 0 {
		return false, -1, -1
	}

	if bestLoad >= maxLoad {
		return false, -1, -1
	}

	return true, bestExpandTo, overloadedIdx
}

func defaultContractDecider(node *BlockNode, targets []WeightedTarget, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (bool, int) {
	if len(targets) <= 1 {
		return false, -1
	}

	activeLoads, activeIdx := collectActiveLoads(targets, loads, cooldowns, now)
	if len(activeLoads) < 2 {
		return false, -1
	}
	stats := computeLoadStats(activeLoads)

	hasLowWeight := false
	lowestWeightIdx := -1
	lowestWeight := math.MaxFloat64
	underUtilizedIdx := -1
	underUtilizedLoad := math.MaxFloat64

	for _, i := range activeIdx {
		t := targets[i]
		l := loads[t.InstanceID]

		if t.Weight < lowestWeight {
			lowestWeight = t.Weight
			lowestWeightIdx = i
		}
		if t.Weight <= cfg.MinWeight+1e-9 {
			hasLowWeight = true
		}
		if isUnderloaded(l, stats, cfg) && l < underUtilizedLoad {
			underUtilizedLoad = l
			underUtilizedIdx = i
		}
	}

	if hasLowWeight && lowestWeightIdx >= 0 {
		return true, lowestWeightIdx
	}

	if underUtilizedIdx >= 0 {
		return true, underUtilizedIdx
	}

	return false, -1
}

func defaultRebalanceDecider(node *BlockNode, targets []WeightedTarget, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time, cfg *RemapConfig) (bool, int, int) {
	if len(targets) < 2 {
		return false, -1, -1
	}

	activeLoads, activeIdx := collectActiveLoads(targets, loads, cooldowns, now)
	if len(activeLoads) < 2 {
		return false, -1, -1
	}
	stats := computeLoadStats(activeLoads)

	fromIdx := -1
	maxLoad := -math.MaxFloat64
	toIdx := -1
	minLoad := math.MaxFloat64

	for _, i := range activeIdx {
		t := targets[i]
		l := loads[t.InstanceID]
		if isOverloaded(l, stats, cfg) && l > maxLoad {
			maxLoad = l
			fromIdx = i
		}
		if isUnderloaded(l, stats, cfg) && l < minLoad {
			minLoad = l
			toIdx = i
		}
	}

	if fromIdx >= 0 && toIdx >= 0 && fromIdx != toIdx {
		return true, fromIdx, toIdx
	}

	return false, -1, -1
}

func (m *NodeMapping) doExpand(node *BlockNode, state *nodeMappingState, allInstances []int, loads map[int]float64, now time.Time) {
	should, expandTo, donorIdx := m.ExpandDecider(node, state.targets, allInstances, loads, state.cooldowns, now, m.config)
	if !should || expandTo < 0 || donorIdx < 0 || donorIdx >= len(state.targets) {
		return
	}
	if len(state.targets) >= m.config.MaxTargets {
		return
	}

	step := m.config.WeightStep
	newTargets := make([]WeightedTarget, len(state.targets))
	copy(newTargets, state.targets)

	shiftWeight := step * newTargets[donorIdx].Weight
	newTargets[donorIdx].Weight -= shiftWeight
	newTargets = append(newTargets, WeightedTarget{InstanceID: expandTo, Weight: shiftWeight})

	state.targets = normalizeWeights(newTargets)
	state.cooldowns[expandTo] = &instanceCooldown{
		expiresAt: now.Add(m.config.CooldownInterval),
	}
	state.remapCount++

	klog.V(4).InfoS("node_mapping_expand",
		"node_id", node.ID(),
		"node_depth", node.Depth(),
		"node_hits", node.WindowHits(),
		"targets", state.targets,
		"expand_to", expandTo,
		"donor_idx", donorIdx,
		"donor_instance", state.targets[donorIdx].InstanceID,
		"cooldown_expires", state.cooldowns[expandTo].expiresAt,
		"remap_count", state.remapCount,
	)
}

func (m *NodeMapping) distributeWeight(targets []WeightedTarget, removedIdx int, removedWeight float64, loads map[int]float64, cooldowns map[int]*instanceCooldown, now time.Time) []WeightedTarget {
	if len(targets) <= 1 {
		if len(targets) == 1 {
			targets[0].Weight += removedWeight
		}
		return targets
	}

	type candidate struct {
		idx      int
		instLoad float64
	}
	candidates := make([]candidate, 0, len(targets)-1)
	var totalInverseLoad float64

	for i := range targets {
		if i == removedIdx {
			continue
		}
		t := targets[i]
		if cd, ok := cooldowns[t.InstanceID]; ok && now.Before(cd.expiresAt) {
			continue
		}
		l := loads[t.InstanceID]
		adjustedLoad := math.Max(l, 0.01)
		inverseLoad := 1.0 / adjustedLoad
		candidates = append(candidates, candidate{idx: i, instLoad: adjustedLoad})
		totalInverseLoad += inverseLoad
	}

	if len(candidates) == 0 {
		targets[0].Weight += removedWeight
		return targets
	}

	for _, c := range candidates {
		inverseLoad := 1.0 / c.instLoad
		share := (inverseLoad / totalInverseLoad) * removedWeight
		targets[c.idx].Weight += share
	}

	return targets
}

func (m *NodeMapping) doContract(node *BlockNode, state *nodeMappingState, loads map[int]float64, now time.Time) {
	should, contractIdx := m.ContractDecider(node, state.targets, loads, state.cooldowns, now, m.config)
	if !should || contractIdx < 0 || contractIdx >= len(state.targets) {
		return
	}
	if len(state.targets) <= 1 {
		return
	}

	removedInstance := state.targets[contractIdx].InstanceID
	removedWeight := state.targets[contractIdx].Weight

	newTargets := make([]WeightedTarget, 0, len(state.targets)-1)
	newTargets = append(newTargets, state.targets[:contractIdx]...)
	newTargets = append(newTargets, state.targets[contractIdx+1:]...)

	newTargets = m.distributeWeight(newTargets, -1, removedWeight, loads, state.cooldowns, now)

	state.targets = normalizeWeights(newTargets)
	state.cooldowns[removedInstance] = &instanceCooldown{
		expiresAt: now.Add(m.config.CooldownInterval),
	}
	state.remapCount++

	klog.V(4).InfoS("node_mapping_contract",
		"node_id", node.ID(),
		"node_depth", node.Depth(),
		"node_hits", node.WindowHits(),
		"targets", state.targets,
		"removed_instance", removedInstance,
		"removed_idx", contractIdx,
		"cooldown_expires", state.cooldowns[removedInstance].expiresAt,
		"remap_count", state.remapCount,
	)
}

func (m *NodeMapping) doRebalance(node *BlockNode, state *nodeMappingState, loads map[int]float64, now time.Time) {
	should, fromIdx, toIdx := m.RebalanceDecider(node, state.targets, loads, state.cooldowns, now, m.config)
	if !should || fromIdx < 0 || toIdx < 0 || fromIdx >= len(state.targets) || toIdx >= len(state.targets) || fromIdx == toIdx {
		return
	}

	step := m.config.WeightStep
	shiftWeight := step * state.targets[fromIdx].Weight
	if shiftWeight >= state.targets[fromIdx].Weight {
		shiftWeight = state.targets[fromIdx].Weight * 0.5
	}

	state.targets[fromIdx].Weight -= shiftWeight
	state.targets[toIdx].Weight += shiftWeight

	state.targets = normalizeWeights(state.targets)
	state.remapCount++

	klog.V(4).InfoS("node_mapping_rebalance",
		"node_id", node.ID(),
		"node_depth", node.Depth(),
		"node_hits", node.WindowHits(),
		"targets", state.targets,
		"from_idx", fromIdx,
		"from_instance", state.targets[fromIdx].InstanceID,
		"to_idx", toIdx,
		"to_instance", state.targets[toIdx].InstanceID,
		"shift_weight", shiftWeight,
		"remap_count", state.remapCount,
	)
}

func (m *NodeMapping) pickWeighted(targets []WeightedTarget, loads map[int]float64) int {
	if len(targets) == 0 {
		return 0
	}
	if len(targets) == 1 {
		return targets[0].InstanceID
	}

	k := m.config.LoadSensitivity
	effective := make([]float64, len(targets))
	var total float64
	for i, t := range targets {
		load := loads[t.InstanceID]
		eff := t.Weight / (1.0 + k*load)
		effective[i] = eff
		total += eff
	}

	if total <= 0 {
		return targets[m.rng.Intn(len(targets))].InstanceID
	}

	r := m.rng.Float64() * total
	var cumulative float64
	for i, eff := range effective {
		cumulative += eff
		if r < cumulative {
			return targets[i].InstanceID
		}
	}
	return targets[len(targets)-1].InstanceID
}

func filterAvailable(targets []WeightedTarget, available []int) []WeightedTarget {
	availSet := make(map[int]struct{}, len(available))
	for _, id := range available {
		availSet[id] = struct{}{}
	}
	result := make([]WeightedTarget, 0, len(targets))
	for _, t := range targets {
		if _, ok := availSet[t.InstanceID]; ok {
			result = append(result, t)
		}
	}
	return result
}

func normalizeWeights(targets []WeightedTarget) []WeightedTarget {
	if len(targets) == 0 {
		return targets
	}
	sort.SliceStable(targets, func(i, j int) bool {
		return targets[i].Weight > targets[j].Weight
	})
	total := 0.0
	for _, t := range targets {
		total += t.Weight
	}
	if total <= 0 {
		for i := range targets {
			targets[i].Weight = 1.0 / float64(len(targets))
		}
		return targets
	}
	for i := range targets {
		targets[i].Weight /= total
	}
	return targets
}

func (m *NodeMapping) NodeCount() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.states)
}

func (m *NodeMapping) RemapCount(nodeID int64) int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	state, ok := m.states[nodeID]
	if !ok {
		return 0
	}
	return state.remapCount
}
