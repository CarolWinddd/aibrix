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

package routingalgorithms

import (
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/plugins/gateway/algorithms/proposed"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	"github.com/vllm-project/aibrix/pkg/utils/prefixcacheindexer"
	"github.com/vllm-project/aibrix/pkg/utils/tokenizer"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const RouterRemap types.RoutingAlgorithm = "remap"

const (
	remapDefaultWindowSize           = 1200 * time.Second
	remapDefaultMaxNodes             = 200000
	remapDefaultHotRatio             = 0.01
	remapDefaultCooldown             = 5 * time.Second
	remapDefaultOverloadStddevFactor = 1
	remapDefaultOverloadAbsCount     = 8
	remapDefaultLoadSensitivity      = 1.0
)

var (
	remapCooldown 			  float64 = utils.LoadEnvFloat("AIBRIX_REMAP_COOLDOWN", remapDefaultCooldown.Seconds())
	remapOverloadStddevFactor int     = utils.LoadEnvInt("AIBRIX_REMAP_OVERLOAD_STDDEV_FACTOR", remapDefaultOverloadStddevFactor)
	remapOverloadAbsCount     int     = utils.LoadEnvInt("AIBRIX_REMAP_OVERLOAD_ABS_COUNT", remapDefaultOverloadAbsCount)
	remapLoadSensitivity      float64 = utils.LoadEnvFloat("AIBRIX_REMAP_LOAD_SENSITIVITY", remapDefaultLoadSensitivity)
)

func init() {
	Register(RouterRemap, NewRemapRouter)
}

type remapRouter struct {
	cache              cache.Cache
	tokenizer          tokenizer.Tokenizer
	tokenizerPool      TokenizerPoolInterface
	prefixCacheIndexer *prefixcacheindexer.PrefixHashTable
	tree               *proposed.BlockTree
	mapping            *proposed.NodeMapping

	mu            sync.Mutex
	rng           *rand.Rand
	nextInstID    int
	podNameToInst map[string]int
	instToPodName map[int]string
}

var (
	remapOnce     sync.Once
	remapInstance *remapRouter
)

func NewRemapRouter() (types.Router, error) {
	var err error
	remapOnce.Do(func() {
		remapInstance, err = newRemapRouterInner()
	})
	if err != nil {
		return nil, err
	}
	return remapInstance, nil
}

func newRemapRouterInner() (*remapRouter, error) {
	c, err := cache.Get()
	if err != nil {
		klog.Error("fail to get cache store in remap router")
		return nil, err
	}

	var useRemoteTokenizer = utils.LoadEnvBool(constants.EnvPrefixCacheUseRemoteTokenizer, false)

	treeCfg := proposed.DefaultConfig()
	treeCfg.WindowSize = remapDefaultWindowSize
	treeCfg.MaxNodes = remapDefaultMaxNodes
	treeCfg.HotRatio = remapDefaultHotRatio

	remapCfg := proposed.DefaultRemapConfig()
	remapCfg.CooldownInterval = remapDefaultCooldown
	remapCfg.OverloadStddevFactor = float64(remapOverloadStddevFactor)
	remapCfg.OverloadAbsCount = float64(remapOverloadAbsCount)
	remapCfg.LoadSensitivity = remapLoadSensitivity

	tree := proposed.NewBlockTree(treeCfg)
	mapping := proposed.NewNodeMapping(tree, remapCfg)

	var tokenizerObj tokenizer.Tokenizer
	var tokenizerPool *TokenizerPool

	if useRemoteTokenizer {
		poolConfig := TokenizerPoolConfig{
			EnableVLLMRemote:     true,
			EndpointTemplate:     utils.LoadEnv("AIBRIX_VLLM_TOKENIZER_ENDPOINT_TEMPLATE", "http://%s:8000"),
			HealthCheckPeriod:    utils.LoadEnvDuration("AIBRIX_TOKENIZER_HEALTH_CHECK_PERIOD", 30) * time.Second,
			TokenizerTTL:         utils.LoadEnvDuration("AIBRIX_TOKENIZER_TTL", 300) * time.Second,
			MaxTokenizersPerPool: utils.LoadEnvInt("AIBRIX_MAX_TOKENIZERS_PER_POOL", 100),
			DefaultTokenizer:     newTokenizer(),
			Timeout:              utils.LoadEnvDuration("AIBRIX_TOKENIZER_REQUEST_TIMEOUT", 5) * time.Second,
			ModelServiceMap:      make(map[string]string),
		}
		pool := NewTokenizerPool(poolConfig, c)
		tokenizerPool = pool
		tokenizerObj = &panicTokenizer{}
		klog.Info("RemapRouter: TokenizerPool initialized with remote tokenizer support")
	} else {
		tokenizerObj = newTokenizer()
	}

	r := &remapRouter{
		cache:              c,
		tokenizer:          tokenizerObj,
		tokenizerPool:      tokenizerPool,
		prefixCacheIndexer: prefixcacheindexer.GetSharedPrefixHashTable(),
		tree:               tree,
		mapping:            mapping,
		rng:                rand.New(rand.NewSource(time.Now().UnixNano())),
		podNameToInst:      make(map[string]int),
		instToPodName:      make(map[int]string),
	}

	klog.InfoS("remap_router_initialized",
		"window_size", treeCfg.WindowSize,
		"max_nodes", treeCfg.MaxNodes,
		"hot_ratio", treeCfg.HotRatio,
		"cooldown", remapCfg.CooldownInterval,
		"overload_stddev_factor", remapCfg.OverloadStddevFactor,
		"overload_abs_count", remapCfg.OverloadAbsCount,
		"load_sensitivity", remapCfg.LoadSensitivity,
	)
	return r, nil
}

func (r *remapRouter) getTokenizerForRequest(ctx *types.RoutingContext, readyPodList types.PodList) tokenizer.Tokenizer {
	if r.tokenizerPool != nil {
		return r.tokenizerPool.GetTokenizer(ctx.Model, readyPodList.All())
	}
	return r.tokenizer
}

func (r *remapRouter) Polarity() types.Polarity {
	return types.PolarityMost
}

func (r *remapRouter) reconcileInstances(pods []*v1.Pod) (available []int, instToPod map[int]*v1.Pod) {
	r.mu.Lock()
	defer r.mu.Unlock()

	currentSet := make(map[string]struct{}, len(pods))
	for _, pod := range pods {
		currentSet[pod.Name] = struct{}{}
	}

	for name := range r.podNameToInst {
		if _, ok := currentSet[name]; !ok {
			instID := r.podNameToInst[name]
			delete(r.podNameToInst, name)
			delete(r.instToPodName, instID)
		}
	}

	for _, pod := range pods {
		if _, exists := r.podNameToInst[pod.Name]; !exists {
			id := r.nextInstID
			r.nextInstID++
			r.podNameToInst[pod.Name] = id
			r.instToPodName[id] = pod.Name
		}
	}

	available = make([]int, 0, len(pods))
	instToPod = make(map[int]*v1.Pod, len(pods))

	for _, pod := range pods {
		instID := r.podNameToInst[pod.Name]
		available = append(available, instID)
		instToPod[instID] = pod
	}

	return
}

func (r *remapRouter) Route(ctx *types.RoutingContext, readyPodList types.PodList) (string, error) {
	pods := readyPodList.All()
	if len(pods) == 0 {
		return "", fmt.Errorf("no pods to forward request")
	}

	tokenizerToUse := r.getTokenizerForRequest(ctx, readyPodList)
	tokens, err := tokenizerToUse.TokenizeInputText(ctx.Message)
	if err != nil {
		return "", err
	}

	blockHashes := r.prefixCacheIndexer.GetPrefixHashes(tokens)

	matchResult := r.tree.GetPrefix(blockHashes)

	available, instToPod := r.reconcileInstances(pods)

	loads := make(map[int]float64, len(available))
	for _, instID := range available {
		pod := instToPod[instID]
		loads[instID] = r.getPodLoad(pod, ctx.Model)
	}

	selectedIdx := r.mapping.SelectInstance(matchResult.MatchNode, available, loads)
	targetPod := instToPod[selectedIdx]
	if targetPod == nil {
		targetPod = pods[r.rng.Intn(len(pods))]
	}

	klog.InfoS("remap_route_decision",
		"request_id", ctx.RequestID,
		"matched_blocks", matchResult.MatchedBlocks,
		"match_node_depth", matchResult.MatchNode.Depth(),
		"selected_instance", selectedIdx,
		"target_pod", targetPod.Name,
		"num_instances", len(available),
		"loads", loads,
	)

	ctx.SetTargetPod(targetPod)
	return ctx.TargetAddress(), nil
}

func (r *remapRouter) getPodLoad(pod *v1.Pod, modelName string) float64 {
	runningReq, err := r.cache.GetMetricValueByPod(pod.Name, pod.Namespace, metrics.RealtimeNumRequestsRunning)
	if err != nil {
		return 0.0
	}
	load := runningReq.GetSimpleValue()

	// normPending, err := r.cache.GetMetricValueByPod(pod.Name, pod.Namespace, metrics.RealtimeNormalizedPendings)
	// if err == nil {
	// 	load = math.Max(load, normPending.GetSimpleValue())
	// }

	waiting, err := r.cache.GetMetricValueByPodModel(pod.Name, pod.Namespace, modelName, metrics.NumRequestsWaiting)
	if err == nil {
		load = math.Max(load, waiting.GetSimpleValue())
	} else {
		klog.V(4).InfoS("failed to get waiting requests metric",
			"pod", pod.Name,
			"model", modelName,
			"error", err,
		)
	}

	return load
}

func (r *remapRouter) SubscribedMetrics() []string {
	return []string{
		// metrics.RealtimeNumRequestsRunning,
		// metrics.RealtimeNormalizedPendings,
		metrics.NumRequestsWaiting,
	}
}
