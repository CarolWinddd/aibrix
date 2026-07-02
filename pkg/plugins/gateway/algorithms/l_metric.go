/*
Copyright 2026 The Aibrix Team.

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
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/vllm-project/aibrix/pkg/cache"
	"github.com/vllm-project/aibrix/pkg/constants"
	"github.com/vllm-project/aibrix/pkg/metrics"
	"github.com/vllm-project/aibrix/pkg/types"
	"github.com/vllm-project/aibrix/pkg/utils"
	"github.com/vllm-project/aibrix/pkg/utils/prefixcacheindexer"
	"github.com/vllm-project/aibrix/pkg/utils/tokenizer"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

const RouterLMetric types.RoutingAlgorithm = "l-metric"

func init() {
	Register(RouterLMetric, NewLMetricRouter)
}

type lMetricRouter struct {
	cache              cache.Cache
	tokenizer          tokenizer.Tokenizer
	prefixCacheIndexer *prefixcacheindexer.PrefixHashTable
	tokensTracker      *LMetricTokensTracker
	tokenizerPool      TokenizerPoolInterface
}

func NewLMetricRouter() (types.Router, error) {
	c, err := cache.Get()
	if err != nil {
		klog.Error("fail to get cache store in l-metric router")
		return nil, err
	}

	var tokenizerObj tokenizer.Tokenizer
	var tokenizerPool *TokenizerPool

	useRemoteTokenizer := utils.LoadEnvBool(constants.EnvPrefixCacheUseRemoteTokenizer, false)

	if useRemoteTokenizer {
		poolConfig := TokenizerPoolConfig{
			EnableVLLMRemote:     true,
			EndpointTemplate:     utils.LoadEnv("AIBRIX_VLLM_TOKENIZER_ENDPOINT_TEMPLATE", "http://%s:8000"),
			HealthCheckPeriod:    utils.LoadEnvDuration("AIBRIX_TOKENIZER_HEALTH_CHECK_PERIOD", 30) * time.Second,
			TokenizerTTL:         utils.LoadEnvDuration("AIBRIX_TOKENIZER_TTL", 300) * time.Second,
			MaxTokenizersPerPool: utils.LoadEnvInt("AIBRIX_MAX_TOKENIZERS_PER_POOL", 100),
			Timeout:              utils.LoadEnvDuration("AIBRIX_TOKENIZER_REQUEST_TIMEOUT", 5) * time.Second,
			ModelServiceMap:      make(map[string]string),
		}

		tokenizerType := utils.LoadEnv(constants.EnvPrefixCacheTokenizerType, "character")
		var defaultTokenizer tokenizer.Tokenizer
		if tokenizerType == tokenizerTypeTiktoken {
			defaultTokenizer = tokenizer.NewTiktokenTokenizer()
		} else {
			defaultTokenizer = tokenizer.NewCharacterTokenizer()
		}
		poolConfig.DefaultTokenizer = defaultTokenizer

		pool := NewTokenizerPool(poolConfig, c)
		tokenizerPool = pool
		tokenizerObj = &panicTokenizer{}
	} else {
		tokenizerType := utils.LoadEnv(constants.EnvPrefixCacheTokenizerType, "character")
		if tokenizerType == tokenizerTypeTiktoken {
			tokenizerObj = tokenizer.NewTiktokenTokenizer()
		} else {
			tokenizerObj = tokenizer.NewCharacterTokenizer()
		}
	}

	tokensTracker := NewLMetricTokensTracker()

	lMetricRegisterTrackerOnce.Do(func() {
		c.RegisterRequestTracker(tokensTracker)
		klog.V(4).Info("Registered LMetricTokensTracker to cache")
	})

	router := &lMetricRouter{
		cache:              c,
		tokenizer:          tokenizerObj,
		prefixCacheIndexer: prefixcacheindexer.GetSharedPrefixHashTable(),
		tokensTracker:      tokensTracker,
	}

	if tokenizerPool != nil {
		router.tokenizerPool = tokenizerPool
	}

	return router, nil
}

var lMetricRegisterTrackerOnce sync.Once

func (r *lMetricRouter) Polarity() types.Polarity {
	return types.PolarityLeast
}

func (r *lMetricRouter) ScoreAll(ctx *types.RoutingContext, readyPodList types.PodList) ([]float64, []bool, error) {
	pods := readyPodList.All()
	scores := make([]float64, len(pods))
	scored := make([]bool, len(pods))

	tokenizerToUse := r.getTokenizerForRequest(ctx, readyPodList)
	tokens, err := tokenizerToUse.TokenizeInputText(ctx.Message)
	if err != nil {
		klog.ErrorS(err, "failed to tokenize input text", "request_id", ctx.RequestID)
		return nil, nil, err
	}
	totalTokens := len(tokens)

	readyPodsMap := map[string]struct{}{}
	for _, pod := range pods {
		readyPodsMap[pod.Name] = struct{}{}
	}
	matchedPods, _ := r.prefixCacheIndexer.MatchPrefix(tokens, ctx.Model, readyPodsMap)

	podRequestCounts := getRequestCounts(r.cache, pods)
	podPendingTokens := r.tokensTracker.GetPendingPrefillTokensForPods(pods)

	for i, pod := range pods {
		matchPercent := matchedPods[pod.Name]
		hitTokens := int64(totalTokens * matchPercent / 100)
		newRequestTokens := int64(totalTokens) - hitTokens

		lMetricValue := podPendingTokens[pod.Name] + newRequestTokens
		bs := float64(podRequestCounts[pod.Name] + 1)

		scores[i] = float64(lMetricValue) * bs
		scored[i] = true

		klog.V(4).InfoS("l_metric_score",
			"request_id", ctx.RequestID,
			"pod_name", pod.Name,
			"total_tokens", totalTokens,
			"hit_tokens", hitTokens,
			"new_request_tokens", newRequestTokens,
			"existing_pending_tokens", podPendingTokens[pod.Name],
			"l_metric", lMetricValue,
			"bs", bs,
			"score", scores[i])
	}

	return scores, scored, nil
}

func (r *lMetricRouter) Route(ctx *types.RoutingContext, readyPodList types.PodList) (string, error) {
	readyPods := readyPodList.All()
	if len(readyPods) == 0 {
		return "", errors.New("no ready pods for routing")
	}

	scores, scored, err := r.ScoreAll(ctx, readyPodList)
	if err != nil {
		return "", err
	}

	var targetPod *v1.Pod
	var targetPods []string
	minScore := math.MaxFloat64

	for i, pod := range readyPods {
		if !scored[i] {
			continue
		}

		if scores[i] < minScore {
			minScore = scores[i]
			targetPods = []string{pod.Name}
		} else if scores[i] == minScore {
			targetPods = append(targetPods, pod.Name)
		}
	}

	if len(targetPods) > 0 {
		targetPod, _ = utils.FilterPodByName(targetPods[rand.Intn(len(targetPods))], readyPods)
	}

	if targetPod == nil {
		targetPod, err = SelectRandomPodAsFallback(ctx, readyPods, rand.Intn)
		if err != nil {
			return "", err
		}
	}

	tokenizerToUse := r.getTokenizerForRequest(ctx, readyPodList)
	tokens, _ := tokenizerToUse.TokenizeInputText(ctx.Message)
	totalTokens := len(tokens)

	readyPodsMap := map[string]struct{}{}
	for _, pod := range readyPods {
		readyPodsMap[pod.Name] = struct{}{}
	}
	matchedPods, _ := r.prefixCacheIndexer.MatchPrefix(tokens, ctx.Model, readyPodsMap)
	matchPercent := matchedPods[targetPod.Name]
	hitTokens := int64(totalTokens * matchPercent / 100)
	newRequestTokens := int64(totalTokens) - hitTokens

	r.tokensTracker.AddPendingPrefillTokens(ctx.RequestID, targetPod.Name, newRequestTokens)

	ctx.SetTargetPod(targetPod)
	return ctx.TargetAddress(), nil
}

func (r *lMetricRouter) PostRouteUpdate(ctx *types.RoutingContext, readyPodList types.PodList, targetPod *v1.Pod) error {
	tokenizerToUse := r.getTokenizerForRequest(ctx, readyPodList)
	tokens, err := tokenizerToUse.TokenizeInputText(ctx.Message)
	if err != nil {
		return err
	}

	prefixHashes := r.prefixCacheIndexer.GetPrefixHashes(tokens)
	if len(prefixHashes) > 0 {
		r.prefixCacheIndexer.AddPrefix(prefixHashes, ctx.Model, targetPod.Name)
	}

	return nil
}

func (r *lMetricRouter) SubscribedMetrics() []string {
	return []string{
		metrics.RealtimeNumRequestsRunning,
	}
}

func (r *lMetricRouter) getTokenizerForRequest(ctx *types.RoutingContext, readyPodList types.PodList) tokenizer.Tokenizer {
	if r.tokenizerPool != nil {
		return r.tokenizerPool.GetTokenizer(ctx.Model, readyPodList.All())
	}
	return r.tokenizer
}