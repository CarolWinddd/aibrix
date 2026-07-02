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
	"sync"
	"sync/atomic"

	"github.com/vllm-project/aibrix/pkg/types"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog/v2"
)

// LMetricTokensTracker tracks pending prefill tokens per pod.
// It is used by the l-metric routing algorithm to calculate the score:
// Score = L-Metric × BS
// where L-Metric is the total pending prefill tokens for the pod.
//
// All methods are safe for concurrent use.
type LMetricTokensTracker struct {
	podTokenCounts     sync.Map // map[string]*atomic.Int64
	requestToPodTokens sync.Map // map[string]podTokens
}

type podTokens struct {
	podName string
	tokens  int64
}

// NewLMetricTokensTracker creates a new, empty LMetricTokensTracker.
func NewLMetricTokensTracker() *LMetricTokensTracker {
	return &LMetricTokensTracker{}
}

// AddPendingPrefillTokens records that requestID has been dispatched to podName with
// the given number of new prefill tokens. Must be paired with a corresponding
// RemovePendingPrefillTokens call.
func (t *LMetricTokensTracker) AddPendingPrefillTokens(requestID, podName string, tokens int64) {
	countInterface, _ := t.podTokenCounts.LoadOrStore(podName, &atomic.Int64{})
	count := countInterface.(*atomic.Int64)

	newCount := count.Add(tokens)
	t.requestToPodTokens.Store(requestID, podTokens{podName: podName, tokens: tokens})

	klog.V(4).InfoS("l_metric_tokens_added",
		"request_id", requestID,
		"pod_name", podName,
		"added_tokens", tokens,
		"new_total", newCount)
}

// RemovePendingPrefillTokens decrements the pending token counter for the pod
// assigned to requestID and removes the request-to-pod mapping.
func (t *LMetricTokensTracker) RemovePendingPrefillTokens(requestID string) {
	valInterface, exists := t.requestToPodTokens.LoadAndDelete(requestID)
	if !exists {
		klog.V(4).InfoS("l_metric_tokens_not_found_for_removal", "request_id", requestID)
		return
	}

	pt := valInterface.(podTokens)
	countInterface, exists := t.podTokenCounts.Load(pt.podName)
	if !exists {
		klog.V(4).InfoS("l_metric_pod_counter_not_found", "pod_name", pt.podName, "request_id", requestID)
		return
	}

	count := countInterface.(*atomic.Int64)
	newCount := count.Add(-pt.tokens)

	if newCount < 0 {
		for {
			v := count.Load()
			if v >= 0 {
				break
			}
			if count.CompareAndSwap(v, 0) {
				break
			}
		}
	}

	klog.V(4).InfoS("l_metric_tokens_removed",
		"request_id", requestID,
		"pod_name", pt.podName,
		"removed_tokens", pt.tokens,
		"new_total", newCount)
}

// GetPendingPrefillTokensForPods returns a map of pod name → pending prefill
// token count for each pod in pods. Pods with no recorded tokens are included
// with a count of 0.
func (t *LMetricTokensTracker) GetPendingPrefillTokensForPods(pods []*v1.Pod) map[string]int64 {
	counts := make(map[string]int64)
	for _, pod := range pods {
		countInterface, exists := t.podTokenCounts.Load(pod.Name)
		if !exists {
			counts[pod.Name] = 0
		} else {
			counts[pod.Name] = countInterface.(*atomic.Int64).Load()
		}
	}
	return counts
}

// GetPendingPrefillTokensForPod returns the current pending prefill token
// count for podName, or 0 if no tokens have been recorded.
func (t *LMetricTokensTracker) GetPendingPrefillTokensForPod(podName string) int64 {
	countInterface, exists := t.podTokenCounts.Load(podName)
	if !exists {
		return 0
	}
	return countInterface.(*atomic.Int64).Load()
}

// === RequestTracker interface implementation ===
// Note: This tracker tracks pending prefill tokens per pod instead of request counts
// below implements RequestTracker interface
func (t *LMetricTokensTracker) AddRequestCount(ctx *types.RoutingContext, requestID string, modelName string) int64 {
	return 0
}

func (t *LMetricTokensTracker) DoneRequestCount(ctx *types.RoutingContext, requestID string, modelName string, traceTerm int64) {
	t.RemovePendingPrefillTokens(requestID)
}

func (t *LMetricTokensTracker) DoneRequestTrace(ctx *types.RoutingContext, requestID string, modelName string, inputTokens, outputTokens, traceTerm int64) {
	t.RemovePendingPrefillTokens(requestID)
}