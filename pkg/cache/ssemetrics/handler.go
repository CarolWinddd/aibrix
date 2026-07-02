// Copyright 2025 The Aibrix Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ssemetrics

import (
	"k8s.io/klog/v2"
)

type EventHandler struct {
	cache SSEMetricsCache
}

func NewEventHandler(cache SSEMetricsCache) *EventHandler {
	return &EventHandler{
		cache: cache,
	}
}

func (h *EventHandler) HandleStepOutput(output EngineStepOutput) error {
	if h.cache == nil {
		klog.V(2).Info("SSEMetricsCache is nil, skipping metrics update")
		return nil
	}

	if err := h.cache.UpdateSSEMetrics(output.PodKey, output); err != nil {
		klog.Warningf("Failed to update SSE metrics for pod %s: %v", output.PodKey, err)
		return err
	}

	klog.V(4).Infof("Processed SSE metrics for pod %s, step %d, latency %dms",
		output.PodKey, output.StepID, output.Latency)

	return nil
}