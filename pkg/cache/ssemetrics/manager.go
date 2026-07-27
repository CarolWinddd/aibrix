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
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/klog/v2"

	"github.com/vllm-project/aibrix/pkg/utils"
)

type PodProvider interface {
	SSEGetPod(ctx context.Context, podKey string) (*PodInfo, bool)
	SSERangePods(ctx context.Context, f func(key string, pod *PodInfo) bool) error
}

type PodInfo struct {
	Name      string
	Namespace string
	PodIP     string
	ModelName string
	Labels    map[string]string
	Models    []string
}

type SSEMetricsCache interface {
	UpdateSSEMetrics(podKey string, output EngineStepOutput) error
}

type Manager struct {
	podProvider  PodProvider
	cache        SSEMetricsCache
	subscribers  utils.SyncMap[string, *SSEClient] // podKey -> SSEClient
	ctx          context.Context
	cancel       context.CancelFunc
	mu           sync.RWMutex
	stopped      bool
}

func NewManager(podProvider PodProvider, cache SSEMetricsCache) *Manager {
	if !Enabled() {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &Manager{
		podProvider: podProvider,
		cache:       cache,
		ctx:         ctx,
		cancel:      cancel,
	}
}

func (m *Manager) Start() error {
	klog.Info("Starting SSE metrics manager")

	if m.cache == nil {
		return fmt.Errorf("SSEMetricsCache is required but nil")
	}

	err := m.podProvider.SSERangePods(m.ctx, func(key string, podInfo *PodInfo) bool {
		if m.canSubscribeToPod(podInfo) {
			go func() {
				subCtx, cancel := context.WithTimeout(m.ctx, 5*time.Second)
				defer cancel()
				if err := m.subscribeToPod(subCtx, key, podInfo); err != nil {
					klog.Errorf("Failed to subscribe to pod %s: %v", key, err)
				}
			}()
		}
		return true
	})

	if err != nil {
		return fmt.Errorf("failed to process existing pods: %w", err)
	}

	klog.Info("SSE metrics manager started successfully")
	return nil
}

func (m *Manager) Stop() {
	m.mu.Lock()
	if m.stopped {
		m.mu.Unlock()
		return
	}
	m.stopped = true
	m.mu.Unlock()

	klog.Info("Stopping SSE metrics manager")

	if m.cancel != nil {
		m.cancel()
	}

	m.subscribers.Range(func(key string, client *SSEClient) bool {
		client.Stop()
		m.subscribers.Delete(key)
		return true
	})

	klog.Info("SSE metrics manager stopped")
}

func (m *Manager) SubscribeToPod(ctx context.Context, podKey string, podInfo *PodInfo) error {
	if !m.canSubscribeToPod(podInfo) {
		return fmt.Errorf("pod %s is not eligible for SSE metrics subscription", podKey)
	}

	return m.subscribeToPod(ctx, podKey, podInfo)
}

func (m *Manager) UnsubscribeFromPod(podKey string) {
	if client, ok := m.subscribers.LoadAndDelete(podKey); ok {
		client.Stop()
		klog.Infof("Unsubscribed from pod %s", podKey)
	}
}

func (m *Manager) canSubscribeToPod(podInfo *PodInfo) bool {
	if podInfo.PodIP == "" {
		return false
	}
	if podInfo.ModelName == "" && len(podInfo.Models) == 0 {
		return false
	}
	return true
}

func (m *Manager) subscribeToPod(ctx context.Context, podKey string, podInfo *PodInfo) error {
	if _, exists := m.subscribers.Load(podKey); exists {
		klog.V(2).Infof("Already subscribed to pod %s", podKey)
		return nil
	}

	modelName := podInfo.ModelName
	if modelName == "" && len(podInfo.Models) > 0 {
		modelName = podInfo.Models[0]
	}

	config := DefaultSSEClientConfig(podKey, podInfo.PodIP, modelName)
	handler := NewEventHandler(m.cache)
	client := NewSSEClient(config, handler)

	if err := client.Start(); err != nil {
		return fmt.Errorf("failed to start SSE client: %w", err)
	}

	m.subscribers.Store(podKey, client)
	klog.Infof("Subscribed to pod %s at %s", podKey, podInfo.PodIP)

	return nil
}