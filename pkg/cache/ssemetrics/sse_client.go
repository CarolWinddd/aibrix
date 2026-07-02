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
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"k8s.io/klog/v2"
)

type SSEClient struct {
	config       *SSEClientConfig
	eventHandler SSEEventHandler
	httpClient   *http.Client
	mu           sync.RWMutex
	connected    bool
	reconnectDelay time.Duration
	ctx          context.Context
	cancel       context.CancelFunc
	wg           sync.WaitGroup
}

func NewSSEClient(config *SSEClientConfig, handler SSEEventHandler) *SSEClient {
	ctx, cancel := context.WithCancel(context.Background())

	httpClient := &http.Client{
		Timeout: 0,
		Transport: &http.Transport{
			MaxIdleConns:        10,
			IdleConnTimeout:     30 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
	}

	return &SSEClient{
		config:        config,
		eventHandler:  handler,
		httpClient:    httpClient,
		reconnectDelay: config.ReconnectDelay,
		ctx:           ctx,
		cancel:        cancel,
	}
}

func formatSSEEndpoint(podIP string, port int) string {
	return fmt.Sprintf("http://%s:%d/v1/metrics", podIP, port)
}

func (c *SSEClient) Connect() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.connected {
		return nil
	}

	c.reconnectDelay = c.config.ReconnectDelay
	c.connected = true

	klog.Infof("SSE client connecting to %s", formatSSEEndpoint(c.config.PodIP, c.config.MetricsPort))
	return nil
}

func (c *SSEClient) Start() error {
	if err := c.Connect(); err != nil {
		return fmt.Errorf("initial connection failed: %w", err)
	}

	c.wg.Add(1)
	go c.eventLoop()

	klog.Infof("SSE client started for pod %s", c.config.PodKey)
	return nil
}

func (c *SSEClient) Stop() {
	c.mu.Lock()
	if !c.connected {
		c.mu.Unlock()
		return
	}
	c.connected = false
	c.mu.Unlock()

	if c.cancel != nil {
		c.cancel()
	}

	c.wg.Wait()
	klog.Infof("SSE client stopped for pod %s", c.config.PodKey)
}

func (c *SSEClient) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.connected
}

func (c *SSEClient) eventLoop() {
	defer c.wg.Done()

	for c.isConnected() {
		if err := c.connectAndStream(); err != nil {
			if !c.isConnected() {
				return
			}
			klog.Warningf("SSE stream error for pod %s: %v", c.config.PodKey, err)
			time.Sleep(c.reconnectDelay)
			c.reconnectDelay = time.Duration(float64(c.reconnectDelay) * ReconnectBackoffFactor)
			if c.reconnectDelay > MaxReconnectInterval {
				c.reconnectDelay = MaxReconnectInterval
			}
		}
	}
}

func (c *SSEClient) connectAndStream() error {
	url := formatSSEEndpoint(c.config.PodIP, c.config.MetricsPort)

	req, err := http.NewRequestWithContext(c.ctx, http.MethodGet, url, nil)
	if err != nil {
		return fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Connection", "keep-alive")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("HTTP request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}

	klog.V(2).Infof("SSE connection established for pod %s", c.config.PodKey)

	reader := bufio.NewReader(resp.Body)
	builder := &sseEventBuilder{}

	for c.isConnected() {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("SSE stream closed by server")
			}
			return fmt.Errorf("failed to read from stream: %w", err)
		}

		if err := builder.processLine(line); err != nil {
			klog.Warningf("SSE parse error for pod %s: %v", c.config.PodKey, err)
			continue
		}

		if builder.isComplete() {
			if err := c.handleEvent(builder); err != nil {
				klog.Warningf("Failed to handle SSE event for pod %s: %v", c.config.PodKey, err)
			}
			builder.reset()
		}
	}

	return nil
}

func (c *SSEClient) handleEvent(builder *sseEventBuilder) error {
	if builder.isHeartbeat() {
		return nil
	}

	var output EngineStepOutput
	if err := json.Unmarshal([]byte(builder.data), &output); err != nil {
		return fmt.Errorf("failed to unmarshal SSE data: %w", err)
	}

	output.PodKey = c.config.PodKey
	output.ModelName = c.config.ModelName

	return c.eventHandler.HandleStepOutput(output)
}

type sseEventBuilder struct {
	data string
}

func (b *sseEventBuilder) processLine(line string) error {
	line = line[:len(line)-1]
	if len(line) == 0 {
		return nil
	}

	if line[0] == ':' {
		return nil
	}

	const prefix = "data: "
	if len(line) >= len(prefix) && line[:len(prefix)] == prefix {
		if b.data != "" {
			b.data += "\n"
		}
		b.data += line[len(prefix):]
	}

	return nil
}

func (b *sseEventBuilder) isComplete() bool {
	return b.data != ""
}

func (b *sseEventBuilder) isHeartbeat() bool {
	return b.data == "heartbeat"
}

func (b *sseEventBuilder) reset() {
	b.data = ""
}