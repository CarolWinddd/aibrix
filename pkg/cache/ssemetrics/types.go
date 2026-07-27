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
	"net"
	"time"

	"github.com/vllm-project/aibrix/pkg/utils"
)

const (
	DefaultMetricsPort        = 8000
	DefaultPollTimeout        = 100 * time.Millisecond
	DefaultReconnectInterval  = 1 * time.Second
	MaxReconnectInterval      = 30 * time.Second
	ReconnectBackoffFactor    = 2.0
	EventChannelBufferSize    = 1000
)

var (
	enabled bool
)

func init() {
	enabled = utils.LoadEnvBool("AIBRIX_ENABLE_SSE_METRICS", false)
}

func Enabled() bool {
	return enabled
}

type SSEClientConfig struct {
	PodKey         string
	PodIP          string
	ModelName      string
	MetricsPort    int
	PollTimeout    time.Duration
	ReconnectDelay time.Duration
}

func DefaultSSEClientConfig(podKey, podIP, modelName string) *SSEClientConfig {
	return &SSEClientConfig{
		PodKey:         podKey,
		PodIP:          podIP,
		ModelName:      modelName,
		MetricsPort:    DefaultMetricsPort,
		PollTimeout:    DefaultPollTimeout,
		ReconnectDelay: DefaultReconnectInterval,
	}
}

func ValidateConfig(config *SSEClientConfig) error {
	if config.PodIP == "" {
		return &ConfigError{msg: "pod IP is required"}
	}
	if ip := net.ParseIP(config.PodIP); ip == nil {
		return &ConfigError{msg: "invalid IP address: " + config.PodIP}
	}
	if config.MetricsPort <= 0 || config.MetricsPort > 65535 {
		return &ConfigError{msg: "invalid metrics port: " + string(rune(config.MetricsPort))}
	}
	return nil
}

type ConfigError struct {
	msg string
}

func (e *ConfigError) Error() string {
	return e.msg
}

type SSEEventHandler interface {
	HandleStepOutput(output EngineStepOutput) error
}

type RequestStepOutput struct {
	RequestID          uint64   `json:"request_id"`
	NewTokenIDs        []uint32 `json:"new_token_ids"`
	State              string   `json:"state"`
	IsFinished         bool     `json:"is_finished"`
	HitTokenCnt        uint64   `json:"hit_token_cnt"`
	PrevComputedTokens uint32   `json:"prev_computed_tokens"`
	TTFT               *float64 `json:"ttft,omitempty"`
}

type EngineStepOutput struct {
	StepID             uint64                   `json:"step_id"`
	Latency            uint64                   `json:"latency"`
	PrefillTokens      int                      `json:"prefill_tokens"`
	PrefillTokenBudget int                      `json:"prefill_token_budget"`
	Outputs            []RequestStepOutput      `json:"outputs"`
	NewBlockHashes     [][]uint64               `json:"new_block_hashes"`
	EvictedBlockHashes [][]uint64               `json:"evicted_block_hashes"`
	EvictedBlockIDs    []uint64                 `json:"evicted_block_ids"`
	CurUsedBlockIDs    map[string][]uint64      `json:"cur_used_block_ids"`
	NewBlockHashesIDs  map[string][]uint64      `json:"new_block_hashes_ids"`
	PreemptedIDs       []uint64                 `json:"preempted_ids"`
	AbortedRequests    []uint64                 `json:"aborted_requests"`

	PodKey   string `json:"-"`
	ModelName string `json:"-"`
}