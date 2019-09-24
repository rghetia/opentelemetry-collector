// Copyright 2019, OpenTelemetry Authors
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

package metricsaggregatorprocessor

import (
	"context"
	"os"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-service/consumer"
	"github.com/open-telemetry/opentelemetry-service/consumer/consumerdata"
	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	"github.com/open-telemetry/opentelemetry-service/processor/metricsaggregatorprocessor/internal"
)

const (
	defaultReportingInterval = 60 * time.Second
)

// aggregator is a component that accepts metrics, and aggregates them over ReportingInterval.
//
// aggregator implements consumer.MetricsConsumer
type aggregator struct {
	jobsMap *internal.JobsMap
	adjuster *internal.MetricsAdjuster
	sender consumer.MetricsConsumer
	name   string
	logger *zap.SugaredLogger
	node   *commonpb.Node

	reportingInterval  time.Duration
	dropResourceKeys   []string
	dropLabelKeys      []string
	dropResourceKeyMap map[string]bool
	dropLabelKeyMap    map[string]bool
	stopCh                   chan struct{}
	stopOnce                 sync.Once
}

var _ consumer.MetricsConsumer = (*aggregator)(nil)

// NewAggregator creates a new aggregator that batches spans by node and resource
func NewAggregator(name string, logger *zap.SugaredLogger, sender consumer.MetricsConsumer, opts ...Option) consumer.MetricsConsumer {
	// Init with defaults
	b := &aggregator{
		name:   name,
		sender: sender,
		logger: logger,
		node: &commonpb.Node{
			Identifier: &commonpb.ProcessIdentifier{
				Pid: uint32(os.Getpid()),
				HostName: func() string { h, _ := os.Hostname() ; return h}(),
			},
		},

		reportingInterval: defaultReportingInterval,
	}

	// Override with options
	for _, opt := range opts {
		opt(b)
	}

	b.jobsMap = internal.NewJobsMap(time.Duration(2 * time.Minute))
	b.adjuster = internal.NewMetricsAdjuster(b.jobsMap.Get("aggregator",  "1"), b.logger)

	b.dropResourceKeyMap = make(map[string]bool, len(b.dropResourceKeys))
	for _, k := range b.dropResourceKeys {
		b.dropResourceKeyMap[k] = true
	}

	b.dropLabelKeyMap = make(map[string]bool, len(b.dropLabelKeys))
	for _, k := range b.dropLabelKeys {
		b.dropLabelKeyMap[k] = true
	}

	//Start timer to export metrics
	ticker := time.NewTicker(60 * time.Second)
	go func(ctx context.Context) {
		defer ticker.Stop()
		for {
			select {
			case <-b.stopCh:
				return
			case <-ticker.C:
				metrics := b.adjuster.ExportTimeSeries()
				md := consumerdata.MetricsData{
					Metrics: metrics,
					Node: b.node,
				}
				sender.ConsumeMetricsData(ctx, md)
			}
		}
	}(context.Background())
	return b
}

// ConsumeTraceData implements aggregator as a SpanProcessor and takes the provided spans and adds them to
// batches
func (b *aggregator) ConsumeMetricsData(ctx context.Context, td consumerdata.MetricsData) error {
	b.adjuster.AdjustMetrics(b.dropResourceKeyMap, b.dropLabelKeyMap, td.Resource, td.Metrics)
	return nil
}
