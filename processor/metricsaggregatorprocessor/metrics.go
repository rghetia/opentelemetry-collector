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
	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"

	"github.com/open-telemetry/opentelemetry-service/internal/collector/telemetry"
	"github.com/open-telemetry/opentelemetry-service/processor"
)

var (
	statAggregationMetricsCacheSize = stats.Int64("aggregation_metrics_cache_size", "Size of metrics aggregation cache", stats.UnitDimensionless)
	statAggregationMetricsCacheAdded = stats.Int64("aggregation_metrics_cache_added", "# of metrics added to aggregation cache", stats.UnitDimensionless)
	statAggregationMetricsCacheRemoved = stats.Int64("aggregation_metrics_cache_removed", "# of metrics removed to aggregation cache", stats.UnitDimensionless)
)

// MetricViews returns the metrics views related to aggregation
func MetricViews(level telemetry.Level) []*view.View {
	if level == telemetry.None {
		return nil
	}

	tagKeys := processor.MetricTagKeys(level)
	if tagKeys == nil {
		return nil
	}

	exporterTagKeys := []tag.Key{processor.TagExporterNameKey}

	statAggregationMetricsCacheSizeView := &view.View{
		Name:        statAggregationMetricsCacheSize.Name(),
		Measure:     statAggregationMetricsCacheSize,
		Description: statAggregationMetricsCacheSize.Description(),
		TagKeys:     exporterTagKeys,
		Aggregation: view.LastValue(),
	}

	statAggregationMetricsCacheAddedView := &view.View{
		Name:        statAggregationMetricsCacheAdded.Name(),
		Measure:     statAggregationMetricsCacheAdded,
		Description: statAggregationMetricsCacheAdded.Description(),
		TagKeys:     exporterTagKeys,
		Aggregation: view.Sum(),
	}

	statAggregationMetricsCacheRemovedView := &view.View{
		Name:        statAggregationMetricsCacheRemoved.Name(),
		Measure:     statAggregationMetricsCacheRemoved,
		Description: statAggregationMetricsCacheRemoved.Description(),
		TagKeys:     exporterTagKeys,
		Aggregation: view.Sum(),
	}

	return []*view.View{
		statAggregationMetricsCacheSizeView,
		statAggregationMetricsCacheAddedView,
		statAggregationMetricsCacheRemovedView,
	}
}
