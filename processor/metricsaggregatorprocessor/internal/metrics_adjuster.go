// Copyright 2019 OpenTelemetry Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"fmt"
	"google.golang.org/genproto/googleapis/api/metric"
	"sort"
	"strings"
	"sync"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"github.com/golang/protobuf/ptypes/wrappers"
	"go.uber.org/zap"
)

// Notes on garbage collection (gc):
//
// Job-level gc:
// The Prometheus receiver will likely execute in a long running service whose lifetime may exceed
// the lifetimes of many of the jobs that it is collecting from. In order to keep the JobsMap from
// leaking memory for entries of no-longer existing jobs, the JobsMap needs to remove entries that
// haven't been accessed for a long period of time.
//
// Timeseries-level gc:
// Some jobs that the Prometheus receiver is collecting from may export timeseries based on metrics
// from other jobs (e.g. cAdvisor). In order to keep the timeseriesMap from leaking memory for entries
// of no-longer existing jobs, the timeseriesMap for each job needs to remove entries that haven't
// been accessed for a long period of time.
//
// The gc strategy uses a standard mark-and-sweep approach - each time a timeseriesMap is accessed,
// it is marked. Similarly, each time a timeseriesinfo is accessed, it is also marked.
//
// At the end of each JobsMap.get(), if the last time the JobsMap was gc'd exceeds the 'gcInterval',
// the JobsMap is locked and any timeseriesMaps that are unmarked are removed from the JobsMap
// otherwise the timeseriesMap is gc'd
//
// The gc for the timeseriesMap is straightforward - the map is locked and, for each timeseriesinfo
// in the map, if it has not been marked, it is removed otherwise it is unmarked.
//
// Alternative Strategies
// 1. If the job-level gc doesn't run often enough, or runs too often, a separate go routine can
//    be spawned at JobMap creation time that gc's at periodic intervals. This approach potentially
//    adds more contention and latency to each scrape so the current approach is used. Note that
//    the go routine will need to be cancelled upon StopMetricsReception().
// 2. If the gc of each timeseriesMap during the gc of the JobsMap causes too much contention,
//    the gc of timeseriesMaps can be moved to the end of MetricsAdjuster().AdjustMetrics(). This
//    approach requires adding 'lastGC' Time and (potentially) a gcInterval duration to
//    timeseriesMap so the current approach is used instead.

type aggResource struct {
	res       *resourcepb.Resource
	node      *commonpb.Node
	metricMap map[string]*aggMetric
}

type aggMetric struct {
	metric *metricspb.Metric
	keyMap map[string]bool // useful for checking if key is dropped or not
	tsMap  map[string]*metricspb.TimeSeries
}

var aggResMap = map[string]*aggResource{}

func getResSignature(resKeys []string, metric *metricspb.Metric) string {
	labelValues := make([]string, 0, len(resKeys))
	for _, k := range resKeys {
		labelValues = append(labelValues, metric.Resource.Labels[k])
	}
	return fmt.Sprintf("%s,%s", metric.Resource.Type, strings.Join(labelValues, ","))

}

func getLabelKeySignature(labelKeys []string, metric *metricspb.Metric) string {
	labelValues := make([]string, 0, len(labelKeys))
	for _, k := range labelKeys {
		labelValues = append(labelValues, metric.Resource.Labels[k])
	}
	return fmt.Sprintf("%s,%s", metric.GetMetricDescriptor().GetName(), strings.Join(labelValues, ","))

}

func getOrCreateAggRes(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) *aggResource {
	var resKeys []string
	var aggRes *aggResource
	if metric.Resource != nil{
		resKeys = make([]string, 0, len(metric.Resource.Labels))
		for key := range metric.Resource.Labels {
			if _, ok := dropResKeys[key] ; !ok {
				resKeys = append(resKeys, key)
			}
		}
		if len(resKeys) > 0 {
			sort.Strings(resKeys)
		}
		resSig := getResSignature(resKeys, metric)
		var ok bool
		aggRes, ok = aggResMap[resSig]
		if !ok {
			aggRes = &aggResource{
				res: &resourcepb.Resource{
					Type: metric.Resource.Type,
					Labels: make(map[string]string, len(resKeys)),
				},
			}
			for _, k := range resKeys {
				aggRes.res.Labels[k] = metric.Resource.Labels[k]
			}
			aggResMap[resSig] = aggRes
		}
	} else {
		aggRes, ok := aggResMap[""]
		if !ok {
			aggRes = &aggResource{
			}
			aggResMap[""] = aggRes
		}
	}
	return aggRes
}

func getOrCreateAggMetric(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) *aggMetric {
	var labelKeys []string
	var aggM *aggMetric

	// GetOrCreate Resource
	// GetOrCreate Metrics
	// GetOrCreate TimeSeries

	aggRes := getOrCreateAggRes(dropResKeys, dropLabelKeys, metric)

	lKeys := metric.GetMetricDescriptor().GetLabelKeys()
	if len(lKeys) > 0 {
		labelKeys = make([]string, 0, len(lKeys))
		for _, key := range lKeys {
			if _, ok := dropLabelKeys[key.Key]; !ok {
				labelKeys = append(labelKeys, key.Key)
			}
		}
		if len(labelKeys) > 0 {
			sort.Strings(labelKeys)
		}
	}
	labelSig := getLabelKeySignature(labelKeys, metric)
	var ok bool
	aggM, ok = aggRes.metricMap[labelSig]
	if !ok {
		aggM = &aggMetric{
			metric: &metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        metric.MetricDescriptor.Name,
					Description: metric.MetricDescriptor.Description,
				},
				Timeseries: []*metricspb.TimeSeries{},
			},
			tsMap: map[string]*metricspb.TimeSeries{},
			keyMap: make(map[string]bool, len(labelKeys)),
		}
		for _, k := range labelKeys {
			aggM.metric.MetricDescriptor.LabelKeys = append(aggM.metric.MetricDescriptor.LabelKeys, &metricspb.LabelKey{Key: k})
			aggM.keyMap[k] = true
		}
		aggRes.metricMap[labelSig] = aggM
	}
	return aggM
}

func getTsSig(labelValues []*metricspb.LabelValue) string {
	var values []string = make([]string, 0, len(labelValues))
	for _, lv := range labelValues {
		values = append(values, lv.Value)
	}
	return strings.Join(values, ",")
}

func copyTimeSeries(ts *metricspb.TimeSeries, labelValues []*metricspb.LabelValue) *metricspb.TimeSeries {
	return ts
}

func getOrCreateTimeSeries(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric, series *metricspb.TimeSeries) *metricspb.TimeSeries {
	var ts *metricspb.TimeSeries
	aggM := getOrCreateAggMetric(dropResKeys, dropLabelKeys, metric)
	var labelValues = make([]*metricspb.LabelValue, 0, len(aggM.keyMap))

	for i, k := range metric.MetricDescriptor.LabelKeys {
		if _, ok := aggM.keyMap[k.Key] ; ok {
			labelValues = append(labelValues, series.LabelValues[i])
		}
	}
	sig := getTsSig(labelValues)
	ts, ok := aggM.tsMap[sig]
	if !ok {
		ts = copyTimeSeries(series, labelValues)
	}
	return ts
}

// timeseriesinfo contains the information necessary to adjust from the initial point and to detect
// resets.
type timeseriesinfo struct {
	mark     bool
	initial  *metricspb.TimeSeries
	previous *metricspb.TimeSeries
	aggTs    *metricspb.TimeSeries
}

// timeseriesMap maps from a timeseries instance (metric * label values) to the timeseries info for
// the instance.
type timeseriesMap struct {
	sync.RWMutex
	mark   bool
	tsiMap map[string]*timeseriesinfo
}

// Get the timeseriesinfo for the timeseries associated with the metric and label values.
func (tsm *timeseriesMap) get(
	metric *metricspb.Metric, res *resourcepb.Resource, values []*metricspb.LabelValue) *timeseriesinfo {
	name := metric.GetMetricDescriptor().GetName()
	sig := getSigWithResAndLabels(name, res, values)
	tsi, ok := tsm.tsiMap[sig]
	if !ok {
		tsi = &timeseriesinfo{}
		tsm.tsiMap[sig] = tsi
	}
	tsm.mark = true
	tsi.mark = true
	return tsi
}

// Remove timeseries that have aged out.
func (tsm *timeseriesMap) gc() {
	tsm.Lock()
	defer tsm.Unlock()
	// this shouldn't happen under the current gc() strategy
	if !tsm.mark {
		return
	}
	for ts, tsi := range tsm.tsiMap {
		if !tsi.mark {
			delete(tsm.tsiMap, ts)
		} else {
			tsi.mark = false
		}
	}
	tsm.mark = false
}

func newTimeseriesMap() *timeseriesMap {
	return &timeseriesMap{mark: true, tsiMap: map[string]*timeseriesinfo{}}
}

func getSigWithResAndLabels(name string, res *resourcepb.Resource, values []*metricspb.LabelValue) string {
	labelValues := make([]string, 0, len(res.Labels)+len(values))
	for _, label := range res.Labels {
		if label != "" {
			labelValues = append(labelValues, label)
		}
	}
	for _, label := range values {
		if label.GetValue() != "" {
			labelValues = append(labelValues, label.GetValue())
		}
	}
	return fmt.Sprintf("%s,%s", name, strings.Join(labelValues, ","))
}

// Create a unique timeseries signature consisting of the metric name and label values.
func getTimeseriesSignature(name string, values []*metricspb.LabelValue) string {
	labelValues := make([]string, 0, len(values))
	for _, label := range values {
		if label.GetValue() != "" {
			labelValues = append(labelValues, label.GetValue())
		}
	}
	return fmt.Sprintf("%s,%s", name, strings.Join(labelValues, ","))
}

// JobsMap maps from a job instance to a map of timeseries instances for the job.
type JobsMap struct {
	sync.RWMutex
	gcInterval time.Duration
	lastGC     time.Time
	jobsMap    map[string]*timeseriesMap
	resMap     map[string]*aggResource
}

// NewJobsMap creates a new (empty) JobsMap.
func NewJobsMap(gcInterval time.Duration) *JobsMap {
	return &JobsMap{gcInterval: gcInterval, lastGC: time.Now(),
		jobsMap: make(map[string]*timeseriesMap),
		resMap:  make(map[string]*aggResource)}
}

// Remove jobs and timeseries that have aged out.
func (jm *JobsMap) gc() {
	jm.Lock()
	defer jm.Unlock()
	// once the structure is locked, confrim that gc() is still necessary
	if time.Since(jm.lastGC) > jm.gcInterval {
		for sig, tsm := range jm.jobsMap {
			if !tsm.mark {
				delete(jm.jobsMap, sig)
			} else {
				tsm.gc()
			}
		}
		jm.lastGC = time.Now()
	}
}

func (jm *JobsMap) maybeGC() {
	// speculatively check if gc() is necessary, recheck once the structure is locked
	if time.Since(jm.lastGC) > jm.gcInterval {
		go jm.gc()
	}
}

func (jm *JobsMap) get(job, instance string) *timeseriesMap {
	sig := job + ":" + instance
	jm.RLock()
	tsm, ok := jm.jobsMap[sig]
	jm.RUnlock()
	defer jm.maybeGC()
	if ok {
		return tsm
	}
	jm.Lock()
	defer jm.Unlock()
	tsm2, ok2 := jm.jobsMap[sig]
	if ok2 {
		return tsm2
	}
	tsm2 = newTimeseriesMap()
	jm.jobsMap[sig] = tsm2
	return tsm2
}

// MetricsAdjuster takes a map from a metric instance to the initial point in the metrics instance
// and provides AdjustMetrics, which takes a sequence of metrics and adjust their values based on
// the initial points.
type MetricsAdjuster struct {
	tsm    *timeseriesMap
	logger *zap.SugaredLogger
}

// NewMetricsAdjuster is a constructor for MetricsAdjuster.
func NewMetricsAdjuster(tsm *timeseriesMap, logger *zap.SugaredLogger) *MetricsAdjuster {
	return &MetricsAdjuster{
		tsm:    tsm,
		logger: logger,
	}
}

// AdjustMetrics takes a sequence of metrics and adjust their values based on the initial and
// previous points in the timeseriesMap. If the metric is the first point in the timeseries, or the
// timeseries has been reset, it is removed from the sequence and added to the the timeseriesMap.
func (ma *MetricsAdjuster) AdjustMetrics(dropResKeys, dropLabelKeys map[string]bool, res *resourcepb.Resource, metrics []*metricspb.Metric) []*metricspb.Metric {
	var adjusted = make([]*metricspb.Metric, 0, len(metrics))
	ma.tsm.Lock()
	defer ma.tsm.Unlock()
	for _, metric := range metrics {
		//TODO(rghetia): assign global resource as per metric resource. Optimize this later if needed.
		if metric.Resource == nil {
			metric.Resource = res
		}
		if ma.adjustMetric(dropResKeys, dropLabelKeys, metric) {
			adjusted = append(adjusted, metric)
		}
	}
	return adjusted
}

// Returns true if at least one of the metric's timeseries was adjusted and false if all of the
// timeseries are an initial occurrence or a reset.
//
// Types of metrics returned supported by prometheus:
// - MetricDescriptor_GAUGE_DOUBLE
// - MetricDescriptor_GAUGE_DISTRIBUTION
// - MetricDescriptor_CUMULATIVE_DOUBLE
// - MetricDescriptor_CUMULATIVE_DISTRIBUTION
// - MetricDescriptor_SUMMARY
func (ma *MetricsAdjuster) adjustMetric(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) bool {
	switch metric.MetricDescriptor.Type {
	case metricspb.MetricDescriptor_GAUGE_DOUBLE, metricspb.MetricDescriptor_GAUGE_DISTRIBUTION:
		// gauges don't need to be adjusted so no additional processing is necessary
		return true
	default:
		return ma.adjustMetricTimeseries(dropResKeys, dropLabelKeys, metric)
	}
}

// Returns true if at least one of the metric's timeseries was adjusted and false if all of the
// timeseries are an initial occurrence or a reset.
func (ma *MetricsAdjuster) adjustMetricTimeseries(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) bool {
	filtered := make([]*metricspb.TimeSeries, 0, len(metric.GetTimeseries()))
	for _, current := range metric.GetTimeseries() {
		tsi := ma.tsm.get(metric, metric.Resource, current.GetLabelValues())
		if tsi.initial == nil {
			tsi.aggTs = getOrCreateTimeSeries(dropResKeys, dropLabelKeys, metric, current)
			// initial timeseries
			tsi.initial = current
			tsi.previous = current

		} else {
			if ma.adjustTimeseries(metric.MetricDescriptor.Type, current, tsi.initial,
				tsi.previous) {
				tsi.previous = current
				filtered = append(filtered, current)
			} else {
				// reset timeseries
				tsi.initial = current
				tsi.previous = current
			}
		}
	}
	metric.Timeseries = filtered
	return len(filtered) > 0
}

// Returns true if 'current' was adjusted and false if 'current' is an the initial occurrence or a
// reset of the timeseries.
func (ma *MetricsAdjuster) adjustTimeseries(metricType metricspb.MetricDescriptor_Type,
	current, initial, previous *metricspb.TimeSeries) bool {
	if !ma.adjustPoints(
		metricType, current.GetPoints(), initial.GetPoints(), previous.GetPoints()) {
		return false
	}
	current.StartTimestamp = initial.StartTimestamp
	return true
}

func (ma *MetricsAdjuster) adjustPoints(metricType metricspb.MetricDescriptor_Type,
	current, initial, previous []*metricspb.Point) bool {
	if len(current) != 1 || len(initial) != 1 || len(current) != 1 {
		ma.logger.Infof(
			"len(current): %v, len(initial): %v, len(previous): %v should all be 1",
			len(current), len(initial), len(previous))
		return true
	}
	return ma.adjustPoint(metricType, current[0], initial[0], previous[0])
}

// Note: There is an important, subtle point here. When a new timeseries or a reset is detected,
// current and initial are the same object. When initial == previous, the previous value/count/sum
// are all the initial value. When initial != previous, the previous value/count/sum has been
// adjusted wrt the initial value so both they must be combined to find the actual previous
// value/count/sum. This happens because the timeseries are updated in-place - if new copies of the
// timeseries were created instead, previous could be used directly but this would mean reallocating
// all of the metrics.
func (ma *MetricsAdjuster) adjustPoint(metricType metricspb.MetricDescriptor_Type,
	current, initial, previous *metricspb.Point) bool {
	switch metricType {
	case metricspb.MetricDescriptor_CUMULATIVE_DOUBLE:
		currentValue := current.GetDoubleValue()
		initialValue := initial.GetDoubleValue()
		previousValue := initialValue
		if initial != previous {
			previousValue += previous.GetDoubleValue()
		}
		if currentValue < previousValue {
			// reset detected
			return false
		}
		current.Value =
			&metricspb.Point_DoubleValue{DoubleValue: currentValue - initialValue}
	case metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION:
		// note: sum of squared deviation not currently supported
		currentDist := current.GetDistributionValue()
		initialDist := initial.GetDistributionValue()
		previousCount := initialDist.Count
		previousSum := initialDist.Sum
		if initial != previous {
			previousCount += previous.GetDistributionValue().Count
			previousSum += previous.GetDistributionValue().Sum
		}
		if currentDist.Count < previousCount || currentDist.Sum < previousSum {
			// reset detected
			return false
		}
		currentDist.Count -= initialDist.Count
		currentDist.Sum -= initialDist.Sum
		ma.adjustBuckets(currentDist.Buckets, initialDist.Buckets)
	case metricspb.MetricDescriptor_SUMMARY:
		// note: for summary, we don't adjust the snapshot
		currentCount := current.GetSummaryValue().Count.GetValue()
		currentSum := current.GetSummaryValue().Sum.GetValue()
		initialCount := initial.GetSummaryValue().Count.GetValue()
		initialSum := initial.GetSummaryValue().Sum.GetValue()
		previousCount := initialCount
		previousSum := initialSum
		if initial != previous {
			previousCount += previous.GetSummaryValue().Count.GetValue()
			previousSum += previous.GetSummaryValue().Sum.GetValue()
		}
		if currentCount < previousCount || currentSum < previousSum {
			// reset detected
			return false
		}
		current.GetSummaryValue().Count =
			&wrappers.Int64Value{Value: currentCount - initialCount}
		current.GetSummaryValue().Sum =
			&wrappers.DoubleValue{Value: currentSum - initialSum}
	default:
		// this shouldn't happen
		ma.logger.Infof("adjust unexpect point type %v, skipping ...", metricType.String())
	}
	return true
}

func (ma *MetricsAdjuster) adjustBuckets(current, initial []*metricspb.DistributionValue_Bucket) {
	if len(current) != len(initial) {
		// this shouldn't happen
		ma.logger.Infof("len(current buckets): %v != len(initial buckets): %v",
			len(current), len(initial))
	}
	for i := 0; i < len(current); i++ {
		current[i].Count -= initial[i].Count
	}
}
