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
	"github.com/golang/protobuf/ptypes/timestamp"
	"sort"
	"strings"
	"sync"
	"time"

	commonpb "github.com/census-instrumentation/opencensus-proto/gen-go/agent/common/v1"
	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	resourcepb "github.com/census-instrumentation/opencensus-proto/gen-go/resource/v1"
	"go.uber.org/zap"
)

// Notes on garbage collection (gc):
//
// This is mainly borrowed from metric_adjuster in prometheusreceiver

type aggResource struct {
	sync.RWMutex
	res       *resourcepb.Resource
	node      *commonpb.Node
	metricMap map[string]*aggMetric
}

type aggMetric struct {
	sync.RWMutex
	metric *metricspb.Metric
	keyMap map[string]bool // useful for checking if key is dropped or not
	tsMap  map[string]*metricspb.TimeSeries
}

type aggResourceMap struct {
	sync.RWMutex
	resMap map[string]*aggResource
}

func getResSignature(resKeys []string, metric *metricspb.Metric) string {
	labelValues := make([]string, 0, len(resKeys))
	for _, k := range resKeys {
		labelValues = append(labelValues, metric.Resource.Labels[k])
	}
	// TODO (rghetia): Check if sort is necessary?
	sort.Strings(labelValues)

	return fmt.Sprintf("%s,%s", metric.Resource.Type, strings.Join(labelValues, ","))

}

func getLabelKeySignature(labelKeys []string, metric *metricspb.Metric) string {
	labelValues := make([]string, 0, len(labelKeys))
	for _, k := range labelKeys {
		labelValues = append(labelValues, metric.Resource.Labels[k])
	}
	// TODO (rghetia): Check if sort is necessary?
	sort.Strings(labelValues)
	return fmt.Sprintf("%s,%s", metric.GetMetricDescriptor().GetName(), strings.Join(labelValues, ","))

}

func (arm *aggResourceMap) getOrCreateAggRes(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) *aggResource {
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

		// TODO(rghetia) : add efficient RLock()
		arm.Lock()
		defer arm.Unlock()
		aggRes, ok = arm.resMap[resSig]
		if !ok {
			aggRes = &aggResource{
				res: &resourcepb.Resource{
					Type: metric.Resource.Type,
					Labels: make(map[string]string, len(resKeys)),
				},
				metricMap: map[string]*aggMetric{},
			}
			for _, k := range resKeys {
				aggRes.res.Labels[k] = metric.Resource.Labels[k]
			}
			arm.resMap[resSig] = aggRes
		}
	} else {
		arm.Lock()
		defer arm.Unlock()
		aggRes, ok := arm.resMap[""]
		if !ok {
			aggRes = &aggResource{
				metricMap: map[string]*aggMetric{},
			}
			arm.resMap[""] = aggRes
		}
	}
	return aggRes
}

func (ar *aggResource) getOrCreateAggMetric(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric) *aggMetric {
	var labelKeys []string
	var aggM *aggMetric

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

	ar.Lock()
	defer ar.Unlock()
	aggM, ok = ar.metricMap[labelSig]
	if !ok {
		aggM = &aggMetric{
			metric: &metricspb.Metric{
				MetricDescriptor: &metricspb.MetricDescriptor{
					Name:        metric.MetricDescriptor.Name,
					Description: metric.MetricDescriptor.Description,
					Unit: 		 metric.MetricDescriptor.Unit,
					Type:        metric.MetricDescriptor.Type,
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
		ar.metricMap[labelSig] = aggM
	}

	return aggM
}

func getTsSig(labelValues []*metricspb.LabelValue) string {
	var values = make([]string, 0, len(labelValues))
	for _, lv := range labelValues {
		values = append(values, lv.Value)
	}
	return strings.Join(values, ",")
}

func copyTimeSeries(metricType metricspb.MetricDescriptor_Type, ts *metricspb.TimeSeries, labelValues []*metricspb.LabelValue) *metricspb.TimeSeries {
	switch metricType {
	case metricspb.MetricDescriptor_CUMULATIVE_DOUBLE:
		newTs := &metricspb.TimeSeries{
			StartTimestamp: timeToProtoTimestamp(time.Now()),
			LabelValues: labelValues,
			Points: []*metricspb.Point{
				{
					Timestamp: timeToProtoTimestamp(time.Now()),
					Value: &metricspb.Point_DoubleValue{
						DoubleValue: 0.0,
					},
				},
			},
		}
		return newTs
	}
	return nil
}

func (am *aggMetric) getOrCreateTimeSeries(dropResKeys, dropLabelKeys map[string]bool, metric *metricspb.Metric, series *metricspb.TimeSeries) *metricspb.TimeSeries {
	var ts *metricspb.TimeSeries
	var labelValues = make([]*metricspb.LabelValue, 0, len(am.keyMap))

	for i, k := range metric.MetricDescriptor.LabelKeys {
		if _, ok := am.keyMap[k.Key] ; ok {
			labelValues = append(labelValues, series.LabelValues[i])
		}
	}
	sig := getTsSig(labelValues)

	am.Lock()
	defer am.Unlock()
	ts, ok := am.tsMap[sig]
	if !ok {
		ts = copyTimeSeries(metric.MetricDescriptor.Type, series, labelValues)
		am.tsMap[sig] = ts
	}
	return ts
}

// timeseriesinfo contains the information necessary to adjust from the initial point and to detect
// resets.
type timeseriesinfo struct {
	mark     bool
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
		tsi = &timeseriesinfo{
		}
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
	sort.Strings(labelValues)
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

func (jm *JobsMap) Get(job, instance string) *timeseriesMap {
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

// MetricsAdjuster takes a map from a metric instance to the initial point in the inMetrics instance
// and provides AdjustMetrics, which takes a sequence of inMetrics and adjust their values based on
// the initial points.
type MetricsAdjuster struct {
	tsm    *timeseriesMap
	asm    *aggResourceMap
	logger *zap.SugaredLogger
}

// NewMetricsAdjuster is a constructor for MetricsAdjuster.
func NewMetricsAdjuster(tsm *timeseriesMap, logger *zap.SugaredLogger) *MetricsAdjuster {
	return &MetricsAdjuster{
		tsm:    tsm,
		logger: logger,
		asm: &aggResourceMap{
			resMap: map[string]*aggResource{},
		},
	}
}

// AdjustMetrics takes a sequence of inMetrics and adjust their values based on the initial and
// previous points in the timeseriesMap. If the metric is the first point in the timeseries, or the
// timeseries has been reset, it is removed from the sequence and added to the the timeseriesMap.
func (ma *MetricsAdjuster) AdjustMetrics(dropResKeys, dropLabelKeys map[string]bool, res *resourcepb.Resource, metrics []*metricspb.Metric) {
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
}

// Returns true if at least one of the metric's timeseries was adjusted and false if all of the
// timeseries are an initial occurrence or a reset.
//
// Types of inMetrics returned supported by prometheus:
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
		if tsi.previous == nil {
			aggRes := ma.asm.getOrCreateAggRes(dropResKeys, dropLabelKeys, metric)
			aggM := aggRes.getOrCreateAggMetric(dropResKeys, dropLabelKeys, metric)
			tsi.aggTs = aggM.getOrCreateTimeSeries(dropResKeys, dropLabelKeys, metric, current)

			// initial timeseries
			tsi.previous = current
		} else {
			ma.adjustAggTimeSeries(metric.MetricDescriptor.Type, tsi, current)
			tsi.previous = current
		}
	}
	metric.Timeseries = filtered
	return len(filtered) > 0
}

func timeToProtoTimestamp(t time.Time) *timestamp.Timestamp {
	unixNano := t.UnixNano()
	return &timestamp.Timestamp{
		Seconds: int64(unixNano / 1e9),
		Nanos:   int32(unixNano % 1e9),
	}
}

func (ma *MetricsAdjuster) adjustAggTimeSeries(metricType metricspb.MetricDescriptor_Type,
	tsi *timeseriesinfo, current *metricspb.TimeSeries) {
	switch metricType {
	case metricspb.MetricDescriptor_CUMULATIVE_DOUBLE:
		currentValue := current.GetPoints()[0].GetDoubleValue()
		previousValue := tsi.previous.GetPoints()[0].GetDoubleValue()
		var delta float64
		if currentValue < previousValue {
			// reset happend
			delta = currentValue
		} else {
			delta = currentValue - previousValue
		}
		aggCurrValue := tsi.aggTs.GetPoints()[0].GetDoubleValue()
		aggNewValue := aggCurrValue + delta

		tsi.aggTs.Points[0].Timestamp = timeToProtoTimestamp(time.Now())
		tsi.aggTs.Points[0].Value = &metricspb.Point_DoubleValue{
			DoubleValue: aggNewValue,
		}
	case metricspb.MetricDescriptor_CUMULATIVE_DISTRIBUTION:
	case metricspb.MetricDescriptor_SUMMARY:
	default:
		// this shouldn't happen
		ma.logger.Infof("adjust unexpect point type %v, skipping ...", metricType.String())
	}
}

func (ma *MetricsAdjuster) ExportTimeSeries() []*metricspb.Metric {
	metrics := make([]*metricspb.Metric, 0)
	ma.asm.Lock()
	defer ma.asm.Unlock()
	for _, resV := range ma.asm.resMap {
		resV.Lock()
		for _, metricV := range resV.metricMap {
			metric := &metricspb.Metric{
				MetricDescriptor: metricV.metric.MetricDescriptor,
				Resource: resV.res,
				Timeseries: []*metricspb.TimeSeries{},
			}
			metricV.Lock()
			for _, tsV := range metricV.tsMap {
				metric.Timeseries = append(metric.Timeseries, tsV)
			}
			metrics = append(metrics, metric)
			metricV.Unlock()
		}
		resV.Unlock()
	}
	return metrics
}
