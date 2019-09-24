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

package internal

import (
	"strings"
	"testing"
	"time"

	metricspb "github.com/census-instrumentation/opencensus-proto/gen-go/metrics/v1"
	"go.opencensus.io/resource/resourcekeys"

	"github.com/golang/protobuf/proto"
	"github.com/golang/protobuf/ptypes/timestamp"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"go.uber.org/zap"
)

func Test_CumulativeDouble(t *testing.T) {
	script := []*metricsAdjusterTest{
		{
			description: "Drop Container Key Name for Cumulative Double",
			dropResKeyMap: map[string]bool{
				resourcekeys.ContainerKeyName: true,
			},
			dropLabelKeyMap: map[string]bool{},
			inDir:           "CumulativeDouble",
		},
		{
			description: "Drop Container Key Name for Cumulative Int64",
			dropResKeyMap: map[string]bool{
				resourcekeys.ContainerKeyName: true,
			},
			dropLabelKeyMap: map[string]bool{},
			inDir:           "CumulativeInt64",
		},
		{
			description: "Drop Container Key Name for Cumulative Distribution",
			dropResKeyMap: map[string]bool{
				resourcekeys.ContainerKeyName: true,
			},
			dropLabelKeyMap: map[string]bool{},
			inDir:           "CumulativeDistribution",
		},
	}
	runScript(t, NewJobsMap(time.Duration(time.Minute)).Get("job", "0"), script)
}

func readTestCaseFromFiles(t *testing.T, mat *metricsAdjusterTest) {
	// Read input Metrics proto.
	filename := "../testdata/" + mat.inDir + "/inMetrics.txt"
	f, err := readFile(filename)
	if err != nil {
		t.Fatalf("error opening file " + filename)
	}

	strMetrics := strings.Split(string(f), "---")
	for _, strMetric := range strMetrics {
		in := metricspb.Metric{}
		err = proto.UnmarshalText(strMetric, &in)
		if err != nil {
			t.Fatalf("error unmarshalling Metric protos from file " + mat.inDir)
		}
		mat.inMetrics = append(mat.inMetrics, &in)
	}

	// Read output Metrics proto.
	filename = "../testdata/" + mat.inDir + "/outMetrics.txt"
	f, err = readFile(filename)
	if err != nil {
		t.Fatalf("error opening file " + filename)
	}

	strMetrics = strings.Split(string(f), "---")
	for _, strMetric := range strMetrics {
		in := metricspb.Metric{}
		err = proto.UnmarshalText(strMetric, &in)
		if err != nil {
			t.Fatalf("error unmarshalling Metric protos from file " + mat.inDir)
		}
		mat.outMetrics = append(mat.outMetrics, &in)
	}
}

type metricsAdjusterTest struct {
	description     string
	dropResKeyMap   map[string]bool
	dropLabelKeyMap map[string]bool
	inDir           string
	inMetrics       []*metricspb.Metric
	outMetrics      []*metricspb.Metric
}

func runScript(t *testing.T, tsm *timeseriesMap, script []*metricsAdjusterTest) {
	l, _ := zap.NewProduction()
	defer l.Sync() // flushes buffer, if any

	for _, test := range script {
		ma := NewMetricsAdjuster(tsm, l.Sugar())
		readTestCaseFromFiles(t, test)
		ma.AggregateMetrics(test.dropResKeyMap, test.dropLabelKeyMap, nil, test.inMetrics)
		gotMetrics := ma.ExportTimeSeries()
		diff := cmp.Diff(gotMetrics, test.outMetrics, cmpopts.IgnoreFields(timestamp.Timestamp{}, "XXX_sizecache"),
			cmpopts.IgnoreFields(metricspb.TimeSeries{}, "StartTimestamp"),
			cmpopts.IgnoreFields(metricspb.Point{}, "Timestamp"))
		if diff != "" {
			t.Errorf("Error: %v -got +want %v\n, %v\n", test.description, diff, gotMetrics)
		}
	}
}
