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

// Package tests contains test cases. To run the tests go to tests directory and run:
// TESTBED_CONFIG=local.yaml go test -v

package tests

// This file contains Test functions which initiate the tests. The tests can be either
// coded in this file or use scenarios from perf_scenarios.go.

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"testing"

	tracepb "github.com/census-instrumentation/opencensus-proto/gen-go/trace/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/open-telemetry/opentelemetry-collector/consumer/pdata"
	"github.com/open-telemetry/opentelemetry-collector/testbed/testbed"
	"github.com/open-telemetry/opentelemetry-collector/translator/conventions"
	"github.com/open-telemetry/opentelemetry-collector/translator/internaldata"
)

// TestMain is used to initiate setup, execution and tear down of testbed.
func TestMain(m *testing.M) {
	testbed.DoTestMain(m)
}

func TestTrace10kSPS(t *testing.T) {
	tests := []struct {
		name         string
		sender       testbed.DataSender
		receiver     testbed.DataReceiver
		resourceSpec testbed.ResourceSpec
	}{
		{
			"JaegerGRPC",
			testbed.NewJaegerGRPCDataSender(testbed.GetAvailablePort(t)),
			testbed.NewJaegerDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 40,
				ExpectedMaxRAM: 60,
			},
		},
		{
			"OpenCensus",
			testbed.NewOCTraceDataSender(testbed.GetAvailablePort(t)),
			testbed.NewOCDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 30,
				ExpectedMaxRAM: 60,
			},
		},
		{
			"OTLP",
			testbed.NewOTLPTraceDataSender(testbed.GetAvailablePort(t)),
			testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 20,
				ExpectedMaxRAM: 60,
			},
		},
		{
			"Zipkin",
			testbed.NewZipkinDataSender(testbed.GetAvailablePort(t)),
			testbed.NewZipkinDataReceiver(testbed.GetAvailablePort(t)),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 60,
			},
		},
	}

	processors := map[string]string{
		"batch": `
  batch:
`,
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			Scenario10kItemsPerSecond(
				t,
				test.sender,
				test.receiver,
				test.resourceSpec,
				processors,
			)
		})
	}
}

func TestTraceNoBackend10kSPS(t *testing.T) {

	limitProcessors := map[string]string{
		"memory_limiter": `
  memory_limiter:
   check_interval: 1s
   limit_mib: 10
`,
		"queued_retry": `
  queued_retry:
`,
	}

	noLimitProcessors := map[string]string{
		"queued_retry": `
  queued_retry:
`,
	}

	var processorsConfig = []processorConfig{
		{
			Name:                "NoMemoryLimit",
			Processor:           noLimitProcessors,
			ExpectedMaxRAM:      200,
			ExpectedMinFinalRAM: 100,
		},
		{
			Name:                "MemoryLimit",
			Processor:           limitProcessors,
			ExpectedMaxRAM:      60,
			ExpectedMinFinalRAM: 10,
		},
	}

	var testSenders = []struct {
		name          string
		sender        testbed.DataSender
		receiver      testbed.DataReceiver
		resourceSpec  testbed.ResourceSpec
		configuration []processorConfig
	}{
		{
			"JaegerGRPC",
			testbed.NewJaegerGRPCDataSender(testbed.DefaultJaegerPort),
			testbed.NewOCDataReceiver(testbed.DefaultOCPort),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 60,
				ExpectedMaxRAM: 198,
			},
			processorsConfig,
		},
		{
			"Zipkin",
			testbed.NewZipkinDataSender(testbed.DefaultZipkinAddressPort),
			testbed.NewOCDataReceiver(testbed.DefaultOCPort),
			testbed.ResourceSpec{
				ExpectedMaxCPU: 80,
				ExpectedMaxRAM: 198,
			},
			processorsConfig,
		},
	}

	for _, test := range testSenders {
		for _, testConf := range test.configuration {
			testName := fmt.Sprintf("%s/%s", test.name, testConf.Name)
			t.Run(testName, func(t *testing.T) {
				ScenarioTestTraceNoBackend10kSPS(
					t,
					test.sender,
					test.receiver,
					test.resourceSpec,
					testConf,
				)
			})
		}
	}
}

func TestTrace1kSPSWithAttrs(t *testing.T) {
	Scenario1kSPSWithAttrs(t, []string{}, []TestCase{
		// No attributes.
		{
			attrCount:      0,
			attrSizeByte:   0,
			expectedMaxCPU: 30,
			expectedMaxRAM: 100,
		},

		// We generate 10 attributes each with average key length of 100 bytes and
		// average value length of 50 bytes so total size of attributes values is
		// 15000 bytes.
		{
			attrCount:      100,
			attrSizeByte:   50,
			expectedMaxCPU: 120,
			expectedMaxRAM: 100,
		},

		// Approx 10 KiB attributes.
		{
			attrCount:      10,
			attrSizeByte:   1000,
			expectedMaxCPU: 100,
			expectedMaxRAM: 100,
		},

		// Approx 100 KiB attributes.
		{
			attrCount:      20,
			attrSizeByte:   5000,
			expectedMaxCPU: 250,
			expectedMaxRAM: 100,
		},
	})
}

func TestTraceBallast1kSPSWithAttrs(t *testing.T) {
	args := []string{"--mem-ballast-size-mib", "1000"}
	Scenario1kSPSWithAttrs(t, args, []TestCase{
		// No attributes.
		{
			attrCount:      0,
			attrSizeByte:   0,
			expectedMaxCPU: 30,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      100,
			attrSizeByte:   50,
			expectedMaxCPU: 80,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      10,
			attrSizeByte:   1000,
			expectedMaxCPU: 80,
			expectedMaxRAM: 2000,
		},
		{
			attrCount:      20,
			attrSizeByte:   5000,
			expectedMaxCPU: 120,
			expectedMaxRAM: 2000,
		},
	})
}

func TestTraceBallast1kSPSAddAttrs(t *testing.T) {
	args := []string{"--mem-ballast-size-mib", "1000"}
	Scenario1kSPSWithAttrs(
		t,
		args,
		[]TestCase{
			{
				attrCount:      0,
				attrSizeByte:   0,
				expectedMaxCPU: 30,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      100,
				attrSizeByte:   50,
				expectedMaxCPU: 80,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      10,
				attrSizeByte:   1000,
				expectedMaxCPU: 80,
				expectedMaxRAM: 2000,
			},
			{
				attrCount:      20,
				attrSizeByte:   5000,
				expectedMaxCPU: 120,
				expectedMaxRAM: 2000,
			},
		},
		testbed.WithConfigFile(path.Join("testdata", "add-attributes-config.yaml")),
	)
}

// verifySingleSpan sends a single span to Collector, waits until the span is forwarded
// and received by MockBackend and calls user-supplied verification functions on
// received span.
// Temporarily, we need two verification functions in order to verify spans in
// new and old format received by MockBackend.
func verifySingleSpan(
	t *testing.T,
	tc *testbed.TestCase,
	serviceName string,
	spanName string,
	verifyReceived func(span pdata.Span),
	verifyReceivedOld func(span *tracepb.Span),
) {

	// Clear previously received traces.
	tc.MockBackend.ClearReceivedItems()
	startCounter := tc.MockBackend.DataItemsReceived()

	// Send one span.
	td := pdata.NewTraces()
	td.ResourceSpans().Resize(1)
	td.ResourceSpans().At(0).Resource().InitEmpty()
	td.ResourceSpans().At(0).Resource().Attributes().InitFromMap(map[string]pdata.AttributeValue{
		conventions.AttributeServiceName: pdata.NewAttributeValueString(serviceName),
	})
	td.ResourceSpans().At(0).InstrumentationLibrarySpans().Resize(1)
	spans := td.ResourceSpans().At(0).InstrumentationLibrarySpans().At(0).Spans()
	spans.Resize(1)
	spans.At(0).SetTraceID(testbed.GenerateTraceID(1))
	spans.At(0).SetSpanID(testbed.GenerateSpanID(1))
	spans.At(0).SetName(spanName)

	if sender, ok := tc.Sender.(testbed.TraceDataSender); ok {
		sender.SendSpans(td)
	} else {
		senderOld := tc.Sender.(testbed.TraceDataSenderOld)
		senderOld.SendSpans(internaldata.TraceDataToOC(td)[0])
	}

	// We bypass the load generator in this test, but make sure to increment the
	// counter since it is used in final reports.
	tc.LoadGenerator.IncDataItemsSent()

	// Wait until span is received.
	tc.WaitFor(func() bool { return tc.MockBackend.DataItemsReceived() == startCounter+1 },
		"span received")

	// Verify received span.
	count := 0
	for _, td := range tc.MockBackend.ReceivedTraces {
		rs := td.ResourceSpans()
		for i := 0; i < rs.Len(); i++ {
			ils := rs.At(i).InstrumentationLibrarySpans()
			for j := 0; j < ils.Len(); j++ {
				spans := ils.At(j).Spans()
				for k := 0; k < spans.Len(); k++ {
					verifyReceived(spans.At(k))
					count++
				}
			}
		}
	}
	for _, td := range tc.MockBackend.ReceivedTracesOld {
		for _, span := range td.Spans {
			verifyReceivedOld(span)
			count++
		}
	}
	assert.EqualValues(t, 1, count, "must receive one span")
}

func TestTraceAttributesProcessor(t *testing.T) {
	tests := []struct {
		name     string
		sender   testbed.DataSender
		receiver testbed.DataReceiver
	}{
		{
			"JaegerGRPC",
			testbed.NewJaegerGRPCDataSender(testbed.GetAvailablePort(t)),
			testbed.NewJaegerDataReceiver(testbed.GetAvailablePort(t)),
		},
		{
			"OTLP",
			testbed.NewOTLPTraceDataSender(testbed.GetAvailablePort(t)),
			testbed.NewOTLPDataReceiver(testbed.GetAvailablePort(t)),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			resultDir, err := filepath.Abs(path.Join("results", t.Name()))
			if err != nil {
				t.Fatal(err)
			}

			// Use processor to add attributes to certain spans.
			processors := map[string]string{
				"batch": `
  batch:
`,
				"attributes": `
  attributes:
    include:
      match_type: regexp
      services: ["service-to-add.*"]
      span_names: ["span-to-add-.*"]
    actions:
      - action: insert
        key: "new_attr"
        value: "string value"
`,
				"queued_retry": `
  queued_retry:
`,
			}

			configFile := createConfigFile(t, test.sender, test.receiver, resultDir, processors)
			defer os.Remove(configFile)

			if configFile == "" {
				t.Fatal("Cannot create config file")
			}

			tc := testbed.NewTestCase(t, test.sender, test.receiver, testbed.WithConfigFile(configFile))
			defer tc.Stop()

			tc.StartBackend()
			tc.StartAgent()
			defer tc.StopAgent()

			tc.EnableRecording()

			test.sender.Start()

			// Create a span that matches "include" filter.
			spanToInclude := "span-to-add-attr"
			// Create a service name that matches "include" filter.
			nodeToInclude := "service-to-add-attr"

			// verifySpan verifies that attributes was added to the internal data span.
			verifySpan := func(span pdata.Span) {
				require.NotNil(t, span)
				require.Equal(t, span.Attributes().Cap(), 1)
				attrVal, ok := span.Attributes().Get("new_attr")
				assert.True(t, ok)
				assert.EqualValues(t, "string value", attrVal.StringVal())
			}

			// verifySpanOld verifies that attributes was added to the OC data span.
			verifySpanOld := func(span *tracepb.Span) {
				require.NotNil(t, span)
				require.NotNil(t, span.Attributes)
				require.NotNil(t, span.Attributes.AttributeMap)
				attrVal, ok := span.Attributes.AttributeMap["new_attr"]
				assert.True(t, ok)
				assert.NotNil(t, attrVal)
				assert.EqualValues(t, "string value", attrVal.GetStringValue().Value)
			}

			verifySingleSpan(t, tc, nodeToInclude, spanToInclude, verifySpan, verifySpanOld)

			// Create a service name that does not match "include" filter.
			nodeToExclude := "service-not-to-add-attr"

			verifySingleSpan(t, tc, nodeToExclude, spanToInclude, func(span pdata.Span) {
				// Verify attributes was not added to the new internal data span.
				assert.Equal(t, span.Attributes().Cap(), 0)
			}, func(span *tracepb.Span) {
				// Verify attributes was not added to the OC span.
				assert.Nil(t, span.Attributes)
			})

			// Create another span that does not match "include" filter.
			spanToExclude := "span-not-to-add-attr"
			verifySingleSpan(t, tc, nodeToInclude, spanToExclude, func(span pdata.Span) {
				// Verify attributes was not added to the new internal data span.
				assert.Equal(t, span.Attributes().Cap(), 0)
			}, func(span *tracepb.Span) {
				// Verify attributes was not added to the OC span.
				assert.Nil(t, span.Attributes)
			})
		})
	}
}
