# Copyright 2021 Yoshi Yamaguchi
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import logging
import os
import random
import time

import google.auth
from opencensus.common.transports.sync import SyncTransport
from opencensus.ext.stackdriver import stats_exporter, trace_exporter
from opencensus.stats import aggregation, measure
from opencensus.stats import stats as stats_module
from opencensus.stats import view
from opencensus.trace import tracer as tracer_module
from opencensus.trace.propagation import google_cloud_format
from opencensus.trace.samplers import AlwaysOnSampler

logging.basicConfig(level=logging.INFO)

m_latency_ms = measure.MeasureFloat(
    "task_latency", "The task latency in milliseconds", "ms"
)

latency_view = view.View(
    "task_latency_distribution_exemplar",
    "The distribution of the task latencies",
    [],
    m_latency_ms,
    aggregation.DistributionAggregation(
        [100.0, 200.0, 400.0, 1000.0, 2000.0, 4000.0])
)

global project_id


def init():
    global project_id
    try:
        _, project_id = google.auth.default()
    except google.auth.exceptions.DefaultCredentialsError:
        try:
            project_id = os.environ.get("GCP_PROJECT_ID")
        except KeyError:
            raise ValueError("Couldn't find Google Cloud credentials, set the ",
                             "project ID with 'gcloud set project' or $GCP_PROJECT_ID")

    logging.info(f"Running in the project: {project_id}")

    cte = trace_exporter.StackdriverExporter(
        project_id=project_id,
        transport=SyncTransport
    )

    cme = stats_exporter.new_stats_exporter(
        stats_exporter.Options(project_id=project_id))
    vm = stats_module.stats.view_manager
    vm.register_exporter(cme)
    vm.register_view(latency_view)

    return cte


def main():
    trace_exporter = init()
    logging.info("starting loop")
    while True:
        logging.info("loop start")
        tracer = tracer_module.Tracer(
            exporter=trace_exporter,
            propagator=google_cloud_format.GoogleCloudFormatPropagator,
            sampler=AlwaysOnSampler(),
        )
        root(tracer)
        tracer.finish()
        time.sleep(1.0)
        logging.info("loop end")


def root(tracer):
    mmap = stats_module.stats.stats_recorder.new_measurement_map()

    start = datetime.datetime.now()
    with tracer.span(name="root") as span:
        global project_id
        context = tracer.span_context
        trace_id = context.trace_id
        span_id = span.span_id
        span_name = f"projects/{project_id}/traces/{trace_id}/spans/{span_id}"
        foo(span)
    end = datetime.datetime.now()
    logging.info(f"span name: {span_name}")

    ms = (end - start).microseconds / 1000.0
    logging.info(f"task elapsed: {ms}ms")
    mmap.measure_float_put(m_latency_ms, ms)
    mmap.measure_put_attachment(
        "@type", "type.googleapis.com/google.monitoring.v3.SpanContext")
    mmap.measure_put_attachment(
        "value", span_name
    )
    mmap.record()


def foo(parent):
    with parent.span("child_foo") as span:
        ms = random.random() * 2 * 1000
        logging.info(f"task foo blocked: {ms}ms")
        span.add_attribute("foo_wait", str(ms))
        bar(span)
        sec = ms / 1000
        time.sleep(sec)


def bar(parent):
    with parent.span("child_bar") as span:
        ms = random.random() * 1 * 1000
        logging.info(f"task bar blocked: {ms}ms")
        span.add_attribute("bar_wait", str(ms))
        sec = ms / 1000
        time.sleep(sec)


if __name__ == "__main__":
    main()
