/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.dolphinscheduler.server.master.metrics;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

import com.google.common.collect.ImmutableSet;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Metrics;
import io.micrometer.core.instrument.Timer;

@UtilityClass
@Slf4j
public class WorkflowInstanceMetrics {

    private final Set<String> workflowInstanceStates = ImmutableSet.of(
            "submit", "timeout", "finish", "failover", "success", "fail", "stop");

    static {
        for (final String state : workflowInstanceStates) {
            Counter.builder("ds.workflow.instance.count")
                    .tags("state", state, "workflow.definition.code", "dummy")
                    .description(String.format("workflow instance total count by state and definition code"))
                    .register(Metrics.globalRegistry);
        }

    }

    private final Timer commandQueryTimer =
            Timer.builder("ds.workflow.command.query.duration")
                    .description("Command query duration")
                    .register(Metrics.globalRegistry);

    private final Timer workflowInstanceGenerateTimer =
            Timer.builder("ds.workflow.instance.generate.duration")
                    .description("workflow instance generated duration")
                    .register(Metrics.globalRegistry);

    public void recordCommandQueryTime(long milliseconds) {
        commandQueryTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    public void recordWorkflowInstanceGenerateTime(long milliseconds) {
        workflowInstanceGenerateTimer.record(milliseconds, TimeUnit.MILLISECONDS);
    }

    public synchronized void registerWorkflowInstanceRunningGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.running", function)
                .description("The current running workflow instance count")
                .register(Metrics.globalRegistry);
    }

    public synchronized void registerWorkflowInstanceResubmitGauge(Supplier<Number> function) {
        Gauge.builder("ds.workflow.instance.resubmit", function)
                .description("The current workflow instance need to resubmit count")
                .register(Metrics.globalRegistry);
    }

    public void incWorkflowInstanceByStateAndWorkflowDefinitionCode(final String state,
                                                                    final String workflowDefinitionCode) {
        // When tags need to be determined from local context,
        // you have no choice but to construct or lookup the Meter inside your method body.
        // The lookup cost is just a single hash lookup, so it is acceptable for most use cases.
        Metrics.globalRegistry.counter(
                "ds.workflow.instance.count",
                "state", state,
                "workflow.definition.code", workflowDefinitionCode)
                .increment();
    }

    public void cleanUpWorkflowInstanceCountMetricsByDefinitionCode(final Long workflowDefinitionCode) {
        for (final String state : workflowInstanceStates) {
            final Counter counter = Metrics.globalRegistry.counter(
                    "ds.workflow.instance.count",
                    "state", state,
                    "workflow.definition.code", String.valueOf(workflowDefinitionCode));
            Metrics.globalRegistry.remove(counter);
        }
    }

}
