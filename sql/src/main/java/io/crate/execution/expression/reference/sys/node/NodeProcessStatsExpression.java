/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.expression.reference.sys.node;

import org.elasticsearch.monitor.process.ProcessStats;

public class NodeProcessStatsExpression extends NestedNodeStatsExpression {

    private static final String OPEN_FILE_DESCRIPTORS = "open_file_descriptors";
    private static final String MAX_OPEN_FILE_DESCRIPTORS = "max_open_file_descriptors";
    private static final String PROBE_TIMESTAMP = "probe_timestamp";
    private static final String CPU = "cpu";

    public NodeProcessStatsExpression() {
        childImplementations.put(OPEN_FILE_DESCRIPTORS, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                ProcessStats processStats = this.row.processStats();
                if (processStats != null) {
                    return processStats.getOpenFileDescriptors();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(MAX_OPEN_FILE_DESCRIPTORS, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                ProcessStats processStats = this.row.processStats();
                if (processStats != null) {
                    return processStats.getMaxFileDescriptors();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(PROBE_TIMESTAMP, new SimpleNodeStatsExpression<Long>() {
            @Override
            public Long innerValue() {
                ProcessStats processStats = this.row.processStats();
                if (processStats != null) {
                    return processStats.getTimestamp();
                } else {
                    return -1L;
                }
            }
        });
        childImplementations.put(CPU, new NodeProcessCpuStatsExpression());
    }
}