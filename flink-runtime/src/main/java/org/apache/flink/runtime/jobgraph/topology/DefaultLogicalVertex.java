/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobgraph.topology;

import org.apache.flink.runtime.jobgraph.IntermediateDataSet;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.JobEdge;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** Default implementation of {@link LogicalVertex}. It is an adapter of {@link JobVertex}. */
public class DefaultLogicalVertex implements LogicalVertex {
    /** JobVertex */
    private final JobVertex jobVertex;
    /**
     * 最终的（不可变的）成员变量 resultRetriever。是一个函数式接口（Functional Interface）的实例，
     * 具体来说，它是 Function 接口的一个实例。
     * IntermediateDataSetID JobVertex 输出结果Id
     * DefaultLogicalResult IntermediateDataSet拓步后的结构
     */
    private final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever;
    /**
     *  LogicalVertex输入的所有边List<LogicalEdge>
     */
    private final List<LogicalEdge> inputEdges;

    DefaultLogicalVertex(
            final JobVertex jobVertex,
            final Function<IntermediateDataSetID, DefaultLogicalResult> resultRetriever) {

        this.jobVertex = checkNotNull(jobVertex);
        this.resultRetriever = checkNotNull(resultRetriever);
        /** 根据 List<JobEdge> 循环构建出 List<LogicalEdge> inputEdges*/
        this.inputEdges =
                jobVertex.getInputs().stream()
                        .map(DefaultLogicalEdge::new)
                        .collect(Collectors.toList());
    }

    @Override
    public JobVertexID getId() {
        return jobVertex.getID();
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 获取JobVertext要消费的Input输入管道对应的IntermediateDataSet
     * 最终封装为DefaultLogicalResult
    */
    @Override
    public Iterable<DefaultLogicalResult> getConsumedResults() {
        return jobVertex.getInputs().stream()
                .map(JobEdge::getSource)
                .map(IntermediateDataSet::getId)
                .map(resultRetriever)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<DefaultLogicalResult> getProducedResults() {
        return jobVertex.getProducedDataSets().stream()
                .map(IntermediateDataSet::getId)
                .map(resultRetriever)
                .collect(Collectors.toList());
    }

    @Override
    public Iterable<? extends LogicalEdge> getInputs() {
        return inputEdges;
    }
}
