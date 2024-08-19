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

package org.apache.flink.table.planner.plan.nodes.exec;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.common.CommonIntermediateTableScan;
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel;

import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * A generator that generates a {@link ExecNode} graph from a graph of {@link FlinkPhysicalRel}s.
 *
 * <p>This traverses the tree of {@link FlinkPhysicalRel} starting from the sinks. At each rel we
 * recursively transform the inputs, then create a {@link ExecNode}. Each rel will be visited only
 * once, that means a rel will only generate one ExecNode instance.
 *
 * <p>Exchange and Union will create a actual node in the {@link ExecNode} graph as the first step,
 * once all ExecNodes' implementation are separated from physical rel, we will use {@link
 * InputProperty} to replace them.
 */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 从Flink物理关系图（FlinkPhysicalRel）生成执行节点图（ExecNode）的生成器。
 *
 * <p>此生成器从sink节点开始遍历FlinkPhysicalRel树。在每个关系节点上，它递归地转换输入，然后创建一个ExecNode。
 * 每个关系节点只会被访问一次，这意味着每个关系节点只会生成一个ExecNode实例。
 *
 * <p>Exchange和Union等节点在执行节点图（ExecNode）中会首先创建实际的节点。一旦所有ExecNode的实现都与物理关系节点分离，
 * 我们将使用InputProperty来替代它们（注：这里的描述可能基于未来的设计考虑，当前实现可能还未完全达到此状态）。
 */
public class ExecNodeGraphGenerator {
    // 用于存储已访问的关系节点及其对应的执行节点，以避免重复生成
    private final Map<FlinkPhysicalRel, ExecNode<?>> visitedRels;

    public ExecNodeGraphGenerator() {
        this.visitedRels = new IdentityHashMap<>();
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 生成执行节点图。
     * @param relNodes Flink物理关系节点的列表，这些节点是生成执行节点图的起点。
     * @param isCompiled 一个标志，指示是否是在编译模式下生成执行节点图（可能影响节点的生成方式）。
     * @return 包含所有根执行节点的执行节点图。
     */
    public ExecNodeGraph generate(List<FlinkPhysicalRel> relNodes, boolean isCompiled) {
        List<ExecNode<?>> rootNodes = new ArrayList<>(relNodes.size());
        // 遍历每个关系节点，生成对应的执行节点，并收集根节点
        for (FlinkPhysicalRel relNode : relNodes) {
            rootNodes.add(generate(relNode, isCompiled));
        }
        // 使用收集到的根节点创建执行节点图
        return new ExecNodeGraph(rootNodes);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 递归地生成执行节点。
     *
     * @param rel 当前正在处理的关系节点。
     * @param isCompiled 是否在编译模式下生成执行节点。
     * @return 生成的执行节点。
     */
    private ExecNode<?> generate(FlinkPhysicalRel rel, boolean isCompiled) {
        // 检查是否已访问过此关系节点
        ExecNode<?> execNode = visitedRels.get(rel);
        // 遍历每个关系节点，生成对应的执行节点，并收集根节点
        if (execNode != null) {
            // 如果已访问，则直接返回对应的执行节点
            return execNode;
        }
        // 如果遇到中间表扫描等特殊节点，则抛出异常（这些节点通常不应直接转换为执行节点）
        if (rel instanceof CommonIntermediateTableScan) {
            throw new TableException("Intermediate RelNode can't be converted to ExecNode.");
        }
        // 递归地生成输入节点的执行节点
        List<ExecNode<?>> inputNodes = new ArrayList<>();
        for (RelNode input : rel.getInputs()) {
            inputNodes.add(generate((FlinkPhysicalRel) input, isCompiled));
        }
        // 调用关系节点的translateToExecNode方法生成执行节点
        execNode = rel.translateToExecNode(isCompiled);
        // connects the input nodes
        // 连接输入节点到当前执行节点
        List<ExecEdge> inputEdges = new ArrayList<>(inputNodes.size());
        for (ExecNode<?> inputNode : inputNodes) {
            inputEdges.add(ExecEdge.builder().source(inputNode).target(execNode).build());
        }
        //设置输入边
        execNode.setInputEdges(inputEdges);
        // 将关系节点及其对应的执行节点添加到已访问映射中
        visitedRels.put(rel, execNode);
        return execNode;
    }
}
