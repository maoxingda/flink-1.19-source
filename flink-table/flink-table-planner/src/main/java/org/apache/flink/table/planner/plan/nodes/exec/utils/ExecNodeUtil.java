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

package org.apache.flink.table.planner.plan.nodes.exec.utils;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.core.memory.ManagedMemoryUseCase;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorFactory;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.transformations.LegacySourceTransformation;
import org.apache.flink.streaming.api.transformations.OneInputTransformation;
import org.apache.flink.streaming.api.transformations.TwoInputTransformation;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** An Utility class that helps translating {@link ExecNode} to {@link Transformation}. */
public class ExecNodeUtil {
    /**
     * Sets {Transformation#declareManagedMemoryUseCaseAtOperatorScope(ManagedMemoryUseCase, int)}
     * using the given bytes for {@link ManagedMemoryUseCase#OPERATOR}.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 为指定的转换（Transformation）设置操作符范围内的托管内存使用场景（OPERATOR）的权重。
     * 这个方法根据给定的字节数来计算并设置权重，专门用于操作符的托管内存使用场景。
     *
     * @param <T> 转换的输入数据类型
     * @param transformation 要设置托管内存权重的转换对象
     * @param memoryBytes 要分配给转换的托管内存的大小（以字节为单位）
     */
    public static <T> void setManagedMemoryWeight(
            Transformation<T> transformation, long memoryBytes) {
        // 如果分配的内存字节数大于0，则进行计算和设置
        if (memoryBytes > 0) {
            // 将字节数转换为兆字节（Mebibyte，注意这里不是Megabyte，但这里可能是个笔误，通常应视为MB，即1MB=1024*1024字节）
            // 这里为了简化计算，使用了位移操作（>> 20）来快速得到大约的MB数（实际为MiB，即1MiB=1024*1024字节）
            // 并且使用Math.max确保权重至少为1
            final int weightInMebibyte = Math.max(1, (int) (memoryBytes >> 20));
            // 尝试为OPERATOR托管内存使用场景设置权重
            // 注意：declareManagedMemoryUseCaseAtOperatorScope方法通常应该返回一个布尔值来表示设置是否成功，
            // 但这里假设它返回一个Optional<Integer>来表示之前的权重（这在实际Flink API中可能不是这样）。
            // 实际使用时，请根据Flink API的文档来调用正确的方法。
            final Optional<Integer> previousWeight =
                    transformation.declareManagedMemoryUseCaseAtOperatorScope(
                            ManagedMemoryUseCase.OPERATOR, weightInMebibyte);
            // 如果设置了权重，并且之前已经设置了权重（根据这个假设的返回值判断），则抛出异常
            if (previousWeight.isPresent()) {
                throw new TableException(
                        "Managed memory weight has been set, this should not happen.");
            }
        }
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            TransformationMetadata transformationMeta,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                transformationMeta,
                operator,
                outputType,
                parallelism,
                0,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                new TransformationMetadata(name, desc),
                operator,
                outputType,
                parallelism,
                0,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            TransformationMetadata transformationMeta,
            StreamOperator<O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                transformationMeta,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            TransformationMetadata transformationMeta,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                transformationMeta,
                operatorFactory,
                outputType,
                parallelism,
                0,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation}. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                new TransformationMetadata(name, desc),
                operatorFactory,
                outputType,
                parallelism,
                0,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        return createOneInputTransformation(
                input,
                new TransformationMetadata(name, desc),
                operatorFactory,
                outputType,
                parallelism,
                memoryBytes,
                parallelismConfigured);
    }

    /** Create a {@link OneInputTransformation} with memoryBytes. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建一个单输入转换（OneInputTransformation）的静态方法。
     * 这个方法用于构建Flink流处理中的转换操作，它接收一个输入转换（Transformation<I>），
     * 并基于这个输入和其他配置参数来创建一个新的转换。
     *
     * @param <I> 输入数据的类型
     * @param <O> 输出数据的类型
     * @param input 输入的转换，它定义了此转换的输入源
     * @param transformationMeta 转换的元数据，包含转换的名称等信息
     * @param operatorFactory 操作符工厂，用于创建处理输入数据的操作符
     * @param outputType 输出数据的类型信息，用于序列化和反序列化
     * @param parallelism 转换的并行度
     * @param memoryBytes 分配给此转换的内存大小（以字节为单位）
     * @param parallelismConfigured 是否已经明确配置了并行度
     */
    public static <I, O> OneInputTransformation<I, O> createOneInputTransformation(
            Transformation<I> input,
            TransformationMetadata transformationMeta,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        // 创建一个新的单输入转换实例
        OneInputTransformation<I, O> transformation =
                new OneInputTransformation<>(
                        input,
                        transformationMeta.getName(),
                        operatorFactory,
                        outputType,
                        parallelism,
                        parallelismConfigured);
        // 设置转换的内存权重
        setManagedMemoryWeight(transformation, memoryBytes);
        // 使用转换的元数据填充转换实例
        transformationMeta.fill(transformation);
        // 返回构建好的转换实例
        return transformation;
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            TransformationMetadata transformationMeta,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            boolean parallelismConfigured) {
        return createTwoInputTransformation(
                input1,
                input2,
                transformationMeta,
                operator,
                outputType,
                parallelism,
                0,
                parallelismConfigured);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String desc,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism) {
        return createTwoInputTransformation(
                input1,
                input2,
                new TransformationMetadata(name, desc),
                operator,
                outputType,
                parallelism,
                0);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            TransformationMetadata transformationMeta,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        return createTwoInputTransformation(
                input1,
                input2,
                transformationMeta,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes);
    }

    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            TransformationMetadata transformationMeta,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        return createTwoInputTransformation(
                input1,
                input2,
                transformationMeta,
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes,
                parallelismConfigured);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <IN1, IN2, O> TwoInputTransformation<IN1, IN2, O> createTwoInputTransformation(
            Transformation<IN1> input1,
            Transformation<IN2> input2,
            String name,
            String desc,
            TwoInputStreamOperator<IN1, IN2, O> operator,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        return createTwoInputTransformation(
                input1,
                input2,
                new TransformationMetadata(name, desc),
                SimpleOperatorFactory.of(operator),
                outputType,
                parallelism,
                memoryBytes);
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <I1, I2, O> TwoInputTransformation<I1, I2, O> createTwoInputTransformation(
            Transformation<I1> input1,
            Transformation<I2> input2,
            TransformationMetadata transformationMeta,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes) {
        TwoInputTransformation<I1, I2, O> transformation =
                new TwoInputTransformation<>(
                        input1,
                        input2,
                        transformationMeta.getName(),
                        operatorFactory,
                        outputType,
                        parallelism);
        setManagedMemoryWeight(transformation, memoryBytes);
        transformationMeta.fill(transformation);
        return transformation;
    }

    public static <I1, I2, O> TwoInputTransformation<I1, I2, O> createTwoInputTransformation(
            Transformation<I1> input1,
            Transformation<I2> input2,
            TransformationMetadata transformationMeta,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        TwoInputTransformation<I1, I2, O> transformation =
                new TwoInputTransformation<>(
                        input1,
                        input2,
                        transformationMeta.getName(),
                        operatorFactory,
                        outputType,
                        parallelism,
                        parallelismConfigured);
        setManagedMemoryWeight(transformation, memoryBytes);
        transformationMeta.fill(transformation);
        return transformation;
    }

    /** Create a {@link TwoInputTransformation} with memoryBytes. */
    public static <I1, I2, O> TwoInputTransformation<I1, I2, O> createTwoInputTransformation(
            Transformation<I1> input1,
            Transformation<I2> input2,
            String name,
            String desc,
            StreamOperatorFactory<O> operatorFactory,
            TypeInformation<O> outputType,
            int parallelism,
            long memoryBytes,
            boolean parallelismConfigured) {
        return createTwoInputTransformation(
                input1,
                input2,
                new TransformationMetadata(name, desc),
                operatorFactory,
                outputType,
                parallelism,
                memoryBytes,
                parallelismConfigured);
    }

    /** Return description for multiple input node. */
    public static String getMultipleInputDescription(
            ExecNode<?> rootNode,
            List<ExecNode<?>> inputNodes,
            List<InputProperty> inputProperties) {
        String members =
                ExecNodePlanDumper.treeToString(rootNode, inputNodes, true).replace("\n", "\\n");
        StringBuilder sb = new StringBuilder();
        sb.append("MultipleInput(");
        List<String> readOrders =
                inputProperties.stream()
                        .map(InputProperty::getPriority)
                        .map(Object::toString)
                        .collect(Collectors.toList());
        boolean hasDiffReadOrder = readOrders.stream().distinct().count() > 1;
        if (hasDiffReadOrder) {
            sb.append("readOrder=[").append(String.join(",", readOrders)).append("], ");
        }
        sb.append("members=[\\n").append(members).append("]");
        sb.append(")");
        return sb.toString();
    }

    /**
     * The planner might have more information than expressed in legacy source transformations. This
     * enforces planner information about boundedness to the affected transformations.
     */
    public static void makeLegacySourceTransformationsBounded(Transformation<?> transformation) {
        if (transformation instanceof LegacySourceTransformation) {
            ((LegacySourceTransformation<?>) transformation).setBoundedness(Boundedness.BOUNDED);
        }
        transformation.getInputs().forEach(ExecNodeUtil::makeLegacySourceTransformationsBounded);
    }
}
