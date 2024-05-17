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

package org.apache.flink.streaming.runtime.io;

import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.Input;
import org.apache.flink.streaming.api.operators.KeyContextHandler;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingConsumer;

/** Utility class for creating record processor for {@link Input} {@link StreamOperator}. */
public class RecordProcessorUtils {

    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT = "setKeyContextElement";
    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT1 = "setKeyContextElement1";
    private static final String METHOD_SET_KEY_CONTEXT_ELEMENT2 = "setKeyContextElement2";

    /**
     * Get record processor for {@link Input}, which will omit call of {@link
     * Input#setKeyContextElement} if it doesn't have key context.
     *
     * @param input the {@link Input}
     * @return the record processor
     */
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor(
            Input<T> input) {
        boolean canOmitSetKeyContext;
        // 检查输入的Input对象是否是AbstractStreamOperator或其子类的实例
        if (input instanceof AbstractStreamOperator) {
            // 如果是，则调用辅助方法来判断是否可以省略设置键上下文
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) input, 0);
        } else {
            // 如果不是，检查输入对象是否实现了KeyContextHandler接口，并且当前没有键上下文
            canOmitSetKeyContext =
                    input instanceof KeyContextHandler
                            && !((KeyContextHandler) input).hasKeyContext();
        }
        // 根据是否可以设置键上下文的操作，返回不同的处理函数
        if (canOmitSetKeyContext) {
            //返回Lambda表达式，也就是触发StreamOperator中的processElement
            return input::processElement;
        } else {
            // 如果不能省略，返回一个Lambda表达式，该表达式在处理StreamRecord之前先设置键上下文
            // 然后再调用Input对象的processElement方法
            return record -> {
                input.setKeyContextElement(record);
                input.processElement(record);
            };
        }
    }

    /**
     * Get record processor for the first input of {@link TwoInputStreamOperator}, which will omit
     * call of {@link StreamOperator#setKeyContextElement1} if it doesn't have key context.
     *
     * @param operator the {@link TwoInputStreamOperator}
     * @return the record processor
     */
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor1(
            TwoInputStreamOperator<T, ?, ?> operator) {
        boolean canOmitSetKeyContext;
        if (operator instanceof AbstractStreamOperator) {
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) operator, 0);
        } else {
            canOmitSetKeyContext =
                    operator instanceof KeyContextHandler
                            && !((KeyContextHandler) operator).hasKeyContext1();
        }

        if (canOmitSetKeyContext) {
            return operator::processElement1;
        } else {
            return record -> {
                operator.setKeyContextElement1(record);
                operator.processElement1(record);
            };
        }
    }

    /**
     * Get record processor for the second input of {@link TwoInputStreamOperator}, which will omit
     * call of {@link StreamOperator#setKeyContextElement2} if it doesn't have key context.
     *
     * @param operator the {@link TwoInputStreamOperator}
     * @return the record processor
     */
    public static <T> ThrowingConsumer<StreamRecord<T>, Exception> getRecordProcessor2(
            TwoInputStreamOperator<?, T, ?> operator) {
        boolean canOmitSetKeyContext;
        if (operator instanceof AbstractStreamOperator) {
            canOmitSetKeyContext = canOmitSetKeyContext((AbstractStreamOperator<?>) operator, 1);
        } else {
            canOmitSetKeyContext =
                    operator instanceof KeyContextHandler
                            && !((KeyContextHandler) operator).hasKeyContext2();
        }

        if (canOmitSetKeyContext) {
            return operator::processElement2;
        } else {
            return record -> {
                operator.setKeyContextElement2(record);
                operator.processElement2(record);
            };
        }
    }

    private static boolean canOmitSetKeyContext(
            AbstractStreamOperator<?> streamOperator, int input) {
        // Since AbstractStreamOperator is @PublicEvolving, we need to check whether the
        // "SetKeyContextElement" is overridden by the (user-implemented) subclass. If it is
        // overridden, we cannot omit it due to the subclass may maintain different key selectors on
        // its own.
        return !hasKeyContext(streamOperator, input)
                && !methodSetKeyContextIsOverridden(streamOperator, input);
    }

    private static boolean hasKeyContext(AbstractStreamOperator<?> operator, int input) {
        if (input == 0) {
            return operator.hasKeyContext1();
        } else {
            return operator.hasKeyContext2();
        }
    }

    private static boolean methodSetKeyContextIsOverridden(
            AbstractStreamOperator<?> operator, int input) {
        if (input == 0) {
            if (operator instanceof OneInputStreamOperator) {
                return methodIsOverridden(
                                operator,
                                OneInputStreamOperator.class,
                                METHOD_SET_KEY_CONTEXT_ELEMENT,
                                StreamRecord.class)
                        || methodIsOverridden(
                                operator,
                                AbstractStreamOperator.class,
                                METHOD_SET_KEY_CONTEXT_ELEMENT1,
                                StreamRecord.class);
            } else {
                return methodIsOverridden(
                        operator,
                        AbstractStreamOperator.class,
                        METHOD_SET_KEY_CONTEXT_ELEMENT1,
                        StreamRecord.class);
            }
        } else {
            return methodIsOverridden(
                    operator,
                    AbstractStreamOperator.class,
                    METHOD_SET_KEY_CONTEXT_ELEMENT2,
                    StreamRecord.class);
        }
    }

    private static boolean methodIsOverridden(
            AbstractStreamOperator<?> operator,
            Class<?> expectedDeclaringClass,
            String methodName,
            Class<?>... parameterTypes) {
        try {
            Class<?> methodDeclaringClass =
                    operator.getClass().getMethod(methodName, parameterTypes).getDeclaringClass();
            return methodDeclaringClass != expectedDeclaringClass;
        } catch (NoSuchMethodException exception) {
            throw new FlinkRuntimeException(
                    String.format(
                            "BUG: Can't find '%s' method in '%s'",
                            methodName, operator.getClass()));
        }
    }

    /** Private constructor to prevent instantiation. */
    private RecordProcessorUtils() {}
}
