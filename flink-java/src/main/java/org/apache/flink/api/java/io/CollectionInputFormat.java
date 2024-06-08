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

package org.apache.flink.api.java.io;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.io.GenericInputFormat;
import org.apache.flink.api.common.io.NonParallelInput;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.io.GenericInputSplit;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * An input format that returns objects from a collection.
 *
 * @deprecated All Flink DataSet APIs are deprecated since Flink 1.18 and will be removed in a
 *     future Flink major version. You can still build your application in DataSet, but you should
 *     move to either the DataStream and/or Table API.
 * @see <a href="https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=158866741">
 *     FLIP-131: Consolidate the user-facing Dataflow SDKs/APIs (and deprecate the DataSet API</a>
 */
@Deprecated
@PublicEvolving
public class CollectionInputFormat<T> extends GenericInputFormat<T> implements NonParallelInput {

    private static final long serialVersionUID = 1L;
    private static final int MAX_TO_STRING_LEN = 100;

    private TypeSerializer<T> serializer;

    // input data as collection. transient, because it will be serialized in a custom way
    private transient Collection<T> dataSet;

    private transient Iterator<T> iterator;

    public CollectionInputFormat(Collection<T> dataSet, TypeSerializer<T> serializer) {
        if (dataSet == null) {
            throw new NullPointerException();
        }

        this.serializer = serializer;

        this.dataSet = dataSet;
    }

    @Override
    public boolean reachedEnd() throws IOException {
        return !this.iterator.hasNext();
    }

    @Override
    public void open(GenericInputSplit split) throws IOException {
        super.open(split);

        this.iterator = this.dataSet.iterator();
    }

    @Override
    public T nextRecord(T record) throws IOException {
        return this.iterator.next();
    }

    // --------------------------------------------------------------------------------------------

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 将当前对象的状态写入 ObjectOutputStream 中。
     *
     * @param out 用于写入对象的 ObjectOutputStream 对象
     * @throws IOException 如果在写入过程中发生 I/O 错误
    */
    private void writeObject(ObjectOutputStream out) throws IOException {
        // 调用默认的对象序列化方法，用于序列化对象的非静态和非瞬态字段
        out.defaultWriteObject();
        // 获取 dataSet 的大小
        final int size = dataSet.size();
        // 将 dataSet 的大小写入输出流
        out.writeInt(size);
        // 如果 dataSet 的大小大于 0
        if (size > 0) {
            // 创建一个 DataOutputView 的包装器，用于将 ObjectOutputStream 转换为 DataOutputView 接口
            DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(out);
            // 遍历 dataSet 中的每个元素
            for (T element : dataSet) {
                // 使用 serializer 将元素序列化为字节，并写入 wrapper（即输出流）
                serializer.serialize(element, wrapper);
            }
        }
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 从 ObjectInputStream 中读取对象的状态。
     *
     * @param in 用于读取对象的 ObjectInputStream 对象
     * @throws IOException 如果在读取过程中发生 I/O 错误
     * @throws ClassNotFoundException 如果在读取过程中找不到类定义
    */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        // 调用默认的对象反序列化方法，用于反序列化对象的非静态和非瞬态字段
        in.defaultReadObject();
        // 读取并获取集合的长度
        int collectionLength = in.readInt();
        // 创建一个新的 ArrayList，并为其指定初始容量（即集合长度）
        List<T> list = new ArrayList<T>(collectionLength);
        // 如果集合长度大于 0
        if (collectionLength > 0) {
            try {
                // 创建一个 DataInputView 的包装器，用于将 ObjectInputStream 转换为 DataInputView 接口
                DataInputViewStreamWrapper wrapper = new DataInputViewStreamWrapper(in);
                // 遍历集合中的每个元素
                for (int i = 0; i < collectionLength; i++) {
                    // 使用 serializer 反序列化元素
                    T element = serializer.deserialize(wrapper);
                    // 将反序列化后的元素添加到列表中
                    list.add(element);
                }
            } catch (Throwable t) {
                // 如果在反序列化元素时发生异常，则抛出 IOException 并带上原始异常信息
                throw new IOException("Error while deserializing element from collection", t);
            }
        }
        // 将反序列化后的列表赋值给 dataSet 成员变量
        dataSet = list;
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append('[');

        int num = 0;
        for (T e : dataSet) {
            sb.append(e);
            if (num != dataSet.size() - 1) {
                sb.append(", ");
                if (sb.length() > MAX_TO_STRING_LEN) {
                    sb.append("...");
                    break;
                }
            }
            num++;
        }
        sb.append(']');
        return sb.toString();
    }

    // --------------------------------------------------------------------------------------------

    public static <X> void checkCollection(Collection<X> elements, Class<X> viewedAs) {
        if (elements == null || viewedAs == null) {
            throw new NullPointerException();
        }

        for (X elem : elements) {
            if (elem == null) {
                throw new IllegalArgumentException(
                        "The collection must not contain null elements.");
            }

            // The second part of the condition is a workaround for the situation that can arise
            // from eg.
            // "env.fromElements((),(),())"
            // In this situation, UnitTypeInfo.getTypeClass returns void.class (when we are in the
            // Java world), but
            // the actual objects that we will be working with, will be BoxedUnits.
            // Note: TypeInformationGenTest.testUnit tests this condition.
            if (!viewedAs.isAssignableFrom(elem.getClass())
                    && !(elem.getClass().toString().equals("class scala.runtime.BoxedUnit")
                            && viewedAs.equals(void.class))) {

                throw new IllegalArgumentException(
                        "The elements in the collection are not all subclasses of "
                                + viewedAs.getCanonicalName());
            }
        }
    }
}
