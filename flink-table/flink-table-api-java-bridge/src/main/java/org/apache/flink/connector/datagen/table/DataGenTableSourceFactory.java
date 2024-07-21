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

package org.apache.flink.connector.datagen.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.configuration.ConfigOptions.key;
import static org.apache.flink.table.factories.FactoryUtil.CONNECTOR;

/**
 * Factory for creating configured instances of {@link DataGenTableSource} in a stream environment.
 */
@Internal
public class DataGenTableSourceFactory implements DynamicTableSourceFactory {

    public static final String IDENTIFIER = "datagen";

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return new HashSet<>();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        Set<ConfigOption<?>> options = new HashSet<>();
        options.add(DataGenConnectorOptions.ROWS_PER_SECOND);
        options.add(DataGenConnectorOptions.NUMBER_OF_ROWS);
        options.add(DataGenConnectorOptions.SOURCE_PARALLELISM);

        // Placeholder options
        options.add(DataGenConnectorOptions.FIELD_KIND);
        options.add(DataGenConnectorOptions.FIELD_MIN);
        options.add(DataGenConnectorOptions.FIELD_MAX);
        options.add(DataGenConnectorOptions.FIELD_MAX_PAST);
        options.add(DataGenConnectorOptions.FIELD_LENGTH);
        options.add(DataGenConnectorOptions.FIELD_START);
        options.add(DataGenConnectorOptions.FIELD_END);
        options.add(DataGenConnectorOptions.FIELD_NULL_RATE);
        options.add(DataGenConnectorOptions.FIELD_VAR_LEN);

        return options;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建一个DynamicTableSource实例的方法
     */
    @Override
    public DynamicTableSource createDynamicTableSource(Context context) {
        // 创建一个配置对象，用于存储表源的配置选项
        Configuration options = new Configuration();
        // 将上下文中的Catalog表的选项复制到配置对象中
        context.getCatalogTable().getOptions().forEach(options::setString);
        // 获取表的物理行数据类型
        DataType rowDataType = context.getPhysicalRowDataType();
        // 初始化字段生成器数组，其大小与行数据类型的字段数相同
        DataGenerator<?>[] fieldGenerators = new DataGenerator[DataType.getFieldCount(rowDataType)];
        // 存储可选配置选项的集合，用于后续验证
        Set<ConfigOption<?>> optionalOptions = new HashSet<>();

        // 获取行数据类型的字段名和字段数据类型列表
        List<String> fieldNames = DataType.getFieldNames(rowDataType);
        List<DataType> fieldDataTypes = DataType.getFieldDataTypes(rowDataType);
        // 遍历每个字段，为每个字段创建数据生成器
        for (int i = 0; i < fieldGenerators.length; i++) {
            String name = fieldNames.get(i); // 当前字段名
            DataType type = fieldDataTypes.get(i);// 当前字段的数据类型

            // 创建一个配置选项，用于指定字段的数据生成类型（如随机、常量等），并设置默认值
            ConfigOption<String> kind =
                    key(DataGenConnectorOptionsUtil.FIELDS
                                    + "."
                                    + name
                                    + "."
                                    + DataGenConnectorOptionsUtil.KIND)
                            .stringType()
                            .defaultValue(DataGenConnectorOptionsUtil.RANDOM);
            // 根据字段名、数据类型、配置选项（包括kind）和其他全局配置选项，创建数据生成器容器
            DataGeneratorContainer container =
                    createContainer(name, type, options.get(kind), options);
            // 从容器中获取实际的数据生成器
            fieldGenerators[i] = container.getGenerator();
            // 将当前字段的kind选项添加到可选配置选项集合中
            optionalOptions.add(kind);
            // 并将容器中的所有配置选项也添加到可选配置选项集合中
            optionalOptions.addAll(container.getOptions());
        }
        // 验证配置选项，确保所有必需的选项都已设置，并且没有未知的配置选项
        FactoryUtil.validateFactoryOptions(requiredOptions(), optionalOptions, options);
        // 创建一个集合，用于记录哪些配置选项已被消费（即已用于数据生成器的创建）
        Set<String> consumedOptionKeys = new HashSet<>();
        // 添加一些已知会被消费的配置选项键
        consumedOptionKeys.add(CONNECTOR.key()); // 连接器选项
        consumedOptionKeys.add(DataGenConnectorOptions.ROWS_PER_SECOND.key()); // 每秒行数
        consumedOptionKeys.add(DataGenConnectorOptions.NUMBER_OF_ROWS.key());// 总行数
        consumedOptionKeys.add(DataGenConnectorOptions.SOURCE_PARALLELISM.key());// 源并行度
        // 将所有可选配置选项的键也添加到已消费选项键的集合中
        optionalOptions.stream().map(ConfigOption::key).forEach(consumedOptionKeys::add);
        // 验证未使用的配置选项键，确保没有遗漏未处理的选项
        FactoryUtil.validateUnconsumedKeys(
                factoryIdentifier(), options.keySet(), consumedOptionKeys);
        // 从上下文中获取表的唯一标识符（如表名）
        String name = context.getObjectIdentifier().toString();
        // 创建一个DataGenTableSource实例，用于作为动态表源
        // 它接受以下参数：
        // - fieldGenerators: 字段生成器数组，用于生成表中的每一列数据
        // - name: 表的名称
        // - rowDataType: 表的物理行数据类型
        // - rowsPerSecond: 每秒生成的行数，从配置选项中获取
        // - numberOfRows: 总共要生成的行数，从配置选项中获取
        // - sourceParallelism: 源的并行度，从配置选项中获取，如果未设置则默认为null
        return new DataGenTableSource(
                fieldGenerators,
                name,
                rowDataType,
                options.get(DataGenConnectorOptions.ROWS_PER_SECOND),
                options.get(DataGenConnectorOptions.NUMBER_OF_ROWS),
                options.getOptional(DataGenConnectorOptions.SOURCE_PARALLELISM).orElse(null));
    }

    private DataGeneratorContainer createContainer(
            String name, DataType type, String kind, ReadableConfig options) {
        switch (kind) {
            case DataGenConnectorOptionsUtil.RANDOM:
                validateFieldOptions(name, type, options);
                return type.getLogicalType().accept(new RandomGeneratorVisitor(name, options));
            case DataGenConnectorOptionsUtil.SEQUENCE:
                return type.getLogicalType().accept(new SequenceGeneratorVisitor(name, options));
            default:
                throw new ValidationException("Unsupported generator kind: " + kind);
        }
    }

    private void validateFieldOptions(String name, DataType type, ReadableConfig options) {
        ConfigOption<Boolean> varLenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.VAR_LEN)
                        .booleanType()
                        .defaultValue(false);
        options.getOptional(varLenOption)
                .filter(option -> option)
                .ifPresent(
                        option -> {
                            LogicalType logicalType = type.getLogicalType();
                            if (!(logicalType instanceof VarCharType
                                    || logicalType instanceof VarBinaryType)) {
                                throw new ValidationException(
                                        String.format(
                                                "Only supports specifying '%s' option for variable-length types (VARCHAR/STRING/VARBINARY/BYTES). The type of field '%s' is not within this range.",
                                                DataGenConnectorOptions.FIELD_VAR_LEN.key(), name));
                            }
                        });

        ConfigOption<Integer> lenOption =
                key(DataGenConnectorOptionsUtil.FIELDS
                                + "."
                                + name
                                + "."
                                + DataGenConnectorOptionsUtil.LENGTH)
                        .intType()
                        .noDefaultValue();
        options.getOptional(lenOption)
                .ifPresent(
                        option -> {
                            LogicalType logicalType = type.getLogicalType();
                            if (logicalType instanceof CharType
                                    || logicalType instanceof BinaryType) {
                                throw new ValidationException(
                                        String.format(
                                                "Custom length for fixed-length type (CHAR/BINARY) field '%s' is not supported.",
                                                name));
                            }
                            if (logicalType instanceof VarCharType
                                    || logicalType instanceof VarBinaryType) {
                                int length =
                                        logicalType instanceof VarCharType
                                                ? ((VarCharType) logicalType).getLength()
                                                : ((VarBinaryType) logicalType).getLength();
                                if (option > length) {
                                    throw new ValidationException(
                                            String.format(
                                                    "Custom length '%d' for variable-length type (VARCHAR/STRING/VARBINARY/BYTES) field '%s' should be shorter than '%d' defined in the schema.",
                                                    option, name, length));
                                }
                            }
                        });
    }
}
