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

package org.apache.flink.table.planner.catalog;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.ContextResolvedFunction;
import org.apache.flink.table.catalog.ContextResolvedProcedure;
import org.apache.flink.table.catalog.DataTypeFactory;
import org.apache.flink.table.catalog.FunctionCatalog;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.functions.AggregateFunctionDefinition;
import org.apache.flink.table.functions.BuiltInFunctionDefinition;
import org.apache.flink.table.functions.FunctionDefinition;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.functions.FunctionKind;
import org.apache.flink.table.functions.ScalarFunctionDefinition;
import org.apache.flink.table.functions.TableFunctionDefinition;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlAggFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction;
import org.apache.flink.table.planner.functions.bridging.BridgingSqlProcedure;
import org.apache.flink.table.planner.functions.utils.UserDefinedFunctionUtils;
import org.apache.flink.table.types.inference.TypeInference;
import org.apache.flink.table.types.inference.TypeStrategies;
import org.apache.flink.table.types.utils.TypeConversions;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Optional;

/** Thin adapter between {@link SqlOperatorTable} and {@link FunctionCatalog}. */
@Internal
public class FunctionCatalogOperatorTable implements SqlOperatorTable {

    private final FunctionCatalog functionCatalog;
    private final DataTypeFactory dataTypeFactory;
    private final FlinkTypeFactory typeFactory;
    private final RexFactory rexFactory;

    public FunctionCatalogOperatorTable(
            FunctionCatalog functionCatalog,
            DataTypeFactory dataTypeFactory,
            FlinkTypeFactory typeFactory,
            RexFactory rexFactory) {
        this.functionCatalog = functionCatalog;
        this.dataTypeFactory = dataTypeFactory;
        this.typeFactory = typeFactory;
        this.rexFactory = rexFactory;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 查找操作符重载。
     *
     * @param opName 操作符的名称。
     * @param category 函数或过程的类别。
     * @param syntax SQL语法，这里可能用于确定如何解析或处理操作符。
     * @param operatorList 用于存储找到的操作符的列表。
     * @param nameMatcher 名称匹配器，尽管在这个方法体内未直接使用，但可能用于更复杂的匹配逻辑
     */
    @Override
    public void lookupOperatorOverloads(
            SqlIdentifier opName,
            SqlFunctionCategory category,
            SqlSyntax syntax,
            List<SqlOperator> operatorList,
            SqlNameMatcher nameMatcher) {
        // 如果操作符名称是星号（*），则直接返回，因为星号通常用于通配符匹配，而不是特定操作符的查找。
        if (opName.isStar()) {
            return;
        }
        // 将SqlIdentifier转换为UnresolvedIdentifier，以便进行后续的查找操作。
        final UnresolvedIdentifier identifier = UnresolvedIdentifier.of(opName.names);
        // 根据函数类别处理查找逻辑
        if (category == SqlFunctionCategory.USER_DEFINED_PROCEDURE) {
            // 如果是用户定义的过程，则在函数目录中查找过程
            functionCatalog
                    .lookupProcedure(identifier)// 在函数目录中查找过程
                    .flatMap(this::convertToSqlProcedure)
                    .ifPresent(operatorList::add);
        } else {
            // 否则，在函数目录中查找函数
            functionCatalog
                    .lookupFunction(identifier)// 在函数目录中查找过程
                    .flatMap(resolvedFunction -> convertToSqlFunction(category, resolvedFunction))
                    .ifPresent(operatorList::add);
        }
    }

    private Optional<SqlFunction> convertToSqlProcedure(
            ContextResolvedProcedure resolvedProcedure) {
        return Optional.of(BridgingSqlProcedure.of(dataTypeFactory, resolvedProcedure));
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换SQLFunction
     */
    private Optional<SqlFunction> convertToSqlFunction(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        // 获取函数的定义
        final FunctionDefinition definition = resolvedFunction.getDefinition();
        // 获取函数的标识符，如果不存在则返回null
        final FunctionIdentifier identifier = resolvedFunction.getIdentifier().orElse(null);
        // legacy
        // 处理旧有逻辑
        // 如果函数定义是聚合函数定义
        if (definition instanceof AggregateFunctionDefinition) {
            // 转换为SQL聚合函数
            return convertAggregateFunction(identifier, (AggregateFunctionDefinition) definition);
        } else if (definition instanceof ScalarFunctionDefinition) {
            ScalarFunctionDefinition def = (ScalarFunctionDefinition) definition;
            // 如果函数定义是标量函数定义
            return convertScalarFunction(identifier, def);
        } else if (definition instanceof TableFunctionDefinition
                && category != null
                && category.isTableFunction()) {
            // 如果函数定义是表函数定义
            return convertTableFunction(identifier, (TableFunctionDefinition) definition);
        }
        // new stack
        // 处理新逻辑栈
        // 如果以上旧有逻辑都不匹配，则尝试使用新的方式转换函数
        return convertToBridgingSqlFunction(category, resolvedFunction);
    }

    private Optional<SqlFunction> convertToBridgingSqlFunction(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        final FunctionDefinition definition = resolvedFunction.getDefinition();

        if (!verifyFunctionKind(category, resolvedFunction)) {
            return Optional.empty();
        }

        final TypeInference typeInference;
        try {
            typeInference = definition.getTypeInference(dataTypeFactory);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format(
                            "An error occurred in the type inference logic of function '%s'.",
                            resolvedFunction),
                    t);
        }
        if (typeInference.getOutputTypeStrategy() == TypeStrategies.MISSING) {
            return Optional.empty();
        }

        final SqlFunction function;
        if (definition.getKind() == FunctionKind.AGGREGATE
                || definition.getKind() == FunctionKind.TABLE_AGGREGATE) {
            function =
                    BridgingSqlAggFunction.of(
                            dataTypeFactory,
                            typeFactory,
                            SqlKind.OTHER_FUNCTION,
                            resolvedFunction,
                            typeInference);
        } else {
            function =
                    BridgingSqlFunction.of(
                            dataTypeFactory,
                            typeFactory,
                            rexFactory,
                            SqlKind.OTHER_FUNCTION,
                            resolvedFunction,
                            typeInference);
        }
        return Optional.of(function);
    }

    /**
     * Verifies which kinds of functions are allowed to be returned from the catalog given the
     * context information.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 根据上下文信息验证从目录中返回的函数类型是否被允许。
     */
    private boolean verifyFunctionKind(
            @Nullable SqlFunctionCategory category, ContextResolvedFunction resolvedFunction) {
        final FunctionDefinition definition = resolvedFunction.getDefinition();

        // built-in functions without implementation are handled separately
       // 内置函数且没有运行时实现的情况需要单独处理
        if (definition instanceof BuiltInFunctionDefinition) {
            final BuiltInFunctionDefinition builtInFunction =
                    (BuiltInFunctionDefinition) definition;
            if (!builtInFunction.hasRuntimeImplementation()) {
                return false;
            }
        }
        // 获取函数的种类
        final FunctionKind kind = definition.getKind();
        // 如果是表函数，则直接允许
        if (kind == FunctionKind.TABLE) {
            return true;
            // 如果是标量函数、异步标量函数、聚合函数或表聚合函数
        } else if (kind == FunctionKind.SCALAR
                || kind == FunctionKind.ASYNC_SCALAR
                || kind == FunctionKind.AGGREGATE
                || kind == FunctionKind.TABLE_AGGREGATE) {
            // 如果指定了函数类别且该类别为表函数，但当前函数并非表函数，则抛出异常
            if (category != null && category.isTableFunction()) {
                throw new ValidationException(
                        String.format(
                                "Function '%s' cannot be used as a table function.",
                                resolvedFunction));
            }
            // 否则，这些函数类型是被允许的
            return true;
        }
        // 其他函数种类不被允许
        return false;
    }

    private Optional<SqlFunction> convertAggregateFunction(
            FunctionIdentifier identifier, AggregateFunctionDefinition functionDefinition) {
        SqlFunction aggregateFunction =
                UserDefinedFunctionUtils.createAggregateSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getAggregateFunction(),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getResultTypeInfo()),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getAccumulatorTypeInfo()),
                        typeFactory);
        return Optional.of(aggregateFunction);
    }

    private Optional<SqlFunction> convertScalarFunction(
            FunctionIdentifier identifier, ScalarFunctionDefinition functionDefinition) {
        SqlFunction scalarFunction =
                UserDefinedFunctionUtils.createScalarSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getScalarFunction(),
                        typeFactory);
        return Optional.of(scalarFunction);
    }

    private Optional<SqlFunction> convertTableFunction(
            FunctionIdentifier identifier, TableFunctionDefinition functionDefinition) {
        SqlFunction tableFunction =
                UserDefinedFunctionUtils.createTableSqlFunction(
                        identifier,
                        identifier.toString(),
                        functionDefinition.getTableFunction(),
                        TypeConversions.fromLegacyInfoToDataType(
                                functionDefinition.getResultType()),
                        typeFactory);
        return Optional.of(tableFunction);
    }

    @Override
    public List<SqlOperator> getOperatorList() {
        throw new UnsupportedOperationException("This should never be called");
    }
}
