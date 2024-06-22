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

package org.apache.flink.table.planner.delegation;

import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.catalog.CatalogManager;
import org.apache.flink.table.catalog.UnresolvedIdentifier;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.expressions.ResolvedExpression;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.calcite.FlinkPlannerImpl;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.calcite.RexFactory;
import org.apache.flink.table.planner.calcite.SqlToRexConverter;
import org.apache.flink.table.planner.expressions.RexNodeExpression;
import org.apache.flink.table.planner.operations.SqlNodeToOperationConversion;
import org.apache.flink.table.planner.parse.CalciteParser;
import org.apache.flink.table.planner.parse.ExtendedParser;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.util.Preconditions;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.advise.SqlAdvisor;
import org.apache.calcite.sql.advise.SqlAdvisorValidator;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/** Implementation of {@link Parser} that uses Calcite. */
public class ParserImpl implements Parser {

    private final CatalogManager catalogManager;

    // we use supplier pattern here in order to use the most up to
    // date configuration. Users might change the parser configuration in a TableConfig in between
    // multiple statements parsing
    private final Supplier<FlinkPlannerImpl> validatorSupplier;
    private final Supplier<CalciteParser> calciteParserSupplier;
    private final RexFactory rexFactory;
    private static final ExtendedParser EXTENDED_PARSER = ExtendedParser.INSTANCE;

    public ParserImpl(
            CatalogManager catalogManager,
            Supplier<FlinkPlannerImpl> validatorSupplier,
            Supplier<CalciteParser> calciteParserSupplier,
            RexFactory rexFactory) {
        this.catalogManager = catalogManager;
        this.validatorSupplier = validatorSupplier;
        this.calciteParserSupplier = calciteParserSupplier;
        this.rexFactory = rexFactory;
    }

    /**
     * When parsing statement, it first uses {@link ExtendedParser} to parse statements. If {@link
     * ExtendedParser} fails to parse statement, it uses the {@link CalciteParser} to parse
     * statements.
     *
     * @param statement input statement.
     * @return parsed operations.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 解析给定的 SQL 语句并返回操作列表。
     *
     * @param statement 要解析的 SQL 语句
     * @return 包含解析后操作的列表（通常为单个操作）
     * @throws TableException 如果解析失败或不支持查询
    */
    @Override
    public List<Operation> parse(String statement) {
        // 获取 Calcite 解析器实例
        CalciteParser parser = calciteParserSupplier.get();
        // 获取 Flink 规划器实例 用来校验层使用
        FlinkPlannerImpl planner = validatorSupplier.get();

        // 尝试使用扩展解析器解析 SQL 语句为命令
        Optional<Operation> command = EXTENDED_PARSER.parse(statement);
        if (command.isPresent()) {
            // 如果解析成功，直接返回包含单个命令的列表
            return Collections.singletonList(command.get());
        }

        // parse the sql query
        // use parseSqlList here because we need to support statement end with ';' in sql client.
        /**
         * 解析 SQL 查询（如果扩展解析器未返回结果）
         * 使用 parseSqlList 方法是因为我们需要支持 SQL 客户端中以分号结尾的语句
         */
        SqlNodeList sqlNodeList = parser.parseSqlList(statement);
        //其实内部只会有一个解析语句
        List<SqlNode> parsed = sqlNodeList.getList();
        // 验证只解析了一个语句
        Preconditions.checkArgument(parsed.size() == 1, "only single statement supported");
        /**
         * 将解析的 SqlNode 转换为 Operation
         * 如果转换失败，则抛出 TableException 异常
         */
        return Collections.singletonList(
                SqlNodeToOperationConversion.convert(planner, catalogManager, parsed.get(0))
                        .orElseThrow(() -> new TableException("Unsupported query: " + statement)));
    }

    @Override
    public UnresolvedIdentifier parseIdentifier(String identifier) {
        CalciteParser parser = calciteParserSupplier.get();
        SqlIdentifier sqlIdentifier = parser.parseIdentifier(identifier);
        return UnresolvedIdentifier.of(sqlIdentifier.names);
    }

    @Override
    public ResolvedExpression parseSqlExpression(
            String sqlExpression, RowType inputRowType, @Nullable LogicalType outputType) {
        try {
            final SqlToRexConverter sqlToRexConverter =
                    rexFactory.createSqlToRexConverter(inputRowType, outputType);
            final RexNode rexNode = sqlToRexConverter.convertToRexNode(sqlExpression);
            final LogicalType logicalType = FlinkTypeFactory.toLogicalType(rexNode.getType());
            // expand expression for serializable expression strings similar to views
            final String sqlExpressionExpanded = sqlToRexConverter.expand(sqlExpression);
            return new RexNodeExpression(
                    rexNode,
                    TypeConversions.fromLogicalToDataType(logicalType),
                    sqlExpression,
                    sqlExpressionExpanded);
        } catch (Throwable t) {
            throw new ValidationException(
                    String.format("Invalid SQL expression: %s", sqlExpression), t);
        }
    }

    public String[] getCompletionHints(String statement, int cursor) {
        List<String> candidates =
                new ArrayList<>(
                        Arrays.asList(EXTENDED_PARSER.getCompletionHints(statement, cursor)));

        // use sql advisor
        SqlAdvisorValidator validator = validatorSupplier.get().getSqlAdvisorValidator();
        SqlAdvisor advisor =
                new SqlAdvisor(validator, validatorSupplier.get().config().getParserConfig());
        String[] replaced = new String[1];

        List<String> sqlHints =
                advisor.getCompletionHints(statement, cursor, replaced).stream()
                        .map(item -> item.toIdentifier().toString())
                        .collect(Collectors.toList());

        candidates.addAll(sqlHints);

        return candidates.toArray(new String[0]);
    }

    public CatalogManager getCatalogManager() {
        return catalogManager;
    }
}
