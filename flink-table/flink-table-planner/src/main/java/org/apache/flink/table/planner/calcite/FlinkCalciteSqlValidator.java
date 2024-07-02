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

package org.apache.flink.table.planner.calcite;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.api.config.TableConfigOptions;
import org.apache.flink.table.api.config.TableConfigOptions.ColumnExpansionStrategy;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.catalog.CatalogSchemaTable;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;
import org.apache.flink.table.types.logical.DecimalType;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAsOperator;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.IdentifierSnapshotNamespace;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorCatalogReader;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;

import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.calcite.sql.type.SqlTypeName.DECIMAL;
import static org.apache.flink.table.expressions.resolver.lookups.FieldReferenceLookup.includeExpandedColumn;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Extends Calcite's {@link SqlValidator} by Flink-specific behavior. */
@Internal
public final class FlinkCalciteSqlValidator extends SqlValidatorImpl {

    // Enables CallContext#getOutputDataType() when validating SQL expressions.
    private SqlNode sqlNodeForExpectedOutputType;
    private RelDataType expectedOutputType;

    private final RelOptCluster relOptCluster;

    private final RelOptTable.ToRelContext toRelContext;

    private final FrameworkConfig frameworkConfig;

    private final List<ColumnExpansionStrategy> columnExpansionStrategies;

    public FlinkCalciteSqlValidator(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            SqlValidator.Config config,
            RelOptTable.ToRelContext toRelcontext,
            RelOptCluster relOptCluster,
            FrameworkConfig frameworkConfig) {
        super(opTab, catalogReader, typeFactory, config);
        this.relOptCluster = relOptCluster;
        this.toRelContext = toRelcontext;
        this.frameworkConfig = frameworkConfig;
        this.columnExpansionStrategies =
                ShortcutUtils.unwrapTableConfig(relOptCluster)
                        .get(TableConfigOptions.TABLE_COLUMN_EXPANSION_STRATEGY);
    }

    public void setExpectedOutputType(SqlNode sqlNode, RelDataType expectedOutputType) {
        this.sqlNodeForExpectedOutputType = sqlNode;
        this.expectedOutputType = expectedOutputType;
    }

    public Optional<RelDataType> getExpectedOutputType(SqlNode sqlNode) {
        if (sqlNode == sqlNodeForExpectedOutputType) {
            return Optional.of(expectedOutputType);
        }
        return Optional.empty();
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        if (literal.getTypeName() == DECIMAL) {
            final BigDecimal decimal = literal.getValueAs(BigDecimal.class);
            if (decimal.precision() > DecimalType.MAX_PRECISION) {
                throw newValidationError(
                        literal, Static.RESOURCE.numberLiteralOutOfRange(decimal.toString()));
            }
        }
        super.validateLiteral(literal);
    }

    @Override
    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        // Due to the improper translation of lateral table left outer join in Calcite, we need to
        // temporarily forbid the common predicates until the problem is fixed (see FLINK-7865).
        if (join.getJoinType() == JoinType.LEFT
                && SqlUtil.stripAs(join.getRight()).getKind() == SqlKind.COLLECTION_TABLE) {
            SqlNode right = SqlUtil.stripAs(join.getRight());
            if (right instanceof SqlBasicCall) {
                SqlBasicCall call = (SqlBasicCall) right;
                SqlNode operand0 = call.operand(0);
                if (operand0 instanceof SqlBasicCall
                        && ((SqlBasicCall) operand0).getOperator()
                                instanceof org.apache.calcite.sql.SqlWindowTableFunction) {
                    return;
                }
            }
            final SqlNode condition = join.getCondition();
            if (condition != null
                    && (!SqlUtil.isLiteral(condition)
                            || ((SqlLiteral) condition).getValueAs(Boolean.class)
                                    != Boolean.TRUE)) {
                throw new ValidationException(
                        String.format(
                                "Left outer joins with a table function do not accept a predicate such as %s. "
                                        + "Only literal TRUE is accepted.",
                                condition));
            }
        }
        super.validateJoin(join, scope);
    }

    @Override
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        // we don't support column lists and translate them into the unknown type in the type
        // factory,
        // this makes it possible to ignore them in the validator and fall back to regular row types
        // see also SqlFunction#deriveType
    }

    @Override
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable) {

        // 为时间旅行场景生成一个新的验证器命名空间。
        // 因为时间旅行仅支持常量表达式，我们需要确保快照的时间段不是一个标识符。

        // Generate a new validator namespace for time travel scenario.
        // Since time travel only supports constant expressions, we need to ensure that the period
        // of
        // snapshot is not an identifier.

        Optional<SqlSnapshot> snapshot = getSnapShotNode(ns);
        // 如果父作用域存在、快照存在，并且快照的时间段不是一个SqlIdentifier类型的节点
        if (usingScope != null
                && snapshot.isPresent()
                && !(snapshot.get().getPeriod() instanceof SqlIdentifier)) {
            // 获取快照对象
            SqlSnapshot sqlSnapshot = snapshot.get();
            // 获取快照的时间段节点
            SqlNode periodNode = sqlSnapshot.getPeriod();
            // 创建一个SqlToRelConverter对象，用于将SqlNode转换为RexNode
            SqlToRelConverter sqlToRelConverter = this.createSqlToRelConverter();
            // 将时间段节点转换为RexNode
            RexNode rexNode = sqlToRelConverter.convertExpression(periodNode);
            // 对RexNode进行简化
            RexNode simplifiedRexNode =
                    FlinkRexUtil.simplify(
                            sqlToRelConverter.getRexBuilder(),
                            rexNode,
                            relOptCluster.getPlanner().getExecutor());
            // 用于存储简化后的RexNode的列表
            List<RexNode> reducedNodes = new ArrayList<>();
            // 使用优化器的执行器对简化后的RexNode进行进一步减少（可能是消除冗余操作）
            relOptCluster
                    .getPlanner()
                    .getExecutor()
                    .reduce(
                            relOptCluster.getRexBuilder(),
                            Collections.singletonList(simplifiedRexNode),
                            reducedNodes);
            // check whether period is the unsupported expression
            // 检查时间段是否是不支持的表达式（即，是否不能被简化为常量）
            if (!(reducedNodes.get(0) instanceof RexLiteral)) {
                // 如果时间段表达式不能被简化为一个常量，则抛出异常
                throw new UnsupportedOperationException(
                        String.format(
                                "Unsupported time travel expression: %s for the expression can not be reduced to a constant by Flink.",
                                periodNode));
            }
            // 从简化的节点列表中获取第一个RexNode，并假设它是一个RexLiteral（常量）
            RexLiteral rexLiteral = (RexLiteral) (reducedNodes).get(0);
            // 将RexLiteral的值尝试作为TimestampString类型获取
           // TimestampString可能是Flink内部用于表示时间戳的字符串类
            TimestampString timestampString = rexLiteral.getValueAs(TimestampString.class);

            // 检查是否成功获取了有效的TimestampString
            checkNotNull(
                    timestampString,
                    "The time travel expression %s can not reduce to a valid timestamp string. This is a bug. Please file an issue.",
                    periodNode);
            // 从relOptCluster的上下文中获取TableConfig
            // TableConfig包含了Flink表操作的一些配置信息
            TableConfig tableConfig = ShortcutUtils.unwrapContext(relOptCluster).getTableConfig();
            // 从TableConfig中获取本地时区设置
            ZoneId zoneId = tableConfig.getLocalTimeZone();

            // 将TimestampString转换为毫秒时间戳，并转换为本地时区对应的LocalDateTime
           // 然后将其转换为Instant，并再次转换为毫秒时间戳
            long timeTravelTimestamp =
                    TimestampData.fromEpochMillis(timestampString.getMillisSinceEpoch())
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();

            // 使用时间戳创建一个SchemaVersion对象
            // SchemaVersion可能表示数据库或表在某个时间点的快照版本
            SchemaVersion schemaVersion = TimestampSchemaVersion.of(timeTravelTimestamp);
            // 将其转换为IdentifierSnapshotNamespace，表示一个带有时间旅行快照的命名空间
           // 同时传入schemaVersion和usingScope的父作用域作为参数
            IdentifierNamespace identifierNamespace = (IdentifierNamespace) ns;
            ns =
                    new IdentifierSnapshotNamespace(
                            identifierNamespace,
                            schemaVersion,
                            ((DelegatingScope) usingScope).getParent());
            // 更新sqlSnapshot的第二个操作数（索引为1，因为索引可能从0开始）
            sqlSnapshot.setOperand(
                    1,
                    SqlLiteral.createTimestamp(
                            timestampString,
                            rexLiteral.getType().getPrecision(),
                            sqlSnapshot.getPeriod().getParserPosition()));
        }
        // 调用父类的registerNamespace方法，注册更新后的命名空间和其他相关信息
        super.registerNamespace(usingScope, alias, ns, forceNullable);
    }

    /**
     * Get the {@link SqlSnapshot} node in a {@link SqlValidatorNamespace}.
     *
     * <p>In general, if there is a snapshot expression, the enclosing node of IdentifierNamespace
     * is usually SqlSnapshot. However, if we encounter a situation with an "as" operator, we need
     * to identify whether the enclosingNode is an "as" call and if its first operand is
     * SqlSnapshot.
     *
     * @param ns The namespace used to find SqlSnapshot
     * @return SqlSnapshot found in {@param ns}, empty if not found
     */
    private Optional<SqlSnapshot> getSnapShotNode(SqlValidatorNamespace ns) {
        if (ns instanceof IdentifierNamespace) {
            SqlNode enclosingNode = ns.getEnclosingNode();
            // FOR SYSTEM_TIME AS OF [expression]
            if (enclosingNode instanceof SqlSnapshot) {
                return Optional.of((SqlSnapshot) enclosingNode);
                // FOR SYSTEM_TIME AS OF [expression] as [identifier]
            } else if (enclosingNode instanceof SqlBasicCall
                    && ((SqlBasicCall) enclosingNode).getOperator() instanceof SqlAsOperator
                    && ((SqlBasicCall) enclosingNode).getOperandList().get(0)
                            instanceof SqlSnapshot) {
                return Optional.of(
                        (SqlSnapshot) ((SqlBasicCall) enclosingNode).getOperandList().get(0));
            }
        }
        return Optional.empty();
    }

    private SqlToRelConverter createSqlToRelConverter() {
        return new SqlToRelConverter(
                toRelContext,
                this,
                this.getCatalogReader().unwrap(CalciteCatalogReader.class),
                relOptCluster,
                frameworkConfig.getConvertletTable(),
                frameworkConfig.getSqlToRelConverterConfig());
    }

    @Override
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            boolean includeSystemVars) {
        // Extract column's origin to apply strategy
        if (!columnExpansionStrategies.isEmpty() && exp instanceof SqlIdentifier) {
            final SqlQualified qualified = scope.fullyQualify((SqlIdentifier) exp);
            if (qualified.namespace != null && qualified.namespace.getTable() != null) {
                final CatalogSchemaTable schemaTable =
                        (CatalogSchemaTable) qualified.namespace.getTable().table();
                final ResolvedSchema resolvedSchema =
                        schemaTable.getContextResolvedTable().getResolvedSchema();
                final String columnName = qualified.suffix().get(0);
                final Column column = resolvedSchema.getColumn(columnName).orElse(null);
                if (qualified.suffix().size() == 1 && column != null) {
                    if (includeExpandedColumn(column, columnExpansionStrategies)
                            || declaredDescriptorColumn(scope, column)) {
                        super.addToSelectList(
                                list, aliases, fieldList, exp, scope, includeSystemVars);
                    }
                    return;
                }
            }
        }

        // Always add to list
        super.addToSelectList(list, aliases, fieldList, exp, scope, includeSystemVars);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 首先对传入的 SQL 执行无条件表达式重写。将表达式树转换为标准形式
     */
    @Override
    protected @PolyNull SqlNode performUnconditionalRewrites(
            @PolyNull SqlNode node, boolean underFrom) {

        // Special case for window TVFs like:
        // TUMBLE(TABLE t, DESCRIPTOR(metadata_virtual), INTERVAL '1' MINUTE))
        //
        // "TABLE t" is translated into an implicit "SELECT * FROM t". This would ignore columns
        // that are not expanded by default. However, the descriptor explicitly states the need
        // for this column. Therefore, explicit table expressions (for window TVFs at most one)
        // are captured before rewriting and replaced with a "marker" SqlSelect that contains the
        // descriptor information. The "marker" SqlSelect is considered during column expansion.

        // 对于窗口表值函数（TVF）的特殊情况，例如：
        // TUMBLE(TABLE t, DESCRIPTOR(metadata_virtual), INTERVAL '1' MINUTE)
        //
        // 在这种情况下，"TABLE t" 被翻译成一个隐式的 "SELECT * FROM t"。但是，这样的隐式选择可能会忽略默认不会扩展的列。
        // 然而，描述符明确指定了需要这个列。因此，在重写之前，会捕获窗口 TVF 中的显式表表达式（最多一个），
        // 并用一个包含描述符信息的“标记”SqlSelect替换它。这个“标记”SqlSelect 在列扩展时会被考虑。

        final List<SqlIdentifier> explicitTableArgs = getExplicitTableOperands(node);

        // 调用父类的无条件重写方法，可能进行了一些通用的重写或验证
        final SqlNode rewritten = super.performUnconditionalRewrites(node, underFrom);

        // 如果节点不是 SqlBasicCall 类型的，则直接返回重写后的节点
        if (!(node instanceof SqlBasicCall)) {
            return rewritten;
        }
        // 将节点强制转换为 SqlBasicCall 类型，并获取其操作符
        final SqlBasicCall call = (SqlBasicCall) node;
        final SqlOperator operator = call.getOperator();
        // 如果操作符是 SqlWindowTableFunction 类型（例如 TUMBLE、HOP 等窗口表函数）
        if (operator instanceof SqlWindowTableFunction) {
            // 如果所有明确指定的表参数都是 null，则不需要进一步处理，直接返回重写后的节点
            if (explicitTableArgs.stream().allMatch(Objects::isNull)) {
                return rewritten;
            }
            // 提取出所有描述符（可能是别名、列名等）
            final List<SqlIdentifier> descriptors =
                    call.getOperandList().stream()
                            .flatMap(FlinkCalciteSqlValidator::extractDescriptors)
                            .collect(Collectors.toList());
            // 遍历操作数列表
            for (int i = 0; i < call.operandCount(); i++) {
                final SqlIdentifier tableArg = explicitTableArgs.get(i);
                // 如果当前操作数是明确指定的表参数
                if (tableArg != null) {
                    // 创建一个 ExplicitTableSqlSelect 节点，用于替换原始的操作数
                    final SqlNode opReplacement = new ExplicitTableSqlSelect(tableArg, descriptors);
                    if (call.operand(i).getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
                        // for TUMBLE(DATA => TABLE t3, ...)
                        // 判断当前操作数是否是 ARGUMENT_ASSIGNMENT 类型的（例如 DATA => TABLE t3）
                        final SqlCall assignment = call.operand(i);
                        // 如果是，则将 ARGUMENT_ASSIGNMENT 的第一个操作数替换为 ExplicitTableSqlSelect
                        assignment.setOperand(0, opReplacement);
                    } else {
                        // for TUMBLE(TABLE t3, ...)
                        // 如果不是 ARGUMENT_ASSIGNMENT 类型，则直接替换操作数
                        call.setOperand(i, opReplacement);
                    }
                }
                // for TUMBLE([DATA =>] SELECT ..., ...)
            }
        }
        // 返回重写后的节点
        return rewritten;
    }

    // --------------------------------------------------------------------------------------------
    // Column expansion
    // --------------------------------------------------------------------------------------------

    /**
     * A special {@link SqlSelect} to capture the origin of a {@link SqlKind#EXPLICIT_TABLE} within
     * TVF operands.
     */
    private static class ExplicitTableSqlSelect extends SqlSelect {

        private final List<SqlIdentifier> descriptors;

        public ExplicitTableSqlSelect(SqlIdentifier table, List<SqlIdentifier> descriptors) {
            super(
                    SqlParserPos.ZERO,
                    null,
                    SqlNodeList.of(SqlIdentifier.star(SqlParserPos.ZERO)),
                    table,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null,
                    null);
            this.descriptors = descriptors;
        }
    }

    /**
     * Returns whether the given column has been declared in a {@link SqlKind#DESCRIPTOR} next to a
     * {@link SqlKind#EXPLICIT_TABLE} within TVF operands.
     */
    private static boolean declaredDescriptorColumn(SelectScope scope, Column column) {
        if (!(scope.getNode() instanceof ExplicitTableSqlSelect)) {
            return false;
        }
        final ExplicitTableSqlSelect select = (ExplicitTableSqlSelect) scope.getNode();
        return select.descriptors.stream()
                .map(SqlIdentifier::getSimple)
                .anyMatch(id -> id.equals(column.getName()));
    }

    /**
     * Returns all {@link SqlKind#EXPLICIT_TABLE} operands within TVF operands. A list entry is
     * {@code null} if the operand is not an {@link SqlKind#EXPLICIT_TABLE}.
     */
    private static List<SqlIdentifier> getExplicitTableOperands(SqlNode node) {
        if (!(node instanceof SqlBasicCall)) {
            return null;
        }
        final SqlBasicCall call = (SqlBasicCall) node;

        if (!(call.getOperator() instanceof SqlFunction)) {
            return null;
        }
        final SqlFunction function = (SqlFunction) call.getOperator();

        if (!isTableFunction(function)) {
            return null;
        }

        return call.getOperandList().stream()
                .map(FlinkCalciteSqlValidator::extractExplicitTable)
                .collect(Collectors.toList());
    }

    private static @Nullable SqlIdentifier extractExplicitTable(SqlNode op) {
        if (op.getKind() == SqlKind.EXPLICIT_TABLE) {
            final SqlBasicCall opCall = (SqlBasicCall) op;
            if (opCall.operandCount() == 1 && opCall.operand(0) instanceof SqlIdentifier) {
                // for TUMBLE(TABLE t3, ...)
                return opCall.operand(0);
            }
        } else if (op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // for TUMBLE(DATA => TABLE t3, ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractExplicitTable(opCall.operand(0));
        }
        return null;
    }

    private static Stream<SqlIdentifier> extractDescriptors(SqlNode op) {
        if (op.getKind() == SqlKind.DESCRIPTOR) {
            // for TUMBLE(..., DESCRIPTOR(col), ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return opCall.getOperandList().stream()
                    .filter(SqlIdentifier.class::isInstance)
                    .map(SqlIdentifier.class::cast);
        } else if (op.getKind() == SqlKind.ARGUMENT_ASSIGNMENT) {
            // for TUMBLE(..., TIMECOL => DESCRIPTOR(col), ...)
            final SqlBasicCall opCall = (SqlBasicCall) op;
            return extractDescriptors(opCall.operand(0));
        }
        return Stream.empty();
    }

    private static boolean isTableFunction(SqlFunction function) {
        return function instanceof SqlTableFunction
                || function.getFunctionType() == SqlFunctionCategory.USER_DEFINED_TABLE_FUNCTION;
    }
}
