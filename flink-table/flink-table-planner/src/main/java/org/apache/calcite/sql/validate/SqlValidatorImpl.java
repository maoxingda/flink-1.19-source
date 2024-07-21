/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql.validate;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.function.Functions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.type.DynamicRecordType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.runtime.CalciteException;
import org.apache.calcite.runtime.Feature;
import org.apache.calcite.runtime.Resources;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.ModifiableViewTable;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAccessEnum;
import org.apache.calcite.sql.SqlAccessType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplain;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalLiteral;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.SqlTableFunction;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWindowTableFunction;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.TableCharacteristic;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.AssignableOperandTypeChecker;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlOperandTypeInference;
import org.apache.calcite.sql.type.SqlTypeCoercionRule;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.IdPair;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.implicit.TypeCoercion;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.Bug;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.ImmutableNullableList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Optionality;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.Static;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.apiguardian.api.API;
import org.checkerframework.checker.nullness.qual.KeyFor;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.checkerframework.checker.nullness.qual.PolyNull;
import org.checkerframework.dataflow.qual.Pure;
import org.slf4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCharset;
import static org.apache.calcite.sql.type.NonNullableAccessors.getCollation;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getCondition;
import static org.apache.calcite.sql.validate.SqlNonNullableAccessors.getTable;
import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Default implementation of {@link SqlValidator}, the class was copied over because of
 * CALCITE-4554f.
 *
 * <p>Lines 1958 ~ 1978, Flink improves error message for functions without appropriate arguments in
 * handleUnresolvedFunction at {@link SqlValidatorImpl#handleUnresolvedFunction}.
 *
 * <p>Lines 3736 ~ 3740, Flink improves Optimize the retrieval of sub-operands in SqlCall when using
 * NamedParameters at {@link SqlValidatorImpl#checkRollUp}.
 *
 * <p>Lines 5108 ~ 5121, Flink enables TIMESTAMP and TIMESTAMP_LTZ for system time period
 * specification type at {@link org.apache.calcite.sql.validate.SqlValidatorImpl#validateSnapshot}.
 *
 * <p>Lines 5465 ~ 5471, Flink enables TIMESTAMP and TIMESTAMP_LTZ for first orderBy column in
 * matchRecognize at {@link SqlValidatorImpl#validateMatchRecognize}.
 */
public class SqlValidatorImpl implements SqlValidatorWithHints {
    // ~ Static fields/initializers ---------------------------------------------

    public static final Logger TRACER = CalciteTrace.PARSER_LOGGER;

    /** Alias generated for the source table when rewriting UPDATE to MERGE. */
    public static final String UPDATE_SRC_ALIAS = "SYS$SRC";

    /**
     * Alias generated for the target table when rewriting UPDATE to MERGE if no alias was specified
     * by the user.
     */
    public static final String UPDATE_TGT_ALIAS = "SYS$TGT";

    /** Alias prefix generated for source columns when rewriting UPDATE to MERGE. */
    public static final String UPDATE_ANON_PREFIX = "SYS$ANON";

    private static final ExtraCalciteResource EXTRA_RESOURCE =
            Resources.create(ExtraCalciteResource.class);

    // ~ Instance fields --------------------------------------------------------

    private final SqlOperatorTable opTab;
    final SqlValidatorCatalogReader catalogReader;

    /**
     * Maps {@link SqlParserPos} strings to the {@link SqlIdentifier} identifier objects at these
     * positions.
     */
    protected final Map<String, IdInfo> idPositions = new HashMap<>();

    /**
     * Maps {@link SqlNode query node} objects to the {@link SqlValidatorScope} scope created from
     * them.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将{@link SqlNode query node}对象映射到由这些对象创建的{@link SqlValidatorScope}范围。
     * 存在一个映射关系，它将SQL查询的节点（如SELECT语句、FROM子句中的表等，这些节点由SqlNode接口表示）
     * 与基于这些节点创建的作用域（SqlValidatorScope）关联起来。
     * 这个作用域用于在SQL验证过程中解析和引用查询中的元素，确保SQL语句的语义正确性。
     * 比如我们查询的SqlSelect=>SelectScope,意识是将SQL查询节点(SELECT，这些节点由SqlNode表示) 与基于这个查询节点创建的作用域关联起来
     */
    protected final IdentityHashMap<SqlNode, SqlValidatorScope> scopes = new IdentityHashMap<>();

    /** Maps a {@link SqlSelect} and a clause to the scope used by that clause. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将一个{@link SqlSelect}（SQL选择语句）和一个子句映射到该子句所使用的作用域。
     * 存在一种映射关系，它能够将一个特定的SQL选择语句（SqlSelect）以及该选择语句中的一个子句（如WHERE子句、ORDER BY子句等）
     * 关联到一个特定的作用域（SqlValidatorScope）。这个作用域定义了在该子句中可以访问的元素（如表名、列名、别名等），
     * 以确保子句中的引用是有效的和正确的。
     * 比如SqlSelect=>（GROUP_BY、WHERE、SELECT）将我们的SQLSelect语句 条件语句Clause与对应的SqlValidatorScope作用域关联起来
     */
    private final Map<IdPair<SqlSelect, Clause>, SqlValidatorScope> clauseScopes = new HashMap<>();

    /** The name-resolution scope of a LATERAL TABLE clause. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * LATERAL TABLE 子句的名称解析范围
     * SqlNode节点中表明对应的TableScope
     */
    private @Nullable TableScope tableScope = null;

    /**
     * Maps a {@link SqlNode node} to the {@link SqlValidatorNamespace namespace} which describes
     * what columns they contain.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将一个{@link SqlNode 节点}映射到{@link SqlValidatorNamespace 命名空间}，该命名空间描述了该节点包含的列。
     * SqlNode对象（代表了SQL语句中的一个节点，比如一个表、一个子查询、一个函数等）被映射到一个SqlValidatorNamespace对象上。
     */
    protected final IdentityHashMap<SqlNode, SqlValidatorNamespace> namespaces =
            new IdentityHashMap<>();

    /**
     * Set of select expressions used as cursor definitions. In standard SQL, only the top-level
     * SELECT is a cursor; Calcite extends this with cursors as inputs to table functions.
     */
    private final Set<SqlNode> cursorSet = Sets.newIdentityHashSet();

    /**
     * Stack of objects that maintain information about function calls. A stack is needed to handle
     * nested function calls. The function call currently being validated is at the top of the
     * stack.
     */
    protected final Deque<FunctionParamInfo> functionCallStack = new ArrayDeque<>();

    private int nextGeneratedId;
    protected final RelDataTypeFactory typeFactory;

    /** The type of dynamic parameters until a type is imposed on them. */
    protected final RelDataType unknownType;

    private final RelDataType booleanType;

    /**
     * Map of derived RelDataType for each node. This is an IdentityHashMap since in some cases
     * (such as null literals) we need to discriminate by instance.
     */
    private final IdentityHashMap<SqlNode, RelDataType> nodeToTypeMap = new IdentityHashMap<>();

    /** Provides the data for {@link #getValidatedOperandTypes(SqlCall)}. */
    public final IdentityHashMap<SqlCall, List<RelDataType>> callToOperandTypesMap =
            new IdentityHashMap<>();

    private final AggFinder aggFinder;
    private final AggFinder aggOrOverFinder;
    private final AggFinder aggOrOverOrGroupFinder;
    private final AggFinder groupFinder;
    private final AggFinder overFinder;

    private Config config;

    private final Map<SqlNode, SqlNode> originalExprs = new HashMap<>();

    private @Nullable SqlNode top;

    // TODO jvs 11-Dec-2008:  make this local to performUnconditionalRewrites
    // if it's OK to expand the signature of that method.
    private boolean validatingSqlMerge;

    private boolean inWindow; // Allow nested aggregates

    private final SqlValidatorImpl.ValidationErrorFunction validationErrorFunction =
            new SqlValidatorImpl.ValidationErrorFunction();

    // TypeCoercion instance used for implicit type coercion.
    private TypeCoercion typeCoercion;

    // ~ Constructors -----------------------------------------------------------

    /**
     * Creates a validator.
     *
     * @param opTab Operator table
     * @param catalogReader Catalog reader
     * @param typeFactory Type factory
     * @param config Config
     */
    protected SqlValidatorImpl(
            SqlOperatorTable opTab,
            SqlValidatorCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            Config config) {
        this.opTab = requireNonNull(opTab, "opTab");
        this.catalogReader = requireNonNull(catalogReader, "catalogReader");
        this.typeFactory = requireNonNull(typeFactory, "typeFactory");
        this.config = requireNonNull(config, "config");

        unknownType = typeFactory.createUnknownType();
        booleanType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);

        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        aggFinder = new AggFinder(opTab, false, true, false, null, nameMatcher);
        aggOrOverFinder = new AggFinder(opTab, true, true, false, null, nameMatcher);
        overFinder = new AggFinder(opTab, true, false, false, aggOrOverFinder, nameMatcher);
        groupFinder = new AggFinder(opTab, false, false, true, null, nameMatcher);
        aggOrOverOrGroupFinder = new AggFinder(opTab, true, true, true, null, nameMatcher);
        @SuppressWarnings("argument.type.incompatible")
        TypeCoercion typeCoercion = config.typeCoercionFactory().create(typeFactory, this);
        this.typeCoercion = typeCoercion;
        if (config.typeCoercionRules() != null) {
            SqlTypeCoercionRule.THREAD_PROVIDERS.set(config.typeCoercionRules());
        }
    }

    // ~ Methods ----------------------------------------------------------------

    public SqlConformance getConformance() {
        return config.conformance();
    }

    @Pure
    @Override
    public SqlValidatorCatalogReader getCatalogReader() {
        return catalogReader;
    }

    @Pure
    @Override
    public SqlOperatorTable getOperatorTable() {
        return opTab;
    }

    @Pure
    @Override
    public RelDataTypeFactory getTypeFactory() {
        return typeFactory;
    }

    @Override
    public RelDataType getUnknownType() {
        return unknownType;
    }

    @Override
    public SqlNodeList expandStar(
            SqlNodeList selectList, SqlSelect select, boolean includeSystemVars) {
        final List<SqlNode> list = new ArrayList<>();
        final List<Map.Entry<String, RelDataType>> types = new ArrayList<>();
        for (int i = 0; i < selectList.size(); i++) {
            final SqlNode selectItem = selectList.get(i);
            final RelDataType originalType = getValidatedNodeTypeIfKnown(selectItem);
            expandSelectItem(
                    selectItem,
                    select,
                    Util.first(originalType, unknownType),
                    list,
                    catalogReader.nameMatcher().createSet(),
                    types,
                    includeSystemVars);
        }
        getRawSelectScopeNonNull(select).setExpandedSelectList(list);
        return new SqlNodeList(list, SqlParserPos.ZERO);
    }

    // implement SqlValidator
    @Override
    public void declareCursor(SqlSelect select, SqlValidatorScope parentScope) {
        cursorSet.add(select);

        // add the cursor to a map that maps the cursor to its select based on
        // the position of the cursor relative to other cursors in that call
        FunctionParamInfo funcParamInfo = requireNonNull(functionCallStack.peek(), "functionCall");
        Map<Integer, SqlSelect> cursorMap = funcParamInfo.cursorPosToSelectMap;
        int numCursors = cursorMap.size();
        cursorMap.put(numCursors, select);

        // create a namespace associated with the result of the select
        // that is the argument to the cursor constructor; register it
        // with a scope corresponding to the cursor
        SelectScope cursorScope = new SelectScope(parentScope, null, select);
        clauseScopes.put(IdPair.of(select, Clause.CURSOR), cursorScope);
        final SelectNamespace selectNs = createSelectNamespace(select, select);
        String alias = deriveAlias(select, nextGeneratedId++);
        registerNamespace(cursorScope, alias, selectNs, false);
    }

    // implement SqlValidator
    @Override
    public void pushFunctionCall() {
        FunctionParamInfo funcInfo = new FunctionParamInfo();
        functionCallStack.push(funcInfo);
    }

    // implement SqlValidator
    @Override
    public void popFunctionCall() {
        functionCallStack.pop();
    }

    // implement SqlValidator
    @Override
    public @Nullable String getParentCursor(String columnListParamName) {
        FunctionParamInfo funcParamInfo = requireNonNull(functionCallStack.peek(), "functionCall");
        Map<String, String> parentCursorMap = funcParamInfo.columnListParamToParentCursorMap;
        return parentCursorMap.get(columnListParamName);
    }

    /**
     * If <code>selectItem</code> is "*" or "TABLE.*", expands it and returns true; otherwise writes
     * the unexpanded item.
     *
     * @param selectItem Select-list item
     * @param select Containing select clause
     * @param selectItems List that expanded items are written to
     * @param aliases Set of aliases
     * @param fields List of field names and types, in alias order
     * @param includeSystemVars If true include system vars in lists
     * @return Whether the node was expanded
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 展开SelectItem进行校验，增加全限定名
     * eg:展开校验列字段
     * @param selectItem 选择列表中的项目
     * @param select 包含此选择项目的SELECT子句
     * @param targetType 目标类型，可能用于类型检查和转换
     * @param selectItems 展开后的项目将被写入此列表
     * @param aliases 别名集合，用于确定项目是否已有别名
     * @param fields 字段名和类型的列表，按别名顺序排列
     * @param includeSystemVars 如果为true，则在列表中包括系统变量
     * @return 是否展开了该节点
     */
    private boolean expandSelectItem(
            final SqlNode selectItem,
            SqlSelect select,
            RelDataType targetType,
            List<SqlNode> selectItems,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fields,
            final boolean includeSystemVars) {
        // 获取SELECT语句的作用域
        final SelectScope scope = (SelectScope) getWhereScope(select);
        //展开所有的字段
        if (expandStar(selectItems, aliases, fields, includeSystemVars, scope, selectItem)) {
            return true;
        }

        // Expand the select item: fully-qualify columns, and convert
        // parentheses-free functions such as LOCALTIME into explicit function
        // calls.
        // 展开选择项：完全限定列名，并将无括号的函数（如LOCALTIME）转换为显式函数调用
        SqlNode expanded = expandSelectExpr(selectItem, scope, select);
        // 推导别名，如果项目尚未有别名，则生成一个新的
        final String alias = deriveAliasNonNull(selectItem, aliases.size());

        // If expansion has altered the natural alias, supply an explicit 'AS'.
        // 如果展开后改变了自然别名，则提供一个显式的'AS'来指定别名
        final SqlValidatorScope selectScope = getSelectScope(select);
        // 如果展开后的节点与原始节点不同
        if (expanded != selectItem) {
            // 为展开后的节点推导新别名
            String newAlias = deriveAliasNonNull(expanded, aliases.size());
            // 如果新别名与原始别名不同
            if (!Objects.equals(newAlias, alias)) {
                // 使用SqlStdOperatorTable.AS创建带有显式别名的函数调用
                expanded =
                        SqlStdOperatorTable.AS.createCall(
                                selectItem.getParserPosition(),
                                expanded,
                                new SqlIdentifier(alias, SqlParserPos.ZERO));
                //基于作用域推导节点的类型
                deriveTypeImpl(selectScope, expanded);
            }
        }
         // 将展开（或未展开但已检查别名）的节点添加到选择列表中
        selectItems.add(expanded);
        // 将别名添加到别名集合中，以便后续检查
        aliases.add(alias);

        // 如果展开后的节点不为空，则进行未知类型的推断
       // 这可能涉及到基于上下文（如目标类型、作用域等）来推断节点的具体类型
        if (expanded != null) {
            inferUnknownTypes(targetType, scope, expanded);
        }
        // 推导展开后节点的数据类型
        RelDataType type = deriveType(selectScope, expanded);
        // Re-derive SELECT ITEM's data type that may be nullable in AggregatingSelectScope when it
        // appears in advanced grouping elements such as CUBE, ROLLUP , GROUPING SETS.
        // For example, SELECT CASE WHEN c = 1 THEN '1' ELSE '23' END AS x FROM t GROUP BY CUBE(x),
        // the 'x' should be nullable even if x's literal values are not null.
        // 如果当前作用域是聚合选择作用域（如CUBE、ROLLUP、GROUPING SETS等），
       // 则可能需要将某些列的类型标记为可为空，因为它们在聚合操作中可能会涉及到空值
        if (selectScope instanceof AggregatingSelectScope) {
            // 使用nullifyType方法（假设它是AggregatingSelectScope的一部分）
            // 来将节点（去除了AS部分，如果有的话）的类型标记为可为空
            // 并将结果类型赋值给type
            type = requireNonNull(selectScope.nullifyType(stripAs(expanded), type));
        }
        // 设置节点的验证类型和验证后的节点类型
        setValidatedNodeType(expanded, type);
        // 将别名和推导出的类型作为条目添加到字段列表中
        fields.add(Pair.of(alias, type));
        // 返回值通常取决于方法的设计目标，但在这个上下文中，返回false可能表示
        // 并没有因为"*"或"TABLE.*"的展开而特别改变选择列表的结构
        return false;
    }

    private static SqlNode expandExprFromJoin(
            SqlJoin join, SqlIdentifier identifier, @Nullable SelectScope scope) {
        if (join.getConditionType() != JoinConditionType.USING) {
            return identifier;
        }

        for (String name : SqlIdentifier.simpleNames((SqlNodeList) getCondition(join))) {
            if (identifier.getSimple().equals(name)) {
                final List<SqlNode> qualifiedNode = new ArrayList<>();
                for (ScopeChild child : requireNonNull(scope, "scope").children) {
                    if (child.namespace.getRowType().getFieldNames().indexOf(name) >= 0) {
                        final SqlIdentifier exp =
                                new SqlIdentifier(
                                        ImmutableList.of(child.name, name),
                                        identifier.getParserPosition());
                        qualifiedNode.add(exp);
                    }
                }

                assert qualifiedNode.size() == 2;
                final SqlNode finalNode =
                        SqlStdOperatorTable.AS.createCall(
                                SqlParserPos.ZERO,
                                SqlStdOperatorTable.COALESCE.createCall(
                                        SqlParserPos.ZERO,
                                        qualifiedNode.get(0),
                                        qualifiedNode.get(1)),
                                new SqlIdentifier(name, SqlParserPos.ZERO));
                return finalNode;
            }
        }

        // Only need to try to expand the expr from the left input of join
        // since it is always left-deep join.
        final SqlNode node = join.getLeft();
        if (node instanceof SqlJoin) {
            return expandExprFromJoin((SqlJoin) node, identifier, scope);
        } else {
            return identifier;
        }
    }

    /**
     * Returns the set of field names in the join condition specified by USING or implicitly by
     * NATURAL, de-duplicated and in order.
     */
    public @Nullable List<String> usingNames(SqlJoin join) {
        switch (join.getConditionType()) {
            case USING:
                SqlNodeList condition = (SqlNodeList) getCondition(join);
                List<String> simpleNames = SqlIdentifier.simpleNames(condition);
                return catalogReader.nameMatcher().distinctCopy(simpleNames);

            case NONE:
                if (join.isNatural()) {
                    return deriveNaturalJoinColumnList(join);
                }
                return null;

            default:
                return null;
        }
    }

    private List<String> deriveNaturalJoinColumnList(SqlJoin join) {
        return SqlValidatorUtil.deriveNaturalJoinColumnList(
                catalogReader.nameMatcher(),
                getNamespaceOrThrow(join.getLeft()).getRowType(),
                getNamespaceOrThrow(join.getRight()).getRowType());
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 扩展通用的列引用，以处理SQL选择语句中的列标识符。
     *
     * @param sqlSelect SQL选择语句对象。
     * @param selectItem 当前正在处理的选择项（列）。
     * @param scope 选择的作用域，可能包含额外的上下文信息，如当前查询的级别或嵌套情况。
     * @param validator SQL验证器实例，用于验证SQL语句的正确性。
     * @return 扩展后的SqlNode，如果不需要扩展则直接返回原始的selectItem。
     */
    private static SqlNode expandCommonColumn(
            SqlSelect sqlSelect,
            SqlNode selectItem,
            @Nullable SelectScope scope,
            SqlValidatorImpl validator) {
        // 如果selectItem不是SqlIdentifier类型（即不是简单的列名），则直接返回原对象
        if (!(selectItem instanceof SqlIdentifier)) {
            return selectItem;
        }

        // 获取SQL选择语句中的FROM子句部分
        final SqlNode from = sqlSelect.getFrom();
        // 如果FROM子句不是SqlJoin类型（即没有涉及到表的连接），则直接返回原对象
        if (!(from instanceof SqlJoin)) {
            return selectItem;
        }
        // 将selectItem转换为SqlIdentifier类型
        final SqlIdentifier identifier = (SqlIdentifier) selectItem;
        // 如果列标识符不是简单的（即包含多个部分，可能是完全限定的列名）
        if (!identifier.isSimple()) {
            // 则根据配置可能需要执行额外的验证
            if (!validator.config().conformance().allowQualifyingCommonColumn()) {
                // 执行对限定通用列的验证
                validateQualifiedCommonColumn((SqlJoin) from, identifier, scope, validator);
            }
            // 即使进行了验证，也直接返回原始的选择项
            return selectItem;
        }
        // 如果列标识符是简单的，并且FROM子句是SqlJoin类型，则尝试从连接中扩展表达式
        return expandExprFromJoin((SqlJoin) from, identifier, scope);
    }

    private static void validateQualifiedCommonColumn(
            SqlJoin join,
            SqlIdentifier identifier,
            @Nullable SelectScope scope,
            SqlValidatorImpl validator) {
        List<String> names = validator.usingNames(join);
        if (names == null) {
            // Not USING or NATURAL.
            return;
        }

        requireNonNull(scope, "scope");
        // First we should make sure that the first component is the table name.
        // Then check whether the qualified identifier contains common column.
        for (ScopeChild child : scope.children) {
            if (Objects.equals(child.name, identifier.getComponent(0).toString())) {
                if (names.contains(identifier.getComponent(1).toString())) {
                    throw validator.newValidationError(
                            identifier,
                            RESOURCE.disallowsQualifyingCommonColumn(identifier.toString()));
                }
            }
        }

        // Only need to try to validate the expr from the left input of join
        // since it is always left-deep join.
        final SqlNode node = join.getLeft();
        if (node instanceof SqlJoin) {
            validateQualifiedCommonColumn((SqlJoin) node, identifier, scope, validator);
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 展开所有字段
     * @param selectItems SQL查询的选择项列表
     * @param aliases   别名集合
     * @param fields 字段名和对应数据类型的列
     * @param includeSystemVars 是否包含系统变量
     * @param scope 查询的作用域
     * @param node 当前处理的SQL节点
     */
    private boolean expandStar(
            List<SqlNode> selectItems,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fields,
            boolean includeSystemVars,
            SelectScope scope,
            SqlNode node) {
        // 如果节点不是SqlIdentifier类型，则直接返回false
        if (!(node instanceof SqlIdentifier)) {
            return false;
        }
        final SqlIdentifier identifier = (SqlIdentifier) node;
        // 如果SqlIdentifier 中的name 为空，则直接返回false
        if (!identifier.isStar()) {
            return false;
        }
        // SqlIdentifier在SQL中的位置
        final SqlParserPos startPosition = identifier.getParserPosition();
        // 根据SqlIdentifier中names名称数量进行处理
        switch (identifier.names.size()) {
            case 1:
                boolean hasDynamicStruct = false;
                // 遍历当前作用域下的所有子作用域
                for (ScopeChild child : scope.children) {
                    // 记录当前fields列表的大小
                    final int before = fields.size();
                    // 检查子作用域对应的表是否是动态结构
                    if (child.namespace.getRowType().isDynamicStruct()) {
                        // 标记找到了动态结构表
                        hasDynamicStruct = true;
                        // don't expand star if the underneath table is dynamic.
                        // Treat this star as a special field in validation/conversion and
                        // wait until execution time to expand this star.
                        // 如果表是动态的，则不直接扩展星号，而是将星号作为一个特殊字段处理
                        // 等待执行时再进行扩展
                        final SqlNode exp =
                                new SqlIdentifier(
                                        ImmutableList.of(
                                                child.name, DynamicRecordType.DYNAMIC_STAR_PREFIX),
                                        startPosition);
                        // 将这个特殊情况添加到选择项列表中
                        addToSelectList(
                                selectItems, aliases, fields, exp, scope, includeSystemVars);
                        // 如果当前作用域的子作用域不是动态结构表，则进行以下处理
                    } else {
                        // 获取当前子作用域对应的FROM子句中的节点
                        final SqlNode from = SqlNonNullableAccessors.getNode(child);
                        // 获取该节点对应的命名空间（Namespace），用于后续的字段和类型查询
                        final SqlValidatorNamespace fromNs = getNamespaceOrThrow(from, scope);
                        // 获取该命名空间对应的行类型（即表结构）
                        final RelDataType rowType = fromNs.getRowType();
                        // 遍历行类型中的所有字段
                        for (RelDataTypeField field : rowType.getFieldList()) {
                            // 获取字段的名称
                            String columnName = field.getName();

                            // TODO: do real implicit collation here
                            // 创建一个新的SqlIdentifier来表示这个字段，用于后续的SQL节点构建
                            final SqlIdentifier exp =
                                    new SqlIdentifier(
                                            ImmutableList.of(child.name, columnName),
                                            startPosition);
                            // Don't add expanded rolled up columns
                            // 检查这个字段是否是一个rolled up的列，如果是，则不添加到选择项中
                            if (!isRolledUpColumn(exp, scope)) {
                                // 如果不是被卷起的列，则将其添加到选择项列表中，并可能进行字段的扩展
                                addOrExpandField(
                                        selectItems,
                                        aliases,
                                        fields,
                                        includeSystemVars,
                                        scope,
                                        exp,
                                        field);
                            }
                        }
                    }
                    // 如果当前子作用域是可空的,进行以下处理
                    if (child.nullable) {
                        // 遍历自“before”以来添加到fields列表中的所有字段
                        for (int i = before; i < fields.size(); i++) {
                            final Map.Entry<String, RelDataType> entry = fields.get(i);
                            final RelDataType type = entry.getValue();
                            // 如果字段的类型不是可空的，但子作用域是可空的，则修改该字段的类型为可空
                            if (!type.isNullable()) {
                                // 使用类型工厂创建一个新的类型，该类型与原始类型相同但具有可空性
                                fields.set(
                                        i,
                                        Pair.of(
                                                entry.getKey(),
                                                typeFactory.createTypeWithNullability(type, true)));
                            }
                        }
                    }
                }
                // If NATURAL JOIN or USING is present, move key fields to the front of
                // the list, per standard SQL. Disabled if there are dynamic fields.
                // 如果存在NATURAL JOIN或USING子句，则根据标准SQL将关键字段移动到列表的前面。
                // 如果存在动态字段，则此操作被禁用，或者如果CALCITE_2400_FIXED修复了相关bug，则启用此操作。
                if (!hasDynamicStruct || Bug.CALCITE_2400_FIXED) {
                    // 从当前作用域中获取FROM子句对应的节点
                    SqlNode from =
                            requireNonNull(
                                    scope.getNode().getFrom(),
                                    () -> "getFrom for " + scope.getNode());
                    // 使用Permute类（假设这是一个用于重新排列选择项的类）对selectItems和fields进行排列
                    new Permute(from, 0).permute(selectItems, fields);
                }
                // 返回true表示已处理
                return true;

            default:
                // 跳过最后一个标识符部分，获取前缀标识符（例如，对于"a.b.c.*"，前缀为"a.b"）
                final SqlIdentifier prefixId = identifier.skipLast(1);
                // 创建一个ResolvedImpl实例，用于存储解析结果
                final SqlValidatorScope.ResolvedImpl resolved =
                        new SqlValidatorScope.ResolvedImpl();
                // 获取名称匹配器，用于解析标识符
                final SqlNameMatcher nameMatcher = scope.validator.catalogReader.nameMatcher();
                // 解析前缀标识符，并尝试找到对应的命名空间或表
                scope.resolve(prefixId.names, nameMatcher, true, resolved);
                // 如果没有找到任何匹配项
                if (resolved.count() == 0) {
                    // e.g. "select s.t.* from e"
                    // or "select r.* from e"
                    // 抛出验证错误，指出未知标识符
                    throw newValidationError(
                            prefixId, RESOURCE.unknownIdentifier(prefixId.toString()));
                }
                // 获取解析到的唯一结果
                final RelDataType rowType = resolved.only().rowType();
                // 如果行类型是动态结构，则不展开星号（*），直接将其作为动态记录类型的一部分处理
                if (rowType.isDynamicStruct()) {
                    // don't expand star if the underneath table is dynamic.
                    // 添加动态星号到选择列表中，不展开其字段
                    addToSelectList(
                            selectItems,
                            aliases,
                            fields,
                            prefixId.plus(DynamicRecordType.DYNAMIC_STAR_PREFIX, startPosition),
                            scope,
                            includeSystemVars);
                    // 如果行类型是结构化类型
                } else if (rowType.isStruct()) {
                    for (RelDataTypeField field : rowType.getFieldList()) {
                        String columnName = field.getName();

                        // TODO: do real implicit collation here
                        addOrExpandField(
                                selectItems,
                                aliases,
                                fields,
                                includeSystemVars,
                                scope,
                                prefixId.plus(columnName, startPosition),
                                field);
                    }
                } else {
                    throw newValidationError(prefixId, RESOURCE.starRequiresRecordType());
                }
                return true;
        }
    }

    private SqlNode maybeCast(SqlNode node, RelDataType currentType, RelDataType desiredType) {
        return SqlTypeUtil.equalSansNullability(typeFactory, currentType, desiredType)
                ? node
                : SqlStdOperatorTable.CAST.createCall(
                        SqlParserPos.ZERO, node, SqlTypeUtil.convertTypeToSpec(desiredType));
    }

    private boolean addOrExpandField(
            List<SqlNode> selectItems,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fields,
            boolean includeSystemVars,
            SelectScope scope,
            SqlIdentifier id,
            RelDataTypeField field) {
        switch (field.getType().getStructKind()) {
            case PEEK_FIELDS:
            case PEEK_FIELDS_DEFAULT:
                final SqlNode starExp = id.plusStar();
                expandStar(selectItems, aliases, fields, includeSystemVars, scope, starExp);
                return true;

            default:
                addToSelectList(selectItems, aliases, fields, id, scope, includeSystemVars);
        }

        return false;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *
     * 验证 SQL 节点
     * @param topNode 需要验证的 SQL 节点
     * @return 验证后的 SQL 节点
     */
    @Override
    public SqlNode validate(SqlNode topNode) {
        // 创建一个空的 SqlValidatorScope 对象，用于存储验证过程中的作用域信息
        SqlValidatorScope scope = new EmptyScope(this);
        // 在空的作用域基础上，创建一个 CatalogScope 对象，并设置其包含的 catalog 列表为 "CATALOG"
        scope = new CatalogScope(scope, ImmutableList.of("CATALOG"));
        // 使用给定的作用域（scope）对传入的 SQL 节点（topNode）进行验证，并返回验证后的节点
        final SqlNode topNode2 = validateScopedExpression(topNode, scope);
        // 获取验证后的节点的数据类型，但在本方法中并未使用此类型，只是调用了 Util.discard 方法将其丢弃
        final RelDataType type = getValidatedNodeType(topNode2);
        Util.discard(type);
        // 返回验证后的 SQL 节点
        return topNode2;
    }

    @Override
    public List<SqlMoniker> lookupHints(SqlNode topNode, SqlParserPos pos) {
        SqlValidatorScope scope = new EmptyScope(this);
        SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
        cursorSet.add(outermostNode);
        if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
            registerQuery(scope, null, outermostNode, outermostNode, null, false);
        }
        final SqlValidatorNamespace ns = getNamespace(outermostNode);
        if (ns == null) {
            throw new AssertionError("Not a query: " + outermostNode);
        }
        Collection<SqlMoniker> hintList = Sets.newTreeSet(SqlMoniker.COMPARATOR);
        lookupSelectHints(ns, pos, hintList);
        return ImmutableList.copyOf(hintList);
    }

    @Override
    public @Nullable SqlMoniker lookupQualifiedName(SqlNode topNode, SqlParserPos pos) {
        final String posString = pos.toString();
        IdInfo info = idPositions.get(posString);
        if (info != null) {
            final SqlQualified qualified = info.scope.fullyQualify(info.id);
            return new SqlIdentifierMoniker(qualified.identifier);
        } else {
            return null;
        }
    }

    /**
     * Looks up completion hints for a syntactically correct select SQL that has been parsed into an
     * expression tree.
     *
     * @param select the Select node of the parsed expression tree
     * @param pos indicates the position in the sql statement we want to get completion hints for
     * @param hintList list of {@link SqlMoniker} (sql identifiers) that can fill in at the
     *     indicated position
     */
    void lookupSelectHints(SqlSelect select, SqlParserPos pos, Collection<SqlMoniker> hintList) {
        IdInfo info = idPositions.get(pos.toString());
        if ((info == null) || (info.scope == null)) {
            SqlNode fromNode = select.getFrom();
            final SqlValidatorScope fromScope = getFromScope(select);
            lookupFromHints(fromNode, fromScope, pos, hintList);
        } else {
            lookupNameCompletionHints(
                    info.scope, info.id.names, info.id.getParserPosition(), hintList);
        }
    }

    private void lookupSelectHints(
            SqlValidatorNamespace ns, SqlParserPos pos, Collection<SqlMoniker> hintList) {
        final SqlNode node = ns.getNode();
        if (node instanceof SqlSelect) {
            lookupSelectHints((SqlSelect) node, pos, hintList);
        }
    }

    private void lookupFromHints(
            @Nullable SqlNode node,
            @Nullable SqlValidatorScope scope,
            SqlParserPos pos,
            Collection<SqlMoniker> hintList) {
        if (node == null) {
            // This can happen in cases like "select * _suggest_", so from clause is absent
            return;
        }
        final SqlValidatorNamespace ns = getNamespaceOrThrow(node);
        if (ns.isWrapperFor(IdentifierNamespace.class)) {
            IdentifierNamespace idNs = ns.unwrap(IdentifierNamespace.class);
            final SqlIdentifier id = idNs.getId();
            for (int i = 0; i < id.names.size(); i++) {
                if (pos.toString().equals(id.getComponent(i).getParserPosition().toString())) {
                    final List<SqlMoniker> objNames = new ArrayList<>();
                    SqlValidatorUtil.getSchemaObjectMonikers(
                            getCatalogReader(), id.names.subList(0, i + 1), objNames);
                    for (SqlMoniker objName : objNames) {
                        if (objName.getType() != SqlMonikerType.FUNCTION) {
                            hintList.add(objName);
                        }
                    }
                    return;
                }
            }
        }
        switch (node.getKind()) {
            case JOIN:
                lookupJoinHints((SqlJoin) node, scope, pos, hintList);
                break;
            default:
                lookupSelectHints(ns, pos, hintList);
                break;
        }
    }

    private void lookupJoinHints(
            SqlJoin join,
            @Nullable SqlValidatorScope scope,
            SqlParserPos pos,
            Collection<SqlMoniker> hintList) {
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        SqlNode condition = join.getCondition();
        lookupFromHints(left, scope, pos, hintList);
        if (hintList.size() > 0) {
            return;
        }
        lookupFromHints(right, scope, pos, hintList);
        if (hintList.size() > 0) {
            return;
        }
        final JoinConditionType conditionType = join.getConditionType();
        switch (conditionType) {
            case ON:
                requireNonNull(condition, () -> "join.getCondition() for " + join)
                        .findValidOptions(this, getScopeOrThrow(join), pos, hintList);
                return;
            default:

                // No suggestions.
                // Not supporting hints for other types such as 'Using' yet.
        }
    }

    /**
     * Populates a list of all the valid alternatives for an identifier.
     *
     * @param scope Validation scope
     * @param names Components of the identifier
     * @param pos position
     * @param hintList a list of valid options
     */
    public final void lookupNameCompletionHints(
            SqlValidatorScope scope,
            List<String> names,
            SqlParserPos pos,
            Collection<SqlMoniker> hintList) {
        // Remove the last part of name - it is a dummy
        List<String> subNames = Util.skipLast(names);

        if (subNames.size() > 0) {
            // If there's a prefix, resolve it to a namespace.
            SqlValidatorNamespace ns = null;
            for (String name : subNames) {
                if (ns == null) {
                    final SqlValidatorScope.ResolvedImpl resolved =
                            new SqlValidatorScope.ResolvedImpl();
                    final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
                    scope.resolve(ImmutableList.of(name), nameMatcher, false, resolved);
                    if (resolved.count() == 1) {
                        ns = resolved.only().namespace;
                    }
                } else {
                    ns = ns.lookupChild(name);
                }
                if (ns == null) {
                    break;
                }
            }
            if (ns != null) {
                RelDataType rowType = ns.getRowType();
                if (rowType.isStruct()) {
                    for (RelDataTypeField field : rowType.getFieldList()) {
                        hintList.add(new SqlMonikerImpl(field.getName(), SqlMonikerType.COLUMN));
                    }
                }
            }

            // builtin function names are valid completion hints when the
            // identifier has only 1 name part
            findAllValidFunctionNames(names, this, hintList, pos);
        } else {
            // No prefix; use the children of the current scope (that is,
            // the aliases in the FROM clause)
            scope.findAliases(hintList);

            // If there's only one alias, add all child columns
            SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope(scope);
            if ((selectScope != null) && (selectScope.getChildren().size() == 1)) {
                RelDataType rowType = selectScope.getChildren().get(0).getRowType();
                for (RelDataTypeField field : rowType.getFieldList()) {
                    hintList.add(new SqlMonikerImpl(field.getName(), SqlMonikerType.COLUMN));
                }
            }
        }

        findAllValidUdfNames(names, this, hintList);
    }

    private static void findAllValidUdfNames(
            List<String> names, SqlValidator validator, Collection<SqlMoniker> result) {
        final List<SqlMoniker> objNames = new ArrayList<>();
        SqlValidatorUtil.getSchemaObjectMonikers(validator.getCatalogReader(), names, objNames);
        for (SqlMoniker objName : objNames) {
            if (objName.getType() == SqlMonikerType.FUNCTION) {
                result.add(objName);
            }
        }
    }

    private static void findAllValidFunctionNames(
            List<String> names,
            SqlValidator validator,
            Collection<SqlMoniker> result,
            SqlParserPos pos) {
        // a function name can only be 1 part
        if (names.size() > 1) {
            return;
        }
        for (SqlOperator op : validator.getOperatorTable().getOperatorList()) {
            SqlIdentifier curOpId = new SqlIdentifier(op.getName(), pos);

            final SqlCall call = validator.makeNullaryCall(curOpId);
            if (call != null) {
                result.add(new SqlMonikerImpl(op.getName(), SqlMonikerType.FUNCTION));
            } else {
                if ((op.getSyntax() == SqlSyntax.FUNCTION)
                        || (op.getSyntax() == SqlSyntax.PREFIX)) {
                    if (op.getOperandTypeChecker() != null) {
                        String sig = op.getAllowedSignatures();
                        sig = sig.replace("'", "");
                        result.add(new SqlMonikerImpl(sig, SqlMonikerType.FUNCTION));
                        continue;
                    }
                    result.add(new SqlMonikerImpl(op.getName(), SqlMonikerType.FUNCTION));
                }
            }
        }
    }

    @Override
    public SqlNode validateParameterizedExpression(
            SqlNode topNode, final Map<String, RelDataType> nameToTypeMap) {
        SqlValidatorScope scope = new ParameterScope(this, nameToTypeMap);
        return validateScopedExpression(topNode, scope);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *
     * 验证带有作用域的 SQL 表达式
     * @param topNode 需要验证的 SQL 节点
     * @param scope SQL 验证的作用域
     * @return 验证后的 SQL 节点
     */
    private SqlNode validateScopedExpression(SqlNode topNode, SqlValidatorScope scope) {
        // 首先对传入的 SQL 执行无条件表达式重写。将表达式树转换为标准形式
        SqlNode outermostNode = performUnconditionalRewrites(topNode, false);
        // 将重写后的节点添加到 cursorSet 中（cursorSet 可能是用于跟踪验证过程中的节点）
        cursorSet.add(outermostNode);
        // 更新 top 引用为重写后的节点（top 可能是类中的一个字段，用于存储当前验证的顶层节点）
        top = outermostNode;
        // 跟踪日志输出，显示无条件重写后的节点
        TRACER.trace("After unconditional rewrite: {}", outermostNode);
        // 如果重写后的节点是顶级节点（比如一个完整的查询）
        if (outermostNode.isA(SqlKind.TOP_LEVEL)) {
            // 在作用域中注册查询（可能是用于后续的作用域查找或解析）
            registerQuery(scope, null, outermostNode, outermostNode, null, false);
        }
        // 对重写后的节点进行验证（使用当前的验证器和作用域）
        outermostNode.validate(this, scope);
        // 如果重写后的节点不是顶级节点（可能是一个子表达式或片段）
        if (!outermostNode.isA(SqlKind.TOP_LEVEL)) {
            // force type derivation so that we can provide it to the
            // caller later without needing the scope
            // 强制推导类型，以便稍后可以无需作用域即可向调用者提供该类型
            deriveType(scope, outermostNode);
        }
        // 跟踪日志输出，显示验证后的节点
        TRACER.trace("After validation: {}", outermostNode);
        // 返回验证后的节点
        return outermostNode;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 校验查询节点
     */
    @Override
    public void validateQuery(
            SqlNode node, @Nullable SqlValidatorScope scope, RelDataType targetRowType) {
        // 从给定的节点和作用域中获取命名空间，如果无法获取则抛出异常
        final SqlValidatorNamespace ns = getNamespaceOrThrow(node, scope);
        // 检查节点类型是否为TABLESAMPLE
        if (node.getKind() == SqlKind.TABLESAMPLE) {
            // 获取TABLESAMPLE调用的操作数列表
            List<SqlNode> operands = ((SqlCall) node).getOperandList();
            // 从第二个操作数中获取采样规范（假设第一个操作数是表名，第二个操作数是采样规范）
            SqlSampleSpec sampleSpec = SqlLiteral.sampleValue(operands.get(1));
            // 判断采样规范类型
            if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
                // 如果是表采样规范，验证SQL特性T613（可能是特定于某个数据库系统的表采样功能）
                validateFeature(RESOURCE.sQLFeature_T613(), node.getParserPosition());
            } else if (sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
                // 如果是替代采样规范，验证SQL扩展特性T613的替代版本
                validateFeature(
                        RESOURCE.sQLFeatureExt_T613_Substitution(), node.getParserPosition());
            }
        }
        // 验证命名空间与目标行类型的一致性
        validateNamespace(ns, targetRowType);
        // 根据节点类型进行不同的处理
        switch (node.getKind()) {
            case EXTEND:
                // Until we have a dedicated namespace for EXTEND
                // 如果节点类型为EXTEND（扩展），且直到我们有专用的EXTEND命名空间前，
                // 使用给定的作用域推导节点类型
                deriveType(requireNonNull(scope, "scope"), node);
                break;
            default:
                // 对于其他类型的节点，不做特殊处理
                break;
        }
        // 如果当前节点是顶级节点则验证模态性
        if (node == top) {
            validateModality(node);
        }
        // 验证对表（或命名空间中的对象）的访问权限
        validateAccess(node, ns.getTable(), SqlAccessEnum.SELECT);
        // 验证与快照相关的逻辑
        validateSnapshot(node, scope, ns);
    }

    /**
     * Validates a namespace.
     *
     * @param namespace Namespace
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 校验命名空间
     */
    protected void validateNamespace(
            final SqlValidatorNamespace namespace, RelDataType targetRowType) {
        // 调用命名空间的validate方法来验证目标行类型
        namespace.validate(targetRowType);
        // 获取命名空间对应的节点
        SqlNode node = namespace.getNode();
        // 如果节点不为空，则设置节点的验证后类型
        if (node != null) {
            setValidatedNodeType(node, namespace.getType());
        }
    }

    @VisibleForTesting
    public SqlValidatorScope getEmptyScope() {
        return new EmptyScope(this);
    }

    private SqlValidatorScope getScope(SqlSelect select, Clause clause) {
        return requireNonNull(
                clauseScopes.get(IdPair.of(select, clause)),
                () -> "no " + clause + " scope for " + select);
    }

    public SqlValidatorScope getCursorScope(SqlSelect select) {
        return getScope(select, Clause.CURSOR);
    }

    @Override
    public SqlValidatorScope getWhereScope(SqlSelect select) {
        return getScope(select, Clause.WHERE);
    }

    @Override
    public SqlValidatorScope getSelectScope(SqlSelect select) {
        return getScope(select, Clause.SELECT);
    }

    @Override
    public @Nullable SelectScope getRawSelectScope(SqlSelect select) {
        SqlValidatorScope scope = clauseScopes.get(IdPair.of(select, Clause.SELECT));
        if (scope instanceof AggregatingSelectScope) {
            scope = ((AggregatingSelectScope) scope).getParent();
        }
        return (SelectScope) scope;
    }

    private SelectScope getRawSelectScopeNonNull(SqlSelect select) {
        return requireNonNull(getRawSelectScope(select), () -> "getRawSelectScope for " + select);
    }

    @Override
    public SqlValidatorScope getHavingScope(SqlSelect select) {
        // Yes, it's the same as getSelectScope
        return getScope(select, Clause.SELECT);
    }

    @Override
    public SqlValidatorScope getGroupScope(SqlSelect select) {
        // Yes, it's the same as getWhereScope
        return getScope(select, Clause.WHERE);
    }

    @Override
    public @Nullable SqlValidatorScope getFromScope(SqlSelect select) {
        return scopes.get(select);
    }

    @Override
    public SqlValidatorScope getOrderScope(SqlSelect select) {
        return getScope(select, Clause.ORDER);
    }

    @Override
    public SqlValidatorScope getMatchRecognizeScope(SqlMatchRecognize node) {
        return getScopeOrThrow(node);
    }

    @Override
    public @Nullable SqlValidatorScope getJoinScope(SqlNode node) {
        return scopes.get(stripAs(node));
    }

    @Override
    public SqlValidatorScope getOverScope(SqlNode node) {
        return getScopeOrThrow(node);
    }

    private SqlValidatorScope getScopeOrThrow(SqlNode node) {
        return requireNonNull(scopes.get(node), () -> "scope for " + node);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * @param node 需要获取命名空间的SQL节点
     * @param scope 命名空间的作用域，可为null
     * @return 节点对应的命名空间，如果无法找到则可能返回null
     */
    private @Nullable SqlValidatorNamespace getNamespace(
            SqlNode node, @Nullable SqlValidatorScope scope) {
        // 如果节点是SqlIdentifier类型且作用域是DelegatingScope类型
        if (node instanceof SqlIdentifier && scope instanceof DelegatingScope) {
            final SqlIdentifier id = (SqlIdentifier) node;
            final DelegatingScope idScope = (DelegatingScope) ((DelegatingScope) scope).getParent();
            // 递归调用，尝试获取命名空间
            return getNamespace(id, idScope);
            // 如果是SqlCall
        } else if (node instanceof SqlCall) {
            // Handle extended identifiers.
            final SqlCall call = (SqlCall) node;
            switch (call.getOperator().getKind()) {
                case TABLE_REF:
                    // 引用表，获取操作数的命名空间
                    return getNamespace(call.operand(0), scope);
                case EXTEND:
                    final SqlNode operand0 = call.getOperandList().get(0);
                    final SqlIdentifier identifier =
                            operand0.getKind() == SqlKind.TABLE_REF
                                    ? ((SqlCall) operand0).operand(0)
                                    : (SqlIdentifier) operand0;
                    final DelegatingScope idScope = (DelegatingScope) scope;
                    // 递归调用，尝试获取命名空间
                    return getNamespace(identifier, idScope);
                case AS:
                    final SqlNode nested = call.getOperandList().get(0);
                    switch (nested.getKind()) {
                        case TABLE_REF:
                        case EXTEND:
                            // AS操作，获取嵌套操作的命名空间
                            return getNamespace(nested, scope);
                        default:
                            break;
                    }
                    break;
                default:
                    break;
            }
        }
        // 如果以上条件都不满足，则尝试直接对节点进行命名空间查找
        return getNamespace(node);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 获取Namespace
     */
    private @Nullable SqlValidatorNamespace getNamespace(
            SqlIdentifier id, @Nullable DelegatingScope scope) {
        // 如果标识符是简单的（即不包含点分隔的多个部分）
        if (id.isSimple()) {
            // 获取名称匹配器
            final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
            // 创建解析实现对象
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            // 确保作用域不为null，解析标识符
            requireNonNull(scope, () -> "scope needed to lookup " + id)
                    .resolve(id.names, nameMatcher, false, resolved);
            // 如果解析结果只有一个，则返回其命名空间
            if (resolved.count() == 1) {
                return resolved.only().namespace;
            }
        }
        // 如果标识符不是简单的，或者解析结果不是唯一的，调用另一个getNamespace方法
        return getNamespace(id);
    }

    @Override
    public @Nullable SqlValidatorNamespace getNamespace(SqlNode node) {
        switch (node.getKind()) {
            case AS:

                // AS has a namespace if it has a column list 'AS t (c1, c2, ...)'
                final SqlValidatorNamespace ns = namespaces.get(node);
                if (ns != null) {
                    return ns;
                }
                // fall through
            case TABLE_REF:
            case SNAPSHOT:
            case OVER:
            case COLLECTION_TABLE:
            case ORDER_BY:
            case TABLESAMPLE:
                return getNamespace(((SqlCall) node).operand(0));
            default:
                return namespaces.get(node);
        }
    }

    /**
     * Namespace for the given node.
     *
     * @param node node to compute the namespace for
     * @return namespace for the given node, never null
     * @see #getNamespace(SqlNode)
     */
    @API(since = "1.27", status = API.Status.INTERNAL)
    SqlValidatorNamespace getNamespaceOrThrow(SqlNode node) {
        return requireNonNull(getNamespace(node), () -> "namespace for " + node);
    }

    /**
     * Namespace for the given node.
     *
     * @param node node to compute the namespace for
     * @param scope namespace scope
     * @return namespace for the given node, never null
     * @see #getNamespace(SqlNode)
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 为给定的节点获取命名空间。
     *
     * @param node 需要计算命名空间的节点
     * @param scope 命名空间的作用域
     * @return 给定节点的命名空间，此方法保证不会返回null
     * @see #getNamespace(SqlNode) 参考此方法以获取更多关于命名空间获取的信息
     */
    @API(since = "1.27", status = API.Status.INTERNAL)
    SqlValidatorNamespace getNamespaceOrThrow(SqlNode node, @Nullable SqlValidatorScope scope) {
        // 使用requireNonNull方法确保getNamespace(node, scope)不会返回null，如果返回null则抛出NullPointerException
        return requireNonNull(
                getNamespace(node, scope), () -> "namespace for " + node + ", scope " + scope);
    }

    /**
     * Namespace for the given node.
     *
     * @param id identifier to resolve
     * @param scope namespace scope
     * @return namespace for the given node, never null
     * @see #getNamespace(SqlIdentifier, DelegatingScope)
     */
    @API(since = "1.26", status = API.Status.INTERNAL)
    SqlValidatorNamespace getNamespaceOrThrow(SqlIdentifier id, @Nullable DelegatingScope scope) {
        return requireNonNull(
                getNamespace(id, scope), () -> "namespace for " + id + ", scope " + scope);
    }

    private void handleOffsetFetch(@Nullable SqlNode offset, @Nullable SqlNode fetch) {
        if (offset instanceof SqlDynamicParam) {
            setValidatedNodeType(offset, typeFactory.createSqlType(SqlTypeName.INTEGER));
        }
        if (fetch instanceof SqlDynamicParam) {
            setValidatedNodeType(fetch, typeFactory.createSqlType(SqlTypeName.INTEGER));
        }
    }

    /**
     * Performs expression rewrites which are always used unconditionally. These rewrites massage
     * the expression tree into a standard form so that the rest of the validation logic can be
     * simpler.
     *
     * <p>Returns null if and only if the original expression is null.
     *
     * @param node expression to be rewritten
     * @param underFrom whether node appears directly under a FROM clause
     * @return rewritten expression, or null if the original expression is null
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 执行无条件表达式重写。这些重写将表达式树转换为标准形式，以便后续的验证逻辑可以更简单。
     * @param node 要重写的表达式
     * @param underFrom 节点是否直接出现在FROM子句下
     * @return 重写后的表达式，如果原始表达式为null则返回null
     */
    protected @PolyNull SqlNode performUnconditionalRewrites(
            @PolyNull SqlNode node, boolean underFrom) {
        if (node == null) {
            // 如果原始节点为null，则直接返回null
            return null;
        }

        // first transform operands and invoke generic call rewrite
        // 首先转换操作数并调用通用的调用重写 如果节点是一个SqlCall（即函数调用或SQL操作）
        if (node instanceof SqlCall) {
            if (node instanceof SqlMerge) {
                // 如果节点是SqlMerge类型，则设置validatingSqlMerge为true（可能是为了标记当前正在验证SqlMerge）
                validatingSqlMerge = true;
            }
            // 将Node节点转换转换为SqlCall
            SqlCall call = (SqlCall) node;
            // 获取SqlCall的类型
            final SqlKind kind = call.getKind();
            // 获取SqlCall的操作数列表
            final List<SqlNode> operands = call.getOperandList();
            //遍历操作数列表
            for (int i = 0; i < operands.size(); i++) {
                SqlNode operand = operands.get(i);
                // 根据当前SqlCall的类型和位置，确定子节点是否出现在FROM子句下
                boolean childUnderFrom;
                if (kind == SqlKind.SELECT) {
                    // 如果是SELECT操作，并且当前操作数是FROM子句的操作数
                    childUnderFrom = i == SqlSelect.FROM_OPERAND;
                } else if (kind == SqlKind.AS && (i == 0)) {
                    // for an aliased expression, it is under FROM if
                    // the AS expression is under FROM
                    // 如果是AS操作（别名），并且AS操作数是第一个操作数（即被别名的表达式）
                    // 那么它是否出现在FROM子句下取决于其父节点是否出现在FROM子句下
                    childUnderFrom = underFrom;
                } else {
                    // 其他情况，默认子节点不出现在FROM子句下
                    childUnderFrom = false;
                }
                // 对子节点进行无条件重写
                SqlNode newOperand = performUnconditionalRewrites(operand, childUnderFrom);
                // 如果新的操作数不为null且与原操作数不同，则更新SqlCall的操作数
                if (newOperand != null && newOperand != operand) {
                    call.setOperand(i, newOperand);
                }
            }

            // 如果 SqlCall 的操作符是一个未解析的函数
            if (call.getOperator() instanceof SqlUnresolvedFunction) {
                // 断言该 SqlCall 实际上是一个 SqlBasicCall 的实例
                // 因为只有 SqlBasicCall 会有操作符（operator）的概念
                assert call instanceof SqlBasicCall;
                // 获取未解析的函数
                final SqlUnresolvedFunction function = (SqlUnresolvedFunction) call.getOperator();
                // This function hasn't been resolved yet.  Perform
                // a half-hearted resolution now in case it's a
                // builtin function requiring special casing.  If it's
                // not, we'll handle it later during overload resolution.
                // 这个函数还没有被解析。现在进行一个初步的解析，
                // 如果它是一个需要特殊处理的内置函数的话。
                // 如果不是，我们将在后面的重载解析过程中处理它。
                final List<SqlOperator> overloads = new ArrayList<>();
                // 在操作符表（opTab）中查找函数名称对应的操作符重载，
                // 就是查早对应的函数是否存在，查找SQL语句使用的函数是否有对应的实现
                opTab.lookupOperatorOverloads(
                        function.getNameAsId(),// 函数名称
                        function.getFunctionType(),// 函数类型
                        SqlSyntax.FUNCTION,// 语法类型，这里是函数
                        overloads,// 重载列表
                        catalogReader.nameMatcher());// 名称匹配器
                // 如果找到了唯一的一个重载
                if (overloads.size() == 1) {
                    // 将 SqlBasicCall 的操作符设置为找到的重载操作符
                    ((SqlBasicCall) call).setOperator(overloads.get(0));
                }
            }
            // 如果配置中启用了调用重写
            if (config.callRewrite()) {
                // 调用操作符的 rewriteCall 方法，可能会进一步重写 SqlCall
                node = call.getOperator().rewriteCall(this, call);
            }
            // 如果节点是一个 SqlNodeList（表示一个节点的列表，如 SQL 中的 IN 子句）
        } else if (node instanceof SqlNodeList) {
            final SqlNodeList list = (SqlNodeList) node;
            // 遍历 SqlNodeList 中的每个节点
            for (int i = 0; i < list.size(); i++) {
                // 获取当前节点
                SqlNode operand = list.get(i);
                // 对当前节点进行无条件重写
                SqlNode newOperand = performUnconditionalRewrites(operand, false);
                // 如果新的节点不为 null，则替换旧的节点
                if (newOperand != null) {
                    list.set(i, newOperand);
                }
            }
        }

        // now transform node itself
        // 获取节点的 SqlKind 类型
        final SqlKind kind = node.getKind();
        // 根据 SqlKind 类型进行不同的处理
        switch (kind) {
            case VALUES:
                // Do not rewrite VALUES clauses.
                // At some point we used to rewrite VALUES(...) clauses
                // to (SELECT * FROM VALUES(...)) but this was problematic
                // in various cases such as FROM (VALUES(...)) [ AS alias ]
                // where the rewrite was invoked over and over making the
                // expression grow indefinitely.
                // 不重写 VALUES 子句。
                // 在某些时候，我们曾经将 VALUES(...) 子句重写为 (SELECT * FROM VALUES(...))，
                // 但这在各种情况下都存在问题，比如 FROM (VALUES(...)) [ AS alias ]，
                // 在这种情况下，重写会被反复调用，导致表达式无限增长。
                return node;
            // 如果节点是 ORDER BY 子句
            case ORDER_BY:
                {
                    SqlOrderBy orderBy = (SqlOrderBy) node;
                    // 处理 OFFSET 和 FETCH 子句（如果存在）
                    handleOffsetFetch(orderBy.offset, orderBy.fetch);
                    // 如果 ORDER BY 的查询是一个 SqlSelect
                    if (orderBy.query instanceof SqlSelect) {
                        SqlSelect select = (SqlSelect) orderBy.query;

                        // Don't clobber existing ORDER BY.  It may be needed for
                        // an order-sensitive function like RANK.
                        // 不要覆盖现有的 ORDER BY 子句。它可能对于像 RANK 这样的顺序敏感函数是必要的。
                        if (select.getOrderList() == null) {
                            // push ORDER BY into existing select
                            // 将 ORDER BY 子句推入现有的 SELECT 语句中
                            select.setOrderBy(orderBy.orderList);
                            select.setOffset(orderBy.offset);
                            select.setFetch(orderBy.fetch);
                            // 返回修改后的 SqlSelect 节点
                            return select;
                        }
                    }
                    // 如果 ORDER BY 的查询是一个 SqlWith，并且 SqlWith 的主体是一个 SqlSelect
                    if (orderBy.query instanceof SqlWith
                            && ((SqlWith) orderBy.query).body instanceof SqlSelect) {
                        // 将查询强制转换为 SqlWith 类型
                        SqlWith with = (SqlWith) orderBy.query;
                        // 将 SqlWith 的主体强制转换为 SqlSelect 类型
                        SqlSelect select = (SqlSelect) with.body;

                        // Don't clobber existing ORDER BY.  It may be needed for
                        // an order-sensitive function like RANK.
                        // 不要覆盖现有的 ORDER BY 子句。它可能对于像 RANK 这样的顺序敏感函数是必要的。
                        // 检查 SqlSelect 是否没有现有的 ORDER BY 子句
                        if (select.getOrderList() == null) {
                            // push ORDER BY into existing select
                            // 将外部的 ORDER BY 子句推入到现有的 SqlSelect 语句中
                            select.setOrderBy(orderBy.orderList);
                            select.setOffset(orderBy.offset);// 设置 OFFSET（如果有）
                            select.setFetch(orderBy.fetch);// 设置 FETCH（如果有）
                            // 因为我们修改了 SqlWith 的主体 SqlSelect，所以直接返回修改后的 SqlWith
                            return with;
                        }
                    }
                    // 创建一个新的 SqlNodeList，用于构建新的 SELECT 查询的字段列表
                    // 这里默认使用 "*" 作为字段，表明选择所有字段
                    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
                    // 定义一个 SqlNodeList 类型的变量 orderList，
                    final SqlNodeList orderList;
                    // 调用方法 getInnerSelect 来获取内部的 SqlSelect 节点
                    SqlSelect innerSelect = getInnerSelect(node);
                    // 检查是否成功获取到了内部 SqlSelect 节点，并且该 SqlSelect 是否包含聚合函数
                    if (innerSelect != null && isAggregate(innerSelect)) {
                        // 复制原始的 ORDER BY 子句列表，避免直接修改原始对象
                        orderList = SqlNode.clone(orderBy.orderList);
                        // We assume that ORDER BY item does not have ASC etc.
                        // We assume that ORDER BY item is present in SELECT list.
                        // 假设 ORDER BY 子句中的项没有 ASC、DESC 等排序关键字
                        // 假设 ORDER BY 子句中的项都存在于 SELECT 列表中
                        for (int i = 0; i < orderList.size(); i++) {
                            // 遍历 ORDER BY 子句中的每一项
                            SqlNode sqlNode = orderList.get(i);
                            // 获取内部 SqlSelect 的 SELECT 列表
                            SqlNodeList selectList2 =
                                    SqlNonNullableAccessors.getSelectList(innerSelect);
                            // 遍历内部 SqlSelect 的 SELECT 列表中的每一项，并且跟踪其索引位置
                            for (Ord<SqlNode> sel : Ord.zip(selectList2)) {
                                // 忽略可能的别名（AS 关键字后的部分），并比较两个 SqlNode 是否相等
                                if (stripAs(sel.e).equalsDeep(sqlNode, Litmus.IGNORE)) {
                                    // 如果找到了匹配的项，则将 ORDER BY 子句中的该项替换为对应在 SELECT 列表中的索引位置（从1开始）
                                    // 这通常用于在聚合查询中根据 SELECT 列表中的列位置进行排序
                                    orderList.set(
                                            i,
                                            SqlLiteral.createExactNumeric(
                                                    Integer.toString(sel.i + 1),// 注意索引是从0开始的，所以+1来转换为从1开始
                                                    SqlParserPos.ZERO));
                                }
                            }
                        }
                    } else {
                        // 如果内部 SqlSelect 不包含聚合函数，或者没有成功获取到内部 SqlSelect，则直接使用原始的 ORDER BY 子句
                        orderList = orderBy.orderList;
                    }
                    // 创建一个新的 SqlSelect 节点，并设置其各个属性
                    return new SqlSelect(
                            SqlParserPos.ZERO,
                            null,
                            selectList,
                            orderBy.query,
                            null,
                            null,
                            null,
                            null,
                            orderList,
                            orderBy.offset,
                            orderBy.fetch,
                            null);
                }

            case EXPLICIT_TABLE:
                {
                    // (TABLE t) is equivalent to (SELECT * FROM t)
                    // EXPLICIT_TABLE 情况，即形如 (TABLE t) 的表达式，这等价于 (SELECT * FROM t)
                    // 将其转换为一个 SqlSelect 表达式
                    SqlCall call = (SqlCall) node;// 将节点强制转换为 SqlCall 类型
                    // 创建一个新的 SqlNodeList，用于存储 SELECT 列表中的项
                    final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
                    // 添加一个 "*" 到 SELECT 列表中，表示选择所有列
                    selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
                    // 创建一个新的 SqlSelect 表达式，表示 SELECT * FROM [给定的表]
                    return new SqlSelect(
                            SqlParserPos.ZERO,
                            null,
                            selectList,
                            call.operand(0),
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
                }

            case DELETE:
                {
                    // DELETE 情况，处理 DELETE 语句
                    SqlDelete call = (SqlDelete) node;// 将节点强制转换为 SqlDelete 类型
                    // 创建一个用于 DELETE 语句的源 SELECT 表达式
                    SqlSelect select = createSourceSelectForDelete(call);
                    // 将创建的源 SELECT 表达式设置到 DELETE 语句中
                    call.setSourceSelect(select);
                    break;
                }
            // 根据节点的类型进行不同的处理
            case UPDATE:
                {
                    // 处理 UPDATE 语句
                    SqlUpdate call = (SqlUpdate) node;// 将节点强制转换为 SqlUpdate 类型
                    SqlSelect select = createSourceSelectForUpdate(call);// 为 UPDATE 语句创建源 SELECT 表达式
                    call.setSourceSelect(select); // 将源 SELECT 表达式设置到 UPDATE 语句中

                    // See if we're supposed to rewrite UPDATE to MERGE
                    // (unless this is the UPDATE clause of a MERGE,
                    // in which case leave it alone).
                    // 检查是否需要将 UPDATE 语句重写为 MERGE 语句
                    // （除非这个 UPDATE 语句是 MERGE 语句中的一部分，如果是的话则保持原样）
                    if (!validatingSqlMerge) {// 如果当前不是在验证 MERGE 语句
                        SqlNode selfJoinSrcExpr =
                                getSelfJoinExprForUpdate(call.getTargetTable(), UPDATE_SRC_ALIAS);
                        if (selfJoinSrcExpr != null) {
                            node = rewriteUpdateToMerge(call, selfJoinSrcExpr);
                        }
                    }
                    break;
                }

            case MERGE:
                {
                    // 处理 MERGE 语句
                    SqlMerge call = (SqlMerge) node;// 将节点强制转换为 SqlMerge 类型
                    rewriteMerge(call);// 重写 MERGE 语句（可能是添加一些默认行为、优化等）
                    break;
                }
            default:
                // 对于其他类型的节点，不进行任何处理
                break;
        }
        //返回Node
        return node;
    }

    private static @Nullable SqlSelect getInnerSelect(SqlNode node) {
        for (; ; ) {
            if (node instanceof SqlSelect) {
                return (SqlSelect) node;
            } else if (node instanceof SqlOrderBy) {
                node = ((SqlOrderBy) node).query;
            } else if (node instanceof SqlWith) {
                node = ((SqlWith) node).body;
            } else {
                return null;
            }
        }
    }

    private static void rewriteMerge(SqlMerge call) {
        SqlNodeList selectList;
        SqlUpdate updateStmt = call.getUpdateCall();
        if (updateStmt != null) {
            // if we have an update statement, just clone the select list
            // from the update statement's source since it's the same as
            // what we want for the select list of the merge source -- '*'
            // followed by the update set expressions
            SqlSelect sourceSelect = SqlNonNullableAccessors.getSourceSelect(updateStmt);
            selectList = SqlNode.clone(SqlNonNullableAccessors.getSelectList(sourceSelect));
        } else {
            // otherwise, just use select *
            selectList = new SqlNodeList(SqlParserPos.ZERO);
            selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
        }
        SqlNode targetTable = call.getTargetTable();
        if (call.getAlias() != null) {
            targetTable = SqlValidatorUtil.addAlias(targetTable, call.getAlias().getSimple());
        }

        // Provided there is an insert substatement, the source select for
        // the merge is a left outer join between the source in the USING
        // clause and the target table; otherwise, the join is just an
        // inner join.  Need to clone the source table reference in order
        // for validation to work
        SqlNode sourceTableRef = call.getSourceTableRef();
        SqlInsert insertCall = call.getInsertCall();
        JoinType joinType = (insertCall == null) ? JoinType.INNER : JoinType.LEFT;
        final SqlNode leftJoinTerm = SqlNode.clone(sourceTableRef);
        SqlNode outerJoin =
                new SqlJoin(
                        SqlParserPos.ZERO,
                        leftJoinTerm,
                        SqlLiteral.createBoolean(false, SqlParserPos.ZERO),
                        joinType.symbol(SqlParserPos.ZERO),
                        targetTable,
                        JoinConditionType.ON.symbol(SqlParserPos.ZERO),
                        call.getCondition());
        SqlSelect select =
                new SqlSelect(
                        SqlParserPos.ZERO,
                        null,
                        selectList,
                        outerJoin,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        call.setSourceSelect(select);

        // Source for the insert call is a select of the source table
        // reference with the select list being the value expressions;
        // note that the values clause has already been converted to a
        // select on the values row constructor; so we need to extract
        // that via the from clause on the select
        if (insertCall != null) {
            SqlCall valuesCall = (SqlCall) insertCall.getSource();
            SqlCall rowCall = valuesCall.operand(0);
            selectList = new SqlNodeList(rowCall.getOperandList(), SqlParserPos.ZERO);
            final SqlNode insertSource = SqlNode.clone(sourceTableRef);
            select =
                    new SqlSelect(
                            SqlParserPos.ZERO,
                            null,
                            selectList,
                            insertSource,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null,
                            null);
            insertCall.setSource(select);
        }
    }

    private SqlNode rewriteUpdateToMerge(SqlUpdate updateCall, SqlNode selfJoinSrcExpr) {
        // Make sure target has an alias.
        SqlIdentifier updateAlias = updateCall.getAlias();
        if (updateAlias == null) {
            updateAlias = new SqlIdentifier(UPDATE_TGT_ALIAS, SqlParserPos.ZERO);
            updateCall.setAlias(updateAlias);
        }
        SqlNode selfJoinTgtExpr =
                getSelfJoinExprForUpdate(updateCall.getTargetTable(), updateAlias.getSimple());
        assert selfJoinTgtExpr != null;

        // Create join condition between source and target exprs,
        // creating a conjunction with the user-level WHERE
        // clause if one was supplied
        SqlNode condition = updateCall.getCondition();
        SqlNode selfJoinCond =
                SqlStdOperatorTable.EQUALS.createCall(
                        SqlParserPos.ZERO, selfJoinSrcExpr, selfJoinTgtExpr);
        if (condition == null) {
            condition = selfJoinCond;
        } else {
            condition =
                    SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, selfJoinCond, condition);
        }
        SqlNode target = updateCall.getTargetTable().clone(SqlParserPos.ZERO);

        // For the source, we need to anonymize the fields, so
        // that for a statement like UPDATE T SET I = I + 1,
        // there's no ambiguity for the "I" in "I + 1";
        // this is OK because the source and target have
        // identical values due to the self-join.
        // Note that we anonymize the source rather than the
        // target because downstream, the optimizer rules
        // don't want to see any projection on top of the target.
        IdentifierNamespace ns = new IdentifierNamespace(this, target, null, castNonNull(null));
        RelDataType rowType = ns.getRowType();
        SqlNode source = updateCall.getTargetTable().clone(SqlParserPos.ZERO);
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        int i = 1;
        for (RelDataTypeField field : rowType.getFieldList()) {
            SqlIdentifier col = new SqlIdentifier(field.getName(), SqlParserPos.ZERO);
            selectList.add(SqlValidatorUtil.addAlias(col, UPDATE_ANON_PREFIX + i));
            ++i;
        }
        source =
                new SqlSelect(
                        SqlParserPos.ZERO,
                        null,
                        selectList,
                        source,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null,
                        null);
        source = SqlValidatorUtil.addAlias(source, UPDATE_SRC_ALIAS);
        SqlMerge mergeCall =
                new SqlMerge(
                        updateCall.getParserPosition(),
                        target,
                        condition,
                        source,
                        updateCall,
                        null,
                        null,
                        updateCall.getAlias());
        rewriteMerge(mergeCall);
        return mergeCall;
    }

    /**
     * Allows a subclass to provide information about how to convert an UPDATE into a MERGE via
     * self-join. If this method returns null, then no such conversion takes place. Otherwise, this
     * method should return a suitable unique identifier expression for the given table.
     *
     * @param table identifier for table being updated
     * @param alias alias to use for qualifying columns in expression, or null for unqualified
     *     references; if this is equal to {@value #UPDATE_SRC_ALIAS}, then column references have
     *     been anonymized to "SYS$ANONx", where x is the 1-based column number.
     * @return expression for unique identifier, or null to prevent conversion
     */
    protected @Nullable SqlNode getSelfJoinExprForUpdate(SqlNode table, String alias) {
        return null;
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into an UPDATE statement to be
     * updated.
     *
     * @param call Call to the UPDATE operator
     * @return select statement
     */
    protected SqlSelect createSourceSelectForUpdate(SqlUpdate call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
        int ordinal = 0;
        for (SqlNode exp : call.getSourceExpressionList()) {
            // Force unique aliases to avoid a duplicate for Y with
            // SET X=Y
            String alias = SqlUtil.deriveAliasFromOrdinal(ordinal);
            selectList.add(SqlValidatorUtil.addAlias(exp, alias));
            ++ordinal;
        }
        SqlNode sourceTable = call.getTargetTable();
        SqlIdentifier alias = call.getAlias();
        if (alias != null) {
            sourceTable = SqlValidatorUtil.addAlias(sourceTable, alias.getSimple());
        }
        return new SqlSelect(
                SqlParserPos.ZERO,
                null,
                selectList,
                sourceTable,
                call.getCondition(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Creates the SELECT statement that putatively feeds rows into a DELETE statement to be
     * deleted.
     *
     * @param call Call to the DELETE operator
     * @return select statement
     */
    protected SqlSelect createSourceSelectForDelete(SqlDelete call) {
        final SqlNodeList selectList = new SqlNodeList(SqlParserPos.ZERO);
        selectList.add(SqlIdentifier.star(SqlParserPos.ZERO));
        SqlNode sourceTable = call.getTargetTable();
        SqlIdentifier alias = call.getAlias();
        if (alias != null) {
            sourceTable = SqlValidatorUtil.addAlias(sourceTable, alias.getSimple());
        }
        return new SqlSelect(
                SqlParserPos.ZERO,
                null,
                selectList,
                sourceTable,
                call.getCondition(),
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }

    /**
     * Returns null if there is no common type. E.g. if the rows have a different number of columns.
     */
    @Nullable
    RelDataType getTableConstructorRowType(SqlCall values, SqlValidatorScope scope) {
        final List<SqlNode> rows = values.getOperandList();
        assert rows.size() >= 1;
        final List<RelDataType> rowTypes = new ArrayList<>();
        for (final SqlNode row : rows) {
            assert row.getKind() == SqlKind.ROW;
            SqlCall rowConstructor = (SqlCall) row;

            // REVIEW jvs 10-Sept-2003: Once we support single-row queries as
            // rows, need to infer aliases from there.
            final List<String> aliasList = new ArrayList<>();
            final List<RelDataType> typeList = new ArrayList<>();
            for (Ord<SqlNode> column : Ord.zip(rowConstructor.getOperandList())) {
                final String alias = deriveAliasNonNull(column.e, column.i);
                aliasList.add(alias);
                final RelDataType type = deriveType(scope, column.e);
                typeList.add(type);
            }
            rowTypes.add(typeFactory.createStructType(typeList, aliasList));
        }
        if (rows.size() == 1) {
            // TODO jvs 10-Oct-2005:  get rid of this workaround once
            // leastRestrictive can handle all cases
            return rowTypes.get(0);
        }
        return typeFactory.leastRestrictive(rowTypes);
    }

    @Override
    public RelDataType getValidatedNodeType(SqlNode node) {
        RelDataType type = getValidatedNodeTypeIfKnown(node);
        if (type == null) {
            if (node.getKind() == SqlKind.IDENTIFIER) {
                throw newValidationError(node, RESOURCE.unknownIdentifier(node.toString()));
            }
            throw Util.needToImplement(node);
        } else {
            return type;
        }
    }

    @Override
    public @Nullable RelDataType getValidatedNodeTypeIfKnown(SqlNode node) {
        final RelDataType type = nodeToTypeMap.get(node);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(node);
        if (ns != null) {
            return ns.getType();
        }
        final SqlNode original = originalExprs.get(node);
        if (original != null && original != node) {
            return getValidatedNodeType(original);
        }
        if (node instanceof SqlIdentifier) {
            return getCatalogReader().getNamedType((SqlIdentifier) node);
        }
        return null;
    }

    @Override
    public @Nullable List<RelDataType> getValidatedOperandTypes(SqlCall call) {
        return callToOperandTypesMap.get(call);
    }

    /**
     * Saves the type of a {@link SqlNode}, now that it has been validated.
     *
     * <p>Unlike the base class method, this method is not deprecated. It is available from within
     * Calcite, but is not part of the public API.
     *
     * @param node A SQL parse tree node, never null
     * @param type Its type; must not be null
     */
    @Override
    public final void setValidatedNodeType(SqlNode node, RelDataType type) {
        requireNonNull(type, "type");
        requireNonNull(node, "node");
        if (type.equals(unknownType)) {
            // don't set anything until we know what it is, and don't overwrite
            // a known type with the unknown type
            return;
        }
        nodeToTypeMap.put(node, type);
    }

    @Override
    public void removeValidatedNodeType(SqlNode node) {
        nodeToTypeMap.remove(node);
    }

    @Override
    public @Nullable SqlCall makeNullaryCall(SqlIdentifier id) {
        if (id.names.size() == 1 && !id.isComponentQuoted(0)) {
            final List<SqlOperator> list = new ArrayList<>();
            opTab.lookupOperatorOverloads(
                    id, null, SqlSyntax.FUNCTION, list, catalogReader.nameMatcher());
            for (SqlOperator operator : list) {
                if (operator.getSyntax() == SqlSyntax.FUNCTION_ID) {
                    // Even though this looks like an identifier, it is a
                    // actually a call to a function. Construct a fake
                    // call to this function, so we can use the regular
                    // operator validation.
                    return new SqlBasicCall(
                                    operator, ImmutableList.of(), id.getParserPosition(), null)
                            .withExpanded(true);
                }
            }
        }
        return null;
    }

    @Override
    public RelDataType deriveType(SqlValidatorScope scope, SqlNode expr) {
        requireNonNull(scope, "scope");
        requireNonNull(expr, "expr");

        // if we already know the type, no need to re-derive
        RelDataType type = nodeToTypeMap.get(expr);
        if (type != null) {
            return type;
        }
        final SqlValidatorNamespace ns = getNamespace(expr);
        if (ns != null) {
            return ns.getType();
        }
        type = deriveTypeImpl(scope, expr);
        Preconditions.checkArgument(type != null, "SqlValidator.deriveTypeInternal returned null");
        setValidatedNodeType(expr, type);
        return type;
    }

    /** Derives the type of a node, never null. */
    RelDataType deriveTypeImpl(SqlValidatorScope scope, SqlNode operand) {
        DeriveTypeVisitor v = new DeriveTypeVisitor(scope);
        final RelDataType type = operand.accept(v);
        return requireNonNull(scope.nullifyType(operand, type));
    }

    @Override
    public RelDataType deriveConstructorType(
            SqlValidatorScope scope,
            SqlCall call,
            SqlFunction unresolvedConstructor,
            @Nullable SqlFunction resolvedConstructor,
            List<RelDataType> argTypes) {
        SqlIdentifier sqlIdentifier = unresolvedConstructor.getSqlIdentifier();
        assert sqlIdentifier != null;
        RelDataType type = catalogReader.getNamedType(sqlIdentifier);
        if (type == null) {
            // TODO jvs 12-Feb-2005:  proper type name formatting
            throw newValidationError(
                    sqlIdentifier, RESOURCE.unknownDatatypeName(sqlIdentifier.toString()));
        }

        if (resolvedConstructor == null) {
            if (call.operandCount() > 0) {
                // This is not a default constructor invocation, and
                // no user-defined constructor could be found
                throw handleUnresolvedFunction(call, unresolvedConstructor, argTypes, null);
            }
        } else {
            SqlCall testCall =
                    resolvedConstructor.createCall(call.getParserPosition(), call.getOperandList());
            RelDataType returnType = resolvedConstructor.validateOperands(this, scope, testCall);
            assert type == returnType;
        }

        if (config.identifierExpansion()) {
            if (resolvedConstructor != null) {
                ((SqlBasicCall) call).setOperator(resolvedConstructor);
            } else {
                // fake a fully-qualified call to the default constructor
                ((SqlBasicCall) call)
                        .setOperator(
                                new SqlFunction(
                                        requireNonNull(
                                                type.getSqlIdentifier(),
                                                () -> "sqlIdentifier of " + type),
                                        ReturnTypes.explicit(type),
                                        null,
                                        null,
                                        null,
                                        SqlFunctionCategory.USER_DEFINED_CONSTRUCTOR));
            }
        }
        return type;
    }

    @Override
    public CalciteException handleUnresolvedFunction(
            SqlCall call,
            SqlOperator unresolvedFunction,
            List<RelDataType> argTypes,
            @Nullable List<String> argNames) {
        // For builtins, we can give a better error message
        final List<SqlOperator> overloads = new ArrayList<>();
        opTab.lookupOperatorOverloads(
                unresolvedFunction.getNameAsId(),
                null,
                SqlSyntax.FUNCTION,
                overloads,
                catalogReader.nameMatcher());
        if (overloads.size() == 1) {
            SqlFunction fun = (SqlFunction) overloads.get(0);
            if ((fun.getSqlIdentifier() == null) && (fun.getSyntax() != SqlSyntax.FUNCTION_ID)) {
                final int expectedArgCount = fun.getOperandCountRange().getMin();
                throw newValidationError(
                        call,
                        RESOURCE.invalidArgCount(call.getOperator().getName(), expectedArgCount));
            }
        }

        final String signature;
        if (unresolvedFunction instanceof SqlFunction) {
            // ----- FLINK MODIFICATION BEGIN -----
            final SqlOperandTypeChecker typeChecking =
                    new AssignableOperandTypeChecker(argTypes, argNames);
            final String invocation =
                    typeChecking.getAllowedSignatures(
                            unresolvedFunction, unresolvedFunction.getName());
            if (unresolvedFunction.getOperandTypeChecker() != null) {
                final String allowedSignatures =
                        unresolvedFunction
                                .getOperandTypeChecker()
                                .getAllowedSignatures(
                                        unresolvedFunction, unresolvedFunction.getName());
                throw newValidationError(
                        call,
                        EXTRA_RESOURCE.validatorNoFunctionMatch(invocation, allowedSignatures));
            } else {
                signature =
                        typeChecking.getAllowedSignatures(
                                unresolvedFunction, unresolvedFunction.getName());
            }
            // ----- FLINK MODIFICATION END -----
        } else {
            signature = unresolvedFunction.getName();
        }
        throw newValidationError(call, RESOURCE.validatorUnknownFunction(signature));
    }

    protected void inferUnknownTypes(
            RelDataType inferredType, SqlValidatorScope scope, SqlNode node) {
        // 确保传入的参数不为null
        requireNonNull(inferredType, "inferredType");
        requireNonNull(scope, "scope");
        requireNonNull(node, "node");
        // 尝试从节点到作用域映射中获取一个新的作用域，如果存在则更新当前作用域
        final SqlValidatorScope newScope = scopes.get(node);
        if (newScope != null) {
            scope = newScope;
        }
        // 检查节点是否为null字面量
        boolean isNullLiteral = SqlUtil.isNullLiteral(node, false);
        // 如果节点是动态参数或null字面量，则进行特殊处理
        if ((node instanceof SqlDynamicParam) || isNullLiteral) {
            // 如果推断的类型是未知类型，则进一步处理
            if (inferredType.equals(unknownType)) {
                if (isNullLiteral) {
                    // 如果是null字面量
                    if (config.typeCoercionEnabled()) {
                        // derive type of null literal
                        // 如果类型强制转换被启用，则尝试推导null字面量的类型
                        deriveType(scope, node);
                        return;
                    } else {
                        // 如果类型强制转换未启用，则抛出错误，因为null字面量在此上下文中不合法
                        throw newValidationError(node, RESOURCE.nullIllegal());
                    }
                } else {
                    // 如果是动态参数，但类型未知，则抛出错误
                    throw newValidationError(node, RESOURCE.dynamicParamIllegal());
                }
            }

            // REVIEW:  should dynamic parameter types always be nullable?
            // 审查：动态参数类型是否应该总是可为空？
            RelDataType newInferredType = typeFactory.createTypeWithNullability(inferredType, true);
            // 如果推断的类型是字符族（如VARCHAR, CHAR等），则还需要设置字符集和校对规则
            if (SqlTypeUtil.inCharFamily(inferredType)) {
                newInferredType =
                        typeFactory.createTypeWithCharsetAndCollation(
                                newInferredType,
                                getCharset(inferredType),
                                getCollation(inferredType));
            }
            // 设置验证后的节点类型为新的推断类型
            setValidatedNodeType(node, newInferredType);
            // 处理SqlNodeList的情况，即包含多个SqlNode的列表，如INSERT语句中的值列表
        } else if (node instanceof SqlNodeList) {
            SqlNodeList nodeList = (SqlNodeList) node;
            // 如果推断的类型是结构体类型，检查结构体的字段数量是否与节点列表的大小匹配
            if (inferredType.isStruct()) {
                if (inferredType.getFieldCount() != nodeList.size()) {
                    // this can happen when we're validating an INSERT
                    // where the source and target degrees are different;
                    // bust out, and the error will be detected higher up
                    //在验证INSERT语句时，源和目标度数不同；跳出
                    return;
                }
            }
            // 遍历节点列表，对每个子节点进行类型推断
            int i = 0;
            for (SqlNode child : nodeList) {
                RelDataType type;
                if (inferredType.isStruct()) {
                    // 如果推断的类型是结构体，则从结构体中获取对应字段的类型
                    type = inferredType.getFieldList().get(i).getType();
                    ++i;
                } else {
                    // 否则，使用整个推断的类型
                    type = inferredType;
                }
                // 对子节点进行类型推断
                inferUnknownTypes(type, scope, child);
            }
            // 处理SqlCase的情况，即CASE WHEN THEN ELSE语句
        } else if (node instanceof SqlCase) {
            final SqlCase caseCall = (SqlCase) node;
            // 确定WHEN子句的类型，如果CASE语句有值操作数，则为该值的类型，否则为布尔类型
            final RelDataType whenType =
                    caseCall.getValueOperand() == null ? booleanType : unknownType;
            // 对每个WHEN子句的操作数进行类型推断
            for (SqlNode sqlNode : caseCall.getWhenOperands()) {
                inferUnknownTypes(whenType, scope, sqlNode);
            }
            // 推导CASE语句的返回类型，这通常基于THEN子句的类型，但也可能有其他逻辑
            RelDataType returnType = deriveType(scope, node);
            // 对每个THEN子句的操作数进行类型推断
            for (SqlNode sqlNode : caseCall.getThenOperands()) {
                inferUnknownTypes(returnType, scope, sqlNode);
            }
            // 处理ELSE子句，如果ELSE子句不是NULL字面量，则进行类型推断
            SqlNode elseOperand =
                    requireNonNull(caseCall.getElseOperand(), () -> "elseOperand for " + caseCall);
            if (!SqlUtil.isNullLiteral(elseOperand, false)) {
                inferUnknownTypes(returnType, scope, elseOperand);
            } else {
                // 如果ELSE子句是NULL字面量，则直接设置其验证后的类型为RETURN_TYPE
                setValidatedNodeType(elseOperand, returnType);
            }
        } else if (node.getKind() == SqlKind.AS) {
            // For AS operator, only infer the operand not the alias
            // 处理AS操作符，只对其操作数进行类型推断，别名不需要类型推断
            inferUnknownTypes(inferredType, scope, ((SqlCall) node).operand(0));
        } else if (node instanceof SqlCall) {
            // 处理SqlCall节点，即函数调用
            final SqlCall call = (SqlCall) node;
            final SqlOperandTypeInference operandTypeInference =
                    call.getOperator().getOperandTypeInference();
            // 创建SqlCallBinding以绑定调用上下文和参数
            final SqlCallBinding callBinding = new SqlCallBinding(this, scope, call);
            final List<SqlNode> operands = callBinding.operands();
            final RelDataType[] operandTypes = new RelDataType[operands.size()];
            // 初始化操作数类型为UNKNOWN
            Arrays.fill(operandTypes, unknownType);
            // TODO:  eventually should assert(operandTypeInference != null)
            // instead; for now just eat it
            // 如果操作数类型推断不为空，则根据调用绑定和期望的返回类型推断操作数类型
            if (operandTypeInference != null) {
                operandTypeInference.inferOperandTypes(callBinding, inferredType, operandTypes);
            }
            // 遍历SqlCall的所有操作数
            for (int i = 0; i < operands.size(); ++i) {
                // 获取当前索引对应的操作数
                final SqlNode operand = operands.get(i);
                // 检查操作数是否为null，避免对null进行操作
                if (operand != null) {
                    // 对当前操作数进行类型推断
                    // operandTypes[i] 是之前通过 operandTypeInference.inferOperandTypes(...) 方法推断出的该操作数的期望类型
                    // scope 是当前的作用域，用于在类型推断过程中查找变量、类型等信息
                    // operand 是当前正在处理的操作数节点
                    inferUnknownTypes(operandTypes[i], scope, operand);
                }
            }
        }
    }

    /**
     * Adds an expression to a select list, ensuring that its alias does not clash with any existing
     * expressions on the list.
     */
    protected void addToSelectList(
            List<SqlNode> list,
            Set<String> aliases,
            List<Map.Entry<String, RelDataType>> fieldList,
            SqlNode exp,
            SelectScope scope,
            final boolean includeSystemVars) {
        String alias = SqlValidatorUtil.getAlias(exp, -1);
        String uniqueAlias =
                SqlValidatorUtil.uniquify(alias, aliases, SqlValidatorUtil.EXPR_SUGGESTER);
        if (!Objects.equals(alias, uniqueAlias)) {
            exp = SqlValidatorUtil.addAlias(exp, uniqueAlias);
        }
        fieldList.add(Pair.of(uniqueAlias, deriveType(scope, exp)));
        list.add(exp);
    }

    @Override
    public @Nullable String deriveAlias(SqlNode node, int ordinal) {
        return SqlValidatorUtil.getAlias(node, ordinal);
    }

    private String deriveAliasNonNull(SqlNode node, int ordinal) {
        return requireNonNull(
                deriveAlias(node, ordinal),
                () -> "non-null alias expected for node = " + node + ", ordinal = " + ordinal);
    }

    protected boolean shouldAllowIntermediateOrderBy() {
        return true;
    }

    private void registerMatchRecognize(
            SqlValidatorScope parentScope,
            SqlValidatorScope usingScope,
            SqlMatchRecognize call,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable) {

        final MatchRecognizeNamespace matchRecognizeNamespace =
                createMatchRecognizeNameSpace(call, enclosingNode);
        registerNamespace(usingScope, alias, matchRecognizeNamespace, forceNullable);

        final MatchRecognizeScope matchRecognizeScope = new MatchRecognizeScope(parentScope, call);
        scopes.put(call, matchRecognizeScope);

        // parse input query
        SqlNode expr = call.getTableRef();
        SqlNode newExpr =
                registerFrom(
                        usingScope,
                        matchRecognizeScope,
                        true,
                        expr,
                        expr,
                        null,
                        null,
                        forceNullable,
                        false);
        if (expr != newExpr) {
            call.setOperand(0, newExpr);
        }
    }

    protected MatchRecognizeNamespace createMatchRecognizeNameSpace(
            SqlMatchRecognize call, SqlNode enclosingNode) {
        return new MatchRecognizeNamespace(this, call, enclosingNode);
    }

    private void registerPivot(
            SqlValidatorScope parentScope,
            SqlValidatorScope usingScope,
            SqlPivot pivot,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable) {
        final PivotNamespace namespace = createPivotNameSpace(pivot, enclosingNode);
        registerNamespace(usingScope, alias, namespace, forceNullable);

        final SqlValidatorScope scope = new PivotScope(parentScope, pivot);
        scopes.put(pivot, scope);

        // parse input query
        SqlNode expr = pivot.query;
        SqlNode newExpr =
                registerFrom(
                        parentScope, scope, true, expr, expr, null, null, forceNullable, false);
        if (expr != newExpr) {
            pivot.setOperand(0, newExpr);
        }
    }

    protected PivotNamespace createPivotNameSpace(SqlPivot call, SqlNode enclosingNode) {
        return new PivotNamespace(this, call, enclosingNode);
    }

    private void registerUnpivot(
            SqlValidatorScope parentScope,
            SqlValidatorScope usingScope,
            SqlUnpivot call,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable) {
        final UnpivotNamespace namespace = createUnpivotNameSpace(call, enclosingNode);
        registerNamespace(usingScope, alias, namespace, forceNullable);

        final SqlValidatorScope scope = new UnpivotScope(parentScope, call);
        scopes.put(call, scope);

        // parse input query
        SqlNode expr = call.query;
        SqlNode newExpr =
                registerFrom(
                        parentScope, scope, true, expr, expr, null, null, forceNullable, false);
        if (expr != newExpr) {
            call.setOperand(0, newExpr);
        }
    }

    protected UnpivotNamespace createUnpivotNameSpace(SqlUnpivot call, SqlNode enclosingNode) {
        return new UnpivotNamespace(this, call, enclosingNode);
    }

    /**
     * Registers a new namespace, and adds it as a child of its parent scope. Derived class can
     * override this method to tinker with namespaces as they are created.
     *
     * @param usingScope Parent scope (which will want to look for things in this namespace)
     * @param alias Alias by which parent will refer to this namespace
     * @param ns Namespace
     * @param forceNullable Whether to force the type of namespace to be nullable
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 注册一个新的命名空间，并将其作为其父作用域的子节点添加。派生类可以重写此方法以在创建命名空间时对其进行修改。
     *
     * @param usingScope 父作用域（该作用域希望在此命名空间中查找事物）
     * @param alias 别名，父作用域将使用该别名引用此命名空间
     * @param ns 命名空间
     * @param forceNullable 是否强制命名空间的类型为可为空
     */
    protected void registerNamespace(
            @Nullable SqlValidatorScope usingScope,
            @Nullable String alias,
            SqlValidatorNamespace ns,
            boolean forceNullable) {
        // 将命名空间添加到命名空间集合中，使用命名空间节点的唯一标识作为键
        namespaces.put(requireNonNull(ns.getNode(), () -> "ns.getNode() for " + ns), ns);
        // 如果提供了父作用域
        if (usingScope != null) {
            // 如果别名为空，则抛出异常并显示错误消息
            assert alias != null
                    : "Registering namespace "
                            + ns
                            + ", into scope "
                            + usingScope
                            + ", so alias must not be null";
            // 将命名空间作为子节点添加到父作用域中，并使用指定的别名和是否可空标记
            usingScope.addChild(ns, alias, forceNullable);
        }
    }

    /**
     * Registers scopes and namespaces implied a relational expression in the FROM clause.
     *
     * <p>{@code parentScope} and {@code usingScope} are often the same. They differ when the
     * namespace are not visible within the parent. (Example needed.)
     *
     * <p>Likewise, {@code enclosingNode} and {@code node} are often the same. {@code enclosingNode}
     * is the topmost node within the FROM clause, from which any decorations like an alias (<code>
     * AS alias</code>) or a table sample clause are stripped away to get {@code node}. Both are
     * recorded in the namespace.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param register Whether to register this scope as a child of {@code usingScope}
     * @param node Node which namespace is based on
     * @param enclosingNode Outermost node for namespace, including decorations such as alias and
     *     sample clause
     * @param alias Alias
     * @param extendList Definitions of extended columns
     * @param forceNullable Whether to force the type of namespace to be nullable because it is in
     *     an outer join
     * @param lateral Whether LATERAL is specified, so that items to the left of this in the JOIN
     *     tree are visible in the scope
     * @return registered node, usually the same as {@code node}
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 作用域和命名空间隐含了FROM子句中的关系表达式。
     * 注册FROM子句中的节点，并可能为其添加别名。
     *
     * @param parentScope 父作用域
     * @param usingScope USING子句的作用域（如果存在）
     * @param register 是否需要注册该节点
     * @param node 需要注册的SQL节点
     * @param enclosingNode 包含此节点的外部节点（如SELECT语句）
     * @param alias 节点别名，可能为null
     * @param extendList 扩展列表（如LATERAL JOIN的扩展条件），可能为null
     * @param forceNullable 是否强制节点为可空
     * @param lateral 是否为LATERAL JOIN
     * @return 注册后的SQL节点（可能包含别名）
     */
    private SqlNode registerFrom(
            SqlValidatorScope parentScope,
            SqlValidatorScope usingScope,
            boolean register,
            final SqlNode node,
            SqlNode enclosingNode,
            @Nullable String alias,
            @Nullable SqlNodeList extendList,
            boolean forceNullable,
            final boolean lateral) {
        final SqlKind kind = node.getKind();

        SqlNode expr;
        SqlNode newExpr;

        // Add an alias if necessary.
        // 如果需要，为节点添加别名。
        SqlNode newNode = node;
        if (alias == null) {
            switch (kind) {
                case IDENTIFIER:// 如果节点是标识符（如表名或列名）
                case OVER:// 如果节点是OVER子句（如窗口函数）
                    // 尝试从节点本身推导出别名
                    alias = deriveAlias(node, -1);
                    // 如果无法推导出别名，则使用自动生成的ID作为别名
                    if (alias == null) {
                        alias = deriveAliasNonNull(node, nextGeneratedId++);
                    }
                    // 如果配置要求标识符扩展，则为节点添加别名
                    if (config.identifierExpansion()) {
                        newNode = SqlValidatorUtil.addAlias(node, alias);
                    }
                    break;

                // 当节点是以下类型之一时：
                // SELECT：选择语句
                // UNION：联合查询
                // INTERSECT：交集查询
                // EXCEPT：差集查询
                // VALUES：值构造器
                // UNNEST：展开集合（常用于将数组展开成行）
                // OTHER_FUNCTION：其他函数（可能是某个特定的函数节点类型）
                // COLLECTION_TABLE：集合表（例如，一个表值函数返回的集合）
                // PIVOT：透视操作（将行转换为列）
                // UNPIVOT：取消透视操作（将列转换为行）
                // MATCH_RECOGNIZE：模式识别（用于在流或表中识别复杂事件模式）
                case SELECT:
                case UNION:
                case INTERSECT:
                case EXCEPT:
                case VALUES:
                case UNNEST:
                case OTHER_FUNCTION:
                case COLLECTION_TABLE:
                case PIVOT:
                case UNPIVOT:
                case MATCH_RECOGNIZE:

                    // give this anonymous construct a name since later
                    // query processing stages rely on it

                    // 使用自动生成的ID作为别名
                    alias = deriveAliasNonNull(node, nextGeneratedId++);
                    if (config.identifierExpansion()) {
                        // Since we're expanding identifiers, we should make the
                        // aliases explicit too, otherwise the expanded query
                        // will not be consistent if we convert back to SQL, e.g.
                        // "select EXPR$1.EXPR$2 from values (1)".
                        // 展开的查询将不一致，例如："select EXPR$1.EXPR$2 from values (1)"。
                        newNode = SqlValidatorUtil.addAlias(node, alias);
                    }
                    break;
                default:
                    // 其他情况，不进行特别处理
                    break;
            }
        }
        // 如果该节点是LATERAL JOIN的一部分
        if (lateral) {
            // 从usingScope开始，向上遍历作用域链，直到找到非JoinScope的作用域
            SqlValidatorScope s = usingScope;
            while (s instanceof JoinScope) {
                s = ((JoinScope) s).getUsingScope();
            }
            // 获取最终作用域s对应的节点，如果s为空，则使用原始节点node
            final SqlNode node2 = s != null ? s.getNode() : node;
            // 创建一个新的TableScope，该作用域以parentScope为父作用域，并关联节点node2
            final TableScope tableScope = new TableScope(parentScope, node2);
            // 如果usingScope是ListScope类型（可能包含多个子作用域）
            if (usingScope instanceof ListScope) {
                // 遍历ListScope的子作用域，并将它们的namespace、name和nullable添加到新的TableScope中
                for (ScopeChild child : ((ListScope) usingScope).children) {
                    tableScope.addChild(child.namespace, child.name, child.nullable);
                }
            }
            // 将parentScope更新为新的TableScope，后续处理将使用这个新的作用域
            parentScope = tableScope;
        }

        SqlCall call; // SQL调用对象，表示一个SQL函数调用或SQL操作（如SELECT, AS等）
        SqlNode operand;  // 操作数节点，表示SQL操作中的某个参数或子表达式
        SqlNode newOperand; // 新的操作数节点，可能是在处理过程中生成或修改的
        // 根据kind（操作类型）进行不同的处理
        switch (kind) {
            case AS: // 如果是AS操作
                // 将节点强制转换为SqlCall类型，因为AS通常是一个函数调用或操作
                call = (SqlCall) node;
                // 如果别名alias尚未设置，则从call的第二个操作数中获取别名
                if (alias == null) {
                    alias = String.valueOf(call.operand(1));// 获取AS操作后的别名
                }
                // 获取AS操作前的表达式（即需要设置别名的原始表达式）
                expr = call.operand(0);
                // 判断是否需要为别名添加命名空间
                // 需要添加的情况包括：
                // 1. 操作数数量大于2（AS后可能还有其他参数）
                // 2. 原始表达式是VALUES类型（如VALUES子句）
                // 3. 原始表达式是UNNEST类型（如UNNEST函数）
                final boolean needAliasNamespace =
                        call.operandCount() > 2
                                || expr.getKind() == SqlKind.VALUES
                                || expr.getKind() == SqlKind.UNNEST;
                // 调用registerFrom方法处理原始表达式，并尝试为其注册一个别名
                // 参数包括父作用域、当前作用域、是否需要别名命名空间、原始表达式、封闭节点、别名等
                newExpr =
                        registerFrom(
                                parentScope,
                                usingScope,
                                !needAliasNamespace,
                                expr,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral);
                // 如果处理后的新表达式与原始表达式不同，则更新SqlCall中的操作数
                if (newExpr != expr) {
                    call.setOperand(0, newExpr);// 将AS操作前的表达式替换为新的表达式
                }

                // If alias has a column list, introduce a namespace to translate
                // column names. We skipped registering it just now.
                // 如果别名alias包含了列列表（如子查询中的列），则需要引入一个命名空间来转换列名。
                // 我们刚才并没有直接注册它（因为别名可能是通过AS操作引入的）。
                if (needAliasNamespace) {
                    // 注册一个命名空间，将别名alias与一个新的AliasNamespace对象关联起来。
                    // AliasNamespace对象可能包含了一些用于解析列名的逻辑和上下文信息。
                    // 参数包括：当前作用域usingScope、别名alias、新创建的AliasNamespace对象、是否强制设置为可为空forceNullable。
                    registerNamespace(
                            usingScope,
                            alias,
                            new AliasNamespace(this, call, enclosingNode),
                            forceNullable);
                }
                // 返回原始的节点node
                return node;

            case MATCH_RECOGNIZE:
                registerMatchRecognize(
                        parentScope,
                        usingScope,
                        (SqlMatchRecognize) node,
                        enclosingNode,
                        alias,
                        forceNullable);
                return node;

            case PIVOT:
                registerPivot(
                        parentScope,
                        usingScope,
                        (SqlPivot) node,
                        enclosingNode,
                        alias,
                        forceNullable);
                return node;

            case UNPIVOT:
                registerUnpivot(
                        parentScope,
                        usingScope,
                        (SqlUnpivot) node,
                        enclosingNode,
                        alias,
                        forceNullable);
                return node;

            case TABLESAMPLE:
                call = (SqlCall) node;
                expr = call.operand(0);
                newExpr =
                        registerFrom(
                                parentScope,
                                usingScope,
                                true,
                                expr,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral);
                if (newExpr != expr) {
                    call.setOperand(0, newExpr);
                }
                return node;

            case JOIN:
                final SqlJoin join = (SqlJoin) node;
                final JoinScope joinScope = new JoinScope(parentScope, usingScope, join);
                scopes.put(join, joinScope);
                final SqlNode left = join.getLeft();
                final SqlNode right = join.getRight();
                boolean forceLeftNullable = forceNullable;
                boolean forceRightNullable = forceNullable;
                switch (join.getJoinType()) {
                    case LEFT:
                        forceRightNullable = true;
                        break;
                    case RIGHT:
                        forceLeftNullable = true;
                        break;
                    case FULL:
                        forceLeftNullable = true;
                        forceRightNullable = true;
                        break;
                    default:
                        break;
                }
                final SqlNode newLeft =
                        registerFrom(
                                parentScope,
                                joinScope,
                                true,
                                left,
                                left,
                                null,
                                null,
                                forceLeftNullable,
                                lateral);
                if (newLeft != left) {
                    join.setLeft(newLeft);
                }
                final SqlNode newRight =
                        registerFrom(
                                parentScope,
                                joinScope,
                                true,
                                right,
                                right,
                                null,
                                null,
                                forceRightNullable,
                                lateral);
                if (newRight != right) {
                    join.setRight(newRight);
                }
                registerSubQueries(joinScope, join.getCondition());
                final JoinNamespace joinNamespace = new JoinNamespace(this, join);
                registerNamespace(null, null, joinNamespace, forceNullable);
                return join;

            case IDENTIFIER:
                // 将节点强制转换为SqlIdentifier类型，
                final SqlIdentifier id = (SqlIdentifier) node;
                // 创建一个新的标识符命名空间，该命名空间包含了当前作用域的信息、标识符、扩展列表（如果有的话）、包含该标识符的节点，以及父作用域
                final IdentifierNamespace newNs =
                        new IdentifierNamespace(this, id, extendList, enclosingNode, parentScope);
                // 将新创建的命名空间注册到指定作用域（如果使用scope参数）或忽略注册（如果不使用scope参数）
                registerNamespace(register ? usingScope : null, alias, newNs, forceNullable);
                // 如果tableScope尚未初始化，则基于父作用域和当前节点创建一个新的TableScope
                // TableScope可能用于表示表级别的作用域或类似的上下文
                if (tableScope == null) {
                    tableScope = new TableScope(parentScope, node);
                }
                // 将新创建的命名空间作为子命名空间添加到tableScope中
                // alias是必需的，使用requireNonNull确保不为null，forceNullable指定是否允许该命名空间中的标识符为null
                tableScope.addChild(newNs, requireNonNull(alias, "alias"), forceNullable);
                if (extendList != null && extendList.size() != 0) {
                    return enclosingNode;
                }
                // 这表示处理已完成，可以返回给调用者
                return newNode;

            case LATERAL:
                return registerFrom(
                        parentScope,
                        usingScope,
                        register,
                        ((SqlCall) node).operand(0),
                        enclosingNode,
                        alias,
                        extendList,
                        forceNullable,
                        true);

            case COLLECTION_TABLE:
                call = (SqlCall) node;
                operand = call.operand(0);
                newOperand =
                        registerFrom(
                                parentScope,
                                usingScope,
                                register,
                                operand,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral);
                if (newOperand != operand) {
                    call.setOperand(0, newOperand);
                }
                // If the operator is SqlWindowTableFunction, restricts the scope as
                // its first operand's (the table) scope.
                if (operand instanceof SqlBasicCall) {
                    final SqlBasicCall call1 = (SqlBasicCall) operand;
                    final SqlOperator op = call1.getOperator();
                    if (op instanceof SqlWindowTableFunction
                            && call1.operand(0).getKind() == SqlKind.SELECT) {
                        scopes.put(node, getSelectScope(call1.operand(0)));
                        return newNode;
                    }
                }
                // Put the usingScope which can be a JoinScope
                // or a SelectScope, in order to see the left items
                // of the JOIN tree.
                scopes.put(node, usingScope);
                return newNode;

            case UNNEST:
                if (!lateral) {
                    return registerFrom(
                            parentScope,
                            usingScope,
                            register,
                            node,
                            enclosingNode,
                            alias,
                            extendList,
                            forceNullable,
                            true);
                }
                // fall through
            case SELECT:
            case UNION:
            case INTERSECT:
            case EXCEPT:
            case VALUES:
            case WITH:
            case OTHER_FUNCTION:
                if (alias == null) {
                    alias = deriveAliasNonNull(node, nextGeneratedId++);
                }
                registerQuery(
                        parentScope,
                        register ? usingScope : null,
                        node,
                        enclosingNode,
                        alias,
                        forceNullable);
                return newNode;

            case OVER:
                if (!shouldAllowOverRelation()) {
                    throw Util.unexpected(kind);
                }
                call = (SqlCall) node;
                final OverScope overScope = new OverScope(usingScope, call);
                scopes.put(call, overScope);
                operand = call.operand(0);
                newOperand =
                        registerFrom(
                                parentScope,
                                overScope,
                                true,
                                operand,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral);
                if (newOperand != operand) {
                    call.setOperand(0, newOperand);
                }

                for (ScopeChild child : overScope.children) {
                    registerNamespace(
                            register ? usingScope : null,
                            child.name,
                            child.namespace,
                            forceNullable);
                }

                return newNode;

            case TABLE_REF:
                call = (SqlCall) node;
                registerFrom(
                        parentScope,
                        usingScope,
                        register,
                        call.operand(0),
                        enclosingNode,
                        alias,
                        extendList,
                        forceNullable,
                        lateral);
                if (extendList != null && extendList.size() != 0) {
                    return enclosingNode;
                }
                return newNode;

            case EXTEND:
                final SqlCall extend = (SqlCall) node;
                return registerFrom(
                        parentScope,
                        usingScope,
                        true,
                        extend.getOperandList().get(0),
                        extend,
                        alias,
                        (SqlNodeList) extend.getOperandList().get(1),
                        forceNullable,
                        lateral);

            case SNAPSHOT:
                call = (SqlCall) node;
                operand = call.operand(0);
                newOperand =
                        registerFrom(
                                parentScope,
                                usingScope,
                                register,
                                operand,
                                enclosingNode,
                                alias,
                                extendList,
                                forceNullable,
                                lateral);
                if (newOperand != operand) {
                    call.setOperand(0, newOperand);
                }
                // Put the usingScope which can be a JoinScope
                // or a SelectScope, in order to see the left items
                // of the JOIN tree.
                scopes.put(node, usingScope);
                return newNode;

            default:
                throw Util.unexpected(kind);
        }
    }

    protected boolean shouldAllowOverRelation() {
        return false;
    }

    /**
     * Creates a namespace for a <code>SELECT</code> node. Derived class may override this factory
     * method.
     *
     * @param select Select node
     * @param enclosingNode Enclosing node
     * @return Select namespace
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 为<code>SELECT</code>节点创建一个命名空间。派生类可以覆盖此工厂方法。
     *
     * @param select SELECT节点
     * @param enclosingNode 包围节点（即包含此SELECT节点的上级节点）
     * @return SELECT命名空间对象
     */
    protected SelectNamespace createSelectNamespace(SqlSelect select, SqlNode enclosingNode) {
        return new SelectNamespace(this, select, enclosingNode);
    }

    /**
     * Creates a namespace for a set operation (<code>UNION</code>, <code>
     * INTERSECT</code>, or <code>EXCEPT</code>). Derived class may override this factory method.
     *
     * @param call Call to set operation
     * @param enclosingNode Enclosing node
     * @return Set operation namespace
     */
    protected SetopNamespace createSetopNamespace(SqlCall call, SqlNode enclosingNode) {
        return new SetopNamespace(this, call, enclosingNode);
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if usingScope != null
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 在父作用域中注册一个查询。
     *
     * @param parentScope 父作用域，此作用域在解析对象时会转向的父作用域
     * @param usingScope 使用的作用域，此作用域的子列表应添加当前作用域。如果不需要将当前作用域添加到任何子列表中，则为null
     * @param node 查询节点
     * @param enclosingNode 包含查询节点的节点（通常用于解析上下文）
     * @param alias 在其父作用域中此查询的名称。如果使用作用域（usingScope）不为null，则必须指定此别名
     * @param forceNullable 是否强制将此查询的结果设为可为空（nullable）。这可能会影响查询结果的类型推断
     */
    private void registerQuery(
            SqlValidatorScope parentScope,
            @Nullable SqlValidatorScope usingScope,
            SqlNode node,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable) {
        // 验证参数，确保如果使用作用域（usingScope）不为null，则别名（alias）必须被指定
        Preconditions.checkArgument(usingScope == null || alias != null);
        // 调用另一个重载版本的registerQuery方法，增加了一个额外的参数，可能用于控制额外的注册行为
        registerQuery(parentScope, usingScope, node, enclosingNode, alias, forceNullable, true);
    }

    /**
     * Registers a query in a parent scope.
     *
     * @param parentScope Parent scope which this scope turns to in order to resolve objects
     * @param usingScope Scope whose child list this scope should add itself to
     * @param node Query node
     * @param alias Name of this query within its parent. Must be specified if usingScope != null
     * @param checkUpdate if true, validate that the update feature is supported if validating the
     *     update statement
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 在父作用域中注册一个查询。
     *
     * @param parentScope 父作用域，此作用域在解析对象时会转向的父作用域
     * @param usingScope 使用的作用域，此作用域的子列表应添加当前作用域。如果不需要将当前作用域添加到任何子列表中，则为null
     * @param node 查询节点
     * @param enclosingNode 包含查询节点的节点（通常用于解析上下文）
     * @param alias 在其父作用域中此查询的名称。如果使用作用域（usingScope）不为null，则必须指定此别名
     * @param forceNullable 是否强制将此查询的结果设为可为空（nullable）。这可能会影响查询结果的类型推断
     * @param checkUpdate 如果为true，则在验证更新语句时验证是否支持更新功能
     */
    private void registerQuery(
            SqlValidatorScope parentScope,
            @Nullable SqlValidatorScope usingScope,
            SqlNode node,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable,
            boolean checkUpdate) {
        // 检查查询节点和包含查询节点的节点是否非空
        requireNonNull(node, "node");
        requireNonNull(enclosingNode, "enclosingNode");
        // 验证参数，确保如果使用作用域（usingScope）不为null，则别名（alias）必须被指定
        Preconditions.checkArgument(usingScope == null || alias != null);

        SqlCall call;
        List<SqlNode> operands;
        // 根据node的类型进行判断和处理
        switch (node.getKind()) {
            case SELECT:
                // 如果node是一个SqlSelect类型的节点
                final SqlSelect select = (SqlSelect) node;
                // 创建一个SelectNamespace对象，用于在验证和解析过程中存储关于SELECT语句的信息
                final SelectNamespace selectNs = createSelectNamespace(select, enclosingNode);
                // 在使用作用域中注册此查询的命名空间，如果usingScope为null，则使用parentScope
                registerNamespace(usingScope, alias, selectNs, forceNullable);
                // 设置窗口函数的父作用域，如果usingScope不为null，则使用usingScope，否则使用parentScope
                final SqlValidatorScope windowParentScope =
                        (usingScope != null) ? usingScope : parentScope;
                // 创建一个SelectScope对象，用于在验证和解析过程中存储关于SELECT语句的作用域信息
                SelectScope selectScope = new SelectScope(parentScope, windowParentScope, select);
                // 将SelectScope对象存储到scopes映射中，以SqlSelect节点为键
                scopes.put(select, selectScope);

                // Start by registering the WHERE clause
                // 首先注册WHERE子句的作用域
                // clauseScopes映射存储了子句和它们对应的作用域信息
                clauseScopes.put(IdPair.of(select, Clause.WHERE), selectScope);
                // 注册WHERE子句中的子查询
                registerOperandSubQueries(selectScope, select, SqlSelect.WHERE_OPERAND);

                // Register FROM with the inherited scope 'parentScope', not
                // 'selectScope', otherwise tables in the FROM clause would be
                // able to see each other.
                // 注册FROM子句时，使用继承的父作用域'parentScope'，而不是'selectScope'。
                // 这是因为如果使用'selectScope'，FROM子句中的表将能够相互看到对方，
                // 这在SQL的语义中是不正确的，因为FROM子句中的表是独立的。
                // 因此，我们需要确保它们各自在父作用域中独立注册。

                //获取From对应的 SQLNode
                final SqlNode from = select.getFrom();
                if (from != null) {
                    final SqlNode newFrom =
                            registerFrom(
                                    parentScope,// 使用父作用域
                                    selectScope,// 提供selectScope作为上下文（可能用于某些特殊处理）
                                    true,
                                    from,
                                    from,
                                    null,
                                    null,
                                    false,
                                    false);
                    if (newFrom != from) {
                        // 如果FROM子句在注册过程中被修改（例如，通过别名替换等），则更新select节点的FROM子句
                        select.setFrom(newFrom);
                    }
                }

                // If this is an aggregating query, the SELECT list and HAVING
                // clause use a different scope, where you can only reference
                // columns which are in the GROUP BY clause.

                // 如果这是一个聚合查询（即包含GROUP BY子句或聚合函数），
                // 则SELECT列表和HAVING子句将使用不同的作用域，
                // 在这个作用域中，你只能引用GROUP BY子句中的列。
                SqlValidatorScope aggScope = selectScope;
                if (isAggregate(select)) {
                    // 创建一个AggregatingSelectScope对象，用于处理聚合查询中的SELECT和HAVING子句
                    aggScope = new AggregatingSelectScope(selectScope, select, false);
                    // 将SELECT子句的作用域设置为aggScope
                    clauseScopes.put(IdPair.of(select, Clause.SELECT), aggScope);
                } else {
                    // 如果不是聚合查询，则直接使用selectScope作为SELECT子句的作用域
                    clauseScopes.put(IdPair.of(select, Clause.SELECT), selectScope);
                }
                // 如果SELECT语句包含GROUP BY子句
                if (select.getGroup() != null) {
                    // 创建一个GroupByScope对象，用于处理GROUP BY子句
                    GroupByScope groupByScope =
                            new GroupByScope(selectScope, select.getGroup(), select);
                    // 将GROUP BY子句的作用域设置为groupByScope
                    clauseScopes.put(IdPair.of(select, Clause.GROUP_BY), groupByScope);
                    // 注册GROUP BY子句中的子查询
                    registerSubQueries(groupByScope, select.getGroup());
                }
                // 注册HAVING子句中的子查询（如果有的话）
                registerOperandSubQueries(aggScope, select, SqlSelect.HAVING_OPERAND);
                // 注册SELECT语句中的子查询
                registerSubQueries(aggScope, SqlNonNullableAccessors.getSelectList(select));
                // 获取SELECT语句的ORDER BY子句
                final SqlNodeList orderList = select.getOrderList();
                if (orderList != null) {
                    // If the query is 'SELECT DISTINCT', restrict the columns
                    // available to the ORDER BY clause.
                    // 如果查询是'SELECT DISTINCT'，则限制在ORDER BY子句中可用的列。
                    // 这意味着DISTINCT查询的ORDER BY子句只能引用DISTINCT列表中的列。
                    if (select.isDistinct()) {
                        aggScope = new AggregatingSelectScope(selectScope, select, true);
                    }
                    // 创建一个OrderByScope对象，用于处理ORDER BY子句
                    OrderByScope orderScope = new OrderByScope(aggScope, orderList, select);
                    // 将ORDER BY子句的作用域设置为orderScope
                    clauseScopes.put(IdPair.of(select, Clause.ORDER), orderScope);
                    // 注册ORDER BY子句中的子查询（如果有的话）
                    registerSubQueries(orderScope, orderList);

                    if (!isAggregate(select)) {
                        // Since this is not an aggregating query,
                        // there cannot be any aggregates in the ORDER BY clause.

                        // 因为这不是一个聚合查询，
                        // 所以ORDER BY子句中不能有任何聚合函数。
                        // 查找ORDER BY子句中的聚合函数
                        SqlNode agg = aggFinder.findAgg(orderList);
                        if (agg != null) {
                            // 如果找到了聚合函数，则抛出验证错误
                            throw newValidationError(agg, RESOURCE.aggregateIllegalInOrderBy());
                        }
                    }
                }
                // 结束处理当前SELECT语句
                break;

            case INTERSECT:
                validateFeature(RESOURCE.sQLFeature_F302(), node.getParserPosition());
                registerSetop(parentScope, usingScope, node, node, alias, forceNullable);
                break;

            case EXCEPT:
                validateFeature(RESOURCE.sQLFeature_E071_03(), node.getParserPosition());
                registerSetop(parentScope, usingScope, node, node, alias, forceNullable);
                break;

            case UNION:
                registerSetop(parentScope, usingScope, node, node, alias, forceNullable);
                break;

            case WITH:
                registerWith(
                        parentScope,
                        usingScope,
                        (SqlWith) node,
                        enclosingNode,
                        alias,
                        forceNullable,
                        checkUpdate);
                break;

            case VALUES:
                call = (SqlCall) node;
                scopes.put(call, parentScope);
                final TableConstructorNamespace tableConstructorNamespace =
                        new TableConstructorNamespace(this, call, parentScope, enclosingNode);
                registerNamespace(usingScope, alias, tableConstructorNamespace, forceNullable);
                operands = call.getOperandList();
                for (int i = 0; i < operands.size(); ++i) {
                    assert operands.get(i).getKind() == SqlKind.ROW;

                    // FIXME jvs 9-Feb-2005:  Correlation should
                    // be illegal in these sub-queries.  Same goes for
                    // any non-lateral SELECT in the FROM list.
                    registerOperandSubQueries(parentScope, call, i);
                }
                break;

            case INSERT:
                SqlInsert insertCall = (SqlInsert) node;
                InsertNamespace insertNs =
                        new InsertNamespace(this, insertCall, enclosingNode, parentScope);
                registerNamespace(usingScope, null, insertNs, forceNullable);
                registerQuery(
                        parentScope,
                        usingScope,
                        insertCall.getSource(),
                        enclosingNode,
                        null,
                        false);
                break;

            case DELETE:
                SqlDelete deleteCall = (SqlDelete) node;
                DeleteNamespace deleteNs =
                        new DeleteNamespace(this, deleteCall, enclosingNode, parentScope);
                registerNamespace(usingScope, null, deleteNs, forceNullable);
                registerQuery(
                        parentScope,
                        usingScope,
                        SqlNonNullableAccessors.getSourceSelect(deleteCall),
                        enclosingNode,
                        null,
                        false);
                break;

            case UPDATE:
                if (checkUpdate) {
                    validateFeature(RESOURCE.sQLFeature_E101_03(), node.getParserPosition());
                }
                SqlUpdate updateCall = (SqlUpdate) node;
                UpdateNamespace updateNs =
                        new UpdateNamespace(this, updateCall, enclosingNode, parentScope);
                registerNamespace(usingScope, null, updateNs, forceNullable);
                registerQuery(
                        parentScope,
                        usingScope,
                        SqlNonNullableAccessors.getSourceSelect(updateCall),
                        enclosingNode,
                        null,
                        false);
                break;

            case MERGE:
                validateFeature(RESOURCE.sQLFeature_F312(), node.getParserPosition());
                SqlMerge mergeCall = (SqlMerge) node;
                MergeNamespace mergeNs =
                        new MergeNamespace(this, mergeCall, enclosingNode, parentScope);
                registerNamespace(usingScope, null, mergeNs, forceNullable);
                registerQuery(
                        parentScope,
                        usingScope,
                        SqlNonNullableAccessors.getSourceSelect(mergeCall),
                        enclosingNode,
                        null,
                        false);

                // update call can reference either the source table reference
                // or the target table, so set its parent scope to the merge's
                // source select; when validating the update, skip the feature
                // validation check
                SqlUpdate mergeUpdateCall = mergeCall.getUpdateCall();
                if (mergeUpdateCall != null) {
                    registerQuery(
                            getScope(
                                    SqlNonNullableAccessors.getSourceSelect(mergeCall),
                                    Clause.WHERE),
                            null,
                            mergeUpdateCall,
                            enclosingNode,
                            null,
                            false,
                            false);
                }
                SqlInsert mergeInsertCall = mergeCall.getInsertCall();
                if (mergeInsertCall != null) {
                    registerQuery(parentScope, null, mergeInsertCall, enclosingNode, null, false);
                }
                break;

            case UNNEST:
                call = (SqlCall) node;
                final UnnestNamespace unnestNs =
                        new UnnestNamespace(this, call, parentScope, enclosingNode);
                registerNamespace(usingScope, alias, unnestNs, forceNullable);
                registerOperandSubQueries(parentScope, call, 0);
                scopes.put(node, parentScope);
                break;
            case OTHER_FUNCTION:
                call = (SqlCall) node;
                ProcedureNamespace procNs =
                        new ProcedureNamespace(this, parentScope, call, enclosingNode);
                registerNamespace(usingScope, alias, procNs, forceNullable);
                registerSubQueries(parentScope, call);
                break;

            case MULTISET_QUERY_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
                validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
                call = (SqlCall) node;
                CollectScope cs = new CollectScope(parentScope, usingScope, call);
                final CollectNamespace tableConstructorNs =
                        new CollectNamespace(call, cs, enclosingNode);
                final String alias2 = deriveAliasNonNull(node, nextGeneratedId++);
                registerNamespace(usingScope, alias2, tableConstructorNs, forceNullable);
                operands = call.getOperandList();
                for (int i = 0; i < operands.size(); i++) {
                    registerOperandSubQueries(parentScope, call, i);
                }
                break;

            default:
                throw Util.unexpected(node.getKind());
        }
    }

    private void registerSetop(
            SqlValidatorScope parentScope,
            @Nullable SqlValidatorScope usingScope,
            SqlNode node,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable) {
        SqlCall call = (SqlCall) node;
        final SetopNamespace setopNamespace = createSetopNamespace(call, enclosingNode);
        registerNamespace(usingScope, alias, setopNamespace, forceNullable);

        // A setop is in the same scope as its parent.
        scopes.put(call, parentScope);
        for (SqlNode operand : call.getOperandList()) {
            registerQuery(parentScope, null, operand, operand, null, false);
        }
    }

    private void registerWith(
            SqlValidatorScope parentScope,
            @Nullable SqlValidatorScope usingScope,
            SqlWith with,
            SqlNode enclosingNode,
            @Nullable String alias,
            boolean forceNullable,
            boolean checkUpdate) {
        final WithNamespace withNamespace = new WithNamespace(this, with, enclosingNode);
        registerNamespace(usingScope, alias, withNamespace, forceNullable);

        SqlValidatorScope scope = parentScope;
        for (SqlNode withItem_ : with.withList) {
            final SqlWithItem withItem = (SqlWithItem) withItem_;
            final WithScope withScope = new WithScope(scope, withItem);
            scopes.put(withItem, withScope);

            registerQuery(scope, null, withItem.query, with, withItem.name.getSimple(), false);
            registerNamespace(
                    null, alias, new WithItemNamespace(this, withItem, enclosingNode), false);
            scope = withScope;
        }

        registerQuery(scope, null, with.body, enclosingNode, alias, forceNullable, checkUpdate);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *
     */
    @Override
    public boolean isAggregate(SqlSelect select) {
        if (getAggregate(select) != null) {
            return true;
        }
        // Also when nested window aggregates are present
        for (SqlCall call : overFinder.findAll(SqlNonNullableAccessors.getSelectList(select))) {
            assert call.getKind() == SqlKind.OVER;
            if (isNestedAggregateWindow(call.operand(0))) {
                return true;
            }
            if (isOverAggregateWindow(call.operand(1))) {
                return true;
            }
        }
        return false;
    }

    protected boolean isNestedAggregateWindow(SqlNode node) {
        AggFinder nestedAggFinder =
                new AggFinder(opTab, false, false, false, aggFinder, catalogReader.nameMatcher());
        return nestedAggFinder.findAgg(node) != null;
    }

    protected boolean isOverAggregateWindow(SqlNode node) {
        return aggFinder.findAgg(node) != null;
    }

    /**
     * Returns the parse tree node (GROUP BY, HAVING, or an aggregate function call) that causes
     * {@code select} to be an aggregate query, or null if it is not an aggregate query.
     *
     * <p>The node is useful context for error messages, but you cannot assume that the node is the
     * only aggregate function.
     */
    protected @Nullable SqlNode getAggregate(SqlSelect select) {
        SqlNode node = select.getGroup();
        if (node != null) {
            return node;
        }
        node = select.getHaving();
        if (node != null) {
            return node;
        }
        return getAgg(select);
    }

    /** If there is at least one call to an aggregate function, returns the first. */
    private @Nullable SqlNode getAgg(SqlSelect select) {
        final SelectScope selectScope = getRawSelectScope(select);
        if (selectScope != null) {
            final List<SqlNode> selectList = selectScope.getExpandedSelectList();
            if (selectList != null) {
                return aggFinder.findAgg(selectList);
            }
        }
        return aggFinder.findAgg(SqlNonNullableAccessors.getSelectList(select));
    }

    @Deprecated
    @Override
    public boolean isAggregate(SqlNode selectNode) {
        return aggFinder.findAgg(selectNode) != null;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证给定SQL节点的特性。
     *
     * @param node 需要验证特性的SQL节点
     */
    private void validateNodeFeature(SqlNode node) {
        // 根据节点的类型进行特性验证
        switch (node.getKind()) {
            // 如果节点是多重集值构造器，则验证其特性
            case MULTISET_VALUE_CONSTRUCTOR:
                validateFeature(RESOURCE.sQLFeature_S271(), node.getParserPosition());
                break;
            default:
                // 对于其他类型的节点，不进行特性验证
                break;
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 注册给定节点及其子节点中的所有子查询。
     *
     * @param parentScope 父作用域
     * @param node SQL节点，可能为null
     */
    private void registerSubQueries(SqlValidatorScope parentScope, @Nullable SqlNode node) {
        // 如果节点为空，则直接返回
        if (node == null) {
            return;
        }
        // 如果节点是查询类型、多重集查询构造器或多重集值构造器
        if (node.getKind().belongsTo(SqlKind.QUERY)
                || node.getKind() == SqlKind.MULTISET_QUERY_CONSTRUCTOR
                || node.getKind() == SqlKind.MULTISET_VALUE_CONSTRUCTOR) {
            // 注册该查询
            registerQuery(parentScope, null, node, node, null, false);
        } else if (node instanceof SqlCall) {
            // 如果节点是SQL调用（如函数或操作符）
            validateNodeFeature(node);// 验证节点的特性
            SqlCall call = (SqlCall) node;
            // 遍历调用中的每个操作数
            for (int i = 0; i < call.operandCount(); i++) {
                // 注册操作数中的子查询
                registerOperandSubQueries(parentScope, call, i);
            }
        } else if (node instanceof SqlNodeList) {
            // 如果节点是SQL节点列表（如SELECT语句中的列列表）
            SqlNodeList list = (SqlNodeList) node;
            // 遍历列表中的每个节点
            for (int i = 0, count = list.size(); i < count; i++) {
                SqlNode listNode = list.get(i);
                // 如果列表中的节点是查询类型
                if (listNode.getKind().belongsTo(SqlKind.QUERY)) {
                    // 将该查询包装为一个scalar子查询
                    listNode =
                            SqlStdOperatorTable.SCALAR_QUERY.createCall(
                                    listNode.getParserPosition(), listNode);
                    // 将包装后的scalar子查询设置回列表中
                    list.set(i, listNode);
                }
                // 递归注册列表节点的子查询
                registerSubQueries(parentScope, listNode);
            }
        } else {
            // atomic node -- can be ignored
        }
    }

    /**
     * Registers any sub-queries inside a given call operand, and converts the operand to a scalar
     * sub-query if the operator requires it.
     *
     * @param parentScope Parent scope
     * @param call Call
     * @param operandOrdinal Ordinal of operand within call
     * @see SqlOperator#argumentMustBeScalar(int)
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 注册给定调用操作数内的任何子查询，并在操作符需要时将该操作数转换为 scalar 子查询。
     *
     * @param parentScope 父作用域
     * @param call 调用对象（SqlCall）
     * @param operandOrdinal 调用中操作数的序数（位置索引）
     * @see SqlOperator#argumentMustBeScalar(int) 用于判断操作数是否必须为 scalar 的方法
     */
    private void registerOperandSubQueries(
            SqlValidatorScope parentScope, SqlCall call, int operandOrdinal) {
        // 获取指定序号的操作数
        SqlNode operand = call.operand(operandOrdinal);
        if (operand == null) {
            // 如果操作数为空，则直接返回
            return;
        }
        // 如果操作数是查询类型（SqlKind.QUERY），并且调用该操作符时要求该操作数必须为 scalar
        if (operand.getKind().belongsTo(SqlKind.QUERY)
                && call.getOperator().argumentMustBeScalar(operandOrdinal)) {
            // 将该操作数包装为一个 scalar 子查询
            operand =
                    SqlStdOperatorTable.SCALAR_QUERY.createCall(
                            operand.getParserPosition(), operand);
            // 将包装后的 scalar 子查询设置回调用对象的操作数中
            call.setOperand(operandOrdinal, operand);
        }
        // 注册操作数中的子查询
        registerSubQueries(parentScope, operand);
    }

    @Override
    public void validateIdentifier(SqlIdentifier id, SqlValidatorScope scope) {
        final SqlQualified fqId = scope.fullyQualify(id);
        if (this.config.columnReferenceExpansion()) {
            // NOTE jvs 9-Apr-2007: this doesn't cover ORDER BY, which has its
            // own ideas about qualification.
            id.assignNamesFrom(fqId.identifier);
        } else {
            Util.discard(fqId);
        }
    }

    @Override
    public void validateLiteral(SqlLiteral literal) {
        switch (literal.getTypeName()) {
            case DECIMAL:
                // Decimal and long have the same precision (as 64-bit integers), so
                // the unscaled value of a decimal must fit into a long.

                // REVIEW jvs 4-Aug-2004:  This should probably be calling over to
                // the available calculator implementations to see what they
                // support.  For now use ESP instead.
                //
                // jhyde 2006/12/21: I think the limits should be baked into the
                // type system, not dependent on the calculator implementation.
                BigDecimal bd = literal.getValueAs(BigDecimal.class);
                BigInteger unscaled = bd.unscaledValue();
                long longValue = unscaled.longValue();
                if (!BigInteger.valueOf(longValue).equals(unscaled)) {
                    // overflow
                    throw newValidationError(
                            literal, RESOURCE.numberLiteralOutOfRange(bd.toString()));
                }
                break;

            case DOUBLE:
                validateLiteralAsDouble(literal);
                break;

            case BINARY:
                final BitString bitString = literal.getValueAs(BitString.class);
                if ((bitString.getBitCount() % 8) != 0) {
                    throw newValidationError(literal, RESOURCE.binaryLiteralOdd());
                }
                break;

            case DATE:
            case TIME:
            case TIMESTAMP:
                Calendar calendar = literal.getValueAs(Calendar.class);
                final int year = calendar.get(Calendar.YEAR);
                final int era = calendar.get(Calendar.ERA);
                if (year < 1 || era == GregorianCalendar.BC || year > 9999) {
                    throw newValidationError(
                            literal, RESOURCE.dateLiteralOutOfRange(literal.toString()));
                }
                break;

            case INTERVAL_YEAR:
            case INTERVAL_YEAR_MONTH:
            case INTERVAL_MONTH:
            case INTERVAL_DAY:
            case INTERVAL_DAY_HOUR:
            case INTERVAL_DAY_MINUTE:
            case INTERVAL_DAY_SECOND:
            case INTERVAL_HOUR:
            case INTERVAL_HOUR_MINUTE:
            case INTERVAL_HOUR_SECOND:
            case INTERVAL_MINUTE:
            case INTERVAL_MINUTE_SECOND:
            case INTERVAL_SECOND:
                if (literal instanceof SqlIntervalLiteral) {
                    SqlIntervalLiteral.IntervalValue interval =
                            literal.getValueAs(SqlIntervalLiteral.IntervalValue.class);
                    SqlIntervalQualifier intervalQualifier = interval.getIntervalQualifier();

                    // ensure qualifier is good before attempting to validate literal
                    validateIntervalQualifier(intervalQualifier);
                    String intervalStr = interval.getIntervalLiteral();
                    // throws CalciteContextException if string is invalid
                    int[] values =
                            intervalQualifier.evaluateIntervalLiteral(
                                    intervalStr,
                                    literal.getParserPosition(),
                                    typeFactory.getTypeSystem());
                    Util.discard(values);
                }
                break;
            default:
                // default is to do nothing
        }
    }

    private void validateLiteralAsDouble(SqlLiteral literal) {
        BigDecimal bd = literal.getValueAs(BigDecimal.class);
        double d = bd.doubleValue();
        if (Double.isInfinite(d) || Double.isNaN(d)) {
            // overflow
            throw newValidationError(
                    literal, RESOURCE.numberLiteralOutOfRange(Util.toScientificNotation(bd)));
        }

        // REVIEW jvs 4-Aug-2004:  what about underflow?
    }

    @Override
    public void validateIntervalQualifier(SqlIntervalQualifier qualifier) {
        assert qualifier != null;
        boolean startPrecisionOutOfRange = false;
        boolean fractionalSecondPrecisionOutOfRange = false;
        final RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();

        final int startPrecision = qualifier.getStartPrecision(typeSystem);
        final int fracPrecision = qualifier.getFractionalSecondPrecision(typeSystem);
        final int maxPrecision = typeSystem.getMaxPrecision(qualifier.typeName());
        final int minPrecision = qualifier.typeName().getMinPrecision();
        final int minScale = qualifier.typeName().getMinScale();
        final int maxScale = typeSystem.getMaxScale(qualifier.typeName());
        if (startPrecision < minPrecision || startPrecision > maxPrecision) {
            startPrecisionOutOfRange = true;
        } else {
            if (fracPrecision < minScale || fracPrecision > maxScale) {
                fractionalSecondPrecisionOutOfRange = true;
            }
        }

        if (startPrecisionOutOfRange) {
            throw newValidationError(
                    qualifier,
                    RESOURCE.intervalStartPrecisionOutOfRange(
                            startPrecision, "INTERVAL " + qualifier));
        } else if (fractionalSecondPrecisionOutOfRange) {
            throw newValidationError(
                    qualifier,
                    RESOURCE.intervalFractionalSecondPrecisionOutOfRange(
                            fracPrecision, "INTERVAL " + qualifier));
        }
    }

    /**
     * Validates the FROM clause of a query, or (recursively) a child node of the FROM clause: AS,
     * OVER, JOIN, VALUES, or sub-query.
     *
     * @param node Node in FROM clause, typically a table or derived table
     * @param targetRowType Desired row type of this expression, or {@link #unknownType} if not
     *     fussy. Must not be null.
     * @param scope Scope
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证查询的FROM子句，或（递归地）FROM子句的子节点：AS、OVER、JOIN、VALUES或子查询。
     *
     * @param node FROM子句中的节点，通常是表或派生表
     * @param targetRowType 此表达式的期望行类型，如果不关心则为{@link #unknownType}。不得为null。
     * @param scope 作用域
     */
    protected void validateFrom(SqlNode node, RelDataType targetRowType, SqlValidatorScope scope) {
        // 确保targetRowType不为null
        requireNonNull(targetRowType, "targetRowType");
        switch (node.getKind()) {
            case AS:
            case TABLE_REF:
                // 如果是AS或TABLE_REF节点，递归验证其操作数
                validateFrom(((SqlCall) node).operand(0), targetRowType, scope);
                break;
            // 如果是VALUES节点，验证VALUES表达式
            case VALUES:
                validateValues((SqlCall) node, targetRowType, scope);
                break;
            case JOIN:
                // 如果是JOIN节点，验证JOIN表达式
                validateJoin((SqlJoin) node, scope);
                break;
            case OVER:
                // 如果是OVER节点，验证OVER表达式
                validateOver((SqlCall) node, scope);
                break;
            case UNNEST:
                // 如果是UNNEST节点，验证UNNEST表达式
                validateUnnest((SqlCall) node, scope, targetRowType);
                break;
            case COLLECTION_TABLE:
                // 如果是COLLECTION_TABLE节点，验证表函数
                validateTableFunction((SqlCall) node, scope, targetRowType);
                break;
            default:
                // 对于其他类型的节点，验证查询
                validateQuery(node, scope, targetRowType);
                break;
        }

        // Validate the namespace representation of the node, just in case the
        // validation did not occur implicitly.
        // 验证节点的命名空间表示，以防验证没有隐式发生
        getNamespaceOrThrow(node, scope).validate(targetRowType);
    }

    protected void validateTableFunction(
            SqlCall node, SqlValidatorScope scope, RelDataType targetRowType) {
        // Dig out real call; TABLE() wrapper is just syntactic.
        SqlCall call = node.operand(0);
        if (call.getOperator() instanceof SqlTableFunction) {
            SqlTableFunction tableFunction = (SqlTableFunction) call.getOperator();
            boolean visitedRowSemanticsTable = false;
            for (int idx = 0; idx < call.operandCount(); idx++) {
                TableCharacteristic tableCharacteristic = tableFunction.tableCharacteristic(idx);
                if (tableCharacteristic != null) {
                    // Skip validate if current input table has set semantics
                    if (tableCharacteristic.semantics == TableCharacteristic.Semantics.SET) {
                        continue;
                    }
                    // A table function at most has one input table with row semantics
                    if (visitedRowSemanticsTable) {
                        throw newValidationError(
                                call,
                                RESOURCE.multipleRowSemanticsTables(call.getOperator().getName()));
                    }
                    visitedRowSemanticsTable = true;
                }
                // If table function defines the parameter is not table parameter, or is an input
                // table
                // parameter with row semantics, then it should not be with PARTITION BY OR ORDER
                // BY.
                SqlNode currentNode = call.operand(idx);
                if (currentNode instanceof SqlCall) {
                    SqlOperator op = ((SqlCall) currentNode).getOperator();
                    if (op == SqlStdOperatorTable.ARGUMENT_ASSIGNMENT) {
                        // Dig out the underlying operand
                        SqlNode realNode = ((SqlBasicCall) currentNode).operand(0);
                        if (realNode instanceof SqlCall) {
                            currentNode = realNode;
                            op = ((SqlCall) realNode).getOperator();
                        }
                    }
                    if (op == SqlStdOperatorTable.SET_SEMANTICS_TABLE) {
                        throwInvalidRowSemanticsTable(call, idx, (SqlCall) currentNode);
                    }
                }
            }
        }
        validateQuery(node, scope, targetRowType);
    }

    private void throwInvalidRowSemanticsTable(SqlCall call, int idx, SqlCall table) {
        SqlNodeList partitionList = table.operand(1);
        if (!partitionList.isEmpty()) {
            throw newValidationError(
                    call, RESOURCE.invalidPartitionKeys(idx, call.getOperator().getName()));
        }
        SqlNodeList orderList = table.operand(2);
        if (!orderList.isEmpty()) {
            throw newValidationError(
                    call, RESOURCE.invalidOrderBy(idx, call.getOperator().getName()));
        }
    }

    protected void validateOver(SqlCall call, SqlValidatorScope scope) {
        throw new AssertionError("OVER unexpected in this context");
    }

    protected void validateUnnest(
            SqlCall call, SqlValidatorScope scope, RelDataType targetRowType) {
        for (int i = 0; i < call.operandCount(); i++) {
            SqlNode expandedItem = expand(call.operand(i), scope);
            call.setOperand(i, expandedItem);
        }
        validateQuery(call, scope, targetRowType);
    }

    private void checkRollUpInUsing(
            SqlIdentifier identifier, SqlNode leftOrRight, SqlValidatorScope scope) {
        SqlValidatorNamespace namespace = getNamespace(leftOrRight, scope);
        if (namespace != null) {
            SqlValidatorTable sqlValidatorTable = namespace.getTable();
            if (sqlValidatorTable != null) {
                Table table = sqlValidatorTable.table();
                String column = Util.last(identifier.names);

                if (table.isRolledUp(column)) {
                    throw newValidationError(
                            identifier, RESOURCE.rolledUpNotAllowed(column, "USING"));
                }
            }
        }
    }

    protected void validateJoin(SqlJoin join, SqlValidatorScope scope) {
        final SqlNode left = join.getLeft();
        final SqlNode right = join.getRight();
        final boolean natural = join.isNatural();
        final JoinType joinType = join.getJoinType();
        final JoinConditionType conditionType = join.getConditionType();
        final SqlValidatorScope joinScope = getScopeOrThrow(join); // getJoinScope?
        validateFrom(left, unknownType, joinScope);
        validateFrom(right, unknownType, joinScope);

        // Validate condition.
        switch (conditionType) {
            case NONE:
                Preconditions.checkArgument(join.getCondition() == null);
                break;
            case ON:
                final SqlNode condition = expand(getCondition(join), joinScope);
                join.setOperand(5, condition);
                validateWhereOrOn(joinScope, condition, "ON");
                checkRollUp(null, join, condition, joinScope, "ON");
                break;
            case USING:
                @SuppressWarnings({"rawtypes", "unchecked"})
                List<SqlIdentifier> list = (List) getCondition(join);

                // Parser ensures that using clause is not empty.
                Preconditions.checkArgument(list.size() > 0, "Empty USING clause");
                for (SqlIdentifier id : list) {
                    validateCommonJoinColumn(id, left, right, scope);
                }
                break;
            default:
                throw Util.unexpected(conditionType);
        }

        // Validate NATURAL.
        if (natural) {
            if (join.getCondition() != null) {
                throw newValidationError(getCondition(join), RESOURCE.naturalDisallowsOnOrUsing());
            }

            // Join on fields that occur on each side.
            // Check compatibility of the chosen columns.
            for (String name : deriveNaturalJoinColumnList(join)) {
                final SqlIdentifier id =
                        new SqlIdentifier(name, join.isNaturalNode().getParserPosition());
                validateCommonJoinColumn(id, left, right, scope);
            }
        }

        // Which join types require/allow a ON/USING condition, or allow
        // a NATURAL keyword?
        switch (joinType) {
            case LEFT_SEMI_JOIN:
                if (!this.config.conformance().isLiberal()) {
                    throw newValidationError(
                            join.getJoinTypeNode(),
                            RESOURCE.dialectDoesNotSupportFeature("LEFT SEMI JOIN"));
                }
                // fall through
            case INNER:
            case LEFT:
            case RIGHT:
            case FULL:
                if ((join.getCondition() == null) && !natural) {
                    throw newValidationError(join, RESOURCE.joinRequiresCondition());
                }
                break;
            case COMMA:
            case CROSS:
                if (join.getCondition() != null) {
                    throw newValidationError(
                            join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition());
                }
                if (natural) {
                    throw newValidationError(
                            join.getConditionTypeNode(), RESOURCE.crossJoinDisallowsCondition());
                }
                break;
            default:
                throw Util.unexpected(joinType);
        }
    }

    /**
     * Throws an error if there is an aggregate or windowed aggregate in the given clause.
     *
     * @param aggFinder Finder for the particular kind(s) of aggregate function
     * @param node Parse tree
     * @param clause Name of clause: "WHERE", "GROUP BY", "ON"
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 如果在给定的子句中存在聚合函数或窗口聚合函数，则抛出错误。
     *
     * @param aggFinder 特定类型聚合函数的查找器
     * @param node 解析树节点
     * @param clause 子句的名称："WHERE"、"GROUP BY"、"ON"等
     */
    private void validateNoAggs(AggFinder aggFinder, SqlNode node, String clause) {
        // 使用聚合函数查找器在解析树节点中查找聚合函数
        final SqlCall agg = aggFinder.findAgg(node);
        // 如果没有找到聚合函数，则直接返回
        if (agg == null) {
            return;
        }
        // 获取找到的聚合函数的操作符
        final SqlOperator op = agg.getOperator();
        // 如果操作符是窗口函数（OVER）
        if (op == SqlStdOperatorTable.OVER) {
            // 抛出错误，说明窗口聚合函数在该子句中是非法的
            throw newValidationError(agg, RESOURCE.windowedAggregateIllegalInClause(clause));
            // 如果操作符是组函数或其辅助函数
        } else if (op.isGroup() || op.isGroupAuxiliary()) {
            // 抛出错误，说明组函数必须出现在GROUP BY子句中
            throw newValidationError(
                    agg, RESOURCE.groupFunctionMustAppearInGroupByClause(op.getName()));
        } else {
            // 抛出错误，说明聚合函数在该子句中是非法的
            throw newValidationError(agg, RESOURCE.aggregateIllegalInClause(clause));
        }
    }

    /** Validates a column in a USING clause, or an inferred join key in a NATURAL join. */
    private void validateCommonJoinColumn(
            SqlIdentifier id, SqlNode left, SqlNode right, SqlValidatorScope scope) {
        if (id.names.size() != 1) {
            throw newValidationError(id, RESOURCE.columnNotFound(id.toString()));
        }

        final RelDataType leftColType = validateCommonInputJoinColumn(id, left, scope);
        final RelDataType rightColType = validateCommonInputJoinColumn(id, right, scope);
        if (!SqlTypeUtil.isComparable(leftColType, rightColType)) {
            throw newValidationError(
                    id,
                    RESOURCE.naturalOrUsingColumnNotCompatible(
                            id.getSimple(), leftColType.toString(), rightColType.toString()));
        }
    }

    /**
     * Validates a column in a USING clause, or an inferred join key in a NATURAL join, in the left
     * or right input to the join.
     */
    private RelDataType validateCommonInputJoinColumn(
            SqlIdentifier id, SqlNode leftOrRight, SqlValidatorScope scope) {
        Preconditions.checkArgument(id.names.size() == 1);
        final String name = id.names.get(0);
        final SqlValidatorNamespace namespace = getNamespaceOrThrow(leftOrRight);
        final RelDataType rowType = namespace.getRowType();
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final RelDataTypeField field = nameMatcher.field(rowType, name);
        if (field == null) {
            throw newValidationError(id, RESOURCE.columnNotFound(name));
        }
        if (nameMatcher.frequency(rowType.getFieldNames(), name) > 1) {
            throw newValidationError(id, RESOURCE.columnInUsingNotUnique(name));
        }
        checkRollUpInUsing(id, leftOrRight, scope);
        return field.getType();
    }

    /**
     * Validates a SELECT statement.
     *
     * @param select Select statement
     * @param targetRowType Desired row type, must not be null, may be the data type 'unknown'.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证SELECT语句。
     *
     * @param select SELECT语句
     * @param targetRowType 期望的行类型，不能为null，可以是数据类型'unknown'。
     */
    protected void validateSelect(SqlSelect select, RelDataType targetRowType) {
        // 确保目标行类型不为null
        assert targetRowType != null;
        // Namespace is either a select namespace or a wrapper around one.
        // 命名空间是一个选择命名空间或是一个选择命名空间的包装器。
        final SelectNamespace ns = getNamespaceOrThrow(select).unwrap(SelectNamespace.class);

        // Its rowtype is null, meaning it hasn't been validated yet.
        // This is important, because we need to take the targetRowType into
        // account.
        // 其行类型为null，意味着它还没有被验证。
        // 这是重要的，因为我们需要考虑目标行类型。
        assert ns.rowType == null;
        // 检查SELECT语句是否有DISTINCT修饰符
        SqlNode distinctNode = select.getModifierNode(SqlSelectKeyword.DISTINCT);
        if (distinctNode != null) {
            // 如果有DISTINCT修饰符，则验证该特性是否被支持
            validateFeature(RESOURCE.sQLFeature_E051_01(), distinctNode.getParserPosition());
        }
        // 获取SELECT语句的选择列表
        final SqlNodeList selectItems = SqlNonNullableAccessors.getSelectList(select);
        // 初始化fromType为未知类型
        RelDataType fromType = unknownType;
        // 如果选择列表只有一个元素
        if (selectItems.size() == 1) {
            // 并且这个元素是一个标识符
            final SqlNode selectItem = selectItems.get(0);
            if (selectItem instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) selectItem;
                if (id.isStar() && (id.names.size() == 1)) {
                    // Special case: for INSERT ... VALUES(?,?), the SQL
                    // standard says we're supposed to propagate the target
                    // types down.  So iff the select list is an unqualified
                    // star (as it will be after an INSERT ... VALUES has been
                    // expanded), then propagate.
                    // 特殊情况：对于INSERT ... VALUES(?,?)，SQL标准说我们应该向下传播目标类型。
                    // 所以，如果选择列表是一个未限定的星号（在INSERT ... VALUES展开后将是这样），
                    // 那么就进行传播。
                    fromType = targetRowType;
                }
            }
        }

        // Make sure that items in FROM clause have distinct aliases.
        // 确保FROM子句中的项具有不同的别名。
        final SelectScope fromScope =
                (SelectScope) requireNonNull(getFromScope(select), () -> "fromScope for " + select);
        // 获取FROM作用域中所有子项的名称。
        List<@Nullable String> names = fromScope.getChildNames();
        // 如果目录读取器的名称匹配器不区分大小写，则将所有名称转换为大写。
        if (!catalogReader.nameMatcher().isCaseSensitive()) {
            //noinspection RedundantTypeArguments
            names =
                    names.stream()
                            .<@Nullable String>map(
                                    s -> s == null ? null : s.toUpperCase(Locale.ROOT))
                            .collect(Collectors.toList());
        }
        // 检查是否有重复的别名。
        final int duplicateAliasOrdinal = Util.firstDuplicate(names);
        if (duplicateAliasOrdinal >= 0) {
            // 如果有重复的别名，则获取对应的子项并抛出验证错误。
            final ScopeChild child = fromScope.children.get(duplicateAliasOrdinal);
            throw newValidationError(
                    requireNonNull(
                            child.namespace.getEnclosingNode(),
                            () -> "enclosingNode of namespace of " + child.name),
                    RESOURCE.fromAliasDuplicate(child.name));
        }
         // 如果SELECT语句没有FROM子句，则根据配置决定是否抛出错误。
        if (select.getFrom() == null) {
            if (this.config.conformance().isFromRequired()) {
                //抛出异常
                throw newValidationError(select, RESOURCE.selectMissingFrom());
            }
        } else {
            // 如果有FROM子句，则对其进行验证。
            validateFrom(select.getFrom(), fromType, fromScope);
        }
         // 验证WHERE子句
        validateWhereClause(select);
        // 验证GROUP BY子句
        validateGroupClause(select);
        // 验证HAVING子句
        validateHavingClause(select);
        // 验证WINDOW子句
        validateWindowClause(select);
        // 处理OFFSET和FETCH子句
        handleOffsetFetch(select.getOffset(), select.getFetch());

        // Validate the SELECT clause late, because a select item might
        // depend on the GROUP BY list, or the window function might reference
        // window name in the WINDOW clause etc.
        // 延迟验证SELECT子句，因为SELECT项可能依赖于GROUP BY列表，或者窗口函数可能引用WINDOW子句中的窗口名称等。
        final RelDataType rowType = validateSelectList(selectItems, select, targetRowType);
        ns.setType(rowType);

        // Validate ORDER BY after we have set ns.rowType because in some
        // dialects you can refer to columns of the select list, e.g.
        // "SELECT empno AS x FROM emp ORDER BY x"
        // 在设置了ns.rowType之后验证ORDER BY子句，因为在某些方言中，你可以引用SELECT列表中的列，例如"SELECT empno AS x FROM emp ORDER BY x"
        validateOrderList(select);
       // 如果应该从FROM子句中检查上卷（Roll Up），则执行相关检查
        if (shouldCheckForRollUp(select.getFrom())) {
            checkRollUpInSelectList(select);
            checkRollUp(null, select, select.getWhere(), getWhereScope(select));
            checkRollUp(null, select, select.getHaving(), getHavingScope(select));
            checkRollUpInWindowDecl(select);
            checkRollUpInGroupBy(select);
            checkRollUpInOrderBy(select);
        }
    }

    private void checkRollUpInSelectList(SqlSelect select) {
        SqlValidatorScope scope = getSelectScope(select);
        for (SqlNode item : SqlNonNullableAccessors.getSelectList(select)) {
            checkRollUp(null, select, item, scope);
        }
    }

    private void checkRollUpInGroupBy(SqlSelect select) {
        SqlNodeList group = select.getGroup();
        if (group != null) {
            for (SqlNode node : group) {
                checkRollUp(null, select, node, getGroupScope(select), "GROUP BY");
            }
        }
    }

    private void checkRollUpInOrderBy(SqlSelect select) {
        SqlNodeList orderList = select.getOrderList();
        if (orderList != null) {
            for (SqlNode node : orderList) {
                checkRollUp(null, select, node, getOrderScope(select), "ORDER BY");
            }
        }
    }

    private void checkRollUpInWindow(@Nullable SqlWindow window, SqlValidatorScope scope) {
        if (window != null) {
            for (SqlNode node : window.getPartitionList()) {
                checkRollUp(null, window, node, scope, "PARTITION BY");
            }

            for (SqlNode node : window.getOrderList()) {
                checkRollUp(null, window, node, scope, "ORDER BY");
            }
        }
    }

    private void checkRollUpInWindowDecl(SqlSelect select) {
        for (SqlNode decl : select.getWindowList()) {
            checkRollUpInWindow((SqlWindow) decl, getSelectScope(select));
        }
    }

    /**
     * If the {@code node} is a DOT call, returns its first operand. Recurse, if the first operand
     * is another DOT call.
     *
     * <p>In other words, it converts {@code a DOT b DOT c} to {@code a}.
     *
     * @param node The node to strip DOT
     * @return the DOT's first operand
     */
    private static SqlNode stripDot(SqlNode node) {
        SqlNode res = node;
        while (res.getKind() == SqlKind.DOT) {
            res = requireNonNull(((SqlCall) res).operand(0), "operand");
        }
        return res;
    }

    private void checkRollUp(
            @Nullable SqlNode grandParent,
            @Nullable SqlNode parent,
            @Nullable SqlNode current,
            SqlValidatorScope scope,
            @Nullable String contextClause) {
        current = stripAs(current);
        if (current instanceof SqlCall && !(current instanceof SqlSelect)) {
            // Validate OVER separately
            checkRollUpInWindow(getWindowInOver(current), scope);
            current = stripOver(current);

            SqlNode stripDot = stripDot(current);
            if (stripDot != current) {
                // we stripped the field access. Recurse to this method, the DOT's operand
                // can be another SqlCall, or an SqlIdentifier.
                checkRollUp(grandParent, parent, stripDot, scope, contextClause);
            } else {
                // ----- FLINK MODIFICATION BEGIN -----
                SqlCall call = (SqlCall) stripDot;
                List<? extends @Nullable SqlNode> children =
                        new SqlCallBinding(this, scope, call).operands();
                // ----- FLINK MODIFICATION END -----
                for (SqlNode child : children) {
                    checkRollUp(parent, current, child, scope, contextClause);
                }
            }
        } else if (current instanceof SqlIdentifier) {
            SqlIdentifier id = (SqlIdentifier) current;
            if (!id.isStar() && isRolledUpColumn(id, scope)) {
                if (!isAggregation(requireNonNull(parent, "parent").getKind())
                        || !isRolledUpColumnAllowedInAgg(
                                id, scope, (SqlCall) parent, grandParent)) {
                    String context =
                            contextClause != null ? contextClause : parent.getKind().toString();
                    throw newValidationError(
                            id, RESOURCE.rolledUpNotAllowed(deriveAliasNonNull(id, 0), context));
                }
            }
        }
    }

    private void checkRollUp(
            @Nullable SqlNode grandParent,
            SqlNode parent,
            @Nullable SqlNode current,
            SqlValidatorScope scope) {
        checkRollUp(grandParent, parent, current, scope, null);
    }

    private static @Nullable SqlWindow getWindowInOver(SqlNode over) {
        if (over.getKind() == SqlKind.OVER) {
            SqlNode window = ((SqlCall) over).getOperandList().get(1);
            if (window instanceof SqlWindow) {
                return (SqlWindow) window;
            }
            // SqlIdentifier, gets validated elsewhere
            return null;
        }
        return null;
    }

    private static SqlNode stripOver(SqlNode node) {
        switch (node.getKind()) {
            case OVER:
                return ((SqlCall) node).getOperandList().get(0);
            default:
                return node;
        }
    }

    private @Nullable Pair<String, String> findTableColumnPair(
            SqlIdentifier identifier, SqlValidatorScope scope) {
        final SqlCall call = makeNullaryCall(identifier);
        if (call != null) {
            return null;
        }
        SqlQualified qualified = scope.fullyQualify(identifier);
        List<String> names = qualified.identifier.names;

        if (names.size() < 2) {
            return null;
        }

        return new Pair<>(names.get(names.size() - 2), Util.last(names));
    }

    // Returns true iff the given column is valid inside the given aggCall.
    private boolean isRolledUpColumnAllowedInAgg(
            SqlIdentifier identifier,
            SqlValidatorScope scope,
            SqlCall aggCall,
            @Nullable SqlNode parent) {
        Pair<String, String> pair = findTableColumnPair(identifier, scope);

        if (pair == null) {
            return true;
        }

        String columnName = pair.right;

        Table table = resolveTable(identifier, scope);
        if (table != null) {
            return table.rolledUpColumnValidInsideAgg(
                    columnName, aggCall, parent, catalogReader.getConfig());
        }
        return true;
    }

    private static @Nullable Table resolveTable(SqlIdentifier identifier, SqlValidatorScope scope) {
        SqlQualified fullyQualified = scope.fullyQualify(identifier);
        assert fullyQualified.namespace != null : "namespace must not be null in " + fullyQualified;
        SqlValidatorTable sqlValidatorTable = fullyQualified.namespace.getTable();
        if (sqlValidatorTable != null) {
            return sqlValidatorTable.table();
        }
        return null;
    }

    // Returns true iff the given column is actually rolled up.
    private boolean isRolledUpColumn(SqlIdentifier identifier, SqlValidatorScope scope) {
        Pair<String, String> pair = findTableColumnPair(identifier, scope);

        if (pair == null) {
            return false;
        }

        String columnName = pair.right;

        Table table = resolveTable(identifier, scope);
        if (table != null) {
            return table.isRolledUp(columnName);
        }
        return false;
    }

    private static boolean shouldCheckForRollUp(@Nullable SqlNode from) {
        if (from != null) {
            SqlKind kind = stripAs(from).getKind();
            return kind != SqlKind.VALUES && kind != SqlKind.SELECT;
        }
        return false;
    }

    /**
     * Validates that a query can deliver the modality it promises. Only called on the top-most
     * SELECT or set operator in the tree.
     */
    private void validateModality(SqlNode query) {
        final SqlModality modality = deduceModality(query);
        if (query instanceof SqlSelect) {
            final SqlSelect select = (SqlSelect) query;
            validateModality(select, modality, true);
        } else if (query.getKind() == SqlKind.VALUES) {
            switch (modality) {
                case STREAM:
                    throw newValidationError(query, Static.RESOURCE.cannotStreamValues());
                default:
                    break;
            }
        } else {
            assert query.isA(SqlKind.SET_QUERY);
            final SqlCall call = (SqlCall) query;
            for (SqlNode operand : call.getOperandList()) {
                if (deduceModality(operand) != modality) {
                    throw newValidationError(
                            operand, Static.RESOURCE.streamSetOpInconsistentInputs());
                }
                validateModality(operand);
            }
        }
    }

    /** Return the intended modality of a SELECT or set-op. */
    private static SqlModality deduceModality(SqlNode query) {
        if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            return select.getModifierNode(SqlSelectKeyword.STREAM) != null
                    ? SqlModality.STREAM
                    : SqlModality.RELATION;
        } else if (query.getKind() == SqlKind.VALUES) {
            return SqlModality.RELATION;
        } else {
            assert query.isA(SqlKind.SET_QUERY);
            final SqlCall call = (SqlCall) query;
            return deduceModality(call.getOperandList().get(0));
        }
    }

    @Override
    public boolean validateModality(SqlSelect select, SqlModality modality, boolean fail) {
        final SelectScope scope = getRawSelectScopeNonNull(select);

        switch (modality) {
            case STREAM:
                if (scope.children.size() == 1) {
                    for (ScopeChild child : scope.children) {
                        if (!child.namespace.supportsModality(modality)) {
                            if (fail) {
                                SqlNode node = SqlNonNullableAccessors.getNode(child);
                                throw newValidationError(
                                        node, Static.RESOURCE.cannotConvertToStream(child.name));
                            } else {
                                return false;
                            }
                        }
                    }
                } else {
                    int supportsModalityCount = 0;
                    for (ScopeChild child : scope.children) {
                        if (child.namespace.supportsModality(modality)) {
                            ++supportsModalityCount;
                        }
                    }

                    if (supportsModalityCount == 0) {
                        if (fail) {
                            String inputs = String.join(", ", scope.getChildNames());
                            throw newValidationError(
                                    select,
                                    Static.RESOURCE.cannotStreamResultsForNonStreamingInputs(
                                            inputs));
                        } else {
                            return false;
                        }
                    }
                }
                break;
            default:
                for (ScopeChild child : scope.children) {
                    if (!child.namespace.supportsModality(modality)) {
                        if (fail) {
                            SqlNode node = SqlNonNullableAccessors.getNode(child);
                            throw newValidationError(
                                    node, Static.RESOURCE.cannotConvertToRelation(child.name));
                        } else {
                            return false;
                        }
                    }
                }
        }

        // Make sure that aggregation is possible.
        final SqlNode aggregateNode = getAggregate(select);
        if (aggregateNode != null) {
            switch (modality) {
                case STREAM:
                    SqlNodeList groupList = select.getGroup();
                    if (groupList == null
                            || !SqlValidatorUtil.containsMonotonic(scope, groupList)) {
                        if (fail) {
                            throw newValidationError(
                                    aggregateNode, Static.RESOURCE.streamMustGroupByMonotonic());
                        } else {
                            return false;
                        }
                    }
                    break;
                default:
                    break;
            }
        }

        // Make sure that ORDER BY is possible.
        final SqlNodeList orderList = select.getOrderList();
        if (orderList != null && orderList.size() > 0) {
            switch (modality) {
                case STREAM:
                    if (!hasSortedPrefix(scope, orderList)) {
                        if (fail) {
                            throw newValidationError(
                                    orderList.get(0), Static.RESOURCE.streamMustOrderByMonotonic());
                        } else {
                            return false;
                        }
                    }
                    break;
                default:
                    break;
            }
        }
        return true;
    }

    /** Returns whether the prefix is sorted. */
    private static boolean hasSortedPrefix(SelectScope scope, SqlNodeList orderList) {
        return isSortCompatible(scope, orderList.get(0), false);
    }

    private static boolean isSortCompatible(SelectScope scope, SqlNode node, boolean descending) {
        switch (node.getKind()) {
            case DESCENDING:
                return isSortCompatible(scope, ((SqlCall) node).getOperandList().get(0), true);
            default:
                break;
        }
        final SqlMonotonicity monotonicity = scope.getMonotonicity(node);
        switch (monotonicity) {
            case INCREASING:
            case STRICTLY_INCREASING:
                return !descending;
            case DECREASING:
            case STRICTLY_DECREASING:
                return descending;
            default:
                return false;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    protected void validateWindowClause(SqlSelect select) {
        final SqlNodeList windowList = select.getWindowList();
        if (windowList.isEmpty()) {
            return;
        }

        final SelectScope windowScope =
                (SelectScope) requireNonNull(getFromScope(select), () -> "fromScope for " + select);

        // 1. ensure window names are simple
        // 2. ensure they are unique within this scope
        for (SqlWindow window : (List<SqlWindow>) (List) windowList) {
            SqlIdentifier declName =
                    requireNonNull(
                            window.getDeclName(), () -> "window.getDeclName() for " + window);
            if (!declName.isSimple()) {
                throw newValidationError(declName, RESOURCE.windowNameMustBeSimple());
            }

            if (windowScope.existingWindowName(declName.toString())) {
                throw newValidationError(declName, RESOURCE.duplicateWindowName());
            } else {
                windowScope.addWindowName(declName.toString());
            }
        }

        // 7.10 rule 2
        // Check for pairs of windows which are equivalent.
        for (int i = 0; i < windowList.size(); i++) {
            SqlNode window1 = windowList.get(i);
            for (int j = i + 1; j < windowList.size(); j++) {
                SqlNode window2 = windowList.get(j);
                if (window1.equalsDeep(window2, Litmus.IGNORE)) {
                    throw newValidationError(window2, RESOURCE.dupWindowSpec());
                }
            }
        }

        for (SqlWindow window : (List<SqlWindow>) (List) windowList) {
            final SqlNodeList expandedOrderList =
                    (SqlNodeList) expand(window.getOrderList(), windowScope);
            window.setOrderList(expandedOrderList);
            expandedOrderList.validate(this, windowScope);

            final SqlNodeList expandedPartitionList =
                    (SqlNodeList) expand(window.getPartitionList(), windowScope);
            window.setPartitionList(expandedPartitionList);
            expandedPartitionList.validate(this, windowScope);
        }

        // Hand off to validate window spec components
        windowList.validate(this, windowScope);
    }

    @Override
    public void validateWith(SqlWith with, SqlValidatorScope scope) {
        final SqlValidatorNamespace namespace = getNamespaceOrThrow(with);
        validateNamespace(namespace, unknownType);
    }

    @Override
    public void validateWithItem(SqlWithItem withItem) {
        SqlNodeList columnList = withItem.columnList;
        if (columnList != null) {
            final RelDataType rowType = getValidatedNodeType(withItem.query);
            final int fieldCount = rowType.getFieldCount();
            if (columnList.size() != fieldCount) {
                throw newValidationError(columnList, RESOURCE.columnCountMismatch());
            }
            SqlValidatorUtil.checkIdentifierListForDuplicates(columnList, validationErrorFunction);
        } else {
            // Luckily, field names have not been make unique yet.
            final List<String> fieldNames = getValidatedNodeType(withItem.query).getFieldNames();
            final int i = Util.firstDuplicate(fieldNames);
            if (i >= 0) {
                throw newValidationError(
                        withItem.query, RESOURCE.duplicateColumnAndNoColumnList(fieldNames.get(i)));
            }
        }
    }

    @Override
    public void validateSequenceValue(SqlValidatorScope scope, SqlIdentifier id) {
        // Resolve identifier as a table.
        final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
        scope.resolveTable(
                id.names, catalogReader.nameMatcher(), SqlValidatorScope.Path.EMPTY, resolved);
        if (resolved.count() != 1) {
            throw newValidationError(id, RESOURCE.tableNameNotFound(id.toString()));
        }
        // We've found a table. But is it a sequence?
        final SqlValidatorNamespace ns = resolved.only().namespace;
        if (ns instanceof TableNamespace) {
            final Table table = getTable(ns).table();
            switch (table.getJdbcTableType()) {
                case SEQUENCE:
                case TEMPORARY_SEQUENCE:
                    return;
                default:
                    break;
            }
        }
        throw newValidationError(id, RESOURCE.notASequence(id.toString()));
    }

    @Override
    public @Nullable SqlValidatorScope getWithScope(SqlNode withItem) {
        assert withItem.getKind() == SqlKind.WITH_ITEM;
        return scopes.get(withItem);
    }

    @Override
    public TypeCoercion getTypeCoercion() {
        assert config.typeCoercionEnabled();
        return this.typeCoercion;
    }

    @Override
    public Config config() {
        return this.config;
    }

    @Override
    public SqlValidator transform(UnaryOperator<Config> transform) {
        this.config = transform.apply(this.config);
        return this;
    }

    /**
     * Validates the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     */
    protected void validateOrderList(SqlSelect select) {
        // ORDER BY is validated in a scope where aliases in the SELECT clause
        // are visible. For example, "SELECT empno AS x FROM emp ORDER BY x"
        // is valid.
        SqlNodeList orderList = select.getOrderList();
        if (orderList == null) {
            return;
        }
        if (!shouldAllowIntermediateOrderBy()) {
            if (!cursorSet.contains(select)) {
                throw newValidationError(select, RESOURCE.invalidOrderByPos());
            }
        }
        final SqlValidatorScope orderScope = getOrderScope(select);
        requireNonNull(orderScope, "orderScope");

        List<SqlNode> expandList = new ArrayList<>();
        for (SqlNode orderItem : orderList) {
            SqlNode expandedOrderItem = expand(orderItem, orderScope);
            expandList.add(expandedOrderItem);
        }

        SqlNodeList expandedOrderList = new SqlNodeList(expandList, orderList.getParserPosition());
        select.setOrderBy(expandedOrderList);

        for (SqlNode orderItem : expandedOrderList) {
            validateOrderItem(select, orderItem);
        }
    }

    /**
     * Validates an item in the GROUP BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param groupByItem GROUP BY clause item
     */
    private void validateGroupByItem(SqlSelect select, SqlNode groupByItem) {
        final SqlValidatorScope groupByScope = getGroupScope(select);
        validateGroupByExpr(groupByItem, groupByScope);
        groupByScope.validateExpr(groupByItem);
    }

    private void validateGroupByExpr(SqlNode groupByItem, SqlValidatorScope groupByScope) {
        switch (groupByItem.getKind()) {
            case GROUP_BY_DISTINCT:
                SqlCall call = (SqlCall) groupByItem;
                for (SqlNode operand : call.getOperandList()) {
                    validateGroupByExpr(operand, groupByScope);
                }
                break;
            case GROUPING_SETS:
            case ROLLUP:
            case CUBE:
                call = (SqlCall) groupByItem;
                for (SqlNode operand : call.getOperandList()) {
                    validateExpr(operand, groupByScope);
                }
                break;
            default:
                validateExpr(groupByItem, groupByScope);
        }
    }

    /**
     * Validates an item in the ORDER BY clause of a SELECT statement.
     *
     * @param select Select statement
     * @param orderItem ORDER BY clause item
     */
    private void validateOrderItem(SqlSelect select, SqlNode orderItem) {
        switch (orderItem.getKind()) {
            case DESCENDING:
                validateFeature(
                        RESOURCE.sQLConformance_OrderByDesc(), orderItem.getParserPosition());
                validateOrderItem(select, ((SqlCall) orderItem).operand(0));
                return;
            default:
                break;
        }

        final SqlValidatorScope orderScope = getOrderScope(select);
        validateExpr(orderItem, orderScope);
    }

    @Override
    public SqlNode expandOrderExpr(SqlSelect select, SqlNode orderExpr) {
        final SqlNode newSqlNode = new OrderExpressionExpander(select, orderExpr).go();
        if (newSqlNode != orderExpr) {
            final SqlValidatorScope scope = getOrderScope(select);
            inferUnknownTypes(unknownType, scope, newSqlNode);
            final RelDataType type = deriveType(scope, newSqlNode);
            setValidatedNodeType(newSqlNode, type);
        }
        return newSqlNode;
    }

    /**
     * Validates the GROUP BY clause of a SELECT statement. This method is called even if no GROUP
     * BY clause is present.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *  验证SELECT语句中的GROUP BY子句。此方法即使在没有GROUP BY子句的情况下也会被调用。
     */
    protected void validateGroupClause(SqlSelect select) {
        // 获取SELECT语句中的GROUP BY子句列表
        SqlNodeList groupList = select.getGroup();
        // 如果没有GROUP BY子句，则直接返回
        if (groupList == null) {
            return;
        }
        // 定义GROUP BY子句的字符串表示
        final String clause = "GROUP BY";
        // 验证GROUP BY子句中不应包含聚合函数或OVER子句
        validateNoAggs(aggOrOverFinder, groupList, clause);
        // 获取GROUP BY子句的作用域
        final SqlValidatorScope groupScope = getGroupScope(select);

        // expand the expression in group list.
        // 展开GROUP BY子句中的表达式
        List<SqlNode> expandedList = new ArrayList<>();
        for (SqlNode groupItem : groupList) {
            SqlNode expandedItem = expandGroupByOrHavingExpr(groupItem, groupScope, select, false);
            expandedList.add(expandedItem);
        }
        // 更新SELECT语句中的GROUP BY子句为展开后的列表
        groupList = new SqlNodeList(expandedList, groupList.getParserPosition());
        select.setGroupBy(groupList);
        // 推断未知类型的表达式
        inferUnknownTypes(unknownType, groupScope, groupList);
        // 对展开后的每个GROUP BY项进行验证
        for (SqlNode groupItem : expandedList) {
            validateGroupByItem(select, groupItem);
        }

        // Nodes in the GROUP BY clause are expressions except if they are calls
        // to the GROUPING SETS, ROLLUP or CUBE operators; this operators are not
        // expressions, because they do not have a type.
        // GROUP BY子句中的节点通常是表达式，但如果是GROUPING SETS、ROLLUP或CUBE运算符的调用，则它们不是表达式，因为它们没有类型
        for (SqlNode node : groupList) {
            switch (node.getKind()) {
                // 对于这些特殊的GROUP BY运算符，调用validate方法进行验证
                case GROUP_BY_DISTINCT:
                case GROUPING_SETS:
                case ROLLUP:
                case CUBE:
                    node.validate(this, groupScope);
                    break;
                // 其他情况，按表达式进行验证
                default:
                    node.validateExpr(this, groupScope);
            }
        }

        // Derive the type of each GROUP BY item. We don't need the type, but
        // it resolves functions, and that is necessary for deducing
        // monotonicity.

        /**
         * 为每个GROUP BY项推导类型。虽然我们不直接使用这些类型，但推导过程中会解析函数，
         * 这是推断单调性所必需的。
         */
        //获取SELECT语句的作用域
        final SqlValidatorScope selectScope = getSelectScope(select);
        // 用于聚合操作的SELECT作用域，初始化为null
        AggregatingSelectScope aggregatingScope = null;
        // 如果SELECT作用域是聚合类型的作用域
        if (selectScope instanceof AggregatingSelectScope) {
            // 强制类型转换并赋值
            aggregatingScope = (AggregatingSelectScope) selectScope;
        }
        // 遍历GROUP BY列表中的每个项
        for (SqlNode groupItem : groupList) {
            // 如果GROUP BY项是一个空的SqlNodeList（即空列表），则跳过它
            if (groupItem instanceof SqlNodeList && ((SqlNodeList) groupItem).size() == 0) {
                continue;
            }
            // 验证GROUP BY项，涉及类型的推导、函数的解析以及单调性的检查
            validateGroupItem(groupScope, aggregatingScope, groupItem);
        }
         // 检查GROUP BY列表中是否包含聚合函数
        // 使用聚合函数查找器在GROUP BY列表中查找聚合函数
        SqlNode agg = aggFinder.findAgg(groupList);
        if (agg != null) {// 如果找到了聚合函数
            throw newValidationError(agg, RESOURCE.aggregateIllegalInClause(clause));
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证GROUP BY子句中的项。
     *
     * @param groupScope GROUP BY子句的作用域。
     * @param aggregatingScope 聚合选择的作用域，可能包含聚合函数的上下文信息，可以为null。
     * @param groupItem GROUP BY子句中的单个项，
     */
    private void validateGroupItem(
            SqlValidatorScope groupScope,
            @Nullable AggregatingSelectScope aggregatingScope,
            SqlNode groupItem) {
        // 根据groupItem的类型进行不同的处理
        switch (groupItem.getKind()) {
            case GROUP_BY_DISTINCT: // 如果是一个带有DISTINCT的GROUP BY项
                // 遍历该项中的所有操作数（即DISTINCT后的列或表达式），并对它们进行验证
                for (SqlNode sqlNode : ((SqlCall) groupItem).getOperandList()) {
                    validateGroupItem(groupScope, aggregatingScope, sqlNode);
                }
                break;
            case GROUPING_SETS:// 如果是GROUPING SETS结构
            case ROLLUP: // 如果是ROLLUP结构
            case CUBE: // 如果是CUBE结构
                // 对这些复杂的分组结构进行验证
                validateGroupingSets(groupScope, aggregatingScope, (SqlCall) groupItem);
                break;
            default:
                // 如果groupItem是一个SqlNodeList（通常不会直接用于GROUP BY为了完整性而检查）
                if (groupItem instanceof SqlNodeList) {
                    break;
                }
                // 推导groupItem的类型
                final RelDataType type = deriveType(groupScope, groupItem);
                // 设置groupItem的验证后类型
                setValidatedNodeType(groupItem, type);
        }
    }

    private void validateGroupingSets(
            SqlValidatorScope groupScope,
            @Nullable AggregatingSelectScope aggregatingScope,
            SqlCall groupItem) {
        for (SqlNode node : groupItem.getOperandList()) {
            validateGroupItem(groupScope, aggregatingScope, node);
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证WHERE子句
     */
    protected void validateWhereClause(SqlSelect select) {
        // validate WHERE clause
        // 获取SQL查询中的WHERE子句节点
        final SqlNode where = select.getWhere();
        // 如果WHERE子句为空，则直接返回，不进行后续验证
        if (where == null) {
            return;
        }
        // 获取WHERE子句的作用域，这通常包括表别名、列名等解析时需要的上下文信息
        final SqlValidatorScope whereScope = getWhereScope(select);
        // 对WHERE子句进行扩展，比如解析别名、展开复杂的表达式等
        final SqlNode expandedWhere = expand(where, whereScope);
        // 将扩展后的WHERE子句设置回SQL查询中
        select.setWhere(expandedWhere);
        // 使用扩展后的WHERE子句和对应的作用域进行进一步的验证，确保WHERE子句的语法和逻辑正确
        validateWhereOrOn(whereScope, expandedWhere, "WHERE");
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 校验where条件
     */
    protected void validateWhereOrOn(SqlValidatorScope scope, SqlNode condition, String clause) {
        // 验证条件表达式中不包含聚合函数、窗口函数或分组函数
        // 这是因为在WHERE或ON子句中通常不允许使用这些函数
        validateNoAggs(aggOrOverOrGroupFinder, condition, clause);
        // 推断条件表达式中未知类型的字段或表达式
        inferUnknownTypes(booleanType, scope, condition);
        // 调用条件表达式的validate方法，进行基本的语法和逻辑验证
        condition.validate(this, scope);

        // 推导条件表达式的返回类型
        // 这通常涉及到解析表达式中的操作符、函数和字面量，以确定整个表达式的类型
        final RelDataType type = deriveType(scope, condition);
        // 验证推导出的类型是否是布尔类型
        // 因为在SQL的WHERE或ON子句中，条件表达式必须返回布尔值
        if (!isReturnBooleanType(type)) {
            // 如果不是布尔类型，则抛出验证错误
            throw newValidationError(condition, RESOURCE.condMustBeBoolean(clause));
        }
    }

    private static boolean isReturnBooleanType(RelDataType relDataType) {
        if (relDataType instanceof RelRecordType) {
            RelRecordType recordType = (RelRecordType) relDataType;
            Preconditions.checkState(
                    recordType.getFieldList().size() == 1,
                    "sub-query as condition must return only one column");
            RelDataTypeField recordField = recordType.getFieldList().get(0);
            return SqlTypeUtil.inBooleanFamily(recordField.getType());
        }
        return SqlTypeUtil.inBooleanFamily(relDataType);
    }

    protected void validateHavingClause(SqlSelect select) {
        // HAVING is validated in the scope after groups have been created.
        // For example, in "SELECT empno FROM emp WHERE empno = 10 GROUP BY
        // deptno HAVING empno = 10", the reference to 'empno' in the HAVING
        // clause is illegal.
        SqlNode having = select.getHaving();
        if (having == null) {
            return;
        }
        final AggregatingScope havingScope = (AggregatingScope) getSelectScope(select);
        if (config.conformance().isHavingAlias()) {
            SqlNode newExpr = expandGroupByOrHavingExpr(having, havingScope, select, true);
            if (having != newExpr) {
                having = newExpr;
                select.setHaving(newExpr);
            }
        }
        havingScope.checkAggregateExpr(having, true);
        inferUnknownTypes(booleanType, havingScope, having);
        having.validate(this, havingScope);
        final RelDataType type = deriveType(havingScope, having);
        if (!SqlTypeUtil.inBooleanFamily(type)) {
            throw newValidationError(having, RESOURCE.havingMustBeBoolean());
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 校验SelectList语句
     */
    protected RelDataType validateSelectList(
            final SqlNodeList selectItems, SqlSelect select, RelDataType targetRowType) {
        // First pass, ensure that aliases are unique. "*" and "TABLE.*" items
        // are ignored.

        // Validate SELECT list. Expand terms of the form "*" or "TABLE.*".
        // 验证SELECT列表。展开形式为"*"或"TABLE.*"的项。
        // 获取SELECT语句的作用域
        final SqlValidatorScope selectScope = getSelectScope(select);
        // 存储展开后的SELECT项
        final List<SqlNode> expandedSelectItems = new ArrayList<>();
        // 存储别名，确保唯一性
        final Set<String> aliases = new HashSet<>();
        // 存储字段名和对应的数据类型
        final List<Map.Entry<String, RelDataType>> fieldList = new ArrayList<>();
        // 遍历SELECT列表中的每一项
        for (SqlNode selectItem : selectItems) {
            // 如果SELECT项是一个子查询（SqlSelect），则特殊处理
            if (selectItem instanceof SqlSelect) {
                handleScalarSubQuery(
                        select, (SqlSelect) selectItem, expandedSelectItems, aliases, fieldList);
            } else {
                // Use the field list size to record the field index
                // because the select item may be a STAR(*), which could have been expanded.
                // 使用fieldList的大小来记录字段索引，因为SELECT项可能是"*"，它会被展开
                final int fieldIdx = fieldList.size();
                // 尝试从目标行类型中获取字段的数据类型，如果目标行类型是结构体且索引有效
                final RelDataType fieldType =
                        targetRowType.isStruct() && targetRowType.getFieldCount() > fieldIdx
                                ? targetRowType.getFieldList().get(fieldIdx).getType()
                                : unknownType;
                // 展开SELECT项，并处理别名、数据类型等
                expandSelectItem(
                        selectItem,
                        select,
                        fieldType,
                        expandedSelectItems,
                        aliases,
                        fieldList,
                        false);
            }
        }

        // Create the new select list with expanded items.  Pass through
        // the original parser position so that any overall failures can
        // still reference the original input text.
        // 创建一个新的SELECT列表，包含已展开的项。传递原始的解析器位置，以便任何整体失败仍然可以引用原始输入文本。
        SqlNodeList newSelectList =
                new SqlNodeList(expandedSelectItems, selectItems.getParserPosition());
        // 如果配置中启用了标识符展开，则更新SELECT语句的SELECT列表为新的列表。
        if (config.identifierExpansion()) {
            select.setSelectList(newSelectList);
        }
        // 获取非空的SELECT作用域，并设置已展开的SELECT列表。
        getRawSelectScopeNonNull(select).setExpandedSelectList(expandedSelectItems);

        // TODO: when SELECT appears as a value sub-query, should be using
        // something other than unknownType for targetRowType
        // 这里可能需要进一步的逻辑来确定子查询或复杂查询的返回类型。
        inferUnknownTypes(targetRowType, selectScope, newSelectList);

        // 遍历已展开的SELECT项，进行额外的验证。
        for (SqlNode selectItem : expandedSelectItems) {
            // 验证SELECT项中不包含聚合函数（在不允许使用聚合的上下文中）。
            validateNoAggs(groupFinder, selectItem, "SELECT");
            // 在SELECT作用域中验证表达式。
            validateExpr(selectItem, selectScope);
        }
        // 根据fieldList（包含字段名和对应的数据类型）创建并返回一个新的结构类型（StructType）。
        // 这个类型将代表SELECT查询的结果行类型。
        return typeFactory.createStructType(fieldList);
    }

    /**
     * Validates an expression.
     *
     * @param expr Expression
     * @param scope Scope in which expression occurs
     */
    private void validateExpr(SqlNode expr, SqlValidatorScope scope) {
        if (expr instanceof SqlCall) {
            final SqlOperator op = ((SqlCall) expr).getOperator();
            if (op.isAggregator() && op.requiresOver()) {
                throw newValidationError(expr, RESOURCE.absentOverClause());
            }
            if (op instanceof SqlTableFunction) {
                throw RESOURCE.cannotCallTableFunctionHere(op.getName()).ex();
            }
        }

        // Call on the expression to validate itself.
        expr.validateExpr(this, scope);

        // Perform any validation specific to the scope. For example, an
        // aggregating scope requires that expressions are valid aggregations.
        scope.validateExpr(expr);
    }

    /**
     * Processes SubQuery found in Select list. Checks that is actually Scalar sub-query and makes
     * proper entries in each of the 3 lists used to create the final rowType entry.
     *
     * @param parentSelect base SqlSelect item
     * @param selectItem child SqlSelect from select list
     * @param expandedSelectItems Select items after processing
     * @param aliasList built from user or system values
     * @param fieldList Built up entries for each select list entry
     */
    private void handleScalarSubQuery(
            SqlSelect parentSelect,
            SqlSelect selectItem,
            List<SqlNode> expandedSelectItems,
            Set<String> aliasList,
            List<Map.Entry<String, RelDataType>> fieldList) {
        // A scalar sub-query only has one output column.
        if (1 != SqlNonNullableAccessors.getSelectList(selectItem).size()) {
            throw newValidationError(selectItem, RESOURCE.onlyScalarSubQueryAllowed());
        }

        // No expansion in this routine just append to list.
        expandedSelectItems.add(selectItem);

        // Get or generate alias and add to list.
        final String alias = deriveAliasNonNull(selectItem, aliasList.size());
        aliasList.add(alias);

        final SelectScope scope = (SelectScope) getWhereScope(parentSelect);
        final RelDataType type = deriveType(scope, selectItem);
        setValidatedNodeType(selectItem, type);

        // We do not want to pass on the RelRecordType returned
        // by the sub-query.  Just the type of the single expression
        // in the sub-query select list.
        assert type instanceof RelRecordType;
        RelRecordType rec = (RelRecordType) type;

        RelDataType nodeType = rec.getFieldList().get(0).getType();
        nodeType = typeFactory.createTypeWithNullability(nodeType, true);
        fieldList.add(Pair.of(alias, nodeType));
    }

    /**
     * Derives a row-type for INSERT and UPDATE operations.
     *
     * @param table Target table for INSERT/UPDATE
     * @param targetColumnList List of target columns, or null if not specified
     * @param append Whether to append fields to those in <code>
     *                         baseRowType</code>
     * @return Rowtype
     */
    protected RelDataType createTargetRowType(
            SqlValidatorTable table, @Nullable SqlNodeList targetColumnList, boolean append) {
        RelDataType baseRowType = table.getRowType();
        if (targetColumnList == null) {
            return baseRowType;
        }
        List<RelDataTypeField> targetFields = baseRowType.getFieldList();
        final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
        if (append) {
            for (RelDataTypeField targetField : targetFields) {
                fields.add(
                        Pair.of(
                                SqlUtil.deriveAliasFromOrdinal(fields.size()),
                                targetField.getType()));
            }
        }
        final Set<Integer> assignedFields = new HashSet<>();
        final RelOptTable relOptTable = table instanceof RelOptTable ? ((RelOptTable) table) : null;
        for (SqlNode node : targetColumnList) {
            SqlIdentifier id = (SqlIdentifier) node;
            RelDataTypeField targetField =
                    SqlValidatorUtil.getTargetField(
                            baseRowType, typeFactory, id, catalogReader, relOptTable);
            if (targetField == null) {
                throw newValidationError(id, RESOURCE.unknownTargetColumn(id.toString()));
            }
            if (!assignedFields.add(targetField.getIndex())) {
                throw newValidationError(id, RESOURCE.duplicateTargetColumn(targetField.getName()));
            }
            fields.add(targetField);
        }
        return typeFactory.createStructType(fields);
    }

    @Override
    public void validateInsert(SqlInsert insert) {
        final SqlValidatorNamespace targetNamespace = getNamespaceOrThrow(insert);
        validateNamespace(targetNamespace, unknownType);
        final RelOptTable relOptTable =
                SqlValidatorUtil.getRelOptTable(
                        targetNamespace,
                        catalogReader.unwrap(Prepare.CatalogReader.class),
                        null,
                        null);
        final SqlValidatorTable table =
                relOptTable == null
                        ? getTable(targetNamespace)
                        : relOptTable.unwrapOrThrow(SqlValidatorTable.class);

        // INSERT has an optional column name list.  If present then
        // reduce the rowtype to the columns specified.  If not present
        // then the entire target rowtype is used.
        final RelDataType targetRowType =
                createTargetRowType(table, insert.getTargetColumnList(), false);

        final SqlNode source = insert.getSource();
        if (source instanceof SqlSelect) {
            final SqlSelect sqlSelect = (SqlSelect) source;
            validateSelect(sqlSelect, targetRowType);
        } else {
            final SqlValidatorScope scope = scopes.get(source);
            validateQuery(source, scope, targetRowType);
        }

        // REVIEW jvs 4-Dec-2008: In FRG-365, this namespace row type is
        // discarding the type inferred by inferUnknownTypes (which was invoked
        // from validateSelect above).  It would be better if that information
        // were used here so that we never saw any untyped nulls during
        // checkTypeAssignment.
        final RelDataType sourceRowType = getNamespaceOrThrow(source).getRowType();
        final RelDataType logicalTargetRowType = getLogicalTargetRowType(targetRowType, insert);
        setValidatedNodeType(insert, logicalTargetRowType);
        final RelDataType logicalSourceRowType = getLogicalSourceRowType(sourceRowType, insert);

        final List<ColumnStrategy> strategies =
                table.unwrapOrThrow(RelOptTable.class).getColumnStrategies();

        final RelDataType realTargetRowType =
                typeFactory.createStructType(
                        logicalTargetRowType.getFieldList().stream()
                                .filter(f -> strategies.get(f.getIndex()).canInsertInto())
                                .collect(Collectors.toList()));

        final RelDataType targetRowTypeToValidate =
                logicalSourceRowType.getFieldCount() == logicalTargetRowType.getFieldCount()
                        ? logicalTargetRowType
                        : realTargetRowType;

        checkFieldCount(
                insert.getTargetTable(),
                table,
                strategies,
                targetRowTypeToValidate,
                realTargetRowType,
                source,
                logicalSourceRowType,
                logicalTargetRowType);

        checkTypeAssignment(
                scopes.get(source), table, logicalSourceRowType, targetRowTypeToValidate, insert);

        checkConstraint(table, source, logicalTargetRowType);

        validateAccess(insert.getTargetTable(), table, SqlAccessEnum.INSERT);

        // Refresh the insert row type to keep sync with source.
        setValidatedNodeType(insert, targetRowTypeToValidate);
    }

    /**
     * Validates insert values against the constraint of a modifiable view.
     *
     * @param validatorTable Table that may wrap a ModifiableViewTable
     * @param source The values being inserted
     * @param targetRowType The target type for the view
     */
    private void checkConstraint(
            SqlValidatorTable validatorTable, SqlNode source, RelDataType targetRowType) {
        final ModifiableViewTable modifiableViewTable =
                validatorTable.unwrap(ModifiableViewTable.class);
        if (modifiableViewTable != null && source instanceof SqlCall) {
            final Table table = modifiableViewTable.getTable();
            final RelDataType tableRowType = table.getRowType(typeFactory);
            final List<RelDataTypeField> tableFields = tableRowType.getFieldList();

            // Get the mapping from column indexes of the underlying table
            // to the target columns and view constraints.
            final Map<Integer, RelDataTypeField> tableIndexToTargetField =
                    SqlValidatorUtil.getIndexToFieldMap(tableFields, targetRowType);
            final Map<Integer, RexNode> projectMap =
                    RelOptUtil.getColumnConstraints(
                            modifiableViewTable, targetRowType, typeFactory);

            // Determine columns (indexed to the underlying table) that need
            // to be validated against the view constraint.
            @SuppressWarnings("RedundantCast")
            final ImmutableBitSet targetColumns =
                    ImmutableBitSet.of((Iterable<Integer>) tableIndexToTargetField.keySet());
            @SuppressWarnings("RedundantCast")
            final ImmutableBitSet constrainedColumns =
                    ImmutableBitSet.of((Iterable<Integer>) projectMap.keySet());
            @SuppressWarnings("assignment.type.incompatible")
            List<@KeyFor({"tableIndexToTargetField", "projectMap"}) Integer>
                    constrainedTargetColumns = targetColumns.intersect(constrainedColumns).asList();

            // Validate insert values against the view constraint.
            final List<SqlNode> values = ((SqlCall) source).getOperandList();
            for (final int colIndex : constrainedTargetColumns) {
                final String colName = tableFields.get(colIndex).getName();
                final RelDataTypeField targetField = tableIndexToTargetField.get(colIndex);
                for (SqlNode row : values) {
                    final SqlCall call = (SqlCall) row;
                    final SqlNode sourceValue = call.operand(targetField.getIndex());
                    final ValidationError validationError =
                            new ValidationError(
                                    sourceValue,
                                    RESOURCE.viewConstraintNotSatisfied(
                                            colName, Util.last(validatorTable.getQualifiedName())));
                    RelOptUtil.validateValueAgainstConstraint(
                            sourceValue, projectMap.get(colIndex), validationError);
                }
            }
        }
    }

    /**
     * Validates updates against the constraint of a modifiable view.
     *
     * @param validatorTable A {@link SqlValidatorTable} that may wrap a ModifiableViewTable
     * @param update The UPDATE parse tree node
     * @param targetRowType The target type
     */
    private void checkConstraint(
            SqlValidatorTable validatorTable, SqlUpdate update, RelDataType targetRowType) {
        final ModifiableViewTable modifiableViewTable =
                validatorTable.unwrap(ModifiableViewTable.class);
        if (modifiableViewTable != null) {
            final Table table = modifiableViewTable.getTable();
            final RelDataType tableRowType = table.getRowType(typeFactory);

            final Map<Integer, RexNode> projectMap =
                    RelOptUtil.getColumnConstraints(
                            modifiableViewTable, targetRowType, typeFactory);
            final Map<String, Integer> nameToIndex =
                    SqlValidatorUtil.mapNameToIndex(tableRowType.getFieldList());

            // Validate update values against the view constraint.
            final List<String> targetNames =
                    SqlIdentifier.simpleNames(update.getTargetColumnList());
            final List<SqlNode> sources = update.getSourceExpressionList();
            Pair.forEach(
                    targetNames,
                    sources,
                    (columnName, expr) -> {
                        final Integer columnIndex = nameToIndex.get(columnName);
                        if (projectMap.containsKey(columnIndex)) {
                            final RexNode columnConstraint = projectMap.get(columnIndex);
                            final ValidationError validationError =
                                    new ValidationError(
                                            expr,
                                            RESOURCE.viewConstraintNotSatisfied(
                                                    columnName,
                                                    Util.last(validatorTable.getQualifiedName())));
                            RelOptUtil.validateValueAgainstConstraint(
                                    expr, columnConstraint, validationError);
                        }
                    });
        }
    }

    /**
     * Check the field count of sql insert source and target node row type.
     *
     * @param node target table sql identifier
     * @param table target table
     * @param strategies column strategies of target table
     * @param targetRowTypeToValidate row type to validate mainly for column strategies
     * @param realTargetRowType target table row type exclusive virtual columns
     * @param source source node
     * @param logicalSourceRowType source node row type
     * @param logicalTargetRowType logical target row type, contains only target columns if they are
     *     specified or if the sql dialect allows subset insert, make a subset of fields(start from
     *     the left first field) whose length is equals with the source row type fields number
     */
    private void checkFieldCount(
            SqlNode node,
            SqlValidatorTable table,
            List<ColumnStrategy> strategies,
            RelDataType targetRowTypeToValidate,
            RelDataType realTargetRowType,
            SqlNode source,
            RelDataType logicalSourceRowType,
            RelDataType logicalTargetRowType) {
        final int sourceFieldCount = logicalSourceRowType.getFieldCount();
        final int targetFieldCount = logicalTargetRowType.getFieldCount();
        final int targetRealFieldCount = realTargetRowType.getFieldCount();
        if (sourceFieldCount != targetFieldCount && sourceFieldCount != targetRealFieldCount) {
            // Allows the source row fields count to be equal with either
            // the logical or the real(excludes columns that can not insert into)
            // target row fields count.
            throw newValidationError(
                    node, RESOURCE.unmatchInsertColumn(targetFieldCount, sourceFieldCount));
        }
        // Ensure that non-nullable fields are targeted.
        for (final RelDataTypeField field : table.getRowType().getFieldList()) {
            final RelDataTypeField targetField =
                    targetRowTypeToValidate.getField(field.getName(), true, false);
            switch (strategies.get(field.getIndex())) {
                case NOT_NULLABLE:
                    assert !field.getType().isNullable();
                    if (targetField == null) {
                        throw newValidationError(node, RESOURCE.columnNotNullable(field.getName()));
                    }
                    break;
                case NULLABLE:
                    assert field.getType().isNullable();
                    break;
                case VIRTUAL:
                case STORED:
                    if (targetField != null
                            && !isValuesWithDefault(source, targetField.getIndex())) {
                        throw newValidationError(
                                node, RESOURCE.insertIntoAlwaysGenerated(field.getName()));
                    }
                    break;
                default:
                    break;
            }
        }
    }

    /** Returns whether a query uses {@code DEFAULT} to populate a given column. */
    private static boolean isValuesWithDefault(SqlNode source, int column) {
        switch (source.getKind()) {
            case VALUES:
                for (SqlNode operand : ((SqlCall) source).getOperandList()) {
                    if (!isRowWithDefault(operand, column)) {
                        return false;
                    }
                }
                return true;
            default:
                break;
        }
        return false;
    }

    private static boolean isRowWithDefault(SqlNode operand, int column) {
        switch (operand.getKind()) {
            case ROW:
                final SqlCall row = (SqlCall) operand;
                return row.getOperandList().size() >= column
                        && row.getOperandList().get(column).getKind() == SqlKind.DEFAULT;
            default:
                break;
        }
        return false;
    }

    protected RelDataType getLogicalTargetRowType(RelDataType targetRowType, SqlInsert insert) {
        if (insert.getTargetColumnList() == null
                && this.config.conformance().isInsertSubsetColumnsAllowed()) {
            // Target an implicit subset of columns.
            final SqlNode source = insert.getSource();
            final RelDataType sourceRowType = getNamespaceOrThrow(source).getRowType();
            final RelDataType logicalSourceRowType = getLogicalSourceRowType(sourceRowType, insert);
            final RelDataType implicitTargetRowType =
                    typeFactory.createStructType(
                            targetRowType
                                    .getFieldList()
                                    .subList(0, logicalSourceRowType.getFieldCount()));
            final SqlValidatorNamespace targetNamespace = getNamespaceOrThrow(insert);
            validateNamespace(targetNamespace, implicitTargetRowType);
            return implicitTargetRowType;
        } else {
            // Either the set of columns are explicitly targeted, or target the full
            // set of columns.
            return targetRowType;
        }
    }

    protected RelDataType getLogicalSourceRowType(RelDataType sourceRowType, SqlInsert insert) {
        return sourceRowType;
    }

    /**
     * Checks the type assignment of an INSERT or UPDATE query.
     *
     * <p>Skip the virtual columns(can not insert into) type assignment check if the source fields
     * count equals with the real target table fields count, see how #checkFieldCount was used.
     *
     * @param sourceScope Scope of query source which is used to infer node type
     * @param table Target table
     * @param sourceRowType Source row type
     * @param targetRowType Target row type, it should either contain all the virtual columns (can
     *     not insert into) or exclude all the virtual columns
     * @param query The query
     */
    protected void checkTypeAssignment(
            @Nullable SqlValidatorScope sourceScope,
            SqlValidatorTable table,
            RelDataType sourceRowType,
            RelDataType targetRowType,
            final SqlNode query) {
        // NOTE jvs 23-Feb-2006: subclasses may allow for extra targets
        // representing system-maintained columns, so stop after all sources
        // matched
        boolean isUpdateModifiableViewTable = false;
        if (query instanceof SqlUpdate) {
            final SqlNodeList targetColumnList = ((SqlUpdate) query).getTargetColumnList();
            if (targetColumnList != null) {
                final int targetColumnCnt = targetColumnList.size();
                targetRowType =
                        SqlTypeUtil.extractLastNFields(typeFactory, targetRowType, targetColumnCnt);
                sourceRowType =
                        SqlTypeUtil.extractLastNFields(typeFactory, sourceRowType, targetColumnCnt);
            }
            isUpdateModifiableViewTable = table.unwrap(ModifiableViewTable.class) != null;
        }
        if (SqlTypeUtil.equalAsStructSansNullability(
                typeFactory, sourceRowType, targetRowType, null)) {
            // Returns early if source and target row type equals sans nullability.
            return;
        }
        if (config.typeCoercionEnabled() && !isUpdateModifiableViewTable) {
            // Try type coercion first if implicit type coercion is allowed.
            boolean coerced =
                    typeCoercion.querySourceCoercion(
                            sourceScope, sourceRowType, targetRowType, query);
            if (coerced) {
                return;
            }
        }

        // Fall back to default behavior: compare the type families.
        List<RelDataTypeField> sourceFields = sourceRowType.getFieldList();
        List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final int sourceCount = sourceFields.size();
        for (int i = 0; i < sourceCount; ++i) {
            RelDataType sourceType = sourceFields.get(i).getType();
            RelDataType targetType = targetFields.get(i).getType();
            if (!SqlTypeUtil.canAssignFrom(targetType, sourceType)) {
                SqlNode node = getNthExpr(query, i, sourceCount);
                if (node instanceof SqlDynamicParam) {
                    continue;
                }
                String targetTypeString;
                String sourceTypeString;
                if (SqlTypeUtil.areCharacterSetsMismatched(sourceType, targetType)) {
                    sourceTypeString = sourceType.getFullTypeString();
                    targetTypeString = targetType.getFullTypeString();
                } else {
                    sourceTypeString = sourceType.toString();
                    targetTypeString = targetType.toString();
                }
                throw newValidationError(
                        node,
                        RESOURCE.typeNotAssignable(
                                targetFields.get(i).getName(), targetTypeString,
                                sourceFields.get(i).getName(), sourceTypeString));
            }
        }
    }

    /**
     * Locates the n'th expression in an INSERT or UPDATE query.
     *
     * @param query Query
     * @param ordinal Ordinal of expression
     * @param sourceCount Number of expressions
     * @return Ordinal'th expression, never null
     */
    private static SqlNode getNthExpr(SqlNode query, int ordinal, int sourceCount) {
        if (query instanceof SqlInsert) {
            SqlInsert insert = (SqlInsert) query;
            if (insert.getTargetColumnList() != null) {
                return insert.getTargetColumnList().get(ordinal);
            } else {
                return getNthExpr(insert.getSource(), ordinal, sourceCount);
            }
        } else if (query instanceof SqlUpdate) {
            SqlUpdate update = (SqlUpdate) query;
            if (update.getSourceExpressionList() != null) {
                return update.getSourceExpressionList().get(ordinal);
            } else {
                return getNthExpr(
                        SqlNonNullableAccessors.getSourceSelect(update), ordinal, sourceCount);
            }
        } else if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            SqlNodeList selectList = SqlNonNullableAccessors.getSelectList(select);
            if (selectList.size() == sourceCount) {
                return selectList.get(ordinal);
            } else {
                return query; // give up
            }
        } else {
            return query; // give up
        }
    }

    @Override
    public void validateDelete(SqlDelete call) {
        final SqlSelect sqlSelect = SqlNonNullableAccessors.getSourceSelect(call);
        validateSelect(sqlSelect, unknownType);

        final SqlValidatorNamespace targetNamespace = getNamespaceOrThrow(call);
        validateNamespace(targetNamespace, unknownType);
        final SqlValidatorTable table = targetNamespace.getTable();

        validateAccess(call.getTargetTable(), table, SqlAccessEnum.DELETE);
    }

    @Override
    public void validateUpdate(SqlUpdate call) {
        final SqlValidatorNamespace targetNamespace = getNamespaceOrThrow(call);
        validateNamespace(targetNamespace, unknownType);
        final RelOptTable relOptTable =
                SqlValidatorUtil.getRelOptTable(
                        targetNamespace,
                        castNonNull(catalogReader.unwrap(Prepare.CatalogReader.class)),
                        null,
                        null);
        final SqlValidatorTable table =
                relOptTable == null
                        ? getTable(targetNamespace)
                        : relOptTable.unwrapOrThrow(SqlValidatorTable.class);

        final RelDataType targetRowType =
                createTargetRowType(table, call.getTargetColumnList(), true);

        final SqlSelect select = SqlNonNullableAccessors.getSourceSelect(call);
        validateSelect(select, targetRowType);

        final RelDataType sourceRowType = getValidatedNodeType(select);
        checkTypeAssignment(scopes.get(select), table, sourceRowType, targetRowType, call);

        checkConstraint(table, call, targetRowType);

        validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);
    }

    @Override
    public void validateMerge(SqlMerge call) {
        SqlSelect sqlSelect = SqlNonNullableAccessors.getSourceSelect(call);
        // REVIEW zfong 5/25/06 - Does an actual type have to be passed into
        // validateSelect()?

        // REVIEW jvs 6-June-2006:  In general, passing unknownType like
        // this means we won't be able to correctly infer the types
        // for dynamic parameter markers (SET x = ?).  But
        // maybe validateUpdate and validateInsert below will do
        // the job?

        // REVIEW ksecretan 15-July-2011: They didn't get a chance to
        // since validateSelect() would bail.
        // Let's use the update/insert targetRowType when available.
        IdentifierNamespace targetNamespace =
                (IdentifierNamespace) getNamespaceOrThrow(call.getTargetTable());
        validateNamespace(targetNamespace, unknownType);

        SqlValidatorTable table = targetNamespace.getTable();
        validateAccess(call.getTargetTable(), table, SqlAccessEnum.UPDATE);

        RelDataType targetRowType = unknownType;

        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            requireNonNull(table, () -> "ns.getTable() for " + targetNamespace);
            targetRowType = createTargetRowType(table, updateCall.getTargetColumnList(), true);
        }
        SqlInsert insertCall = call.getInsertCall();
        if (insertCall != null) {
            requireNonNull(table, () -> "ns.getTable() for " + targetNamespace);
            targetRowType = createTargetRowType(table, insertCall.getTargetColumnList(), false);
        }

        validateSelect(sqlSelect, targetRowType);

        SqlUpdate updateCallAfterValidate = call.getUpdateCall();
        if (updateCallAfterValidate != null) {
            validateUpdate(updateCallAfterValidate);
        }
        SqlInsert insertCallAfterValidate = call.getInsertCall();
        if (insertCallAfterValidate != null) {
            validateInsert(insertCallAfterValidate);
        }
    }

    /**
     * Validates access to a table.
     *
     * @param table Table
     * @param requiredAccess Access requested on table
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 验证对表的访问权限。
     *
     * @param node SqlNode SQL语句中的节点，用于在错误消息中定位问题位置
     * @param table SqlValidatorTable 要验证的表对象，可能为null（表示无表或表未找到）
     * @param requiredAccess SqlAccessEnum 请求的访问类型，如读、写等
     */
    private void validateAccess(
            SqlNode node, @Nullable SqlValidatorTable table, SqlAccessEnum requiredAccess) {
        // 如果表对象不为null，则进行访问权限的验证
        if (table != null) {
            // 获取表允许的访问类型
            SqlAccessType access = table.getAllowedAccess();
            // 如果不允许请求的访问类型
            if (!access.allowsAccess(requiredAccess)) {
                // 抛出验证错误，包含请求的访问类型、表的完整名称以及错误位置
                throw newValidationError(
                        node,
                        RESOURCE.accessNotAllowed(
                                requiredAccess.name(), table.getQualifiedName().toString()));
            }
        }
    }

    /**
     * Validates snapshot to a table.
     *
     * @param node The node to validate
     * @param scope Validator scope to derive type
     * @param ns The namespace to lookup table
     */
    private void validateSnapshot(
            SqlNode node, @Nullable SqlValidatorScope scope, SqlValidatorNamespace ns) {
        if (node.getKind() == SqlKind.SNAPSHOT) {
            SqlSnapshot snapshot = (SqlSnapshot) node;
            SqlNode period = snapshot.getPeriod();
            RelDataType dataType = deriveType(requireNonNull(scope, "scope"), period);
            // ----- FLINK MODIFICATION BEGIN -----
            if (!(dataType.getSqlTypeName() == SqlTypeName.TIMESTAMP
                    || dataType.getSqlTypeName() == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                throw newValidationError(
                        period,
                        Static.RESOURCE.illegalExpressionForTemporal(
                                dataType.getSqlTypeName().getName()));
            }
            if (ns instanceof IdentifierNamespace && ns.resolve() instanceof WithItemNamespace) {
                // If the snapshot is used over a CTE, then we don't have a concrete underlying
                // table to operate on. This will be rechecked later in the planner rules.
                return;
            }
            // ----- FLINK MODIFICATION END -----
            SqlValidatorTable table = getTable(ns);
            if (!table.isTemporal()) {
                List<String> qualifiedName = table.getQualifiedName();
                String tableName = qualifiedName.get(qualifiedName.size() - 1);
                throw newValidationError(
                        snapshot.getTableRef(), Static.RESOURCE.notTemporalTable(tableName));
            }
        }
    }

    /**
     * Validates a VALUES clause.
     *
     * @param node Values clause
     * @param targetRowType Row type which expression must conform to
     * @param scope Scope within which clause occurs
     */
    protected void validateValues(
            SqlCall node, RelDataType targetRowType, final SqlValidatorScope scope) {
        assert node.getKind() == SqlKind.VALUES;

        final List<SqlNode> operands = node.getOperandList();
        for (SqlNode operand : operands) {
            if (!(operand.getKind() == SqlKind.ROW)) {
                throw Util.needToImplement("Values function where operands are scalars");
            }

            SqlCall rowConstructor = (SqlCall) operand;
            if (this.config.conformance().isInsertSubsetColumnsAllowed()
                    && targetRowType.isStruct()
                    && rowConstructor.operandCount() < targetRowType.getFieldCount()) {
                targetRowType =
                        typeFactory.createStructType(
                                targetRowType
                                        .getFieldList()
                                        .subList(0, rowConstructor.operandCount()));
            } else if (targetRowType.isStruct()
                    && rowConstructor.operandCount() != targetRowType.getFieldCount()) {
                return;
            }

            inferUnknownTypes(targetRowType, scope, rowConstructor);

            if (targetRowType.isStruct()) {
                for (Pair<SqlNode, RelDataTypeField> pair :
                        Pair.zip(rowConstructor.getOperandList(), targetRowType.getFieldList())) {
                    if (!pair.right.getType().isNullable()
                            && SqlUtil.isNullLiteral(pair.left, false)) {
                        throw newValidationError(
                                node, RESOURCE.columnNotNullable(pair.right.getName()));
                    }
                }
            }
        }

        for (SqlNode operand : operands) {
            operand.validate(this, scope);
        }

        // validate that all row types have the same number of columns
        //  and that expressions in each column are compatible.
        // A values expression is turned into something that looks like
        // ROW(type00, type01,...), ROW(type11,...),...
        final int rowCount = operands.size();
        if (rowCount >= 2) {
            SqlCall firstRow = (SqlCall) operands.get(0);
            final int columnCount = firstRow.operandCount();

            // 1. check that all rows have the same cols length
            for (SqlNode operand : operands) {
                SqlCall thisRow = (SqlCall) operand;
                if (columnCount != thisRow.operandCount()) {
                    throw newValidationError(
                            node,
                            RESOURCE.incompatibleValueType(SqlStdOperatorTable.VALUES.getName()));
                }
            }

            // 2. check if types at i:th position in each row are compatible
            for (int col = 0; col < columnCount; col++) {
                final int c = col;
                final RelDataType type =
                        typeFactory.leastRestrictive(
                                new AbstractList<RelDataType>() {
                                    @Override
                                    public RelDataType get(int row) {
                                        SqlCall thisRow = (SqlCall) operands.get(row);
                                        return deriveType(scope, thisRow.operand(c));
                                    }

                                    @Override
                                    public int size() {
                                        return rowCount;
                                    }
                                });

                if (null == type) {
                    throw newValidationError(
                            node,
                            RESOURCE.incompatibleValueType(SqlStdOperatorTable.VALUES.getName()));
                }
            }
        }
    }

    @Override
    public void validateDataType(SqlDataTypeSpec dataType) {}

    @Override
    public void validateDynamicParam(SqlDynamicParam dynamicParam) {}

    /**
     * Throws a validator exception with access to the validator context. The exception is
     * determined when an instance is created.
     */
    private class ValidationError implements Supplier<CalciteContextException> {
        private final SqlNode sqlNode;
        private final Resources.ExInst<SqlValidatorException> validatorException;

        ValidationError(
                SqlNode sqlNode, Resources.ExInst<SqlValidatorException> validatorException) {
            this.sqlNode = sqlNode;
            this.validatorException = validatorException;
        }

        @Override
        public CalciteContextException get() {
            return newValidationError(sqlNode, validatorException);
        }
    }

    /**
     * Throws a validator exception with access to the validator context. The exception is
     * determined when the function is applied.
     */
    class ValidationErrorFunction
            implements BiFunction<
                    SqlNode, Resources.ExInst<SqlValidatorException>, CalciteContextException> {
        @Override
        public CalciteContextException apply(
                SqlNode v0, Resources.ExInst<SqlValidatorException> v1) {
            return newValidationError(v0, v1);
        }
    }

    public ValidationErrorFunction getValidationErrorFunction() {
        return validationErrorFunction;
    }

    @Override
    public CalciteContextException newValidationError(
            SqlNode node, Resources.ExInst<SqlValidatorException> e) {
        assert node != null;
        final SqlParserPos pos = node.getParserPosition();
        return SqlUtil.newContextException(pos, e);
    }

    protected SqlWindow getWindowByName(SqlIdentifier id, SqlValidatorScope scope) {
        SqlWindow window = null;
        if (id.isSimple()) {
            final String name = id.getSimple();
            window = scope.lookupWindow(name);
        }
        if (window == null) {
            throw newValidationError(id, RESOURCE.windowNotFound(id.toString()));
        }
        return window;
    }

    @Override
    public SqlWindow resolveWindow(SqlNode windowOrRef, SqlValidatorScope scope) {
        SqlWindow window;
        if (windowOrRef instanceof SqlIdentifier) {
            window = getWindowByName((SqlIdentifier) windowOrRef, scope);
        } else {
            window = (SqlWindow) windowOrRef;
        }
        while (true) {
            final SqlIdentifier refId = window.getRefName();
            if (refId == null) {
                break;
            }
            final String refName = refId.getSimple();
            SqlWindow refWindow = scope.lookupWindow(refName);
            if (refWindow == null) {
                throw newValidationError(refId, RESOURCE.windowNotFound(refName));
            }
            window = window.overlay(refWindow, this);
        }

        return window;
    }

    public SqlNode getOriginal(SqlNode expr) {
        SqlNode original = originalExprs.get(expr);
        if (original == null) {
            original = expr;
        }
        return original;
    }

    public void setOriginal(SqlNode expr, SqlNode original) {
        // Don't overwrite the original original.
        originalExprs.putIfAbsent(expr, original);
    }

    @Nullable
    SqlValidatorNamespace lookupFieldNamespace(RelDataType rowType, String name) {
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final RelDataTypeField field = nameMatcher.field(rowType, name);
        if (field == null) {
            return null;
        }
        return new FieldNamespace(this, field.getType());
    }

    @Override
    public void validateWindow(
            SqlNode windowOrId, SqlValidatorScope scope, @Nullable SqlCall call) {
        // Enable nested aggregates with window aggregates (OVER operator)
        inWindow = true;

        final SqlWindow targetWindow;
        switch (windowOrId.getKind()) {
            case IDENTIFIER:
                // Just verify the window exists in this query.  It will validate
                // when the definition is processed
                targetWindow = getWindowByName((SqlIdentifier) windowOrId, scope);
                break;
            case WINDOW:
                targetWindow = (SqlWindow) windowOrId;
                break;
            default:
                throw Util.unexpected(windowOrId.getKind());
        }

        requireNonNull(call, () -> "call is null when validating windowOrId " + windowOrId);
        assert targetWindow.getWindowCall() == null;
        targetWindow.setWindowCall(call);
        targetWindow.validate(this, scope);
        targetWindow.setWindowCall(null);
        call.validate(this, scope);

        validateAggregateParams(call, null, null, null, scope);

        // Disable nested aggregates post validation
        inWindow = false;
    }

    @Override
    public void validateMatchRecognize(SqlCall call) {
        final SqlMatchRecognize matchRecognize = (SqlMatchRecognize) call;
        final MatchRecognizeScope scope =
                (MatchRecognizeScope) getMatchRecognizeScope(matchRecognize);

        final MatchRecognizeNamespace ns =
                getNamespaceOrThrow(call).unwrap(MatchRecognizeNamespace.class);
        assert ns.rowType == null;

        // rows per match
        final SqlLiteral rowsPerMatch = matchRecognize.getRowsPerMatch();
        final boolean allRows =
                rowsPerMatch != null
                        && rowsPerMatch.getValue() == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

        final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();

        // parse PARTITION BY column
        SqlNodeList partitionBy = matchRecognize.getPartitionList();
        if (partitionBy != null) {
            for (SqlNode node : partitionBy) {
                SqlIdentifier identifier = (SqlIdentifier) node;
                identifier.validate(this, scope);
                RelDataType type = deriveType(scope, identifier);
                String name = identifier.names.get(1);
                typeBuilder.add(name, type);
            }
        }

        // parse ORDER BY column
        SqlNodeList orderBy = matchRecognize.getOrderList();
        if (orderBy != null) {
            for (SqlNode node : orderBy) {
                node.validate(this, scope);
                SqlIdentifier identifier;
                if (node instanceof SqlBasicCall) {
                    identifier = (SqlIdentifier) ((SqlBasicCall) node).operand(0);
                } else {
                    identifier =
                            (SqlIdentifier)
                                    requireNonNull(
                                            node,
                                            () -> "order by field is null. All fields: " + orderBy);
                }

                if (allRows) {
                    RelDataType type = deriveType(scope, identifier);
                    String name = identifier.names.get(1);
                    if (!typeBuilder.nameExists(name)) {
                        typeBuilder.add(name, type);
                    }
                }
            }
        }

        if (allRows) {
            final SqlValidatorNamespace sqlNs = getNamespaceOrThrow(matchRecognize.getTableRef());
            final RelDataType inputDataType = sqlNs.getRowType();
            for (RelDataTypeField fs : inputDataType.getFieldList()) {
                if (!typeBuilder.nameExists(fs.getName())) {
                    typeBuilder.add(fs);
                }
            }
        }

        // retrieve pattern variables used in pattern and subset
        SqlNode pattern = matchRecognize.getPattern();
        PatternVarVisitor visitor = new PatternVarVisitor(scope);
        pattern.accept(visitor);

        SqlLiteral interval = matchRecognize.getInterval();
        if (interval != null) {
            interval.validate(this, scope);
            if (((SqlIntervalLiteral) interval).signum() < 0) {
                String intervalValue = interval.toValue();
                throw newValidationError(
                        interval,
                        RESOURCE.intervalMustBeNonNegative(
                                intervalValue != null ? intervalValue : interval.toString()));
            }
            if (orderBy == null || orderBy.size() == 0) {
                throw newValidationError(interval, RESOURCE.cannotUseWithinWithoutOrderBy());
            }

            SqlNode firstOrderByColumn = orderBy.get(0);
            SqlIdentifier identifier;
            if (firstOrderByColumn instanceof SqlBasicCall) {
                identifier = ((SqlBasicCall) firstOrderByColumn).operand(0);
            } else {
                identifier =
                        (SqlIdentifier) requireNonNull(firstOrderByColumn, "firstOrderByColumn");
            }
            RelDataType firstOrderByColumnType = deriveType(scope, identifier);
            // ----- FLINK MODIFICATION BEGIN -----
            if (!(firstOrderByColumnType.getSqlTypeName() == SqlTypeName.TIMESTAMP
                    || firstOrderByColumnType.getSqlTypeName()
                            == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE)) {
                throw newValidationError(interval, RESOURCE.firstColumnOfOrderByMustBeTimestamp());
            }
            // ----- FLINK MODIFICATION END -----

            SqlNode expand = expand(interval, scope);
            RelDataType type = deriveType(scope, expand);
            setValidatedNodeType(interval, type);
        }

        validateDefinitions(matchRecognize, scope);

        SqlNodeList subsets = matchRecognize.getSubsetList();
        if (subsets != null && subsets.size() > 0) {
            for (SqlNode node : subsets) {
                List<SqlNode> operands = ((SqlCall) node).getOperandList();
                String leftString = ((SqlIdentifier) operands.get(0)).getSimple();
                if (scope.getPatternVars().contains(leftString)) {
                    throw newValidationError(
                            operands.get(0), RESOURCE.patternVarAlreadyDefined(leftString));
                }
                scope.addPatternVar(leftString);
                for (SqlNode right : (SqlNodeList) operands.get(1)) {
                    SqlIdentifier id = (SqlIdentifier) right;
                    if (!scope.getPatternVars().contains(id.getSimple())) {
                        throw newValidationError(id, RESOURCE.unknownPattern(id.getSimple()));
                    }
                    scope.addPatternVar(id.getSimple());
                }
            }
        }

        // validate AFTER ... SKIP TO
        final SqlNode skipTo = matchRecognize.getAfter();
        if (skipTo instanceof SqlCall) {
            final SqlCall skipToCall = (SqlCall) skipTo;
            final SqlIdentifier id = skipToCall.operand(0);
            if (!scope.getPatternVars().contains(id.getSimple())) {
                throw newValidationError(id, RESOURCE.unknownPattern(id.getSimple()));
            }
        }

        List<Map.Entry<String, RelDataType>> measureColumns =
                validateMeasure(matchRecognize, scope, allRows);
        for (Map.Entry<String, RelDataType> c : measureColumns) {
            if (!typeBuilder.nameExists(c.getKey())) {
                typeBuilder.add(c.getKey(), c.getValue());
            }
        }

        final RelDataType rowType = typeBuilder.build();
        if (matchRecognize.getMeasureList().size() == 0) {
            ns.setType(getNamespaceOrThrow(matchRecognize.getTableRef()).getRowType());
        } else {
            ns.setType(rowType);
        }
    }

    private List<Map.Entry<String, RelDataType>> validateMeasure(
            SqlMatchRecognize mr, MatchRecognizeScope scope, boolean allRows) {
        final List<String> aliases = new ArrayList<>();
        final List<SqlNode> sqlNodes = new ArrayList<>();
        final SqlNodeList measures = mr.getMeasureList();
        final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();

        for (SqlNode measure : measures) {
            assert measure instanceof SqlCall;
            final String alias = deriveAliasNonNull(measure, aliases.size());
            aliases.add(alias);

            SqlNode expand = expand(measure, scope);
            expand = navigationInMeasure(expand, allRows);
            setOriginal(expand, measure);

            inferUnknownTypes(unknownType, scope, expand);
            final RelDataType type = deriveType(scope, expand);
            setValidatedNodeType(measure, type);

            fields.add(Pair.of(alias, type));
            sqlNodes.add(
                    SqlStdOperatorTable.AS.createCall(
                            SqlParserPos.ZERO,
                            expand,
                            new SqlIdentifier(alias, SqlParserPos.ZERO)));
        }

        SqlNodeList list = new SqlNodeList(sqlNodes, measures.getParserPosition());
        inferUnknownTypes(unknownType, scope, list);

        for (SqlNode node : list) {
            validateExpr(node, scope);
        }

        mr.setOperand(SqlMatchRecognize.OPERAND_MEASURES, list);

        return fields;
    }

    private SqlNode navigationInMeasure(SqlNode node, boolean allRows) {
        final Set<String> prefix = node.accept(new PatternValidator(true));
        Util.discard(prefix);
        final List<SqlNode> ops = ((SqlCall) node).getOperandList();

        final SqlOperator defaultOp =
                allRows ? SqlStdOperatorTable.RUNNING : SqlStdOperatorTable.FINAL;
        final SqlNode op0 = ops.get(0);
        if (!isRunningOrFinal(op0.getKind()) || !allRows && op0.getKind() == SqlKind.RUNNING) {
            SqlNode newNode = defaultOp.createCall(SqlParserPos.ZERO, op0);
            node = SqlStdOperatorTable.AS.createCall(SqlParserPos.ZERO, newNode, ops.get(1));
        }

        node = new NavigationExpander().go(node);
        return node;
    }

    private void validateDefinitions(SqlMatchRecognize mr, MatchRecognizeScope scope) {
        final Set<String> aliases = catalogReader.nameMatcher().createSet();
        for (SqlNode item : mr.getPatternDefList()) {
            final String alias = alias(item);
            if (!aliases.add(alias)) {
                throw newValidationError(item, Static.RESOURCE.patternVarAlreadyDefined(alias));
            }
            scope.addPatternVar(alias);
        }

        final List<SqlNode> sqlNodes = new ArrayList<>();
        for (SqlNode item : mr.getPatternDefList()) {
            final String alias = alias(item);
            SqlNode expand = expand(item, scope);
            expand = navigationInDefine(expand, alias);
            setOriginal(expand, item);

            inferUnknownTypes(booleanType, scope, expand);
            expand.validate(this, scope);

            // Some extra work need required here.
            // In PREV, NEXT, FINAL and LAST, only one pattern variable is allowed.
            sqlNodes.add(
                    SqlStdOperatorTable.AS.createCall(
                            SqlParserPos.ZERO,
                            expand,
                            new SqlIdentifier(alias, SqlParserPos.ZERO)));

            final RelDataType type = deriveType(scope, expand);
            if (!SqlTypeUtil.inBooleanFamily(type)) {
                throw newValidationError(expand, RESOURCE.condMustBeBoolean("DEFINE"));
            }
            setValidatedNodeType(item, type);
        }

        SqlNodeList list = new SqlNodeList(sqlNodes, mr.getPatternDefList().getParserPosition());
        inferUnknownTypes(unknownType, scope, list);
        for (SqlNode node : list) {
            validateExpr(node, scope);
        }
        mr.setOperand(SqlMatchRecognize.OPERAND_PATTERN_DEFINES, list);
    }

    /** Returns the alias of a "expr AS alias" expression. */
    private static String alias(SqlNode item) {
        assert item instanceof SqlCall;
        assert item.getKind() == SqlKind.AS;
        final SqlIdentifier identifier = ((SqlCall) item).operand(1);
        return identifier.getSimple();
    }

    public void validatePivot(SqlPivot pivot) {
        final PivotScope scope =
                (PivotScope) requireNonNull(getJoinScope(pivot), () -> "joinScope for " + pivot);

        final PivotNamespace ns = getNamespaceOrThrow(pivot).unwrap(PivotNamespace.class);
        assert ns.rowType == null;

        // Given
        //   query PIVOT (agg1 AS a, agg2 AS b, ...
        //   FOR (axis1, ..., axisN)
        //   IN ((v11, ..., v1N) AS label1,
        //       (v21, ..., v2N) AS label2, ...))
        // the type is
        //   k1, ... kN, a_label1, b_label1, ..., a_label2, b_label2, ...
        // where k1, ... kN are columns that are not referenced as an argument to
        // an aggregate or as an axis.

        // Aggregates, e.g. "PIVOT (sum(x) AS sum_x, count(*) AS c)"
        final List<Pair<@Nullable String, RelDataType>> aggNames = new ArrayList<>();
        pivot.forEachAgg(
                (alias, call) -> {
                    call.validate(this, scope);
                    final RelDataType type = deriveType(scope, call);
                    aggNames.add(Pair.of(alias, type));
                    if (!(call instanceof SqlCall)
                            || !(((SqlCall) call).getOperator() instanceof SqlAggFunction)) {
                        throw newValidationError(call, RESOURCE.pivotAggMalformed());
                    }
                });

        // Axes, e.g. "FOR (JOB, DEPTNO)"
        final List<RelDataType> axisTypes = new ArrayList<>();
        final List<SqlIdentifier> axisIdentifiers = new ArrayList<>();
        for (SqlNode axis : pivot.axisList) {
            SqlIdentifier identifier = (SqlIdentifier) axis;
            identifier.validate(this, scope);
            final RelDataType type = deriveType(scope, identifier);
            axisTypes.add(type);
            axisIdentifiers.add(identifier);
        }

        // Columns that have been seen as arguments to aggregates or as axes
        // do not appear in the output.
        final Set<String> columnNames = pivot.usedColumnNames();
        final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
        scope.getChild()
                .getRowType()
                .getFieldList()
                .forEach(
                        field -> {
                            if (!columnNames.contains(field.getName())) {
                                typeBuilder.add(field);
                            }
                        });

        // Values, e.g. "IN (('CLERK', 10) AS c10, ('MANAGER, 20) AS m20)"
        pivot.forEachNameValues(
                (alias, nodeList) -> {
                    if (nodeList.size() != axisTypes.size()) {
                        throw newValidationError(
                                nodeList,
                                RESOURCE.pivotValueArityMismatch(
                                        nodeList.size(), axisTypes.size()));
                    }
                    final SqlOperandTypeChecker typeChecker =
                            OperandTypes.COMPARABLE_UNORDERED_COMPARABLE_UNORDERED;
                    Pair.forEach(
                            axisIdentifiers,
                            nodeList,
                            (identifier, subNode) -> {
                                subNode.validate(this, scope);
                                typeChecker.checkOperandTypes(
                                        new SqlCallBinding(
                                                this,
                                                scope,
                                                SqlStdOperatorTable.EQUALS.createCall(
                                                        subNode.getParserPosition(),
                                                        identifier,
                                                        subNode)),
                                        true);
                            });
                    Pair.forEach(
                            aggNames,
                            (aggAlias, aggType) ->
                                    typeBuilder.add(
                                            aggAlias == null ? alias : alias + "_" + aggAlias,
                                            aggType));
                });

        final RelDataType rowType = typeBuilder.build();
        ns.setType(rowType);
    }

    public void validateUnpivot(SqlUnpivot unpivot) {
        final UnpivotScope scope =
                (UnpivotScope) requireNonNull(getJoinScope(unpivot), () -> "scope for " + unpivot);

        final UnpivotNamespace ns = getNamespaceOrThrow(unpivot).unwrap(UnpivotNamespace.class);
        assert ns.rowType == null;

        // Given
        //   query UNPIVOT ((measure1, ..., measureM)
        //   FOR (axis1, ..., axisN)
        //   IN ((c11, ..., c1M) AS (value11, ..., value1N),
        //       (c21, ..., c2M) AS (value21, ..., value2N), ...)
        // the type is
        //   k1, ... kN, axis1, ..., axisN, measure1, ..., measureM
        // where k1, ... kN are columns that are not referenced as an argument to
        // an aggregate or as an axis.

        // First, And make sure that each
        final int measureCount = unpivot.measureList.size();
        final int axisCount = unpivot.axisList.size();
        unpivot.forEachNameValues(
                (nodeList, valueList) -> {
                    // Make sure that each (ci1, ... ciM) list has the same arity as
                    // (measure1, ..., measureM).
                    if (nodeList.size() != measureCount) {
                        throw newValidationError(
                                nodeList,
                                RESOURCE.unpivotValueArityMismatch(nodeList.size(), measureCount));
                    }

                    // Make sure that each (vi1, ... viN) list has the same arity as
                    // (axis1, ..., axisN).
                    if (valueList != null && valueList.size() != axisCount) {
                        throw newValidationError(
                                valueList,
                                RESOURCE.unpivotValueArityMismatch(valueList.size(), axisCount));
                    }

                    // Make sure that each IN expression is a valid column from the input.
                    nodeList.forEach(node -> deriveType(scope, node));
                });

        // What columns from the input are not referenced by a column in the IN
        // list?
        final SqlValidatorNamespace inputNs = requireNonNull(getNamespace(unpivot.query));
        final Set<String> unusedColumnNames = catalogReader.nameMatcher().createSet();
        unusedColumnNames.addAll(inputNs.getRowType().getFieldNames());
        unusedColumnNames.removeAll(unpivot.usedColumnNames());

        // What columns will be present in the output row type?
        final Set<String> columnNames = catalogReader.nameMatcher().createSet();
        columnNames.addAll(unusedColumnNames);

        // Gather the name and type of each measure.
        final List<Pair<String, RelDataType>> measureNameTypes = new ArrayList<>();
        Ord.forEach(
                unpivot.measureList,
                (measure, i) -> {
                    final String measureName = ((SqlIdentifier) measure).getSimple();
                    final List<RelDataType> types = new ArrayList<>();
                    final List<SqlNode> nodes = new ArrayList<>();
                    unpivot.forEachNameValues(
                            (nodeList, valueList) -> {
                                final SqlNode alias = nodeList.get(i);
                                nodes.add(alias);
                                types.add(deriveType(scope, alias));
                            });
                    final RelDataType type0 = typeFactory.leastRestrictive(types);
                    if (type0 == null) {
                        throw newValidationError(
                                nodes.get(0), RESOURCE.unpivotCannotDeriveMeasureType(measureName));
                    }
                    final RelDataType type =
                            typeFactory.createTypeWithNullability(
                                    type0, unpivot.includeNulls || unpivot.measureList.size() > 1);
                    setValidatedNodeType(measure, type);
                    if (!columnNames.add(measureName)) {
                        throw newValidationError(measure, RESOURCE.unpivotDuplicate(measureName));
                    }
                    measureNameTypes.add(Pair.of(measureName, type));
                });

        // Gather the name and type of each axis.
        // Consider
        //   FOR (job, deptno)
        //   IN (a AS ('CLERK', 10),
        //       b AS ('ANALYST', 20))
        // There are two axes, (job, deptno), and so each value list ('CLERK', 10),
        // ('ANALYST', 20) must have arity two.
        //
        // The type of 'job' is derived as the least restrictive type of the values
        // ('CLERK', 'ANALYST'), namely VARCHAR(7). The derived type of 'deptno' is
        // the type of values (10, 20), namely INTEGER.
        final List<Pair<String, RelDataType>> axisNameTypes = new ArrayList<>();
        Ord.forEach(
                unpivot.axisList,
                (axis, i) -> {
                    final String axisName = ((SqlIdentifier) axis).getSimple();
                    final List<RelDataType> types = new ArrayList<>();
                    unpivot.forEachNameValues(
                            (aliasList, valueList) ->
                                    types.add(
                                            valueList == null
                                                    ? typeFactory.createSqlType(
                                                            SqlTypeName.VARCHAR,
                                                            SqlUnpivot.aliasValue(aliasList)
                                                                    .length())
                                                    : deriveType(scope, valueList.get(i))));
                    final RelDataType type = typeFactory.leastRestrictive(types);
                    if (type == null) {
                        throw newValidationError(
                                axis, RESOURCE.unpivotCannotDeriveAxisType(axisName));
                    }
                    setValidatedNodeType(axis, type);
                    if (!columnNames.add(axisName)) {
                        throw newValidationError(axis, RESOURCE.unpivotDuplicate(axisName));
                    }
                    axisNameTypes.add(Pair.of(axisName, type));
                });

        // Columns that have been seen as arguments to aggregates or as axes
        // do not appear in the output.
        final RelDataTypeFactory.Builder typeBuilder = typeFactory.builder();
        scope.getChild()
                .getRowType()
                .getFieldList()
                .forEach(
                        field -> {
                            if (unusedColumnNames.contains(field.getName())) {
                                typeBuilder.add(field);
                            }
                        });
        typeBuilder.addAll(axisNameTypes);
        typeBuilder.addAll(measureNameTypes);

        final RelDataType rowType = typeBuilder.build();
        ns.setType(rowType);
    }

    /**
     * Checks that all pattern variables within a function are the same, and canonizes expressions
     * such as {@code PREV(B.price)} to {@code LAST(B.price, 0)}.
     */
    private SqlNode navigationInDefine(SqlNode node, String alpha) {
        Set<String> prefix = node.accept(new PatternValidator(false));
        Util.discard(prefix);
        node = new NavigationExpander().go(node);
        node = new NavigationReplacer(alpha).go(node);
        return node;
    }

    @Override
    public void validateAggregateParams(
            SqlCall aggCall,
            @Nullable SqlNode filter,
            @Nullable SqlNodeList distinctList,
            @Nullable SqlNodeList orderList,
            SqlValidatorScope scope) {
        // For "agg(expr)", expr cannot itself contain aggregate function
        // invocations.  For example, "SUM(2 * MAX(x))" is illegal; when
        // we see it, we'll report the error for the SUM (not the MAX).
        // For more than one level of nesting, the error which results
        // depends on the traversal order for validation.
        //
        // For a windowed aggregate "agg(expr)", expr can contain an aggregate
        // function. For example,
        //   SELECT AVG(2 * MAX(x)) OVER (PARTITION BY y)
        //   FROM t
        //   GROUP BY y
        // is legal. Only one level of nesting is allowed since non-windowed
        // aggregates cannot nest aggregates.

        // Store nesting level of each aggregate. If an aggregate is found at an invalid
        // nesting level, throw an assert.
        final AggFinder a;
        if (inWindow) {
            a = overFinder;
        } else {
            a = aggOrOverFinder;
        }

        for (SqlNode param : aggCall.getOperandList()) {
            if (a.findAgg(param) != null) {
                throw newValidationError(aggCall, RESOURCE.nestedAggIllegal());
            }
        }
        if (filter != null) {
            if (a.findAgg(filter) != null) {
                throw newValidationError(filter, RESOURCE.aggregateInFilterIllegal());
            }
        }
        if (distinctList != null) {
            for (SqlNode param : distinctList) {
                if (a.findAgg(param) != null) {
                    throw newValidationError(aggCall, RESOURCE.aggregateInWithinDistinctIllegal());
                }
            }
        }
        if (orderList != null) {
            for (SqlNode param : orderList) {
                if (a.findAgg(param) != null) {
                    throw newValidationError(aggCall, RESOURCE.aggregateInWithinGroupIllegal());
                }
            }
        }

        final SqlAggFunction op = (SqlAggFunction) aggCall.getOperator();
        switch (op.requiresGroupOrder()) {
            case MANDATORY:
                if (orderList == null || orderList.size() == 0) {
                    throw newValidationError(
                            aggCall, RESOURCE.aggregateMissingWithinGroupClause(op.getName()));
                }
                break;
            case OPTIONAL:
                break;
            case IGNORED:
                // rewrite the order list to empty
                if (orderList != null) {
                    orderList.clear();
                }
                break;
            case FORBIDDEN:
                if (orderList != null && orderList.size() != 0) {
                    throw newValidationError(
                            aggCall, RESOURCE.withinGroupClauseIllegalInAggregate(op.getName()));
                }
                break;
            default:
                throw new AssertionError(op);
        }

        if (op.isPercentile()) {
            assert op.requiresGroupOrder() == Optionality.MANDATORY;
            assert orderList != null;

            // Validate that percentile function have a single ORDER BY expression
            if (orderList.size() != 1) {
                throw newValidationError(orderList, RESOURCE.orderByRequiresOneKey(op.getName()));
            }

            // Validate that the ORDER BY field is of NUMERIC type
            SqlNode node = orderList.get(0);
            assert node != null;

            final RelDataType type = deriveType(scope, node);
            final @Nullable SqlTypeFamily family = type.getSqlTypeName().getFamily();
            if (family == null || family.allowableDifferenceTypes().isEmpty()) {
                throw newValidationError(
                        orderList,
                        RESOURCE.unsupportedTypeInOrderBy(
                                type.getSqlTypeName().getName(), op.getName()));
            }
        }
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *
     */
    @Override
    public void validateCall(SqlCall call, SqlValidatorScope scope) {
        // 获取调用表达式中的操作符
        final SqlOperator operator = call.getOperator();
        // 检查特殊情况：无操作数、操作符语法为函数ID、且未展开，同时配置不允许无参函数使用括号
        if ((call.operandCount() == 0)
                && (operator.getSyntax() == SqlSyntax.FUNCTION_ID)
                && !call.isExpanded()
                && !this.config.conformance().allowNiladicParentheses()) {
            // For example, "LOCALTIME()" is illegal. (It should be
            // "LOCALTIME", which would have been handled as a
            // SqlIdentifier.)
            // 抛出异常，处理未解析的函数调用
            throw handleUnresolvedFunction(call, operator, ImmutableList.of(), null);
        }
        // 获取操作数的作用域
        SqlValidatorScope operandScope = scope.getOperandScope(call);


        // 检查 MATCH_RECOGNIZE 函数调用的作用域
        // 如果操作符是 SqlFunction 且其函数类型为 MATCH_RECOGNIZE，但操作数作用域不是 MatchRecognizeScope
        // 则抛出异常，因为 MATCH_RECOGNIZE 函数只能在特定的作用域内使用
        if (operator instanceof SqlFunction
                && ((SqlFunction) operator).getFunctionType() == SqlFunctionCategory.MATCH_RECOGNIZE
                && !(operandScope instanceof MatchRecognizeScope)) {
            throw newValidationError(
                    call, Static.RESOURCE.functionMatchRecognizeOnly(call.toString()));
        }
        // Delegate validation to the operator.
        // 将验证委托给操作符本身进行处理
        operator.validateCall(call, this, scope, operandScope);
    }

    /**
     * Validates that a particular feature is enabled. By default, all features are enabled;
     * subclasses may override this method to be more discriminating.
     *
     * @param feature feature being used, represented as a resource instance
     * @param context parser position context for error reporting, or null if
     */
    protected void validateFeature(Feature feature, SqlParserPos context) {
        // By default, do nothing except to verify that the resource
        // represents a real feature definition.
        assert feature.getProperties().get("FeatureDefinition") != null;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 展开SELECT表达式。
     *
     * @param expr 需要展开的原始SQL表达式节点。
     * @param scope 当前选择操作的作用域，用于解析和推导表达式。
     * @param select 当前的SQL选择语句，可能包含用于展开表达式的上下文信息。
     * @return 展开后的SQL表达式节点。
     */
    public SqlNode expandSelectExpr(SqlNode expr, SelectScope scope, SqlSelect select) {
        //
        /**
         * 创建一个SelectExpander实例，用于执行展开操作。
         * 它依赖于当前的SQL解析器实例（this）、作用域（scope）和选择语句（select）来执行展开
         *
         * Expander扩展器，校验解析等操作将任何标识符、表达式转换为规范形式 eg id=>t_user.id
         */
        final Expander expander = new SelectExpander(this, scope, select);
        // 调用expander的go方法，传入需要展开的原始表达式节点，得到展开后的新表达式节点。
        final SqlNode newExpr = expander.go(expr);

        // 如果展开后的节点与原始节点不同，
        // 则设置新节点的原始表达式节点为原始节点，这可能是为了保留一些元信息或进行后续的优化/验证。
        if (expr != newExpr) {
            setOriginal(newExpr, expr);
        }
        // 返回展开后的新表达式节点。
        return newExpr;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *
     */
    @Override
    public SqlNode expand(SqlNode expr, SqlValidatorScope scope) {
        // 创建一个Expander实例，用于扩展SQL表达式
        final Expander expander = new Expander(this, scope);
        // 调用Expander的go方法，传入待扩展的表达式，得到扩展后的表达式
        SqlNode newExpr = expander.go(expr);
        // 如果扩展后的表达式与原始表达式不同（即发生了扩展），则设置扩展后表达式的原始表达式为原始表达式
        // 这可能是为了保留表达式的来源信息，便于后续的错误追踪或优化
        if (expr != newExpr) {
            setOriginal(newExpr, expr);
        }
        // 返回扩展后的表达式
        return newExpr;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     *  展开 GROUP BY 或 HAVING 表达式。
     *  调用go方法进行校验表达式
     */
    public SqlNode expandGroupByOrHavingExpr(
            SqlNode expr, SqlValidatorScope scope, SqlSelect select, boolean havingExpression) {
        //构建Expander兑现
        final Expander expander = new ExtendedExpander(this, scope, select, expr, havingExpression);
        //调用go方法进行校验返回校验后的
        SqlNode newExpr = expander.go(expr);
        if (expr != newExpr) {
            //// 设置新表达式与原始表达式的关联
            setOriginal(newExpr, expr);
        }
        return newExpr;
    }

    @Override
    public boolean isSystemField(RelDataTypeField field) {
        return false;
    }

    @Override
    public List<@Nullable List<String>> getFieldOrigins(SqlNode sqlQuery) {
        if (sqlQuery instanceof SqlExplain) {
            return Collections.emptyList();
        }
        final RelDataType rowType = getValidatedNodeType(sqlQuery);
        final int fieldCount = rowType.getFieldCount();
        if (!sqlQuery.isA(SqlKind.QUERY)) {
            return Collections.nCopies(fieldCount, null);
        }
        final List<@Nullable List<String>> list = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            list.add(getFieldOrigin(sqlQuery, i));
        }
        return ImmutableNullableList.copyOf(list);
    }

    private @Nullable List<String> getFieldOrigin(SqlNode sqlQuery, int i) {
        if (sqlQuery instanceof SqlSelect) {
            SqlSelect sqlSelect = (SqlSelect) sqlQuery;
            final SelectScope scope = getRawSelectScopeNonNull(sqlSelect);
            final List<SqlNode> selectList =
                    requireNonNull(
                            scope.getExpandedSelectList(), () -> "expandedSelectList for " + scope);
            final SqlNode selectItem = stripAs(selectList.get(i));
            if (selectItem instanceof SqlIdentifier) {
                final SqlQualified qualified = scope.fullyQualify((SqlIdentifier) selectItem);
                SqlValidatorNamespace namespace =
                        requireNonNull(qualified.namespace, () -> "namespace for " + qualified);
                final SqlValidatorTable table = namespace.getTable();
                if (table == null) {
                    return null;
                }
                final List<String> origin = new ArrayList<>(table.getQualifiedName());
                for (String name : qualified.suffix()) {
                    namespace = namespace.lookupChild(name);
                    if (namespace == null) {
                        return null;
                    }
                    origin.add(name);
                }
                return origin;
            }
            return null;
        } else if (sqlQuery instanceof SqlOrderBy) {
            return getFieldOrigin(((SqlOrderBy) sqlQuery).query, i);
        } else {
            return null;
        }
    }

    @Override
    public RelDataType getParameterRowType(SqlNode sqlQuery) {
        // NOTE: We assume that bind variables occur in depth-first tree
        // traversal in the same order that they occurred in the SQL text.
        final List<RelDataType> types = new ArrayList<>();
        // NOTE: but parameters on fetch/offset would be counted twice
        // as they are counted in the SqlOrderBy call and the inner SqlSelect call
        final Set<SqlNode> alreadyVisited = new HashSet<>();
        sqlQuery.accept(
                new SqlShuttle() {

                    @Override
                    public SqlNode visit(SqlDynamicParam param) {
                        if (alreadyVisited.add(param)) {
                            RelDataType type = getValidatedNodeType(param);
                            types.add(type);
                        }
                        return param;
                    }
                });
        return typeFactory.createStructType(
                types,
                new AbstractList<String>() {
                    @Override
                    public String get(int index) {
                        return "?" + index;
                    }

                    @Override
                    public int size() {
                        return types.size();
                    }
                });
    }

    @Override
    public void validateColumnListParams(
            SqlFunction function, List<RelDataType> argTypes, List<SqlNode> operands) {
        throw new UnsupportedOperationException();
    }

    private static boolean isPhysicalNavigation(SqlKind kind) {
        return kind == SqlKind.PREV || kind == SqlKind.NEXT;
    }

    private static boolean isLogicalNavigation(SqlKind kind) {
        return kind == SqlKind.FIRST || kind == SqlKind.LAST;
    }

    private static boolean isAggregation(SqlKind kind) {
        return kind == SqlKind.SUM
                || kind == SqlKind.SUM0
                || kind == SqlKind.AVG
                || kind == SqlKind.COUNT
                || kind == SqlKind.MAX
                || kind == SqlKind.MIN;
    }

    private static boolean isRunningOrFinal(SqlKind kind) {
        return kind == SqlKind.RUNNING || kind == SqlKind.FINAL;
    }

    private static boolean isSingleVarRequired(SqlKind kind) {
        return isPhysicalNavigation(kind) || isLogicalNavigation(kind) || isAggregation(kind);
    }

    // ~ Inner Classes ----------------------------------------------------------

    /** Common base class for DML statement namespaces. */
    public static class DmlNamespace extends IdentifierNamespace {
        protected DmlNamespace(
                SqlValidatorImpl validator,
                SqlNode id,
                SqlNode enclosingNode,
                SqlValidatorScope parentScope) {
            super(validator, id, enclosingNode, parentScope);
        }
    }

    /** Namespace for an INSERT statement. */
    private static class InsertNamespace extends DmlNamespace {
        private final SqlInsert node;

        InsertNamespace(
                SqlValidatorImpl validator,
                SqlInsert node,
                SqlNode enclosingNode,
                SqlValidatorScope parentScope) {
            super(validator, node.getTargetTable(), enclosingNode, parentScope);
            this.node = requireNonNull(node, "node");
        }

        @Override
        public @Nullable SqlNode getNode() {
            return node;
        }
    }

    /** Namespace for an UPDATE statement. */
    private static class UpdateNamespace extends DmlNamespace {
        private final SqlUpdate node;

        UpdateNamespace(
                SqlValidatorImpl validator,
                SqlUpdate node,
                SqlNode enclosingNode,
                SqlValidatorScope parentScope) {
            super(validator, node.getTargetTable(), enclosingNode, parentScope);
            this.node = requireNonNull(node, "node");
        }

        @Override
        public @Nullable SqlNode getNode() {
            return node;
        }
    }

    /** Namespace for a DELETE statement. */
    private static class DeleteNamespace extends DmlNamespace {
        private final SqlDelete node;

        DeleteNamespace(
                SqlValidatorImpl validator,
                SqlDelete node,
                SqlNode enclosingNode,
                SqlValidatorScope parentScope) {
            super(validator, node.getTargetTable(), enclosingNode, parentScope);
            this.node = requireNonNull(node, "node");
        }

        @Override
        public @Nullable SqlNode getNode() {
            return node;
        }
    }

    /** Namespace for a MERGE statement. */
    private static class MergeNamespace extends DmlNamespace {
        private final SqlMerge node;

        MergeNamespace(
                SqlValidatorImpl validator,
                SqlMerge node,
                SqlNode enclosingNode,
                SqlValidatorScope parentScope) {
            super(validator, node.getTargetTable(), enclosingNode, parentScope);
            this.node = requireNonNull(node, "node");
        }

        @Override
        public @Nullable SqlNode getNode() {
            return node;
        }
    }

    /** Visitor that retrieves pattern variables defined. */
    private static class PatternVarVisitor implements SqlVisitor<Void> {
        private MatchRecognizeScope scope;

        PatternVarVisitor(MatchRecognizeScope scope) {
            this.scope = scope;
        }

        @Override
        public Void visit(SqlLiteral literal) {
            return null;
        }

        @Override
        public Void visit(SqlCall call) {
            for (int i = 0; i < call.getOperandList().size(); i++) {
                call.getOperandList().get(i).accept(this);
            }
            return null;
        }

        @Override
        public Void visit(SqlNodeList nodeList) {
            throw Util.needToImplement(nodeList);
        }

        @Override
        public Void visit(SqlIdentifier id) {
            Preconditions.checkArgument(id.isSimple());
            scope.addPatternVar(id.getSimple());
            return null;
        }

        @Override
        public Void visit(SqlDataTypeSpec type) {
            throw Util.needToImplement(type);
        }

        @Override
        public Void visit(SqlDynamicParam param) {
            throw Util.needToImplement(param);
        }

        @Override
        public Void visit(SqlIntervalQualifier intervalQualifier) {
            throw Util.needToImplement(intervalQualifier);
        }
    }

    /**
     * Visitor which derives the type of a given {@link SqlNode}.
     *
     * <p>Each method must return the derived type. This visitor is basically a single-use
     * dispatcher; the visit is never recursive.
     */
    private class DeriveTypeVisitor implements SqlVisitor<RelDataType> {
        private final SqlValidatorScope scope;

        DeriveTypeVisitor(SqlValidatorScope scope) {
            this.scope = scope;
        }

        @Override
        public RelDataType visit(SqlLiteral literal) {
            return literal.createSqlType(typeFactory);
        }

        @Override
        public RelDataType visit(SqlCall call) {
            final SqlOperator operator = call.getOperator();
            return operator.deriveType(SqlValidatorImpl.this, scope, call);
        }

        @Override
        public RelDataType visit(SqlNodeList nodeList) {
            // Operand is of a type that we can't derive a type for. If the
            // operand is of a peculiar type, such as a SqlNodeList, then you
            // should override the operator's validateCall() method so that it
            // doesn't try to validate that operand as an expression.
            throw Util.needToImplement(nodeList);
        }

        @Override
        public RelDataType visit(SqlIdentifier id) {
            // First check for builtin functions which don't have parentheses,
            // like "LOCALTIME".
            final SqlCall call = makeNullaryCall(id);
            if (call != null) {
                return call.getOperator().validateOperands(SqlValidatorImpl.this, scope, call);
            }

            RelDataType type = null;
            if (!(scope instanceof EmptyScope)) {
                id = scope.fullyQualify(id).identifier;
            }

            // Resolve the longest prefix of id that we can
            int i;
            for (i = id.names.size() - 1; i > 0; i--) {
                // REVIEW jvs 9-June-2005: The name resolution rules used
                // here are supposed to match SQL:2003 Part 2 Section 6.6
                // (identifier chain), but we don't currently have enough
                // information to get everything right.  In particular,
                // routine parameters are currently looked up via resolve;
                // we could do a better job if they were looked up via
                // resolveColumn.

                final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
                final SqlValidatorScope.ResolvedImpl resolved =
                        new SqlValidatorScope.ResolvedImpl();
                scope.resolve(id.names.subList(0, i), nameMatcher, false, resolved);
                if (resolved.count() == 1) {
                    // There's a namespace with the name we seek.
                    final SqlValidatorScope.Resolve resolve = resolved.only();
                    type = resolve.rowType();
                    for (SqlValidatorScope.Step p : Util.skip(resolve.path.steps())) {
                        type = type.getFieldList().get(p.i).getType();
                    }
                    break;
                }
            }

            // Give precedence to namespace found, unless there
            // are no more identifier components.
            if (type == null || id.names.size() == 1) {
                // See if there's a column with the name we seek in
                // precisely one of the namespaces in this scope.
                RelDataType colType = scope.resolveColumn(id.names.get(0), id);
                if (colType != null) {
                    type = colType;
                }
                ++i;
            }

            if (type == null) {
                final SqlIdentifier last = id.getComponent(i - 1, i);
                throw newValidationError(last, RESOURCE.unknownIdentifier(last.toString()));
            }

            // Resolve rest of identifier
            for (; i < id.names.size(); i++) {
                String name = id.names.get(i);
                final RelDataTypeField field;
                if (name.equals("")) {
                    // The wildcard "*" is represented as an empty name. It never
                    // resolves to a field.
                    name = "*";
                    field = null;
                } else {
                    final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
                    field = nameMatcher.field(type, name);
                }
                if (field == null) {
                    throw newValidationError(id.getComponent(i), RESOURCE.unknownField(name));
                }
                type = field.getType();
            }
            type = SqlTypeUtil.addCharsetAndCollation(type, getTypeFactory());
            return type;
        }

        @Override
        public RelDataType visit(SqlDataTypeSpec dataType) {
            // Q. How can a data type have a type?
            // A. When it appears in an expression. (Say as the 2nd arg to the
            //    CAST operator.)
            validateDataType(dataType);
            return dataType.deriveType(SqlValidatorImpl.this);
        }

        @Override
        public RelDataType visit(SqlDynamicParam param) {
            return unknownType;
        }

        @Override
        public RelDataType visit(SqlIntervalQualifier intervalQualifier) {
            return typeFactory.createSqlIntervalType(intervalQualifier);
        }
    }

    /** Converts an expression into canonical form by fully-qualifying any identifiers. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 通过完全限定任何标识符将表达式转换为规范形式
     */
    private static class Expander extends SqlScopedShuttle {
        protected final SqlValidatorImpl validator;

        Expander(SqlValidatorImpl validator, SqlValidatorScope scope) {
            super(scope);
            this.validator = validator;
        }

        public SqlNode go(SqlNode root) {
            return requireNonNull(root.accept(this), () -> this + " returned null for " + root);
        }


        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 访问者模式 处理SqlIdentifier
         */
        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            // First check for builtin functions which don't have
            // parentheses, like "LOCALTIME".
            // 首先检查是否为无参数的内置函数，如 "LOCALTIME"，这些函数没有括号。
            final SqlCall call = validator.makeNullaryCall(id);
            if (call != null) {
                // 如果确实是一个内置的无参数函数，则接受当前访问者（this）对该SqlCall的访问，并返回处理结果。
                return call.accept(this);
            }
            // 尝试将给定的SqlIdentifier通过作用域进行完全限定，获取其完全限定的标识符。
            final SqlIdentifier fqId = getScope().fullyQualify(id).identifier;
            // 对标识符进行可能的动态星号（*）扩展，这在某些上下文中（如SELECT语句）用于表示所有列。
            SqlNode expandedExpr = expandDynamicStar(id, fqId);
            // 设置原始节点
            validator.setOriginal(expandedExpr, id);
            // 返回扩展后的表达式节点。
            return expandedExpr;
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         *  访问并可能转换一个具有作用域（scoped）的SQL调用（SqlCall）。
         */
        @Override
        protected SqlNode visitScoped(SqlCall call) {
            // 首先检查调用的类型，对于某些特殊类型（如标量子查询、序列操作、WITH子句），直接返回原始调用
            switch (call.getKind()) {
                case SCALAR_QUERY:
                case CURRENT_VALUE:
                case NEXT_VALUE:
                case WITH:
                    return call;
                default:
                    break;
            }
            // Only visits arguments which are expressions. We don't want to
            // qualify non-expressions such as 'x' in 'empno * 5 AS x'.
            // 创建一个CallCopyingArgHandler，用于处理调用参数。
            // 第二个参数false表示不复制别名（即AS子句），因为我们只关心表达式参数。
            CallCopyingArgHandler argHandler = new CallCopyingArgHandler(call, false);
            // 调用当前访问器的acceptCall方法，传入call对象、一个标志（true表示在访问参数前访问操作符），以及argHandler
            // 这将允许我们遍历并可能转换call的参数
            call.getOperator().acceptCall(this, call, true, argHandler);
            // 获取处理后的结果。注意，如果参数没有变化，result可能仍然是原始call
            final SqlNode result = argHandler.result();
            //设置原始节点
            validator.setOriginal(result, call);
            //返回结果
            return result;
        }

        protected SqlNode expandDynamicStar(SqlIdentifier id, SqlIdentifier fqId) {
            if (DynamicRecordType.isDynamicStarColName(Util.last(fqId.names))
                    && !DynamicRecordType.isDynamicStarColName(Util.last(id.names))) {
                // Convert a column ref into ITEM(*, 'col_name')
                // for a dynamic star field in dynTable's rowType.
                return new SqlBasicCall(
                        SqlStdOperatorTable.ITEM,
                        ImmutableList.of(
                                fqId,
                                SqlLiteral.createCharString(
                                        Util.last(id.names), id.getParserPosition())),
                        id.getParserPosition());
            }
            return fqId;
        }
    }

    /**
     * Shuttle which walks over an expression in the ORDER BY clause, replacing usages of aliases
     * with the underlying expression.
     */
    class OrderExpressionExpander extends SqlScopedShuttle {
        private final List<String> aliasList;
        private final SqlSelect select;
        private final SqlNode root;

        OrderExpressionExpander(SqlSelect select, SqlNode root) {
            super(getOrderScope(select));
            this.select = select;
            this.root = root;
            this.aliasList = getNamespaceOrThrow(select).getRowType().getFieldNames();
        }

        public SqlNode go() {
            return requireNonNull(
                    root.accept(this), () -> "OrderExpressionExpander returned null for " + root);
        }

        @Override
        public @Nullable SqlNode visit(SqlLiteral literal) {
            // Ordinal markers, e.g. 'select a, b from t order by 2'.
            // Only recognize them if they are the whole expression,
            // and if the dialect permits.
            if (literal == root && config.conformance().isSortByOrdinal()) {
                switch (literal.getTypeName()) {
                    case DECIMAL:
                    case DOUBLE:
                        final int intValue = literal.intValue(false);
                        if (intValue >= 0) {
                            if (intValue < 1 || intValue > aliasList.size()) {
                                throw newValidationError(
                                        literal, RESOURCE.orderByOrdinalOutOfRange());
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            int ordinal = intValue - 1;
                            return nthSelectItem(ordinal, literal.getParserPosition());
                        }
                        break;
                    default:
                        break;
                }
            }

            return super.visit(literal);
        }

        /** Returns the <code>ordinal</code>th item in the select list. */
        private SqlNode nthSelectItem(int ordinal, final SqlParserPos pos) {
            // TODO: Don't expand the list every time. Maybe keep an expanded
            // version of each expression -- select lists and identifiers -- in
            // the validator.

            SqlNodeList expandedSelectList =
                    expandStar(SqlNonNullableAccessors.getSelectList(select), select, false);
            SqlNode expr = expandedSelectList.get(ordinal);
            expr = stripAs(expr);
            if (expr instanceof SqlIdentifier) {
                expr = getScope().fullyQualify((SqlIdentifier) expr).identifier;
            }

            // Create a copy of the expression with the position of the order
            // item.
            return expr.clone(pos);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            // Aliases, e.g. 'select a as x, b from t order by x'.
            if (id.isSimple() && config.conformance().isSortByAlias()) {
                String alias = id.getSimple();
                final SqlValidatorNamespace selectNs = getNamespaceOrThrow(select);
                final RelDataType rowType = selectNs.getRowTypeSansSystemColumns();
                final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
                RelDataTypeField field = nameMatcher.field(rowType, alias);
                if (field != null) {
                    return nthSelectItem(field.getIndex(), id.getParserPosition());
                }
            }

            // No match. Return identifier unchanged.
            return getScope().fullyQualify(id).identifier;
        }

        @Override
        protected @Nullable SqlNode visitScoped(SqlCall call) {
            // Don't attempt to expand sub-queries. We haven't implemented
            // these yet.
            if (call instanceof SqlSelect) {
                return call;
            }
            return super.visitScoped(call);
        }
    }

    /**
     * Converts an expression into canonical form by fully-qualifying any identifiers. For common
     * columns in USING, it will be converted to COALESCE(A.col, B.col) AS col.
     */
    static class SelectExpander extends Expander {
        final SqlSelect select;

        SelectExpander(SqlValidatorImpl validator, SelectScope scope, SqlSelect select) {
            super(validator, scope);
            this.select = select;
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 访问SQL标识符节点。
         *
         * 当遍历SQL解析树时，如果遇到SqlIdentifier节点（代表SQL中的标识符，如列名、表名等），
         * 则会调用此方法进行处理。
         *
         * @param id 当前正在访问的SqlIdentifier节点。
         */
        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            /**
             * 尝试对SqlIdentifier进行公共列名的展开。
             * 获取要展开的标识符、列明
             * eg:查询的列字段、where语句条件等
             */
            final SqlNode node =
                    expandCommonColumn(select, id, (SelectScope) getScope(), validator);
            // 如果展开后的节点与原始节点不同，说明进行了有效的展开，直接返回新节点。
            if (node != id) {
                return node;
            } else {
                // 则调用父类的visit(id)方法，以便进行进一步的处理或默认行为。
                return super.visit(id);
            }
        }
    }

    /**
     * Shuttle which walks over an expression in the GROUP BY/HAVING clause, replacing usages of
     * aliases or ordinals with the underlying expression.
     */
    static class ExtendedExpander extends Expander {
        final SqlSelect select;
        final SqlNode root;
        final boolean havingExpr;

        ExtendedExpander(
                SqlValidatorImpl validator,
                SqlValidatorScope scope,
                SqlSelect select,
                SqlNode root,
                boolean havingExpr) {
            super(validator, scope);
            this.select = select;
            this.root = root;
            this.havingExpr = havingExpr;
        }
        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 访问SqlIdentifier节点的方法，用于处理标识符（如列名或别名）。
         *
         * @param id 当前正在访问的SqlIdentifier节点
         * @return 处理后的SqlNode，或者如果没有特殊处理则返回原节点
         */
        @Override
        public @Nullable SqlNode visit(SqlIdentifier id) {
            // 如果标识符是简单的，并且根据上下文（HAVING或GROUP BY）配置了允许别名
            if (id.isSimple()
                    && (havingExpr
                            ? validator.config().conformance().isHavingAlias()// 如果是HAVING表达式，则检查配置是否允许HAVING中使用别名
                            : validator.config().conformance().isGroupByAlias())) {// 否则，检查配置是否允许GROUP BY中使用别名
                 // 获取简单的标识符名称
                String name = id.getSimple();
                // 初始化为null，用于存储找到的表达式
                SqlNode expr = null;
                final SqlNameMatcher nameMatcher = validator.catalogReader.nameMatcher();
                // 计数器，用于记录找到的匹配项数量
                int n = 0;
                // 遍历SELECT列表，查找匹配的别名
                for (SqlNode s : SqlNonNullableAccessors.getSelectList(select)) {
                    // 获取表达式的别名
                    final String alias = SqlValidatorUtil.getAlias(s, -1);
                    // 如果别名匹配
                    if (alias != null && nameMatcher.matches(alias, name)) {
                        expr = s;// 保存表达式
                        n++;// 匹配项数量加1
                    }
                }
                // 如果没有找到匹配的别名
                if (n == 0) {
                    // 调用父类的visit方法处理
                    return super.visit(id);
                    // 如果找到多个匹配的别名
                } else if (n > 1) {
                    // More than one column has this alias.
                    // 抛出错误，因为别名不唯一
                    throw validator.newValidationError(id, RESOURCE.columnAmbiguous(name));
                }
                // 如果是HAVING表达式且当前节点是聚合查询的根节点，则直接返回原节点
                if (havingExpr && validator.isAggregate(root)) {
                    return super.visit(id);
                }
                // 移除表达式中的AS关键字（如果存在）
                expr = stripAs(expr);
                // 如果处理后的表达式仍然是SqlIdentifier类型
                if (expr instanceof SqlIdentifier) {
                    SqlIdentifier sid = (SqlIdentifier) expr;
                    // 尝试将标识符完全限定，并可能扩展动态星号（*）
                    final SqlIdentifier fqId = getScope().fullyQualify(sid).identifier;
                    expr = expandDynamicStar(sid, fqId);
                }
                // 返回处理后的表达式
                return expr;
            }
            // 如果标识符是简单的，但不在HAVING或GROUP BY的别名检查中
            if (id.isSimple()) {
                // 获取SELECT的作用域
                final SelectScope scope = validator.getRawSelectScope(select);
                SqlNode node = expandCommonColumn(select, id, scope, validator);//获取表达式节点
                if (node != id) {
                    return node;
                }
            }
            // 如果没有特殊处理，则调用父类的visit方法
            return super.visit(id);
        }

        @Override
        public @Nullable SqlNode visit(SqlLiteral literal) {
            if (havingExpr || !validator.config().conformance().isGroupByOrdinal()) {
                return super.visit(literal);
            }
            boolean isOrdinalLiteral = literal == root;
            switch (root.getKind()) {
                case GROUPING_SETS:
                case ROLLUP:
                case CUBE:
                    if (root instanceof SqlBasicCall) {
                        List<SqlNode> operandList = ((SqlBasicCall) root).getOperandList();
                        for (SqlNode node : operandList) {
                            if (node.equals(literal)) {
                                isOrdinalLiteral = true;
                                break;
                            }
                        }
                    }
                    break;
                default:
                    break;
            }
            if (isOrdinalLiteral) {
                switch (literal.getTypeName()) {
                    case DECIMAL:
                    case DOUBLE:
                        final int intValue = literal.intValue(false);
                        if (intValue >= 0) {
                            if (intValue < 1
                                    || intValue
                                            > SqlNonNullableAccessors.getSelectList(select)
                                                    .size()) {
                                throw validator.newValidationError(
                                        literal, RESOURCE.orderByOrdinalOutOfRange());
                            }

                            // SQL ordinals are 1-based, but Sort's are 0-based
                            int ordinal = intValue - 1;
                            return SqlUtil.stripAs(
                                    SqlNonNullableAccessors.getSelectList(select).get(ordinal));
                        }
                        break;
                    default:
                        break;
                }
            }

            return super.visit(literal);
        }
    }

    /** Information about an identifier in a particular scope. */
    protected static class IdInfo {
        public final SqlValidatorScope scope;
        public final SqlIdentifier id;

        public IdInfo(SqlValidatorScope scope, SqlIdentifier id) {
            this.scope = scope;
            this.id = id;
        }
    }

    /** Utility object used to maintain information about the parameters in a function call. */
    protected static class FunctionParamInfo {
        /**
         * Maps a cursor (based on its position relative to other cursor parameters within a
         * function call) to the SELECT associated with the cursor.
         */
        public final Map<Integer, SqlSelect> cursorPosToSelectMap;

        /**
         * Maps a column list parameter to the parent cursor parameter it references. The parameters
         * are id'd by their names.
         */
        public final Map<String, String> columnListParamToParentCursorMap;

        public FunctionParamInfo() {
            cursorPosToSelectMap = new HashMap<>();
            columnListParamToParentCursorMap = new HashMap<>();
        }
    }

    /** Modify the nodes in navigation function such as FIRST, LAST, PREV AND NEXT. */
    private static class NavigationModifier extends SqlShuttle {
        public SqlNode go(SqlNode node) {
            return requireNonNull(
                    node.accept(this), () -> "NavigationModifier returned for " + node);
        }
    }

    /**
     * Shuttle that expands navigation expressions in a MATCH_RECOGNIZE clause.
     *
     * <p>Examples:
     *
     * <ul>
     *   <li>{@code PREV(A.price + A.amount)} &rarr; {@code PREV(A.price) + PREV(A.amount)}
     *   <li>{@code FIRST(A.price * 2)} &rarr; {@code FIRST(A.PRICE) * 2}
     * </ul>
     */
    private static class NavigationExpander extends NavigationModifier {
        final @Nullable SqlOperator op;
        final @Nullable SqlNode offset;

        NavigationExpander() {
            this(null, null);
        }

        NavigationExpander(@Nullable SqlOperator operator, @Nullable SqlNode offset) {
            this.offset = offset;
            this.op = operator;
        }

        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            SqlKind kind = call.getKind();
            List<SqlNode> operands = call.getOperandList();
            List<@Nullable SqlNode> newOperands = new ArrayList<>();

            if (call.getFunctionQuantifier() != null
                    && call.getFunctionQuantifier().getValue() == SqlSelectKeyword.DISTINCT) {
                final SqlParserPos pos = call.getParserPosition();
                throw SqlUtil.newContextException(
                        pos, Static.RESOURCE.functionQuantifierNotAllowed(call.toString()));
            }

            if (isLogicalNavigation(kind) || isPhysicalNavigation(kind)) {
                SqlNode inner = operands.get(0);
                SqlNode offset = operands.get(1);

                // merge two straight prev/next, update offset
                if (isPhysicalNavigation(kind)) {
                    SqlKind innerKind = inner.getKind();
                    if (isPhysicalNavigation(innerKind)) {
                        List<SqlNode> innerOperands = ((SqlCall) inner).getOperandList();
                        SqlNode innerOffset = innerOperands.get(1);
                        SqlOperator newOperator =
                                innerKind == kind
                                        ? SqlStdOperatorTable.PLUS
                                        : SqlStdOperatorTable.MINUS;
                        offset = newOperator.createCall(SqlParserPos.ZERO, offset, innerOffset);
                        inner =
                                call.getOperator()
                                        .createCall(
                                                SqlParserPos.ZERO, innerOperands.get(0), offset);
                    }
                }
                SqlNode newInnerNode =
                        inner.accept(new NavigationExpander(call.getOperator(), offset));
                if (op != null) {
                    newInnerNode = op.createCall(SqlParserPos.ZERO, newInnerNode, this.offset);
                }
                return newInnerNode;
            }

            if (operands.size() > 0) {
                for (SqlNode node : operands) {
                    if (node != null) {
                        SqlNode newNode = node.accept(new NavigationExpander());
                        if (op != null) {
                            newNode = op.createCall(SqlParserPos.ZERO, newNode, offset);
                        }
                        newOperands.add(newNode);
                    } else {
                        newOperands.add(null);
                    }
                }
                return call.getOperator().createCall(SqlParserPos.ZERO, newOperands);
            } else {
                if (op == null) {
                    return call;
                } else {
                    return op.createCall(SqlParserPos.ZERO, call, offset);
                }
            }
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (op == null) {
                return id;
            } else {
                return op.createCall(SqlParserPos.ZERO, id, offset);
            }
        }
    }

    /**
     * Shuttle that replaces {@code A as A.price > PREV(B.price)} with {@code PREV(A.price, 0) >
     * LAST(B.price, 0)}.
     *
     * <p>Replacing {@code A.price} with {@code PREV(A.price, 0)} makes the implementation of {@link
     * RexVisitor#visitPatternFieldRef(RexPatternFieldRef)} more unified. Otherwise, it's difficult
     * to implement this method. If it returns the specified field, then the navigation such as
     * {@code PREV(A.price, 1)} becomes impossible; if not, then comparisons such as {@code A.price
     * > PREV(A.price, 1)} become meaningless.
     */
    private static class NavigationReplacer extends NavigationModifier {
        private final String alpha;

        NavigationReplacer(String alpha) {
            this.alpha = alpha;
        }

        @Override
        public @Nullable SqlNode visit(SqlCall call) {
            SqlKind kind = call.getKind();
            if (isLogicalNavigation(kind) || isAggregation(kind) || isRunningOrFinal(kind)) {
                return call;
            }

            switch (kind) {
                case PREV:
                    final List<SqlNode> operands = call.getOperandList();
                    if (operands.get(0) instanceof SqlIdentifier) {
                        String name = ((SqlIdentifier) operands.get(0)).names.get(0);
                        return name.equals(alpha)
                                ? call
                                : SqlStdOperatorTable.LAST.createCall(SqlParserPos.ZERO, operands);
                    }
                    break;
                default:
                    break;
            }
            return super.visit(call);
        }

        @Override
        public SqlNode visit(SqlIdentifier id) {
            if (id.isSimple()) {
                return id;
            }
            SqlOperator operator =
                    id.names.get(0).equals(alpha)
                            ? SqlStdOperatorTable.PREV
                            : SqlStdOperatorTable.LAST;

            return operator.createCall(
                    SqlParserPos.ZERO, id, SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO));
        }
    }

    /** Validates that within one navigation function, the pattern var is the same. */
    private class PatternValidator extends SqlBasicVisitor<@Nullable Set<String>> {
        private final boolean isMeasure;
        int firstLastCount;
        int prevNextCount;
        int aggregateCount;

        PatternValidator(boolean isMeasure) {
            this(isMeasure, 0, 0, 0);
        }

        PatternValidator(
                boolean isMeasure, int firstLastCount, int prevNextCount, int aggregateCount) {
            this.isMeasure = isMeasure;
            this.firstLastCount = firstLastCount;
            this.prevNextCount = prevNextCount;
            this.aggregateCount = aggregateCount;
        }

        @Override
        public Set<String> visit(SqlCall call) {
            boolean isSingle = false;
            Set<String> vars = new HashSet<>();
            SqlKind kind = call.getKind();
            List<SqlNode> operands = call.getOperandList();

            if (isSingleVarRequired(kind)) {
                isSingle = true;
                if (isPhysicalNavigation(kind)) {
                    if (isMeasure) {
                        throw newValidationError(
                                call,
                                Static.RESOURCE.patternPrevFunctionInMeasure(call.toString()));
                    }
                    if (firstLastCount != 0) {
                        throw newValidationError(
                                call, Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
                    }
                    prevNextCount++;
                } else if (isLogicalNavigation(kind)) {
                    if (firstLastCount != 0) {
                        throw newValidationError(
                                call, Static.RESOURCE.patternPrevFunctionOrder(call.toString()));
                    }
                    firstLastCount++;
                } else if (isAggregation(kind)) {
                    // cannot apply aggregation in PREV/NEXT, FIRST/LAST
                    if (firstLastCount != 0 || prevNextCount != 0) {
                        throw newValidationError(
                                call,
                                Static.RESOURCE.patternAggregationInNavigation(call.toString()));
                    }
                    if (kind == SqlKind.COUNT && call.getOperandList().size() > 1) {
                        throw newValidationError(call, Static.RESOURCE.patternCountFunctionArg());
                    }
                    aggregateCount++;
                }
            }

            if (isRunningOrFinal(kind) && !isMeasure) {
                throw newValidationError(
                        call, Static.RESOURCE.patternRunningFunctionInDefine(call.toString()));
            }

            for (SqlNode node : operands) {
                if (node != null) {
                    vars.addAll(
                            requireNonNull(
                                    node.accept(
                                            new PatternValidator(
                                                    isMeasure,
                                                    firstLastCount,
                                                    prevNextCount,
                                                    aggregateCount)),
                                    () -> "node.accept(PatternValidator) for node " + node));
                }
            }

            if (isSingle) {
                switch (kind) {
                    case COUNT:
                        if (vars.size() > 1) {
                            throw newValidationError(
                                    call, Static.RESOURCE.patternCountFunctionArg());
                        }
                        break;
                    default:
                        if (operands.size() == 0
                                || !(operands.get(0) instanceof SqlCall)
                                || ((SqlCall) operands.get(0)).getOperator()
                                        != SqlStdOperatorTable.CLASSIFIER) {
                            if (vars.isEmpty()) {
                                throw newValidationError(
                                        call,
                                        Static.RESOURCE.patternFunctionNullCheck(call.toString()));
                            }
                            if (vars.size() != 1) {
                                throw newValidationError(
                                        call,
                                        Static.RESOURCE.patternFunctionVariableCheck(
                                                call.toString()));
                            }
                        }
                        break;
                }
            }
            return vars;
        }

        @Override
        public Set<String> visit(SqlIdentifier identifier) {
            boolean check = prevNextCount > 0 || firstLastCount > 0 || aggregateCount > 0;
            Set<String> vars = new HashSet<>();
            if (identifier.names.size() > 1 && check) {
                vars.add(identifier.names.get(0));
            }
            return vars;
        }

        @Override
        public Set<String> visit(SqlLiteral literal) {
            return ImmutableSet.of();
        }

        @Override
        public Set<String> visit(SqlIntervalQualifier qualifier) {
            return ImmutableSet.of();
        }

        @Override
        public Set<String> visit(SqlDataTypeSpec type) {
            return ImmutableSet.of();
        }

        @Override
        public Set<String> visit(SqlDynamicParam param) {
            return ImmutableSet.of();
        }
    }

    /** Permutation of fields in NATURAL JOIN or USING. */
    private class Permute {
        final List<ImmutableIntList> sources;
        final RelDataType rowType;
        final boolean trivial;

        Permute(SqlNode from, int offset) {
            switch (from.getKind()) {
                case JOIN:
                    final SqlJoin join = (SqlJoin) from;
                    final Permute left = new Permute(join.getLeft(), offset);
                    final int fieldCount =
                            getValidatedNodeType(join.getLeft()).getFieldList().size();
                    final Permute right = new Permute(join.getRight(), offset + fieldCount);
                    final List<String> names = usingNames(join);
                    final List<ImmutableIntList> sources = new ArrayList<>();
                    final Set<ImmutableIntList> sourceSet = new HashSet<>();
                    final RelDataTypeFactory.Builder b = typeFactory.builder();
                    if (names != null) {
                        for (String name : names) {
                            final RelDataTypeField f = left.field(name);
                            final ImmutableIntList source = left.sources.get(f.getIndex());
                            sourceSet.add(source);
                            final RelDataTypeField f2 = right.field(name);
                            final ImmutableIntList source2 = right.sources.get(f2.getIndex());
                            sourceSet.add(source2);
                            sources.add(source.appendAll(source2));
                            final boolean nullable =
                                    (f.getType().isNullable()
                                                    || join.getJoinType().generatesNullsOnLeft())
                                            && (f2.getType().isNullable()
                                                    || join.getJoinType().generatesNullsOnRight());
                            b.add(f).nullable(nullable);
                        }
                    }
                    for (RelDataTypeField f : left.rowType.getFieldList()) {
                        final ImmutableIntList source = left.sources.get(f.getIndex());
                        if (sourceSet.add(source)) {
                            sources.add(source);
                            b.add(f);
                        }
                    }
                    for (RelDataTypeField f : right.rowType.getFieldList()) {
                        final ImmutableIntList source = right.sources.get(f.getIndex());
                        if (sourceSet.add(source)) {
                            sources.add(source);
                            b.add(f);
                        }
                    }
                    rowType = b.build();
                    this.sources = ImmutableList.copyOf(sources);
                    this.trivial =
                            left.trivial && right.trivial && (names == null || names.isEmpty());
                    break;

                default:
                    rowType = getValidatedNodeType(from);
                    this.sources =
                            Functions.generate(
                                    rowType.getFieldCount(), i -> ImmutableIntList.of(offset + i));
                    this.trivial = true;
            }
        }

        private RelDataTypeField field(String name) {
            RelDataTypeField field = catalogReader.nameMatcher().field(rowType, name);
            assert field != null : "field " + name + " was not found in " + rowType;
            return field;
        }

        /** Moves fields according to the permutation. */
        public void permute(
                List<SqlNode> selectItems, List<Map.Entry<String, RelDataType>> fields) {
            if (trivial) {
                return;
            }

            final List<SqlNode> oldSelectItems = ImmutableList.copyOf(selectItems);
            selectItems.clear();
            final List<Map.Entry<String, RelDataType>> oldFields = ImmutableList.copyOf(fields);
            fields.clear();
            for (ImmutableIntList source : sources) {
                final int p0 = source.get(0);
                Map.Entry<String, RelDataType> field = oldFields.get(p0);
                final String name = field.getKey();
                RelDataType type = field.getValue();
                SqlNode selectItem = oldSelectItems.get(p0);
                for (int p1 : Util.skip(source)) {
                    final Map.Entry<String, RelDataType> field1 = oldFields.get(p1);
                    final SqlNode selectItem1 = oldSelectItems.get(p1);
                    final RelDataType type1 = field1.getValue();
                    // output is nullable only if both inputs are
                    final boolean nullable = type.isNullable() && type1.isNullable();
                    RelDataType currentType = type;
                    final RelDataType type2 =
                            requireNonNull(
                                    SqlTypeUtil.leastRestrictiveForComparison(
                                            typeFactory, type, type1),
                                    () ->
                                            "leastRestrictiveForComparison for types "
                                                    + currentType
                                                    + " and "
                                                    + type1);
                    selectItem =
                            SqlStdOperatorTable.AS.createCall(
                                    SqlParserPos.ZERO,
                                    SqlStdOperatorTable.COALESCE.createCall(
                                            SqlParserPos.ZERO,
                                            maybeCast(selectItem, type, type2),
                                            maybeCast(selectItem1, type1, type2)),
                                    new SqlIdentifier(name, SqlParserPos.ZERO));
                    type = typeFactory.createTypeWithNullability(type2, nullable);
                }
                fields.add(Pair.of(name, type));
                selectItems.add(selectItem);
            }
        }
    }

    // ~ Enums ------------------------------------------------------------------

    /** Validation status. */
    public enum Status {
        /** Validation has not started for this scope. */
        UNVALIDATED,

        /** Validation is in progress for this scope. */
        IN_PROGRESS,

        /** Validation has completed (perhaps unsuccessfully). */
        VALID
    }

    /** Allows {@link #clauseScopes} to have multiple values per SELECT. */
    private enum Clause {
        WHERE,
        GROUP_BY,
        SELECT,
        ORDER,
        CURSOR
    }
}
