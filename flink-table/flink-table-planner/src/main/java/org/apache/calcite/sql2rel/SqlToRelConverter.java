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
package org.apache.calcite.sql2rel;

import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.planner.calcite.FlinkOperatorBinding;
import org.apache.flink.table.planner.calcite.TimestampSchemaVersion;
import org.apache.flink.table.planner.hint.ClearQueryHintsWithInvalidPropagationShuttle;
import org.apache.flink.table.planner.hint.FlinkHints;
import org.apache.flink.table.planner.plan.FlinkCalciteCatalogSnapshotReader;
import org.apache.flink.table.planner.plan.utils.FlinkRelOptUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.calcite.avatica.util.Spaces;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.linq4j.tree.TableExpressionFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptSamplingParameters;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.ViewExpanders;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Collect;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.core.Sample;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.Hintable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalMatch;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.logical.LogicalTableModify;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rel.metadata.RelColumnMapping;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.stream.Delta;
import org.apache.calcite.rel.stream.LogicalDelta;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexPatternFieldRef;
import org.apache.calcite.rex.RexRangeRef;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.ColumnStrategy;
import org.apache.calcite.schema.ModifiableTable;
import org.apache.calcite.schema.ModifiableView;
import org.apache.calcite.schema.SchemaVersion;
import org.apache.calcite.schema.Schemas;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.Wrapper;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlMatchRecognize;
import org.apache.calcite.sql.SqlMerge;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSampleSpec;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlSetOperator;
import org.apache.calcite.sql.SqlSnapshot;
import org.apache.calcite.sql.SqlTableRef;
import org.apache.calcite.sql.SqlUnnestOperator;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.SqlUpdate;
import org.apache.calcite.sql.SqlUtil;
import org.apache.calcite.sql.SqlValuesOperator;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlInOperator;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlRowOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlReturnTypeInference;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.type.TableFunctionReturnTypeInference;
import org.apache.calcite.sql.util.SqlBasicVisitor;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.AggregatingSelectScope;
import org.apache.calcite.sql.validate.CollectNamespace;
import org.apache.calcite.sql.validate.DelegatingScope;
import org.apache.calcite.sql.validate.ListScope;
import org.apache.calcite.sql.validate.MatchRecognizeScope;
import org.apache.calcite.sql.validate.ParameterScope;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.calcite.sql.validate.SqlQualified;
import org.apache.calcite.sql.validate.SqlUserDefinedTableFunction;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorImpl;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.NumberUtil;
import org.apache.calcite.util.Pair;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;
import org.apache.calcite.util.trace.CalciteTrace;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.immutables.value.Value;
import org.slf4j.Logger;

import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.time.ZoneId;
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.linq4j.Nullness.castNonNull;
import static org.apache.calcite.sql.SqlUtil.stripAs;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Converts a SQL parse tree (consisting of {@link org.apache.calcite.sql.SqlNode} objects) into a
 * relational algebra expression (consisting of {@link org.apache.calcite.rel.RelNode} objects).
 *
 * <p>The public entry points are: {@link #convertQuery}, {@link #convertExpression(SqlNode)}.
 *
 * <p>FLINK modifications are at lines
 *
 * <ol>
 *   <li>Added in FLINK-29081, FLINK-28682, FLINK-33395: Lines 654 ~ 671
 *   <li>Added in Flink-24024: Lines 1435 ~ 1445, Lines 1459 ~ 1501
 *   <li>Added in FLINK-28682: Lines 2323 ~ 2340
 *   <li>Added in FLINK-28682: Lines 2377 ~ 2405
 *   <li>Added in FLINK-32474: Lines 2875 ~ 2887
 *   <li>Added in FLINK-32474: Lines 2987 ~ 3021
 *   <li>Added in FLINK-20873: Lines 5519 ~ 5528
 *   <li>Added in FLINK-34057, FLINK-34058: Lines 6090 ~ 6116
 * </ol>
 */
@SuppressWarnings("UnstableApiUsage")
@Value.Enclosing
public class SqlToRelConverter {
    // ~ Static fields/initializers ---------------------------------------------

    /** Default configuration. */
    public static final Config CONFIG =
            ImmutableSqlToRelConverter.Config.builder()
                    .withRelBuilderFactory(RelFactories.LOGICAL_BUILDER)
                    .withRelBuilderConfigTransform(c -> c.withPushJoinCondition(true))
                    .withHintStrategyTable(HintStrategyTable.EMPTY)
                    .build();

    protected static final Logger SQL2REL_LOGGER = CalciteTrace.getSqlToRelTracer();

    /** Size of the smallest IN list that will be converted to a semijoin to a static table. */
    public static final int DEFAULT_IN_SUB_QUERY_THRESHOLD = 20;

    @Deprecated // to be removed before 2.0
    public static final int DEFAULT_IN_SUBQUERY_THRESHOLD = DEFAULT_IN_SUB_QUERY_THRESHOLD;

    // ~ Instance fields --------------------------------------------------------

    public final @Nullable SqlValidator validator;
    protected final RexBuilder rexBuilder;
    protected final Prepare.CatalogReader catalogReader;
    protected final RelOptCluster cluster;
    private SubQueryConverter subQueryConverter;
    protected final Map<RelNode, Integer> leaves = new HashMap<>();
    private final List<@Nullable SqlDynamicParam> dynamicParamSqlNodes = new ArrayList<>();
    private final SqlOperatorTable opTab;
    protected final RelDataTypeFactory typeFactory;
    private final SqlNodeToRexConverter exprConverter;
    private final HintStrategyTable hintStrategies;
    private int explainParamCount;
    public final SqlToRelConverter.Config config;
    private final RelBuilder relBuilder;

    /** Fields used in name resolution for correlated sub-queries. */
    private final Map<CorrelationId, DeferredLookup> mapCorrelToDeferred = new HashMap<>();

    /**
     * Stack of names of datasets requested by the <code>
     * TABLE(SAMPLE(&lt;datasetName&gt;, &lt;query&gt;))</code> construct.
     */
    private final Deque<String> datasetStack = new ArrayDeque<>();

    /**
     * Mapping of non-correlated sub-queries that have been converted to their equivalent constants.
     * Used to avoid re-evaluating the sub-query if it's already been evaluated.
     */
    private final Map<SqlNode, RexNode> mapConvertedNonCorrSubqs = new HashMap<>();

    public final RelOptTable.ViewExpander viewExpander;

    // ~ Constructors -----------------------------------------------------------
    /**
     * Creates a converter.
     *
     * @param viewExpander Preparing statement
     * @param validator Validator
     * @param catalogReader Schema
     * @param planner Planner
     * @param rexBuilder Rex builder
     * @param convertletTable Expression converter
     */
    @Deprecated // to be removed before 2.0
    public SqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptPlanner planner,
            RexBuilder rexBuilder,
            SqlRexConvertletTable convertletTable) {
        this(
                viewExpander,
                validator,
                catalogReader,
                RelOptCluster.create(planner, rexBuilder),
                convertletTable,
                SqlToRelConverter.config());
    }

    @Deprecated // to be removed before 2.0
    public SqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable) {
        this(
                viewExpander,
                validator,
                catalogReader,
                cluster,
                convertletTable,
                SqlToRelConverter.config());
    }

    /* Creates a converter. */
    public SqlToRelConverter(
            RelOptTable.ViewExpander viewExpander,
            @Nullable SqlValidator validator,
            Prepare.CatalogReader catalogReader,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            Config config) {
        this.viewExpander = viewExpander;
        this.opTab =
                (validator == null) ? SqlStdOperatorTable.instance() : validator.getOperatorTable();
        this.validator = validator;
        this.catalogReader = catalogReader;
        this.subQueryConverter = new NoOpSubQueryConverter();
        this.rexBuilder = cluster.getRexBuilder();
        this.typeFactory = rexBuilder.getTypeFactory();
        this.exprConverter = new SqlNodeToRexConverterImpl(convertletTable);
        this.explainParamCount = 0;
        this.config = requireNonNull(config, "config");
        this.relBuilder =
                config.getRelBuilderFactory()
                        .create(cluster, null)
                        .transform(config.getRelBuilderConfigTransform());
        this.hintStrategies = config.getHintStrategyTable();

        cluster.setHintStrategies(this.hintStrategies);
        this.cluster = requireNonNull(cluster, "cluster");
    }

    // ~ Methods ----------------------------------------------------------------

    private SqlValidator validator() {
        return requireNonNull(validator, "validator");
    }

    private <T extends SqlValidatorNamespace> T getNamespace(SqlNode node) {
        //noinspection unchecked
        return (T)
                requireNonNull(
                        getNamespaceOrNull(node), () -> "Namespace is not found for " + node);
    }

    @SuppressWarnings("unchecked")
    private <T extends SqlValidatorNamespace> @Nullable T getNamespaceOrNull(SqlNode node) {
        return (@Nullable T) validator().getNamespace(node);
    }

    /** Returns the RelOptCluster in use. */
    public RelOptCluster getCluster() {
        return cluster;
    }

    /** Returns the row-expression builder. */
    public RexBuilder getRexBuilder() {
        return rexBuilder;
    }

    /**
     * Returns the number of dynamic parameters encountered during translation; this must only be
     * called after {@link #convertQuery}.
     *
     * @return number of dynamic parameters
     */
    public int getDynamicParamCount() {
        return dynamicParamSqlNodes.size();
    }

    /**
     * Returns the type inferred for a dynamic parameter.
     *
     * @param index 0-based index of dynamic parameter
     * @return inferred type, never null
     */
    public RelDataType getDynamicParamType(int index) {
        SqlNode sqlNode = dynamicParamSqlNodes.get(index);
        if (sqlNode == null) {
            throw Util.needToImplement("dynamic param type inference");
        }
        return validator().getValidatedNodeType(sqlNode);
    }

    /**
     * Returns the current count of the number of dynamic parameters in an EXPLAIN PLAN statement.
     *
     * @param increment if true, increment the count
     * @return the current count before the optional increment
     */
    public int getDynamicParamCountInExplain(boolean increment) {
        int retVal = explainParamCount;
        if (increment) {
            ++explainParamCount;
        }
        return retVal;
    }

    /**
     * Returns the mapping of non-correlated sub-queries that have been converted to the constants
     * that they evaluate to.
     */
    public Map<SqlNode, RexNode> getMapConvertedNonCorrSubqs() {
        return mapConvertedNonCorrSubqs;
    }

    /**
     * Adds to the current map of non-correlated converted sub-queries the elements from another map
     * that contains non-correlated sub-queries that have been converted by another
     * SqlToRelConverter.
     *
     * @param alreadyConvertedNonCorrSubqs the other map
     */
    public void addConvertedNonCorrSubqs(Map<SqlNode, RexNode> alreadyConvertedNonCorrSubqs) {
        mapConvertedNonCorrSubqs.putAll(alreadyConvertedNonCorrSubqs);
    }

    /**
     * Sets a new SubQueryConverter. To have any effect, this must be called before any convert
     * method.
     *
     * @param converter new SubQueryConverter
     */
    public void setSubQueryConverter(SubQueryConverter converter) {
        subQueryConverter = converter;
    }

    /**
     * Sets the number of dynamic parameters in the current EXPLAIN PLAN statement.
     *
     * @param explainParamCount number of dynamic parameters in the statement
     */
    public void setDynamicParamCountInExplain(int explainParamCount) {
        assert config.isExplain();
        this.explainParamCount = explainParamCount;
    }

    private void checkConvertedType(SqlNode query, RelNode result) {
        if (query.isA(SqlKind.DML)) {
            return;
        }
        // Verify that conversion from SQL to relational algebra did
        // not perturb any type information.  (We can't do this if the
        // SQL statement is something like an INSERT which has no
        // validator type information associated with its result,
        // hence the namespace check above.)
        final List<RelDataTypeField> validatedFields =
                validator().getValidatedNodeType(query).getFieldList();
        final RelDataType validatedRowType =
                validator()
                        .getTypeFactory()
                        .createStructType(
                                Pair.right(validatedFields),
                                SqlValidatorUtil.uniquify(
                                        Pair.left(validatedFields),
                                        catalogReader.nameMatcher().isCaseSensitive()));

        final List<RelDataTypeField> convertedFields =
                result.getRowType().getFieldList().subList(0, validatedFields.size());
        final RelDataType convertedRowType =
                validator().getTypeFactory().createStructType(convertedFields);

        if (!RelOptUtil.equal(
                "validated row type",
                validatedRowType,
                "converted row type",
                convertedRowType,
                Litmus.IGNORE)) {
            throw new AssertionError(
                    "Conversion to relational algebra failed to "
                            + "preserve datatypes:\n"
                            + "validated type:\n"
                            + validatedRowType.getFullTypeString()
                            + "\nconverted type:\n"
                            + convertedRowType.getFullTypeString()
                            + "\nrel:\n"
                            + RelOptUtil.toString(result));
        }
    }

    public RelNode flattenTypes(RelNode rootRel, boolean restructure) {
        RelStructuredTypeFlattener typeFlattener =
                new RelStructuredTypeFlattener(
                        relBuilder,
                        rexBuilder,
                        createToRelContext(ImmutableList.of()),
                        restructure);
        return typeFlattener.rewrite(rootRel);
    }

    /**
     * If sub-query is correlated and decorrelation is enabled, performs decorrelation.
     *
     * @param query Query
     * @param rootRel Root relational expression
     * @return New root relational expression after decorrelation
     */
    public RelNode decorrelate(SqlNode query, RelNode rootRel) {
        if (!config.isDecorrelationEnabled()) {
            return rootRel;
        }
        final RelNode result = decorrelateQuery(rootRel);
        if (result != rootRel) {
            checkConvertedType(query, result);
        }
        return result;
    }

    /**
     * Walks over a tree of relational expressions, replacing each {@link RelNode} with a 'slimmed
     * down' relational expression that projects only the fields required by its consumer.
     *
     * <p>This may make things easier for the optimizer, by removing crud that would expand the
     * search space, but is difficult for the optimizer itself to do it, because optimizer rules
     * must preserve the number and type of fields. Hence, this transform that operates on the
     * entire tree, similar to the {@link RelStructuredTypeFlattener type-flattening transform}.
     *
     * <p>Currently this functionality is disabled in farrago/luciddb; the default implementation of
     * this method does nothing.
     *
     * @param ordered Whether the relational expression must produce results in a particular order
     *     (typically because it has an ORDER BY at top level)
     * @param rootRel Relational expression that is at the root of the tree
     * @return Trimmed relational expression
     */
    public RelNode trimUnusedFields(boolean ordered, RelNode rootRel) {
        // Trim fields that are not used by their consumer.
        if (config.isTrimUnusedFields()) {
            final RelFieldTrimmer trimmer = newFieldTrimmer();
            final List<RelCollation> collations =
                    rootRel.getTraitSet().getTraits(RelCollationTraitDef.INSTANCE);
            rootRel = trimmer.trim(rootRel);
            if (!ordered
                    && collations != null
                    && !collations.isEmpty()
                    && !collations.equals(ImmutableList.of(RelCollations.EMPTY))) {
                final RelTraitSet traitSet =
                        rootRel.getTraitSet().replace(RelCollationTraitDef.INSTANCE, collations);
                rootRel = rootRel.copy(traitSet, rootRel.getInputs());
            }
            if (SQL2REL_LOGGER.isDebugEnabled()) {
                SQL2REL_LOGGER.debug(
                        RelOptUtil.dumpPlan(
                                "Plan after trimming unused fields",
                                rootRel,
                                SqlExplainFormat.TEXT,
                                SqlExplainLevel.EXPPLAN_ATTRIBUTES));
            }
        }
        return rootRel;
    }

    /**
     * Creates a RelFieldTrimmer.
     *
     * @return Field trimmer
     */
    protected RelFieldTrimmer newFieldTrimmer() {
        return new RelFieldTrimmer(validator, relBuilder);
    }

    /**
     * Converts an unvalidated query's parse tree into a relational expression.
     *
     * @param query Query to convert
     * @param needsValidation Whether to validate the query before converting; <code>false</code> if
     *     the query has already been validated.
     * @param top Whether the query is top-level, say if its result will become a JDBC result set;
     *     <code>false</code> if the query will be part of a view.
     */

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将未经验证的查询的解析树转换为关系表达式。
     *
     * @param query 要转换的查询
     */
    public RelRoot convertQuery(SqlNode query, final boolean needsValidation, final boolean top) {
        // 如果需要验证，则先对查询进行验证
        if (needsValidation) {
            query = validator().validate(query);
        }
        // 递归地转换查询为关系节点
        RelNode result = convertQueryRecursive(query, top, null).rel;
        // 如果查询是顶层的并且符合流处理条件，则将其包装为LogicalDelta
        if (top) {
            if (isStream(query)) {
                result = new LogicalDelta(cluster, result.getTraitSet(), result);
            }
        }
        // 如果没有指定排序，则使用空排序
        RelCollation collation = RelCollations.EMPTY;
        // 如果查询不是数据操纵语言（DML）类型的，并且是有序的，则计算所需的排序
        if (!query.isA(SqlKind.DML)) {
            if (isOrdered(query)) {
                collation = requiredCollation(result);
            }
        }
        // 检查转换后的类型是否与预期相符
        checkConvertedType(query, result);

        // 如果开启了调试日志，则打印转换后的计划
        if (SQL2REL_LOGGER.isDebugEnabled()) {
            SQL2REL_LOGGER.debug(
                    RelOptUtil.dumpPlan(
                            "Plan after converting SqlNode to RelNode",
                            result,
                            SqlExplainFormat.TEXT,
                            SqlExplainLevel.EXPPLAN_ATTRIBUTES));
        }
        // 获取经过验证的查询结果的行类型
        final RelDataType validatedRowType = validator().getValidatedNodeType(query);
        // 初始化提示列表
        List<RelHint> hints = new ArrayList<>();
        // 如果查询是SELECT类型且包含提示，则解析这些提示
        if (query.getKind() == SqlKind.SELECT) {
            final SqlSelect select = (SqlSelect) query;
            if (select.hasHints()) {
                hints = SqlUtil.getRelHint(hintStrategies, select.getHints());
            }
        }

        // 如果配置启用了添加JSON类型操作符的功能，则对结果应用NestedJsonFunctionRelRewriter
        if (config.isAddJsonTypeOperatorEnabled()) {
            result = result.accept(new NestedJsonFunctionRelRewriter());
        }

        // ----- FLINK MODIFICATION BEGIN -----
        // propagate the hints.
        // The method FlinkRelOptUtil#propagateRelHints not only finds and propagates hints
        // throughout the entire rel tree but also within subqueries.
        // 传播提示信息
        // FlinkRelOptUtil#propagateRelHints方法不仅在整个关系树中查找并传播提示信息，还会在子查询中传播
        result = FlinkRelOptUtil.propagateRelHints(result, false);

        // replace all query hints with upper case
        // 将所有查询提示转换为大写
        result = FlinkHints.capitalizeQueryHints(result);

        // clear query hints which are propagated into wrong query block
        // The hint QueryBlockAlias will be added when building a RelNode tree before. It is used to
        // distinguish the query block in the SQL.

        // 清除传播到错误查询块的查询提示
        // 在构建RelNode树之前，会添加QueryBlockAlias提示以区分SQL中的查询块。这里移除那些错误传播的提示
        result = result.accept(new ClearQueryHintsWithInvalidPropagationShuttle());

        // clear the hints on some nodes where these hints should not be attached
        // 清除一些节点上的提示，这些节点上不应该附加这些提示
        result = FlinkHints.clearQueryHintsOnUnmatchedNodes(result);

        // ----- FLINK MODIFICATION END -----
        // 返回转换后的RelRoot对象，包含处理后的RelNode、验证后的行类型、原始查询类型、排序信息和提示信息
        return RelRoot.of(result, validatedRowType, query.getKind())
                .withCollation(collation)
                .withHints(hints);
    }

    private static boolean isStream(SqlNode query) {
        return query instanceof SqlSelect
                && ((SqlSelect) query).isKeywordPresent(SqlSelectKeyword.STREAM);
    }

    public static boolean isOrdered(SqlNode query) {
        switch (query.getKind()) {
            case SELECT:
                SqlNodeList orderList = ((SqlSelect) query).getOrderList();
                return orderList != null && orderList.size() > 0;
            case WITH:
                return isOrdered(((SqlWith) query).body);
            case ORDER_BY:
                return ((SqlOrderBy) query).orderList.size() > 0;
            default:
                return false;
        }
    }

    private static RelCollation requiredCollation(RelNode r) {
        if (r instanceof Sort) {
            return ((Sort) r).collation;
        }
        if (r instanceof Project) {
            return requiredCollation(((Project) r).getInput());
        }
        if (r instanceof Delta) {
            return requiredCollation(((Delta) r).getInput());
        }
        throw new AssertionError();
    }

    /** Converts a SELECT statement's parse tree into a relational expression. */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将SqlSelect转换为RelNode（逻辑计划节点）
     */
    public RelNode convertSelect(SqlSelect select, boolean top) {
        // 通过验证器获取SqlSelect的WHERE子句的作用域
        final SqlValidatorScope selectScope = validator().getWhereScope(select);
        // 创建一个Blackboard，用于存储转换过程中的中间结果和状态
        final Blackboard bb = createBlackboard(selectScope, null, top);
        // 调用实现转换逻辑的私有方法，将SqlSelect转换为逻辑计划，并将结果存储在Blackboard中
        convertSelectImpl(bb, select);
        // 将Blackboard的根节点（即转换后的逻辑计划）强制转换为非空的RelNode并返回
        return castNonNull(bb.root);
    }

    /** Factory method for creating translation workspace. */
    protected Blackboard createBlackboard(
            @Nullable SqlValidatorScope scope,
            @Nullable Map<String, RexNode> nameToNodeMap,
            boolean top) {
        return new Blackboard(scope, nameToNodeMap, top);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换Select查询语句
     */
    /** Implementation of {@link #convertSelect(SqlSelect, boolean)}; derived class may override. */
    protected void convertSelectImpl(final Blackboard bb, SqlSelect select) {
        /**
         * 转换 FROM 子句
         * 将 SQL 查询中的 FROM 子句转换为内部表
         */
        convertFrom(bb, select.getFrom());

        // We would like to remove ORDER BY clause from an expanded view, except if
        // it is top-level or affects semantics.
        //
        // Top-level example. Given the view definition
        //   CREATE VIEW v AS SELECT * FROM t ORDER BY x
        // we would retain the view's ORDER BY in
        //   SELECT * FROM v
        // or
        //   SELECT * FROM v WHERE y = 5
        // but remove the view's ORDER BY in
        //   SELECT * FROM v ORDER BY z
        // and
        //   SELECT deptno, COUNT(*) FROM v GROUP BY deptno
        // because the ORDER BY and GROUP BY mean that the view is not 'top level' in
        // the query.
        //
        // Semantics example. Given the view definition
        //   CREATE VIEW v2 AS SELECT * FROM t ORDER BY x LIMIT 10
        // we would never remove the ORDER BY, because "ORDER BY ... LIMIT" is about
        // semantics. It is not a 'pure order'.
        if (RelOptUtil.isPureOrder(castNonNull(bb.root)) && config.isRemoveSortInSubQuery()) {
            // Remove the Sort if the view is at the top level. Also remove the Sort
            // if there are other nodes, which will cause the view to be in the
            // sub-query.
            if (!bb.top
                    || validator().isAggregate(select)
                    || select.isDistinct()
                    || select.hasOrderBy()
                    || select.getFetch() != null
                    || select.getOffset() != null) {
                bb.setRoot(castNonNull(bb.root).getInput(0), true);
            }
        }
        /**
         * 转换 WHERE 子句
         * 将 SQL 查询中的 WHERE 子句转换为内部表示
         */
        convertWhere(bb, select.getWhere());
        // 初始化两个列表，用于存储ORDER BY子句的表达式和排序规则
        final List<SqlNode> orderExprList = new ArrayList<>();
        final List<RelFieldCollation> collationList = new ArrayList<>();
        // 收集ORDER BY子句中的表达式和排序规则
        gatherOrderExprs(bb, select, select.getOrderList(), orderExprList, collationList);
        // 根据收集到的排序规则列表，创建一个RelCollation对象，并进行规范化
        final RelCollation collation = cluster.traitSet().canonize(RelCollations.of(collationList));
        // 判断SELECT语句是否包含聚合函数
        if (validator().isAggregate(select)) {
            // 如果包含，则对聚合函数进行转换
            convertAgg(bb, select, orderExprList);
        } else {
            // 否则，对SELECT列表项进行转换
            convertSelectList(bb, select, orderExprList);
        }

        // 如果SELECT语句指定了DISTINCT，则应用DISTINCT转换
        if (select.isDistinct()) {
            distinctify(bb, true);
        }
        // 对ORDER BY子句进行转换，包括处理OFFSET和FETCH子句
        convertOrder(select, bb, collation, orderExprList, select.getOffset(), select.getFetch());
        // 如果SELECT语句包含了查询提示（Hints）
        if (select.hasHints()) {
            // 获取这些查询提示对应的RelHint列表
            final List<RelHint> hints = SqlUtil.getRelHint(hintStrategies, select.getHints());
            // Attach the hints to the first Hintable node we found from the root node.
            // 遍历逻辑计划的根节点，找到第一个实现Hintable接口的节点，并将查询提示附加到这个节点上
            bb.setRoot(
                    bb.root()
                            .accept(
                                    new RelShuttleImpl() {
                                        boolean attached = false;

                                        @Override
                                        public RelNode visitChild(
                                                RelNode parent, int i, RelNode child) {
                                            // 如果当前父节点实现了Hintable接口且尚未附加提示
                                            if (parent instanceof Hintable && !attached) {
                                                attached = true;
                                                return ((Hintable) parent).attachHints(hints);
                                            } else {
                                                return super.visitChild(parent, i, child);
                                            }
                                        }
                                    }),
                    true);
        } else {
            // 如果没有查询提示，则直接设置根节点（可能只是为了保持API调用的一致性）
            bb.setRoot(bb.root(), true);
        }
    }

    /**
     * Having translated 'SELECT ... FROM ... [GROUP BY ...] [HAVING ...]', adds a relational
     * expression to make the results unique.
     *
     * <p>If the SELECT clause contains duplicate expressions, adds {@link
     * org.apache.calcite.rel.logical.LogicalProject}s so that we are grouping on the minimal set of
     * keys. The performance gain isn't huge, but it is difficult to detect these duplicate
     * expressions later.
     *
     * @param bb Blackboard
     * @param checkForDupExprs Check for duplicate expressions
     */
    private void distinctify(Blackboard bb, boolean checkForDupExprs) {
        // Look for duplicate expressions in the project.
        // Say we have 'select x, y, x, z'.
        // Then dups will be {[2, 0]}
        // and oldToNew will be {[0, 0], [1, 1], [2, 0], [3, 2]}
        RelNode rel = bb.root;
        if (checkForDupExprs && (rel instanceof LogicalProject)) {
            LogicalProject project = (LogicalProject) rel;
            final List<RexNode> projectExprs = project.getProjects();
            final List<Integer> origins = new ArrayList<>();
            int dupCount = 0;
            for (int i = 0; i < projectExprs.size(); i++) {
                int x = projectExprs.indexOf(projectExprs.get(i));
                if (x >= 0 && x < i) {
                    origins.add(x);
                    ++dupCount;
                } else {
                    origins.add(i);
                }
            }
            if (dupCount == 0) {
                distinctify(bb, false);
                return;
            }

            final Map<Integer, Integer> squished = new HashMap<>();
            final List<RelDataTypeField> fields = rel.getRowType().getFieldList();
            final List<Pair<RexNode, String>> newProjects = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                if (origins.get(i) == i) {
                    squished.put(i, newProjects.size());
                    newProjects.add(RexInputRef.of2(i, fields));
                }
            }
            rel =
                    LogicalProject.create(
                            rel,
                            ImmutableList.of(),
                            Pair.left(newProjects),
                            Pair.right(newProjects));
            bb.root = rel;
            distinctify(bb, false);
            rel = bb.root();

            // Create the expressions to reverse the mapping.
            // Project($0, $1, $0, $2).
            final List<Pair<RexNode, String>> undoProjects = new ArrayList<>();
            for (int i = 0; i < fields.size(); i++) {
                final int origin = origins.get(i);
                RelDataTypeField field = fields.get(i);
                undoProjects.add(
                        Pair.of(
                                new RexInputRef(castNonNull(squished.get(origin)), field.getType()),
                                field.getName()));
            }

            rel =
                    LogicalProject.create(
                            rel,
                            ImmutableList.of(),
                            Pair.left(undoProjects),
                            Pair.right(undoProjects));
            bb.setRoot(rel, false);

            return;
        }

        assert rel != null : "rel must not be null, root = " + bb.root;
        // Usual case: all of the expressions in the SELECT clause are
        // different.
        final ImmutableBitSet groupSet = ImmutableBitSet.range(rel.getRowType().getFieldCount());
        rel = createAggregate(bb, groupSet, ImmutableList.of(groupSet), ImmutableList.of());

        bb.setRoot(rel, false);
    }

    /**
     * Converts a query's ORDER BY clause, if any.
     *
     * <p>Ignores the ORDER BY clause if the query is not top-level and FETCH or OFFSET are not
     * present.
     *
     * @param select Query
     * @param bb Blackboard
     * @param collation Collation list
     * @param orderExprList Method populates this list with orderBy expressions not present in
     *     selectList
     * @param offset Expression for number of rows to discard before returning first row
     * @param fetch Expression for number of rows to fetch
     */
    protected void convertOrder(
            SqlSelect select,
            Blackboard bb,
            RelCollation collation,
            List<SqlNode> orderExprList,
            @Nullable SqlNode offset,
            @Nullable SqlNode fetch) {
        if (removeSortInSubQuery(bb.top)
                || select.getOrderList() == null
                || select.getOrderList().isEmpty()) {
            assert removeSortInSubQuery(bb.top) || collation.getFieldCollations().isEmpty();
            if ((offset == null
                            || (offset instanceof SqlLiteral
                                    && Objects.equals(
                                            ((SqlLiteral) offset).bigDecimalValue(),
                                            BigDecimal.ZERO)))
                    && fetch == null) {
                return;
            }
        }

        // Create a sorter using the previously constructed collations.
        bb.setRoot(
                LogicalSort.create(
                        bb.root(),
                        collation,
                        offset == null ? null : convertExpression(offset),
                        fetch == null ? null : convertExpression(fetch)),
                false);

        // If extra expressions were added to the project list for sorting,
        // add another project to remove them. But make the collation empty, because
        // we can't represent the real collation.
        //
        // If it is the top node, use the real collation, but don't trim fields.
        if (orderExprList.size() > 0 && !bb.top) {
            final List<RexNode> exprs = new ArrayList<>();
            final RelDataType rowType = bb.root().getRowType();
            final int fieldCount = rowType.getFieldCount() - orderExprList.size();
            for (int i = 0; i < fieldCount; i++) {
                exprs.add(rexBuilder.makeInputRef(bb.root(), i));
            }
            bb.setRoot(
                    LogicalProject.create(
                            bb.root(),
                            ImmutableList.of(),
                            exprs,
                            rowType.getFieldNames().subList(0, fieldCount)),
                    false);
        }
    }

    /**
     * Returns whether we should remove the sort for the subsequent query conversion.
     *
     * @param top Whether the rel to convert is the root of the query
     */
    private boolean removeSortInSubQuery(boolean top) {
        return config.isRemoveSortInSubQuery() && !top;
    }

    /**
     * Returns whether a given node contains a {@link SqlInOperator}.
     *
     * @param node a RexNode tree
     */
    private static boolean containsInOperator(SqlNode node) {
        try {
            SqlVisitor<Void> visitor =
                    new SqlBasicVisitor<Void>() {
                        @Override
                        public Void visit(SqlCall call) {
                            if (call.getOperator() instanceof SqlInOperator) {
                                throw new Util.FoundOne(call);
                            }
                            return super.visit(call);
                        }
                    };
            node.accept(visitor);
            return false;
        } catch (Util.FoundOne e) {
            Util.swallow(e, null);
            return true;
        }
    }

    /**
     * Push down all the NOT logical operators into any IN/NOT IN operators.
     *
     * @param scope Scope where {@code sqlNode} occurs
     * @param sqlNode the root node from which to look for NOT operators
     * @return the transformed SqlNode representation with NOT pushed down.
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将所有的NOT逻辑操作符向下推动到任何IN/NOT IN操作符中。
     * @param scope SQL节点发生作用的作用域
     * @param sqlNode 从中查找NOT操作符的根节点
     * @return 转换后的SqlNode表示，其中NOT操作符已被向下推动。
     */
    private static SqlNode pushDownNotForIn(SqlValidatorScope scope, SqlNode sqlNode) {
        // 如果sqlNode不是SqlCall类型或不含IN操作符，则直接返回
        if (!(sqlNode instanceof SqlCall) || !containsInOperator(sqlNode)) {
            return sqlNode;
        }
        final SqlCall sqlCall = (SqlCall) sqlNode;
        // 根据SqlCall的类型进行不同的处理
        switch (sqlCall.getKind()) {
            case AND:// 如果是AND操作符
            case OR:// 如果是OR操作符
                final List<SqlNode> operands = new ArrayList<>();
                // 遍历所有操作数，并递归地对每个操作数调用pushDownNotForIn
                for (SqlNode operand : sqlCall.getOperandList()) {
                    operands.add(pushDownNotForIn(scope, operand));
                }
                // 创建新的SqlCall节点并注册
                final SqlCall newCall =
                        sqlCall.getOperator().createCall(sqlCall.getParserPosition(), operands);
                return reg(scope, newCall);

            case NOT: // 如果是NOT操作符
                assert sqlCall.operand(0) instanceof SqlCall;// 断言NOT的第一个操作数是SqlCall类型
                final SqlCall call = sqlCall.operand(0);// 获取NOT的第一个操作数
                switch (sqlCall.operand(0).getKind()) {
                    case CASE:// 如果是CASE表达式
                        final SqlCase caseNode = (SqlCase) call;
                        // 创建一个新的SqlNodeList来存储THEN部分的结果
                        final SqlNodeList thenOperands = new SqlNodeList(SqlParserPos.ZERO);
                        // 遍历CASE表达式的THEN部分，对每个THEN操作数应用NOT，并递归调用pushDownNotForIn
                        for (SqlNode thenOperand : caseNode.getThenOperands()) {
                            final SqlCall not =
                                    SqlStdOperatorTable.NOT.createCall(
                                            SqlParserPos.ZERO, thenOperand);// 创建NOT操作符的SqlCall
                            // 递归调用pushDownNotForIn并添加到新的THEN列表中
                            thenOperands.add(pushDownNotForIn(scope, reg(scope, not)));
                        }
                        // 处理ELSE部分
                        SqlNode elseOperand =
                                requireNonNull(
                                        caseNode.getElseOperand(),
                                        "getElseOperand for " + caseNode);
                        // 如果ELSE操作数不为NULL
                        if (!SqlUtil.isNull(elseOperand)) {
                            // "not(unknown)" is "unknown", so no need to simplify
                            // "not(unknown)"的结果是"unknown"，因此不需要简化
                            final SqlCall not =
                                    SqlStdOperatorTable.NOT.createCall(
                                            SqlParserPos.ZERO, elseOperand);
                            // 调用pushDownNotForIn方法处理并注册NOT表达式
                            elseOperand = pushDownNotForIn(scope, reg(scope, not));
                        }
                        // 创建一个新的CASE表达式，使用原始的条件（WHEN部分）和修改后的THEN及ELSE部分
                        return reg(
                                scope,
                                SqlStdOperatorTable.CASE.createCall(
                                        SqlParserPos.ZERO,
                                        caseNode.getValueOperand(),
                                        caseNode.getWhenOperands(),
                                        thenOperands,
                                        elseOperand));

                    case AND:// 处理AND操作符
                        // 创建一个列表来存储对AND的每个操作数应用NOT操作符后的结果
                        final List<SqlNode> orOperands = new ArrayList<>();
                        for (SqlNode operand : call.getOperandList()) {
                            // 对每个操作数应用NOT操作符，并将结果添加到orOperands列表中
                            orOperands.add(
                                    pushDownNotForIn(
                                            scope,
                                            reg(
                                                    scope,
                                                    SqlStdOperatorTable.NOT.createCall(
                                                            SqlParserPos.ZERO, operand))));
                        }
                        // 使用OR操作符和转换后的操作数列表创建一个新的表达式，并返回其注册或处理后的结果
                        return reg(
                                scope,
                                SqlStdOperatorTable.OR.createCall(SqlParserPos.ZERO, orOperands));
                    // 处理OR操作符
                    case OR:
                        // 创建一个列表来存储对OR的每个操作数应用NOT操作符后的结果
                        final List<SqlNode> andOperands = new ArrayList<>();
                        for (SqlNode operand : call.getOperandList()) {
                            // 对每个操作数应用NOT操作符，并将结果添加到andOperands列表中
                            andOperands.add(
                                    pushDownNotForIn(
                                            scope,
                                            reg(
                                                    scope,
                                                    SqlStdOperatorTable.NOT.createCall(
                                                            SqlParserPos.ZERO, operand))));
                        }
                        // 使用AND操作符和转换后的操作数列表创建一个新的表达式，并返回其注册或处理后的结果
                        return reg(
                                scope,
                                SqlStdOperatorTable.AND.createCall(SqlParserPos.ZERO, andOperands));

                    case NOT:// 处理NOT操作符
                        // 断言NOT操作符只有一个操作数
                        assert call.operandCount() == 1;
                        // 直接对NOT操作符的操作数进行处理（可能是转换、优化或注册）
                        // 并返回处理后的结果
                        return pushDownNotForIn(scope, call.operand(0));

                    case NOT_IN://处理Not IN
                        return reg(
                                scope,
                                SqlStdOperatorTable.IN.createCall(
                                        SqlParserPos.ZERO, call.getOperandList()));

                    case IN://处理IN
                        return reg(
                                scope,
                                SqlStdOperatorTable.NOT_IN.createCall(
                                        SqlParserPos.ZERO, call.getOperandList()));
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        return sqlNode;
    }

    /**
     * Registers with the validator a {@link SqlNode} that has been created during the Sql-to-Rel
     * process.
     */
    private static SqlNode reg(SqlValidatorScope scope, SqlNode e) {
        scope.getValidator().deriveType(scope, e);
        return e;
    }

    /**
     * Converts a WHERE clause.
     *
     * @param bb Blackboard
     * @param where WHERE clause, may be null
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换WHERE子句。
     *
     * @param bb Blackboard对象，用于存储和转换中间结果
     * @param where WHERE子句，可能为null
     */
    private void convertWhere(final Blackboard bb, final @Nullable SqlNode where) {
        // 如果WHERE子句为空，则直接返回
        if (where == null) {
            return;
        }
        // 将NOT操作向下推送到IN操作中，优化查询
        SqlNode newWhere = pushDownNotForIn(bb.scope(), where);
        // 替换子查询，如果子查询的逻辑在上下文中未知，则视为FALSE
        replaceSubQueries(bb, newWhere, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
        // 将SqlNode转换为RexNode
        final RexNode convertedWhere = bb.convertExpression(newWhere);
        // 移除RexNode中的空值检查转换，因为它们在查询执行中可能不是必需的
        final RexNode convertedWhere2 = RexUtil.removeNullabilityCast(typeFactory, convertedWhere);

        // only allocate filter if the condition is not TRUE
        // 如果转换后的WHERE条件总是为真，则不需要分配过滤器
        if (convertedWhere2.isAlwaysTrue()) {
            return;
        }
        // 使用默认的过滤器工厂创建过滤器节点
        final RelFactories.FilterFactory filterFactory = RelFactories.DEFAULT_FILTER_FACTORY;
        final RelNode filter =
                filterFactory.createFilter(bb.root(), convertedWhere2, ImmutableSet.of());
        // 检查是否需要处理相关性
        final RelNode r;
        final CorrelationUse p = getCorrelationUse(bb, filter);
        if (p != null) {
            // 如果存在相关性，并且相关节点是Filter类型，则创建一个新的Filter节点来处理这种相关性
            assert p.r instanceof Filter;
            Filter f = (Filter) p.r;
            r = LogicalFilter.create(f.getInput(), f.getCondition(), ImmutableSet.of(p.id));
        } else {
            // 如果没有相关性，直接使用创建的filter节点
            r = filter;
        }
        // 更新Blackboard中的根节点为转换后的查询节点
        bb.setRoot(r, false);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 替换子查询的方法
     *
     * @param bb         Blackboard ，用于存储和访问中间结果，包括子查询列表
     * @param expr       SqlNode对象，表示待处理的SQL表达式
     */
    private void replaceSubQueries(
            final Blackboard bb, final SqlNode expr, RelOptUtil.Logic logic) {
        // 在SQL表达式中查找子查询，并将它们添加到Blackboard对象的子查询列表中
        // 此处不直接替换子查询，只是寻找并记录它们
        findSubQueries(bb, expr, logic, false);
        // 遍历Blackboard对象中的子查询列表
        for (SubQuery node : bb.subQueryList) {
            // 对每个子查询进行替换操作
            // 具体的替换逻辑可能包括将子查询重写为更高效的查询形式、将子查询内联到外层查询中等
            substituteSubQuery(bb, node);
        }
    }

    private void substituteSubQuery(Blackboard bb, SubQuery subQuery) {
        final RexNode expr = subQuery.expr;
        if (expr != null) {
            // Already done.
            return;
        }

        final SqlBasicCall call;
        final RelNode rel;
        final SqlNode query;
        final RelOptUtil.Exists converted;
        switch (subQuery.node.getKind()) {
            case CURSOR:
                convertCursor(bb, subQuery);
                return;

            case ARRAY_QUERY_CONSTRUCTOR:
            case MAP_QUERY_CONSTRUCTOR:
            case MULTISET_QUERY_CONSTRUCTOR:
                if (!config.isExpand()) {
                    return;
                }
                // fall through
            case MULTISET_VALUE_CONSTRUCTOR:
                rel = convertMultisets(ImmutableList.of(subQuery.node), bb);
                subQuery.expr = bb.register(rel, JoinRelType.INNER);
                return;

            case IN:
            case NOT_IN:
            case SOME:
            case ALL:
                call = (SqlBasicCall) subQuery.node;
                query = call.operand(1);
                if (!config.isExpand() && !(query instanceof SqlNodeList)) {
                    return;
                }
                final SqlNode leftKeyNode = call.operand(0);

                final List<RexNode> leftKeys;
                switch (leftKeyNode.getKind()) {
                    case ROW:
                        leftKeys = new ArrayList<>();
                        for (SqlNode sqlExpr : ((SqlBasicCall) leftKeyNode).getOperandList()) {
                            leftKeys.add(bb.convertExpression(sqlExpr));
                        }
                        break;
                    default:
                        leftKeys = ImmutableList.of(bb.convertExpression(leftKeyNode));
                }

                if (query instanceof SqlNodeList) {
                    SqlNodeList valueList = (SqlNodeList) query;
                    // When the list size under the threshold or the list references columns, we
                    // convert to OR.
                    if (valueList.size() < config.getInSubQueryThreshold()
                            || valueList.accept(new SqlIdentifierFinder())) {
                        subQuery.expr =
                                convertInToOr(
                                        bb,
                                        leftKeys,
                                        valueList,
                                        (SqlInOperator) call.getOperator());
                        return;
                    }

                    // Otherwise, let convertExists translate
                    // values list into an inline table for the
                    // reference to Q below.
                }

                // Project out the search columns from the left side

                // Q1:
                // "select from emp where emp.deptno in (select col1 from T)"
                //
                // is converted to
                //
                // "select from
                //   emp inner join (select distinct col1 from T)) q
                //   on emp.deptno = q.col1
                //
                // Q2:
                // "select from emp where emp.deptno not in (Q)"
                //
                // is converted to
                //
                // "select from
                //   emp left outer join (select distinct col1, TRUE from T) q
                //   on emp.deptno = q.col1
                //   where emp.deptno <> null
                //         and q.indicator <> TRUE"
                //
                // Note: Sub-query can be used as SqlUpdate#condition like below:
                //
                //   UPDATE emp
                //   SET empno = 1 WHERE emp.empno IN (
                //     SELECT emp.empno FROM emp WHERE emp.empno = 2)
                //
                // In such case, when converting SqlUpdate#condition, bb.root is null
                // and it makes no sense to do the sub-query substitution.
                if (bb.root == null) {
                    return;
                }
                final RelDataType targetRowType =
                        SqlTypeUtil.promoteToRowType(
                                typeFactory, validator().getValidatedNodeType(leftKeyNode), null);
                final boolean notIn = call.getOperator().kind == SqlKind.NOT_IN;
                converted =
                        convertExists(
                                query,
                                RelOptUtil.SubQueryType.IN,
                                subQuery.logic,
                                notIn,
                                targetRowType);
                if (converted.indicator) {
                    // Generate
                    //    emp CROSS JOIN (SELECT COUNT(*) AS c,
                    //                       COUNT(deptno) AS ck FROM dept)
                    final RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);
                    final RelNode seek = converted.r.getInput(0); // fragile
                    final int keyCount = leftKeys.size();
                    final List<Integer> args = ImmutableIntList.range(0, keyCount);
                    LogicalAggregate aggregate =
                            LogicalAggregate.create(
                                    seek,
                                    ImmutableList.of(),
                                    ImmutableBitSet.of(),
                                    null,
                                    ImmutableList.of(
                                            AggregateCall.create(
                                                    SqlStdOperatorTable.COUNT,
                                                    false,
                                                    false,
                                                    false,
                                                    ImmutableList.of(),
                                                    -1,
                                                    null,
                                                    RelCollations.EMPTY,
                                                    longType,
                                                    null),
                                            AggregateCall.create(
                                                    SqlStdOperatorTable.COUNT,
                                                    false,
                                                    false,
                                                    false,
                                                    args,
                                                    -1,
                                                    null,
                                                    RelCollations.EMPTY,
                                                    longType,
                                                    null)));
                    LogicalJoin join =
                            LogicalJoin.create(
                                    bb.root(),
                                    aggregate,
                                    ImmutableList.of(),
                                    rexBuilder.makeLiteral(true),
                                    ImmutableSet.of(),
                                    JoinRelType.INNER);
                    bb.setRoot(join, false);
                }
                final RexNode rex =
                        bb.register(
                                converted.r,
                                converted.outerJoin ? JoinRelType.LEFT : JoinRelType.INNER,
                                leftKeys);

                RelOptUtil.Logic logic = subQuery.logic;
                switch (logic) {
                    case TRUE_FALSE_UNKNOWN:
                    case UNKNOWN_AS_TRUE:
                        if (!converted.indicator) {
                            logic = RelOptUtil.Logic.TRUE_FALSE;
                        }
                        break;
                    default:
                        break;
                }
                subQuery.expr = translateIn(logic, bb.root, rex);
                if (notIn) {
                    subQuery.expr = rexBuilder.makeCall(SqlStdOperatorTable.NOT, subQuery.expr);
                }
                return;

            case EXISTS:
                // "select from emp where exists (select a from T)"
                //
                // is converted to the following if the sub-query is correlated:
                //
                // "select from emp left outer join (select AGG_TRUE() as indicator
                // from T group by corr_var) q where q.indicator is true"
                //
                // If there is no correlation, the expression is replaced with a
                // boolean indicating whether the sub-query returned 0 or >= 1 row.
                if (!config.isExpand()) {
                    return;
                }
                call = (SqlBasicCall) subQuery.node;
                query = call.operand(0);
                final SqlValidatorScope seekScope =
                        (query instanceof SqlSelect)
                                ? validator().getSelectScope((SqlSelect) query)
                                : null;
                final Blackboard seekBb = createBlackboard(seekScope, null, false);
                final RelNode seekRel = convertQueryOrInList(seekBb, query, null);
                requireNonNull(seekRel, () -> "seekRel is null for query " + query);
                // An EXIST sub-query whose inner child has at least 1 tuple
                // (e.g. an Aggregate with no grouping columns or non-empty Values
                // node) should be simplified to a Boolean constant expression.
                final RelMetadataQuery mq = seekRel.getCluster().getMetadataQuery();
                final Double minRowCount = mq.getMinRowCount(seekRel);
                if (minRowCount != null && minRowCount >= 1D) {
                    subQuery.expr = rexBuilder.makeLiteral(true);
                    return;
                }
                converted =
                        RelOptUtil.createExistsPlan(
                                seekRel,
                                RelOptUtil.SubQueryType.EXISTS,
                                subQuery.logic,
                                true,
                                relBuilder);
                assert !converted.indicator;
                if (convertNonCorrelatedSubQuery(subQuery, bb, converted.r, true)) {
                    return;
                }
                subQuery.expr = bb.register(converted.r, JoinRelType.LEFT);
                return;
            case UNIQUE:
                return;
            case SCALAR_QUERY:
                // Convert the sub-query.  If it's non-correlated, convert it
                // to a constant expression.
                if (!config.isExpand()) {
                    return;
                }
                call = (SqlBasicCall) subQuery.node;
                query = call.operand(0);
                converted =
                        convertExists(
                                query, RelOptUtil.SubQueryType.SCALAR, subQuery.logic, true, null);
                assert !converted.indicator;
                if (convertNonCorrelatedSubQuery(subQuery, bb, converted.r, false)) {
                    return;
                }
                rel = convertToSingleValueSubq(query, converted.r);
                subQuery.expr = bb.register(rel, JoinRelType.LEFT);
                return;

            case SELECT:
                // This is used when converting multiset queries:
                //
                // select * from unnest(select multiset[deptno] from emps);
                //
                converted =
                        convertExists(
                                subQuery.node,
                                RelOptUtil.SubQueryType.SCALAR,
                                subQuery.logic,
                                true,
                                null);
                assert !converted.indicator;
                subQuery.expr = bb.register(converted.r, JoinRelType.LEFT);

                // This is used when converting window table functions:
                //
                // select * from table(tumble(table emps, descriptor(deptno), interval '3' DAY))
                //
                bb.cursors.add(converted.r);
                return;
            case SET_SEMANTICS_TABLE:
                // ----- FLINK MODIFICATION BEGIN -----
                // We always expand the SET SEMANTICS TABLE for two reasons:
                // 1. Calcite has a bug when not expanding the SET SEMANTICS TABLE. For more
                // information, see CALCITE-6204.
                // 2. Currently, Flink’s built-in Session Window TVF is the only PTF with SET
                // SEMANTICS. We will expand it by default, like other built-in window TVFs, to
                // reuse some subsequent processing and optimization logic.
                // if (!config.isExpand()) {
                //     return;
                // }
                // ----- FLINK MODIFICATION END -----
                substituteSubQueryOfSetSemanticsInputTable(bb, subQuery);
                return;
            default:
                throw new AssertionError("unexpected kind of sub-query: " + subQuery.node);
        }
    }

    private void substituteSubQueryOfSetSemanticsInputTable(Blackboard bb, SubQuery subQuery) {
        SqlBasicCall call;
        SqlNode query;
        call = (SqlBasicCall) subQuery.node;
        query = call.operand(0);

        // FLINK MODIFICATION BEGIN

        // We modified it for two reasons:
        // 1. In Flink, Exchange nodes should not appear in the logical stage, which will bring
        // uncertainty to the implementation of plan optimization in the current logical stage.
        // Instead, Flink will add exchanges based on traits during the physical phase.
        // 2. Currently, Flink’s built-in Session Window TVF is the only SET SEMANTICS
        // TABLE. We will convert it into the same plan tree as other Window TVFs. The partition key
        // and order key will be recorded using a custom RexCall when subsequently converting the
        // SqlCall of SET SEMANTICS TABLE. See more at
        // FlinkConvertletTable#convertSetSemanticsWindowTableFunction

        final RelNode inputOfSetSemanticsTable =
                convertQueryRecursive(query, false, null).project();
        relBuilder.push(inputOfSetSemanticsTable);

        // final SqlValidatorScope innerTableScope =
        //        (query instanceof SqlSelect) ? validator().getSelectScope((SqlSelect) query) :
        // null;
        // final Blackboard setSemanticsTableBb = createBlackboard(innerTableScope, null, false);
        // final RelNode inputOfSetSemanticsTable =
        //         convertQueryRecursive(query, false, null).project();
        // relBuilder.push(inputOfSetSemanticsTable);
        // requireNonNull(inputOfSetSemanticsTable, () -> "input RelNode is null for query " +
        // query);
        // SqlNodeList partitionList = call.operand(1);
        // final ImmutableBitSet partitionKeys =
        //         buildPartitionKeys(setSemanticsTableBb, partitionList);
        // For set semantics table, distribution is singleton if does not specify
        // partition keys
        // RelDistribution distribution =
        //         partitionKeys.isEmpty()
        //                 ? RelDistributions.SINGLETON
        //               : RelDistributions.hash(partitionKeys.asList());
        // ORDER BY
        // final SqlNodeList orderList = call.operand(2);
        // final RelCollation orders = buildCollation(setSemanticsTableBb, orderList);
        // if (orderList.isEmpty()) {
        //     relBuilder.exchange(distribution);
        // } else {
        //     relBuilder.sortExchange(distribution, orders);
        // }

        // FLINK MODIFICATION END

        RelNode tableRel = relBuilder.build();
        subQuery.expr = bb.register(tableRel, JoinRelType.LEFT);
        // This is used when converting window table functions:
        //
        // select * from table(tumble(table emps, descriptor(deptno),
        // interval '3' DAY))
        //
        bb.cursors.add(tableRel);
    }

    private ImmutableBitSet buildPartitionKeys(Blackboard bb, SqlNodeList partitionList) {
        final ImmutableBitSet.Builder partitionKeys = ImmutableBitSet.builder();
        for (SqlNode partition : partitionList) {
            validator().deriveType(bb.scope(), partition);
            RexNode e = bb.convertExpression(partition);
            partitionKeys.set(parseFieldIdx(e));
        }
        return partitionKeys.build();
    }

    /**
     * Note: The ORDER BY clause for input table parameter differs from the ORDER BY clause in some
     * other contexts in that only columns may be sorted (not arbitrary expressions).
     *
     * @param bb Scope within which to resolve identifiers
     * @param orderList Order by clause, may be null
     * @return ordering of input table
     */
    private RelCollation buildCollation(Blackboard bb, SqlNodeList orderList) {
        final List<RelFieldCollation> orderKeys = new ArrayList<>();
        for (SqlNode orderItem : orderList) {
            orderKeys.add(
                    convertOrderItem(
                            bb,
                            orderItem,
                            RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.UNSPECIFIED));
        }
        return cluster.traitSet().canonize(RelCollations.of(orderKeys));
    }

    private RelFieldCollation convertOrderItem(
            Blackboard bb,
            SqlNode orderItem,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection) {
        switch (orderItem.getKind()) {
            case DESCENDING:
                return convertOrderItem(
                        bb,
                        ((SqlCall) orderItem).operand(0),
                        RelFieldCollation.Direction.DESCENDING,
                        nullDirection);
            case NULLS_FIRST:
                return convertOrderItem(
                        bb,
                        ((SqlCall) orderItem).operand(0),
                        direction,
                        RelFieldCollation.NullDirection.FIRST);
            case NULLS_LAST:
                return convertOrderItem(
                        bb,
                        ((SqlCall) orderItem).operand(0),
                        direction,
                        RelFieldCollation.NullDirection.LAST);
            default:
                break;
        }

        switch (nullDirection) {
            case UNSPECIFIED:
                nullDirection =
                        validator().config().defaultNullCollation().last(desc(direction))
                                ? RelFieldCollation.NullDirection.LAST
                                : RelFieldCollation.NullDirection.FIRST;
                break;
            default:
                break;
        }

        RexNode e = bb.convertExpression(orderItem);
        return new RelFieldCollation(parseFieldIdx(e), direction, nullDirection);
    }

    private static int parseFieldIdx(RexNode e) {
        switch (e.getKind()) {
            case FIELD_ACCESS:
                final RexFieldAccess f = (RexFieldAccess) e;
                return f.getField().getIndex();
            case INPUT_REF:
                final RexInputRef ref = (RexInputRef) e;
                return ref.getIndex();
            default:
                throw new AssertionError();
        }
    }

    private RexNode translateIn(RelOptUtil.Logic logic, @Nullable RelNode root, final RexNode rex) {
        switch (logic) {
            case TRUE:
                return rexBuilder.makeLiteral(true);

            case TRUE_FALSE:
            case UNKNOWN_AS_FALSE:
                assert rex instanceof RexRangeRef;
                final int fieldCount = rex.getType().getFieldCount();
                RexNode rexNode = rexBuilder.makeFieldAccess(rex, fieldCount - 1);
                rexNode = rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, rexNode);

                // Then append the IS NOT NULL(leftKeysForIn).
                //
                // RexRangeRef contains the following fields:
                //   leftKeysForIn,
                //   rightKeysForIn (the original sub-query select list),
                //   nullIndicator
                //
                // The first two lists contain the same number of fields.
                final int k = (fieldCount - 1) / 2;
                ImmutableList.Builder<RexNode> rexNodeBuilder = ImmutableList.builder();
                rexNodeBuilder.add(rexNode);
                for (int i = 0; i < k; i++) {
                    rexNodeBuilder.add(
                            rexBuilder.makeCall(
                                    SqlStdOperatorTable.IS_NOT_NULL,
                                    rexBuilder.makeFieldAccess(rex, i)));
                }
                rexNode =
                        rexBuilder.makeCall(
                                rexNode.getType(),
                                SqlStdOperatorTable.AND,
                                RexUtil.flatten(rexNodeBuilder.build(), SqlStdOperatorTable.AND));
                return rexNode;

            case TRUE_FALSE_UNKNOWN:
            case UNKNOWN_AS_TRUE:
                // select e.deptno,
                //   case
                //   when ct.c = 0 then false
                //   when dt.i is not null then true
                //   when e.deptno is null then null
                //   when ct.ck < ct.c then null
                //   else false
                //   end
                // from e
                // cross join (select count(*) as c, count(deptno) as ck from v) as ct
                // left join (select distinct deptno, true as i from v) as dt
                //   on e.deptno = dt.deptno
                final Join join = (Join) requireNonNull(root, "root");
                final Project left = (Project) join.getLeft();
                final RelNode leftLeft = ((Join) left.getInput()).getLeft();
                final int leftLeftCount = leftLeft.getRowType().getFieldCount();
                final RelDataType longType = typeFactory.createSqlType(SqlTypeName.BIGINT);
                final RexNode cRef = rexBuilder.makeInputRef(root, leftLeftCount);
                final RexNode ckRef = rexBuilder.makeInputRef(root, leftLeftCount + 1);
                final RexNode iRef =
                        rexBuilder.makeInputRef(root, root.getRowType().getFieldCount() - 1);

                final RexLiteral zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO, longType);
                final RexLiteral trueLiteral = rexBuilder.makeLiteral(true);
                final RexLiteral falseLiteral = rexBuilder.makeLiteral(false);
                final RexNode unknownLiteral = rexBuilder.makeNullLiteral(trueLiteral.getType());

                final ImmutableList.Builder<RexNode> args = ImmutableList.builder();
                args.add(
                        rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, cRef, zero),
                        falseLiteral,
                        rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, iRef),
                        trueLiteral);
                final JoinInfo joinInfo = join.analyzeCondition();
                for (int leftKey : joinInfo.leftKeys) {
                    final RexNode kRef = rexBuilder.makeInputRef(root, leftKey);
                    args.add(
                            rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL, kRef), unknownLiteral);
                }
                args.add(
                        rexBuilder.makeCall(SqlStdOperatorTable.LESS_THAN, ckRef, cRef),
                        unknownLiteral,
                        falseLiteral);

                return rexBuilder.makeCall(SqlStdOperatorTable.CASE, args.build());

            default:
                throw new AssertionError(logic);
        }
    }

    /**
     * Determines if a sub-query is non-correlated and if so, converts it to a constant.
     *
     * @param subQuery the call that references the sub-query
     * @param bb blackboard used to convert the sub-query
     * @param converted RelNode tree corresponding to the sub-query
     * @param isExists true if the sub-query is part of an EXISTS expression
     * @return Whether the sub-query can be converted to a constant
     */
    private boolean convertNonCorrelatedSubQuery(
            SubQuery subQuery, Blackboard bb, RelNode converted, boolean isExists) {
        SqlCall call = (SqlBasicCall) subQuery.node;
        if (subQueryConverter.canConvertSubQuery() && isSubQueryNonCorrelated(converted, bb)) {
            // First check if the sub-query has already been converted
            // because it's a nested sub-query.  If so, don't re-evaluate
            // it again.
            RexNode constExpr = mapConvertedNonCorrSubqs.get(call);
            if (constExpr == null) {
                constExpr =
                        subQueryConverter.convertSubQuery(call, this, isExists, config.isExplain());
            }
            if (constExpr != null) {
                subQuery.expr = constExpr;
                mapConvertedNonCorrSubqs.put(call, constExpr);
                return true;
            }
        }
        return false;
    }

    /**
     * Converts the RelNode tree for a select statement to a select that produces a single value.
     *
     * @param query the query
     * @param plan the original RelNode tree corresponding to the statement
     * @return the converted RelNode tree
     */
    public RelNode convertToSingleValueSubq(SqlNode query, RelNode plan) {
        // Check whether query is guaranteed to produce a single value.
        if (query instanceof SqlSelect) {
            SqlSelect select = (SqlSelect) query;
            SqlNodeList selectList = select.getSelectList();
            SqlNodeList groupList = select.getGroup();

            if ((selectList.size() == 1) && ((groupList == null) || (groupList.size() == 0))) {
                SqlNode selectExpr = selectList.get(0);
                if (selectExpr instanceof SqlCall) {
                    SqlCall selectExprCall = (SqlCall) selectExpr;
                    if (Util.isSingleValue(selectExprCall)) {
                        return plan;
                    }
                }

                // If there is a limit with 0 or 1,
                // it is ensured to produce a single value
                SqlNode fetch = select.getFetch();
                if (fetch instanceof SqlNumericLiteral) {
                    long value = ((SqlNumericLiteral) fetch).getValueAs(Long.class);
                    if (value < 2) {
                        return plan;
                    }
                }
            }
        } else if (query instanceof SqlCall) {
            // If the query is (values ...),
            // it is necessary to look into the operands to determine
            // whether SingleValueAgg is necessary
            SqlCall exprCall = (SqlCall) query;
            if (exprCall.getOperator() instanceof SqlValuesOperator
                    && Util.isSingleValue(exprCall)) {
                return plan;
            }
        }

        // If not, project SingleValueAgg
        return RelOptUtil.createSingleValueAggRel(cluster, plan);
    }

    /**
     * Converts "x IN (1, 2, ...)" to "x=1 OR x=2 OR ...".
     *
     * @param leftKeys LHS
     * @param valuesList RHS
     * @param op The operator (IN, NOT IN, &gt; SOME, ...)
     * @return converted expression
     */
    private @Nullable RexNode convertInToOr(
            final Blackboard bb,
            final List<RexNode> leftKeys,
            SqlNodeList valuesList,
            SqlInOperator op) {
        final List<RexNode> comparisons = new ArrayList<>();
        for (SqlNode rightVals : valuesList) {
            RexNode rexComparison;
            final SqlOperator comparisonOp;
            if (op instanceof SqlQuantifyOperator) {
                comparisonOp =
                        RelOptUtil.op(
                                ((SqlQuantifyOperator) op).comparisonKind,
                                SqlStdOperatorTable.EQUALS);
            } else {
                comparisonOp = SqlStdOperatorTable.EQUALS;
            }
            if (leftKeys.size() == 1) {
                rexComparison =
                        rexBuilder.makeCall(
                                comparisonOp,
                                leftKeys.get(0),
                                ensureSqlType(
                                        leftKeys.get(0).getType(),
                                        bb.convertExpression(rightVals)));
            } else {
                assert rightVals instanceof SqlCall;
                final SqlBasicCall call = (SqlBasicCall) rightVals;
                assert (call.getOperator() instanceof SqlRowOperator)
                        && call.operandCount() == leftKeys.size();
                rexComparison =
                        RexUtil.composeConjunction(
                                rexBuilder,
                                Util.transform(
                                        Pair.zip(leftKeys, call.getOperandList()),
                                        pair ->
                                                rexBuilder.makeCall(
                                                        comparisonOp,
                                                        pair.left,
                                                        // TODO: remove requireNonNull when
                                                        // checkerframework issue resolved
                                                        ensureSqlType(
                                                                requireNonNull(
                                                                                pair.left,
                                                                                "pair.left")
                                                                        .getType(),
                                                                bb.convertExpression(
                                                                        pair.right)))));
            }
            comparisons.add(rexComparison);
        }

        switch (op.kind) {
            case ALL:
                return RexUtil.composeConjunction(rexBuilder, comparisons, true);
            case NOT_IN:
                return rexBuilder.makeCall(
                        SqlStdOperatorTable.NOT,
                        RexUtil.composeDisjunction(rexBuilder, comparisons));
            case IN:
            case SOME:
                return RexUtil.composeDisjunction(rexBuilder, comparisons, true);
            default:
                throw new AssertionError();
        }
    }

    /**
     * Ensures that an expression has a given {@link SqlTypeName}, applying a cast if necessary. If
     * the expression already has the right type family, returns the expression unchanged.
     */
    private RexNode ensureSqlType(RelDataType type, RexNode node) {
        if (type.getSqlTypeName() == node.getType().getSqlTypeName()
                || (type.getSqlTypeName() == SqlTypeName.VARCHAR
                        && node.getType().getSqlTypeName() == SqlTypeName.CHAR)) {
            return node;
        }
        return rexBuilder.ensureType(type, node, true);
    }

    /**
     * Gets the list size threshold under which {@link #convertInToOr} is used. Lists of this size
     * or greater will instead be converted to use a join against an inline table ({@link
     * org.apache.calcite.rel.logical.LogicalValues}) rather than a predicate. A threshold of 0
     * forces usage of an inline table in all cases; a threshold of Integer.MAX_VALUE forces usage
     * of OR in all cases
     *
     * @return threshold, default {@link #DEFAULT_IN_SUB_QUERY_THRESHOLD}
     */
    @Deprecated // to be removed before 2.0
    protected int getInSubqueryThreshold() {
        return config.getInSubQueryThreshold();
    }

    /**
     * Converts an EXISTS or IN predicate into a join. For EXISTS, the sub-query produces an
     * indicator variable, and the result is a relational expression which outer joins that
     * indicator to the original query. After performing the outer join, the condition will be TRUE
     * if the EXISTS condition holds, NULL otherwise.
     *
     * @param seek A query, for example 'select * from emp' or 'values (1,2,3)' or '('Foo', 34)'.
     * @param subQueryType Whether sub-query is IN, EXISTS or scalar
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE, FALSE, UNKNOWN)
     *     will be required, or whether we can accept an approximation (say representing UNKNOWN as
     *     FALSE)
     * @param notIn Whether the operation is NOT IN
     * @return join expression
     */
    private RelOptUtil.Exists convertExists(
            SqlNode seek,
            RelOptUtil.SubQueryType subQueryType,
            RelOptUtil.Logic logic,
            boolean notIn,
            @Nullable RelDataType targetDataType) {
        final SqlValidatorScope seekScope =
                (seek instanceof SqlSelect) ? validator().getSelectScope((SqlSelect) seek) : null;
        final Blackboard seekBb = createBlackboard(seekScope, null, false);
        RelNode seekRel = convertQueryOrInList(seekBb, seek, targetDataType);
        requireNonNull(seekRel, () -> "seekRel is null for query " + seek);

        return RelOptUtil.createExistsPlan(seekRel, subQueryType, logic, notIn, relBuilder);
    }

    private @Nullable RelNode convertQueryOrInList(
            Blackboard bb, SqlNode seek, @Nullable RelDataType targetRowType) {
        // NOTE: Once we start accepting single-row queries as row constructors,
        // there will be an ambiguity here for a case like X IN ((SELECT Y FROM
        // Z)).  The SQL standard resolves the ambiguity by saying that a lone
        // select should be interpreted as a table expression, not a row
        // expression.  The semantic difference is that a table expression can
        // return multiple rows.
        if (seek instanceof SqlNodeList) {
            return convertRowValues(bb, seek, (SqlNodeList) seek, false, targetRowType);
        } else {
            return convertQueryRecursive(seek, false, null).project();
        }
    }

    private @Nullable RelNode convertRowValues(
            Blackboard bb,
            SqlNode rowList,
            Collection<SqlNode> rows,
            boolean allowLiteralsOnly,
            @Nullable RelDataType targetRowType) {
        // NOTE jvs 30-Apr-2006: We combine all rows consisting entirely of
        // literals into a single LogicalValues; this gives the optimizer a smaller
        // input tree.  For everything else (computed expressions, row
        // sub-queries), we union each row in as a projection on top of a
        // LogicalOneRow.

        final ImmutableList.Builder<ImmutableList<RexLiteral>> tupleList = ImmutableList.builder();
        final RelDataType listType = validator().getValidatedNodeType(rowList);
        final RelDataType rowType;
        if (targetRowType != null) {
            rowType =
                    SqlTypeUtil.keepSourceTypeAndTargetNullability(
                            targetRowType, listType, typeFactory);
        } else {
            rowType = SqlTypeUtil.promoteToRowType(typeFactory, listType, null);
        }

        final List<RelNode> unionInputs = new ArrayList<>();
        for (SqlNode node : rows) {
            SqlBasicCall call;
            if (isRowConstructor(node)) {
                call = (SqlBasicCall) node;
                ImmutableList.Builder<RexLiteral> tuple = ImmutableList.builder();
                for (Ord<SqlNode> operand : Ord.zip(call.getOperandList())) {
                    RexLiteral rexLiteral =
                            convertLiteralInValuesList(operand.e, bb, rowType, operand.i);
                    if ((rexLiteral == null) && allowLiteralsOnly) {
                        return null;
                    }
                    if ((rexLiteral == null) || !config.isCreateValuesRel()) {
                        // fallback to convertRowConstructor
                        tuple = null;
                        break;
                    }
                    tuple.add(rexLiteral);
                }
                if (tuple != null) {
                    tupleList.add(tuple.build());
                    continue;
                }
            } else {
                RexLiteral rexLiteral = convertLiteralInValuesList(node, bb, rowType, 0);
                if ((rexLiteral != null) && config.isCreateValuesRel()) {
                    tupleList.add(ImmutableList.of(rexLiteral));
                    continue;
                } else {
                    if ((rexLiteral == null) && allowLiteralsOnly) {
                        return null;
                    }
                }

                // convert "1" to "row(1)"
                call = (SqlBasicCall) SqlStdOperatorTable.ROW.createCall(SqlParserPos.ZERO, node);
            }
            unionInputs.add(convertRowConstructor(bb, call));
        }
        LogicalValues values = LogicalValues.create(cluster, rowType, tupleList.build());
        RelNode resultRel;
        if (unionInputs.isEmpty()) {
            resultRel = values;
        } else {
            if (!values.getTuples().isEmpty()) {
                unionInputs.add(values);
            }
            resultRel = LogicalUnion.create(unionInputs, true);
        }
        leaves.put(resultRel, resultRel.getRowType().getFieldCount());
        return resultRel;
    }

    private @Nullable RexLiteral convertLiteralInValuesList(
            @Nullable SqlNode sqlNode, Blackboard bb, RelDataType rowType, int iField) {
        if (!(sqlNode instanceof SqlLiteral)) {
            return null;
        }
        RelDataTypeField field = rowType.getFieldList().get(iField);
        RelDataType type = field.getType();
        if (type.isStruct()) {
            // null literals for weird stuff like UDT's need
            // special handling during type flattening, so
            // don't use LogicalValues for those
            return null;
        }
        return convertLiteral((SqlLiteral) sqlNode, bb, type);
    }

    private RexLiteral convertLiteral(SqlLiteral sqlLiteral, Blackboard bb, RelDataType type) {
        RexNode literalExpr = exprConverter.convertLiteral(bb, sqlLiteral);

        if (!(literalExpr instanceof RexLiteral)) {
            assert literalExpr.isA(SqlKind.CAST);
            RexNode child = ((RexCall) literalExpr).getOperands().get(0);
            assert RexLiteral.isNullLiteral(child);

            // NOTE jvs 22-Nov-2006:  we preserve type info
            // in LogicalValues digest, so it's OK to lose it here
            return (RexLiteral) child;
        }

        RexLiteral literal = (RexLiteral) literalExpr;

        Comparable value = literal.getValue();

        if (SqlTypeUtil.isExactNumeric(type) && SqlTypeUtil.hasScale(type)) {
            BigDecimal roundedValue =
                    NumberUtil.rescaleBigDecimal((BigDecimal) value, type.getScale());
            return rexBuilder.makeExactLiteral(roundedValue, type);
        }

        if ((value instanceof NlsString) && (type.getSqlTypeName() == SqlTypeName.CHAR)) {
            // pad fixed character type
            NlsString unpadded = (NlsString) value;
            return rexBuilder.makeCharLiteral(
                    new NlsString(
                            Spaces.padRight(unpadded.getValue(), type.getPrecision()),
                            unpadded.getCharsetName(),
                            unpadded.getCollation()));
        }
        return literal;
    }

    private static boolean isRowConstructor(SqlNode node) {
        if (!(node.getKind() == SqlKind.ROW)) {
            return false;
        }
        SqlCall call = (SqlCall) node;
        return call.getOperator().getName().equalsIgnoreCase("row");
    }

    /**
     * Builds a list of all <code>IN</code> or <code>EXISTS</code> operators inside SQL parse tree.
     * Does not traverse inside queries.
     *
     * @param bb blackboard
     * @param node the SQL parse tree
     * @param logic Whether the answer needs to be in full 3-valued logic (TRUE, FALSE, UNKNOWN)
     *     will be required, or whether we can accept an approximation (say representing UNKNOWN as
     *     FALSE)
     * @param registerOnlyScalarSubQueries if set to true and the parse tree corresponds to a
     *     variation of a select node, only register it if it's a scalar sub-query
     */
    private void findSubQueries(
            Blackboard bb,
            SqlNode node,
            RelOptUtil.Logic logic,
            boolean registerOnlyScalarSubQueries) {
        final SqlKind kind = node.getKind();
        switch (kind) {
            case EXISTS:
            case UNIQUE:
            case SELECT:
            case MULTISET_QUERY_CONSTRUCTOR:
            case MULTISET_VALUE_CONSTRUCTOR:
            case ARRAY_QUERY_CONSTRUCTOR:
            case MAP_QUERY_CONSTRUCTOR:
            case CURSOR:
            case SET_SEMANTICS_TABLE:
            case SCALAR_QUERY:
                if (!registerOnlyScalarSubQueries || (kind == SqlKind.SCALAR_QUERY)) {
                    bb.registerSubQuery(node, RelOptUtil.Logic.TRUE_FALSE);
                }
                return;
            case IN:
                break;
            case NOT_IN:
            case NOT:
                logic = logic.negate();
                break;
            default:
                break;
        }
        if (node instanceof SqlCall) {
            switch (kind) {
                    // Do no change logic for AND, IN and NOT IN expressions;
                    // but do change logic for OR, NOT and others;
                    // EXISTS was handled already.
                case AND:
                case IN:
                case NOT_IN:
                    break;
                default:
                    logic = RelOptUtil.Logic.TRUE_FALSE_UNKNOWN;
                    break;
            }
            for (SqlNode operand : ((SqlCall) node).getOperandList()) {
                if (operand != null) {
                    // In the case of an IN expression, locate scalar
                    // sub-queries so we can convert them to constants
                    findSubQueries(
                            bb,
                            operand,
                            logic,
                            kind == SqlKind.IN
                                    || kind == SqlKind.NOT_IN
                                    || kind == SqlKind.SOME
                                    || kind == SqlKind.ALL
                                    || registerOnlyScalarSubQueries);
                }
            }
        } else if (node instanceof SqlNodeList) {
            for (SqlNode child : (SqlNodeList) node) {
                findSubQueries(
                        bb,
                        child,
                        logic,
                        kind == SqlKind.IN
                                || kind == SqlKind.NOT_IN
                                || kind == SqlKind.SOME
                                || kind == SqlKind.ALL
                                || registerOnlyScalarSubQueries);
            }
        }

        // Now that we've located any scalar sub-queries inside the IN
        // expression, register the IN expression itself.  We need to
        // register the scalar sub-queries first so they can be converted
        // before the IN expression is converted.
        switch (kind) {
            case IN:
            case NOT_IN:
            case SOME:
            case ALL:
                switch (logic) {
                    case TRUE_FALSE_UNKNOWN:
                        RelDataType type = validator().getValidatedNodeTypeIfKnown(node);
                        if (type == null) {
                            // The node might not be validated if we still don't know type of the
                            // node.
                            // Therefore return directly.
                            return;
                        } else {
                            break;
                        }
                    case UNKNOWN_AS_FALSE:
                        logic = RelOptUtil.Logic.TRUE;
                        break;
                    default:
                        break;
                }
                bb.registerSubQuery(node, logic);
                break;
            default:
                break;
        }
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format.
     *
     * @param node Expression to translate
     * @return Converted expression
     */
    public RexNode convertExpression(SqlNode node) {
        Map<String, RelDataType> nameToTypeMap = Collections.emptyMap();
        final ParameterScope scope =
                new ParameterScope((SqlValidatorImpl) validator(), nameToTypeMap);
        final Blackboard bb = createBlackboard(scope, null, false);
        replaceSubQueries(bb, node, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
        return bb.convertExpression(node);
    }

    /**
     * Converts an expression from {@link SqlNode} to {@link RexNode} format, mapping identifier
     * references to predefined expressions.
     *
     * @param node Expression to translate
     * @param nameToNodeMap map from String to {@link RexNode}; when an {@link SqlIdentifier} is
     *     encountered, it is used as a key and translated to the corresponding value from this map
     * @return Converted expression
     */
    public RexNode convertExpression(SqlNode node, Map<String, RexNode> nameToNodeMap) {
        final Map<String, RelDataType> nameToTypeMap = new HashMap<>();
        for (Map.Entry<String, RexNode> entry : nameToNodeMap.entrySet()) {
            nameToTypeMap.put(entry.getKey(), entry.getValue().getType());
        }
        final ParameterScope scope =
                new ParameterScope((SqlValidatorImpl) validator(), nameToTypeMap);
        final Blackboard bb = createBlackboard(scope, nameToNodeMap, false);
        replaceSubQueries(bb, node, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
        return bb.convertExpression(node);
    }

    /**
     * Converts a non-standard expression.
     *
     * <p>This method is an extension-point that derived classes can override. If this method
     * returns a null result, the normal expression translation process will proceed. The default
     * implementation always returns null.
     *
     * @param node Expression
     * @param bb Blackboard
     * @return null to proceed with the usual expression translation process
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换非标准表达式
     */
    protected @Nullable RexNode convertExtendedExpression(SqlNode node, Blackboard bb) {
        return null;
    }

    private RexNode convertOver(Blackboard bb, SqlNode node) {
        SqlCall call = (SqlCall) node;
        SqlCall aggCall = call.operand(0);
        boolean ignoreNulls = false;
        switch (aggCall.getKind()) {
            case IGNORE_NULLS:
                ignoreNulls = true;
                // fall through
            case RESPECT_NULLS:
                aggCall = aggCall.operand(0);
                break;
            default:
                break;
        }

        SqlNode windowOrRef = call.operand(1);
        final SqlWindow window = validator().resolveWindow(windowOrRef, bb.scope());

        SqlNode sqlLowerBound = window.getLowerBound();
        SqlNode sqlUpperBound = window.getUpperBound();
        boolean rows = window.isRows();
        SqlNodeList orderList = window.getOrderList();

        if (!aggCall.getOperator().allowsFraming()) {
            // If the operator does not allow framing, bracketing is implicitly
            // everything up to the current row.
            sqlLowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            sqlUpperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
            if (aggCall.getKind() == SqlKind.ROW_NUMBER) {
                // ROW_NUMBER() expects specific kind of framing.
                rows = true;
            }
        } else if (orderList.size() == 0) {
            // Without ORDER BY, there must be no bracketing.
            sqlLowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            sqlUpperBound = SqlWindow.createUnboundedFollowing(SqlParserPos.ZERO);
        } else if (sqlLowerBound == null && sqlUpperBound == null) {
            sqlLowerBound = SqlWindow.createUnboundedPreceding(SqlParserPos.ZERO);
            sqlUpperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
        } else if (sqlUpperBound == null) {
            sqlUpperBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
        } else if (sqlLowerBound == null) {
            sqlLowerBound = SqlWindow.createCurrentRow(SqlParserPos.ZERO);
        }
        final SqlNodeList partitionList = window.getPartitionList();
        final ImmutableList.Builder<RexNode> partitionKeys = ImmutableList.builder();
        for (SqlNode partition : partitionList) {
            validator().deriveType(bb.scope(), partition);
            partitionKeys.add(bb.convertExpression(partition));
        }
        final RexNode lowerBound =
                bb.convertExpression(requireNonNull(sqlLowerBound, "sqlLowerBound"));
        final RexNode upperBound =
                bb.convertExpression(requireNonNull(sqlUpperBound, "sqlUpperBound"));
        if (orderList.size() == 0 && !rows) {
            // A logical range requires an ORDER BY clause. Use the implicit
            // ordering of this relation. There must be one, otherwise it would
            // have failed validation.
            orderList = bb.scope().getOrderList();
            if (orderList == null) {
                throw new AssertionError("Relation should have sort key for implicit ORDER BY");
            }
        }

        final ImmutableList.Builder<RexNode> orderKeys = ImmutableList.builder();
        for (SqlNode order : orderList) {
            orderKeys.add(
                    bb.convertSortExpression(
                            order,
                            RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.UNSPECIFIED,
                            bb::sortToRex));
        }

        try {
            Preconditions.checkArgument(bb.window == null, "already in window agg mode");
            bb.window = window;
            RexNode rexAgg = exprConverter.convertCall(bb, aggCall);
            rexAgg = rexBuilder.ensureType(validator().getValidatedNodeType(call), rexAgg, false);

            // Walk over the tree and apply 'over' to all agg functions. This is
            // necessary because the returned expression is not necessarily a call
            // to an agg function. For example, AVG(x) becomes SUM(x) / COUNT(x).

            final SqlLiteral q = aggCall.getFunctionQuantifier();
            final boolean isDistinct = q != null && q.getValue() == SqlSelectKeyword.DISTINCT;

            final RexShuttle visitor =
                    new HistogramShuttle(
                            partitionKeys.build(),
                            orderKeys.build(),
                            rows,
                            RexWindowBounds.create(sqlLowerBound, lowerBound),
                            RexWindowBounds.create(sqlUpperBound, upperBound),
                            window.isAllowPartial(),
                            isDistinct,
                            ignoreNulls);
            return rexAgg.accept(visitor);
        } finally {
            bb.window = null;
        }
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 从给定的SqlNode对象转换数据到Blackboard，但不附加任何额外的参数。
     *
     * @param bb 目标Blackboard对象，用于存储转换后的数据。
     * @param from 可为null的SqlNode对象，表示数据来源的SQL节点。如果为null，则此方法可能执行空操作或特殊处理。
     */
    protected void convertFrom(Blackboard bb, @Nullable SqlNode from) {
        // 调用重载的convertFrom方法，传入空的List作为附加参数，表示没有额外的转换参数或条件。
        convertFrom(bb, from, Collections.emptyList());
    }

    // ----- FLINK MODIFICATION BEGIN -----

    private boolean containsQueryHints = false;

    /**
     * To tell this converter that this SqlNode tree contains query hints and then a query block
     * alias will be attached to the root node of the query block.
     *
     * <p>The `containsQueryHints` is false default to be compatible with previous behavior and then
     * planner can reuse some node.
     *
     * <p>TODO At present, it is a relatively hacked way
     */
    public void containsQueryHints() {
        containsQueryHints = true;
    }

    // ----- FLINK MODIFICATION END -----

    /**
     * Converts a FROM clause into a relational expression.
     *
     * @param bb Scope within which to resolve identifiers
     * @param from FROM clause of a query. Examples include:
     *     <ul>
     *       <li>a single table ("SALES.EMP"),
     *       <li>an aliased table ("EMP AS E"),
     *       <li>a list of tables ("EMP, DEPT"),
     *       <li>an ANSI Join expression ("EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"),
     *       <li>a VALUES clause ("VALUES ('Fred', 20)"),
     *       <li>a query ("(SELECT * FROM EMP WHERE GENDER = 'F')"),
     *       <li>or any combination of the above.
     *     </ul>
     *
     * @param fieldNames Field aliases, usually come from AS clause, or null
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将FROM子句转换成关系表达式。
     *
     * @param bb 用于解析标识符的作用域
     * @param from 查询的FROM子句。例子包括：
     * <ul>
     *   <li>单个表（"SALES.EMP"），
     *   <li>带别名的表（"EMP AS E"），
     *   <li>表列表（"EMP, DEPT"），
     *   <li>ANSI连接表达式（"EMP JOIN DEPT ON EMP.DEPTNO = DEPT.DEPTNO"），
     *   <li>VALUES子句（"VALUES ('Fred', 20)"），
     *   <li>子查询（"(SELECT * FROM EMP WHERE GENDER = 'F')"），
     *   <li>或上述任何组合。
     * </ul>
     */
    protected void convertFrom(
            Blackboard bb, @Nullable SqlNode from, @Nullable List<String> fieldNames) {
        // 如果from为null，则设置一个只包含一行的LogicalValues作为根节点，并返回
        if (from == null) {
            bb.setRoot(LogicalValues.createOneRow(cluster), false);
            return;
        }
        // 尝试将from转换为SqlCall类型，以便处理如AS子句等
        final SqlCall call;
        switch (from.getKind()) {
            case AS:
                // 处理带别名的表或子查询
                call = (SqlCall) from;
                SqlNode firstOperand = call.operand(0);// 获取别名前的原始节点
                final List<String> fieldNameList =
                        call.operandCount() > 2
                                ? SqlIdentifier.simpleNames(Util.skip(call.getOperandList(), 2))
                                : null;// 否则别名列表为null
                // 递归调用，处理原始节点
                convertFrom(bb, firstOperand, fieldNameList);

                // ----- FLINK MODIFICATION BEGIN -----

                // Add a query-block alias hint to distinguish different query levels
                // Due to Calcite will expand the whole SQL Rel Node tree that contains query block,
                // but sometimes the query block should be perceived such as query hint propagation.
                // TODO add query-block alias hint in SqlNode instead of here

                // Flink的修改：添加查询块别名提示，以区分不同的查询级别
                // 由于Calcite会展开包含查询块的整个SQL Rel Node树，但有时需要感知查询块，如查询提示传播
                // TODO 在SqlNode中而不是在这里添加查询块别名提示
                if (containsQueryHints) {
                    RelNode root = bb.root;
                    // 如果根节点是一个可以附加提示的节点（Hintable）
                    if (root instanceof Hintable) {
                        // 创建一个查询块别名提示，使用别名作为提示选项
                        RelHint queryBlockAliasHint =
                                RelHint.builder(FlinkHints.HINT_ALIAS)// 使用Flink的HINT_ALIAS作为提示类型
                                        .hintOption(call.operand(1).toString())// 将AS子句中的别名作为提示选项
                                        .build();
                        // 将查询块别名提示附加到根节点上，并获取新的根节点
                        RelNode newRoot =
                                ((Hintable) root)
                                        .attachHints(
                                                Collections.singletonList(queryBlockAliasHint));// 将提示附加到根节点
                        // 检查当前根节点是否是叶子节点（假设leaves是一个存储叶子节点的映射）
                        boolean isLeaf = leaves.containsKey(root);
                        if (isLeaf) {
                            // remove old root node
                            // 如果是叶子节点，则从leaves映射中移除旧的根节点
                            leaves.remove(root);
                        }
                        // 设置新的根节点，并标记是否为叶子节点
                        bb.setRoot(newRoot, isLeaf);
                    }
                }

                // ----- FLINK MODIFICATION END -----
                return;
             // 处理MATCH_RECOGNIZE子句
            case MATCH_RECOGNIZE:
                convertMatchRecognize(bb, (SqlMatchRecognize) from);
                return;
             // 处理PIVOT子句
            case PIVOT:
                convertPivot(bb, (SqlPivot) from);
                return;
            // 处理UNPIVOT子句
            case UNPIVOT:
                convertUnpivot(bb, (SqlUnpivot) from);
                return;
            // 处理WITH_ITEM（WITH子句中的单个子项）
            case WITH_ITEM:
                convertFrom(bb, ((SqlWithItem) from).query);
                return;
            // 处理WITH子句（整个WITH子句体）
            case WITH:
                convertFrom(bb, ((SqlWith) from).body);
                return;
            // 处理TABLESAMPLE子句
            case TABLESAMPLE:
                // 获取TABLESAMPLE调用的操作数列表
                final List<SqlNode> operands = ((SqlCall) from).getOperandList();
                // 从第二个操作数中获取采样规范（通常第一个操作数是表名，第二个操作数是采样参数）
                SqlSampleSpec sampleSpec =
                        SqlLiteral.sampleValue(
                                requireNonNull(operands.get(1), () -> "operand[1] of " + from));// 获取TABLESAMPLE子句的参数
                // 根据采样规范的类型进行处理
                if (sampleSpec instanceof SqlSampleSpec.SqlSubstitutionSampleSpec) {
                    // 如果是替换采样规范，获取采样名称
                    String sampleName =
                            ((SqlSampleSpec.SqlSubstitutionSampleSpec) sampleSpec).getName();
                    // 将采样名称压入datasetStack
                    datasetStack.push(sampleName);
                    // 递归处理表名
                    convertFrom(bb, operands.get(0));
                    // 弹出采样名称
                    datasetStack.pop();
                } else if (sampleSpec instanceof SqlSampleSpec.SqlTableSampleSpec) {
                    // 如果是表采样规范
                    SqlSampleSpec.SqlTableSampleSpec tableSampleSpec =
                            (SqlSampleSpec.SqlTableSampleSpec) sampleSpec;
                    // 递归处理表名
                    convertFrom(bb, operands.get(0));
                    // 创建采样参数
                    RelOptSamplingParameters params =
                            new RelOptSamplingParameters(
                                    tableSampleSpec.isBernoulli(),
                                    tableSampleSpec.getSamplePercentage(),
                                    tableSampleSpec.isRepeatable(),
                                    tableSampleSpec.getRepeatableSeed());
                    // 设置新的根节点为采样节点
                    bb.setRoot(new Sample(cluster, bb.root(), params), false);
                } else {
                    // 如果采样规范类型未知，则抛出断言错误
                    throw new AssertionError("unknown TABLESAMPLE type: " + sampleSpec);
                }
                return;
            // 处理TABLE_REF子句
            case TABLE_REF:
                // 将from转换为SqlCall类型（假设它总是这样）
                call = (SqlCall) from;
                // 转换标识符（可能是表名加别名）
                convertIdentifier(bb, call.operand(0), null, call.operand(1), null);
                return;
           // 处理IDENTIFIER子句
            case IDENTIFIER:
                // 直接转换标识符
                convertIdentifier(bb, (SqlIdentifier) from, null, null, null);
                // 处理完IDENTIFIER后返回
                return;

            case EXTEND:
                // 将from转换为SqlCall类型
                call = (SqlCall) from;
                // 获取第一个操作数，它可能是一个表引用或其他标识符
                final SqlNode operand0 = call.getOperandList().get(0);
                // 根据操作数的类型，将其转换为SqlIdentifier（表名或列名）
                final SqlIdentifier id =
                        operand0.getKind() == SqlKind.TABLE_REF
                                ? ((SqlCall) operand0).operand(0)
                                : (SqlIdentifier) operand0;
                // 获取扩展的列
                SqlNodeList extendedColumns = (SqlNodeList) call.getOperandList().get(1);
                // 转换标识符（可能是表名）和扩展的列
                convertIdentifier(bb, id, extendedColumns, null, null);
                return;

            case SNAPSHOT:
                // 处理时间表快照
                convertTemporalTable(bb, (SqlCall) from);
                return;

            case JOIN:// 处理JOIN操作
                convertJoin(bb, (SqlJoin) from);
                return;

            // 处理SELECT、INTERSECT、EXCEPT、UNION等集合操作
            case SELECT:
            case INTERSECT:
            case EXCEPT:
            case UNION:
                // 递归地转换查询，并获取转换后的RelNode
                final RelNode rel = convertQueryRecursive(from, false, null).project();
                // 将转换后的查询设置为根节点
                bb.setRoot(rel, true);
                return;

            case VALUES:
                // 处理VALUES子句，通常用于生成静态数据
                convertValuesImpl(bb, (SqlCall) from, null);
                // 如果提供了字段名，则重命名输出字段
                if (fieldNames != null) {
                    bb.setRoot(relBuilder.push(bb.root()).rename(fieldNames).build(), true);
                }
                return;

            case UNNEST:
                // 处理UNNEST操作，通常用于展开数组或集合
                convertUnnest(bb, (SqlCall) from, fieldNames);
                return;

            case COLLECTION_TABLE:
                // 处理COLLECTION_TABLE操作，这通常用于将集合表达式映射为表
                call = (SqlCall) from;

                // Dig out real call; TABLE() wrapper is just syntactic.
                // 断言操作数列表大小为1，因为COLLECTION_TABLE通常只包裹一个表达式
                assert call.getOperandList().size() == 1;
                // 获取被包裹的表达式
                final SqlCall call2 = call.operand(0);
                // 转换集合表
                convertCollectionTable(bb, call2);
                return;

            default:
                // 如果操作类型不是上述任何一种，则抛出断言错误
                throw new AssertionError("not a join operator " + from);
        }
    }

    private void convertUnnest(Blackboard bb, SqlCall call, @Nullable List<String> fieldNames) {
        final List<SqlNode> nodes = call.getOperandList();
        final SqlUnnestOperator operator = (SqlUnnestOperator) call.getOperator();
        for (SqlNode node : nodes) {
            replaceSubQueries(bb, node, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
        }
        final List<RexNode> exprs = new ArrayList<>();
        for (Ord<SqlNode> node : Ord.zip(nodes)) {
            exprs.add(
                    relBuilder.alias(
                            bb.convertExpression(node.e), validator().deriveAlias(node.e, node.i)));
        }
        RelNode child = (null != bb.root) ? bb.root : LogicalValues.createOneRow(cluster);
        RelNode uncollect;
        if (validator().config().conformance().allowAliasUnnestItems()) {
            uncollect =
                    relBuilder
                            .push(child)
                            .project(exprs)
                            .uncollect(
                                    requireNonNull(fieldNames, "fieldNames"),
                                    operator.withOrdinality)
                            .build();
        } else {
            // REVIEW danny 2020-04-26: should we unify the normal field aliases and
            // the item aliases?
            uncollect =
                    relBuilder
                            .push(child)
                            .project(exprs)
                            .uncollect(Collections.emptyList(), operator.withOrdinality)
                            .let(r -> fieldNames == null ? r : r.rename(fieldNames))
                            .build();
        }
        bb.setRoot(uncollect, true);
    }

    protected void convertMatchRecognize(Blackboard bb, SqlMatchRecognize matchRecognize) {
        final SqlValidatorNamespace ns = getNamespace(matchRecognize);
        final SqlValidatorScope scope = validator().getMatchRecognizeScope(matchRecognize);

        final Blackboard matchBb = createBlackboard(scope, null, false);
        final RelDataType rowType = ns.getRowType();
        // convert inner query, could be a table name or a derived table
        SqlNode expr = matchRecognize.getTableRef();
        convertFrom(matchBb, expr);
        final RelNode input = matchBb.root();

        // PARTITION BY
        final SqlNodeList partitionList = matchRecognize.getPartitionList();
        final ImmutableBitSet partitionKeys = buildPartitionKeys(matchBb, partitionList);

        // ORDER BY
        // TODO combine with buildCollation method after support NULLS_FIRST/NULLS_LAST
        final SqlNodeList orderList = matchRecognize.getOrderList();
        final List<RelFieldCollation> orderKeys = new ArrayList<>();
        for (SqlNode order : orderList) {
            final RelFieldCollation.Direction direction;
            switch (order.getKind()) {
                case DESCENDING:
                    direction = RelFieldCollation.Direction.DESCENDING;
                    order = ((SqlCall) order).operand(0);
                    break;
                case NULLS_FIRST:
                case NULLS_LAST:
                    throw new AssertionError();
                default:
                    direction = RelFieldCollation.Direction.ASCENDING;
                    break;
            }
            final RelFieldCollation.NullDirection nullDirection =
                    validator().config().defaultNullCollation().last(desc(direction))
                            ? RelFieldCollation.NullDirection.LAST
                            : RelFieldCollation.NullDirection.FIRST;
            RexNode e = matchBb.convertExpression(order);
            orderKeys.add(
                    new RelFieldCollation(((RexInputRef) e).getIndex(), direction, nullDirection));
        }
        final RelCollation orders = cluster.traitSet().canonize(RelCollations.of(orderKeys));

        // convert pattern
        final Set<String> patternVarsSet = new HashSet<>();
        SqlNode pattern = matchRecognize.getPattern();
        final SqlBasicVisitor<@Nullable RexNode> patternVarVisitor =
                new SqlBasicVisitor<@Nullable RexNode>() {
                    @Override
                    public RexNode visit(SqlCall call) {
                        List<SqlNode> operands = call.getOperandList();
                        List<RexNode> newOperands = new ArrayList<>();
                        for (SqlNode node : operands) {
                            RexNode arg = requireNonNull(node.accept(this), node::toString);
                            newOperands.add(arg);
                        }
                        return rexBuilder.makeCall(
                                validator().getUnknownType(), call.getOperator(), newOperands);
                    }

                    @Override
                    public RexNode visit(SqlIdentifier id) {
                        assert id.isSimple();
                        patternVarsSet.add(id.getSimple());
                        return rexBuilder.makeLiteral(id.getSimple());
                    }

                    @Override
                    public RexNode visit(SqlLiteral literal) {
                        if (literal instanceof SqlNumericLiteral) {
                            return rexBuilder.makeExactLiteral(
                                    BigDecimal.valueOf(literal.intValue(true)));
                        } else {
                            return rexBuilder.makeLiteral(literal.booleanValue());
                        }
                    }
                };
        final RexNode patternNode = pattern.accept(patternVarVisitor);
        assert patternNode != null : "pattern is not found in " + pattern;

        SqlLiteral interval = matchRecognize.getInterval();
        RexNode intervalNode = null;
        if (interval != null) {
            intervalNode = matchBb.convertLiteral(interval);
        }

        // convert subset
        final SqlNodeList subsets = matchRecognize.getSubsetList();
        final Map<String, TreeSet<String>> subsetMap = new HashMap<>();
        for (SqlNode node : subsets) {
            List<SqlNode> operands = ((SqlCall) node).getOperandList();
            SqlIdentifier left = (SqlIdentifier) operands.get(0);
            patternVarsSet.add(left.getSimple());
            final SqlNodeList rights = (SqlNodeList) operands.get(1);
            final TreeSet<String> list = new TreeSet<>(SqlIdentifier.simpleNames(rights));
            subsetMap.put(left.getSimple(), list);
        }

        SqlNode afterMatch = matchRecognize.getAfter();
        if (afterMatch == null) {
            afterMatch = SqlMatchRecognize.AfterOption.SKIP_TO_NEXT_ROW.symbol(SqlParserPos.ZERO);
        }

        final RexNode after;
        if (afterMatch instanceof SqlCall) {
            List<SqlNode> operands = ((SqlCall) afterMatch).getOperandList();
            SqlOperator operator = ((SqlCall) afterMatch).getOperator();
            assert operands.size() == 1;
            SqlIdentifier id = (SqlIdentifier) operands.get(0);
            assert patternVarsSet.contains(id.getSimple())
                    : id.getSimple() + " not defined in pattern";
            RexNode rex = rexBuilder.makeLiteral(id.getSimple());
            after =
                    rexBuilder.makeCall(
                            validator().getUnknownType(), operator, ImmutableList.of(rex));
        } else {
            after = matchBb.convertExpression(afterMatch);
        }

        matchBb.setPatternVarRef(true);

        // convert measures
        final ImmutableMap.Builder<String, RexNode> measureNodes = ImmutableMap.builder();
        for (SqlNode measure : matchRecognize.getMeasureList()) {
            List<SqlNode> operands = ((SqlCall) measure).getOperandList();
            String alias = ((SqlIdentifier) operands.get(1)).getSimple();
            RexNode rex = matchBb.convertExpression(operands.get(0));
            measureNodes.put(alias, rex);
        }

        // convert definitions
        final ImmutableMap.Builder<String, RexNode> definitionNodes = ImmutableMap.builder();
        for (SqlNode def : matchRecognize.getPatternDefList()) {
            replaceSubQueries(matchBb, def, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
            List<SqlNode> operands = ((SqlCall) def).getOperandList();
            String alias = ((SqlIdentifier) operands.get(1)).getSimple();
            RexNode rex = matchBb.convertExpression(operands.get(0));
            definitionNodes.put(alias, rex);
        }

        final SqlLiteral rowsPerMatch = matchRecognize.getRowsPerMatch();
        final boolean allRows =
                rowsPerMatch != null
                        && rowsPerMatch.getValue() == SqlMatchRecognize.RowsPerMatchOption.ALL_ROWS;

        matchBb.setPatternVarRef(false);

        final RelFactories.MatchFactory factory = RelFactories.DEFAULT_MATCH_FACTORY;
        final RelNode rel =
                factory.createMatch(
                        input,
                        patternNode,
                        rowType,
                        matchRecognize.getStrictStart().booleanValue(),
                        matchRecognize.getStrictEnd().booleanValue(),
                        definitionNodes.build(),
                        measureNodes.build(),
                        after,
                        subsetMap,
                        allRows,
                        partitionKeys,
                        orders,
                        intervalNode);
        bb.setRoot(rel, false);
    }

    protected void convertPivot(Blackboard bb, SqlPivot pivot) {
        final SqlValidatorScope scope = validator().getJoinScope(pivot);

        final Blackboard pivotBb = createBlackboard(scope, null, false);

        // Convert input
        convertFrom(pivotBb, pivot.query);
        final RelNode input = pivotBb.root();

        final RelDataType inputRowType = input.getRowType();
        relBuilder.push(input);

        // Gather fields.
        final AggConverter aggConverter = new AggConverter(pivotBb, (AggregatingSelectScope) null);
        final Set<String> usedColumnNames = pivot.usedColumnNames();

        // 1. Gather group keys.
        inputRowType.getFieldList().stream()
                .filter(field -> !usedColumnNames.contains(field.getName()))
                .forEach(
                        field ->
                                aggConverter.addGroupExpr(
                                        new SqlIdentifier(field.getName(), SqlParserPos.ZERO)));

        // 2. Gather axes.
        pivot.axisList.forEach(aggConverter::addGroupExpr);

        // 3. Gather columns used as arguments to aggregate functions.
        pivotBb.agg = aggConverter;
        final List<@Nullable String> aggAliasList = new ArrayList<>();
        assert aggConverter.aggCalls.size() == 0;
        pivot.forEachAgg(
                (alias, call) -> {
                    call.accept(aggConverter);
                    aggAliasList.add(alias);
                    assert aggConverter.aggCalls.size() == aggAliasList.size();
                });
        pivotBb.agg = null;

        // Project the fields that we will need.
        relBuilder.project(
                Pair.left(aggConverter.getPreExprs()), Pair.right(aggConverter.getPreExprs()));

        // Build expressions.

        // 1. Build group key
        final RelBuilder.GroupKey groupKey =
                relBuilder.groupKey(
                        inputRowType.getFieldList().stream()
                                .filter(field -> !usedColumnNames.contains(field.getName()))
                                .map(
                                        field ->
                                                aggConverter.addGroupExpr(
                                                        new SqlIdentifier(
                                                                field.getName(),
                                                                SqlParserPos.ZERO)))
                                .collect(ImmutableBitSet.toImmutableBitSet()));

        // 2. Build axes, for example
        // FOR (axis1, axis2 ...) IN ...
        final List<RexNode> axes = new ArrayList<>();
        for (SqlNode axis : pivot.axisList) {
            axes.add(relBuilder.field(aggConverter.addGroupExpr(axis)));
        }

        // 3. Build aggregate expressions, for example
        // PIVOT (sum(a) AS alias1, min(b) AS alias2, ... FOR ... IN ...)
        final List<RelBuilder.AggCall> aggCalls = new ArrayList<>();
        Pair.forEach(
                aggAliasList,
                aggConverter.aggCalls,
                (alias, aggregateCall) ->
                        aggCalls.add(relBuilder.aggregateCall(aggregateCall).as(alias)));

        // 4. Build values, for example
        // IN ((v11, v12, ...) AS label1, (v21, v22, ...) AS label2, ...)
        final ImmutableList.Builder<Pair<String, List<RexNode>>> valueList =
                ImmutableList.builder();
        pivot.forEachNameValues(
                (alias, nodeList) ->
                        valueList.add(
                                Pair.of(
                                        alias,
                                        nodeList.stream()
                                                .map(bb::convertExpression)
                                                .collect(Util.toImmutableList()))));

        final RelNode rel = relBuilder.pivot(groupKey, aggCalls, axes, valueList.build()).build();
        bb.setRoot(rel, true);
    }

    protected void convertUnpivot(Blackboard bb, SqlUnpivot unpivot) {
        final SqlValidatorScope scope = validator().getJoinScope(unpivot);

        final Blackboard unpivotBb = createBlackboard(scope, null, false);

        // Convert input
        convertFrom(unpivotBb, unpivot.query);
        final RelNode input = unpivotBb.root();
        relBuilder.push(input);

        final List<String> measureNames =
                unpivot.measureList.stream()
                        .map(node -> ((SqlIdentifier) node).getSimple())
                        .collect(Util.toImmutableList());
        final List<String> axisNames =
                unpivot.axisList.stream()
                        .map(node -> ((SqlIdentifier) node).getSimple())
                        .collect(Util.toImmutableList());
        final ImmutableList.Builder<Pair<List<RexLiteral>, List<RexNode>>> axisMap =
                ImmutableList.builder();
        unpivot.forEachNameValues(
                (nodeList, valueList) -> {
                    if (valueList == null) {
                        valueList =
                                new SqlNodeList(
                                        Collections.nCopies(
                                                axisNames.size(),
                                                SqlLiteral.createCharString(
                                                        SqlUnpivot.aliasValue(nodeList),
                                                        SqlParserPos.ZERO)),
                                        SqlParserPos.ZERO);
                    }
                    final List<RexLiteral> literals = new ArrayList<>();
                    Pair.forEach(
                            valueList,
                            unpivot.axisList,
                            (value, axis) -> {
                                final RelDataType type = validator().getValidatedNodeType(axis);
                                literals.add(convertLiteral((SqlLiteral) value, bb, type));
                            });
                    final List<RexNode> nodes =
                            nodeList.stream()
                                    .map(unpivotBb::convertExpression)
                                    .collect(Util.toImmutableList());
                    axisMap.add(Pair.of(literals, nodes));
                });
        relBuilder.unpivot(unpivot.includeNulls, measureNames, axisNames, axisMap.build());
        relBuilder.convert(getNamespace(unpivot).getRowType(), false);

        bb.setRoot(relBuilder.build(), true);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换Identifier
     * @param bb  查询构建或处理过程中的 Blackboard，用于存储和传递中间结果
     * @param id // SQL标识符，可能是表名或列名（但在此上下文中更可能是表名
     * @param extendedColumns // 可选的扩展列列表，用于在表的基础上添加额外的列
     * @param tableHints // 可选的表提示列表，
     * @param schemaVersion
     */
    private void convertIdentifier(
            Blackboard bb,
            SqlIdentifier id,
            @Nullable SqlNodeList extendedColumns,
            @Nullable SqlNodeList tableHints,
            @Nullable SchemaVersion schemaVersion) {
        // 根据SqlIdentifier解析命名空间，并获取其对应的节点
        final SqlValidatorNamespace fromNamespace = getNamespace(id).resolve();
        // 如果命名空间已经解析为一个节点（可能是表或视图），则直接转换该节点
        if (fromNamespace.getNode() != null) {
            convertFrom(bb, fromNamespace.getNode());
            return;
        }
        // 如果没有直接解析到节点，则尝试从数据集栈中获取数据集名称（如果可用）
        final String datasetName = datasetStack.isEmpty() ? null : datasetStack.peek();
        // 用于标记是否使用了数据集名称的布尔数组
        final boolean[] usedDataset = {false};
        // ----- FLINK MODIFICATION BEGIN -----
        // 使用Flink特定的逻辑来获取RelOptTable，这可能需要考虑架构版本和数据集名称
        /**
         * 1.namespace.unwrap:返回此命名空间或包装命名空间，将其转换为特定类。
         * 2.getRelOptTable:RelOptTable
         */
        RelOptTable table =
                SqlValidatorUtil.getRelOptTable(
                        fromNamespace,
                        schemaVersion == null
                                ? catalogReader
                                : new FlinkCalciteCatalogSnapshotReader(
                                        catalogReader,
                                        catalogReader.getTypeFactory(),
                                        schemaVersion),
                        datasetName,
                        usedDataset);
        // ----- FLINK MODIFICATION END -----
        // 断言table不为null，确保我们能够继续处理
        assert table != null : "getRelOptTable returned null for " + fromNamespace;
        // 如果存在扩展列，则将这些列添加到表中
        if (extendedColumns != null && extendedColumns.size() > 0) {
            final SqlValidatorTable validatorTable = table.unwrapOrThrow(SqlValidatorTable.class);
            final List<RelDataTypeField> extendedFields =
                    SqlValidatorUtil.getExtendedColumns(validator, validatorTable, extendedColumns);
            table = table.extend(extendedFields);
        }
        // Review Danny 2020-01-13: hacky to construct a new table scan
        // in order to apply the hint strategies.
        // 应用提示策略到表提示上，并基于这些提示和表信息创建一个新的表扫描RelNode
        final List<RelHint> hints =
                hintStrategies.apply(
                        SqlUtil.getRelHint(hintStrategies, tableHints),
                        LogicalTableScan.create(cluster, table, ImmutableList.of()));
        // 将表和提示转换为RelNode，这里假设toRel方法能够处理表和提示并生成相应的RelNode
        final RelNode tableRel = toRel(table, hints);
        //将根节点设置为tableRel
        bb.setRoot(tableRel, true);
         // 如果当前根节点是一个纯排序操作（即没有其他逻辑操作，只有排序），并且满足某些条件（如子查询中可以移除排序），则移除排序
        if (RelOptUtil.isPureOrder(castNonNull(bb.root)) && removeSortInSubQuery(bb.top)) {
            // 移除排序，将根节点设置为排序操作的输入节点
            bb.setRoot(castNonNull(bb.root).getInput(0), true);
        }
        // 如果在处理过程中使用了数据集名称（通过usedDataset数组标记），则将数据集名称设置到黑板上
        if (usedDataset[0]) {
            bb.setDataset(datasetName);
        }
    }

    protected void convertCollectionTable(Blackboard bb, SqlCall call) {
        final SqlOperator operator = call.getOperator();
        if (operator == SqlStdOperatorTable.TABLESAMPLE) {
            final String sampleName = SqlLiteral.unchain(call.operand(0)).getValueAs(String.class);
            datasetStack.push(sampleName);
            SqlCall cursorCall = call.operand(1);
            SqlNode query = cursorCall.operand(0);
            RelNode converted = convertQuery(query, false, false).rel;
            bb.setRoot(converted, false);
            datasetStack.pop();
            return;
        }
        replaceSubQueries(bb, call, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // Expand table macro if possible. It's more efficient than
        // LogicalTableFunctionScan.
        final SqlCallBinding callBinding =
                new SqlCallBinding(bb.scope().getValidator(), bb.scope, call);
        if (operator instanceof SqlUserDefinedTableMacro) {
            final SqlUserDefinedTableMacro udf = (SqlUserDefinedTableMacro) operator;
            final TranslatableTable table = udf.getTable(callBinding);
            final RelDataType rowType = table.getRowType(typeFactory);
            CalciteSchema schema =
                    Schemas.subSchema(
                            catalogReader.getRootSchema(), udf.getNameAsId().skipLast(1).names);
            TableExpressionFactory expressionFunction =
                    clazz ->
                            Schemas.getTableExpression(
                                    Objects.requireNonNull(schema, "schema").plus(),
                                    Util.last(udf.getNameAsId().names),
                                    table,
                                    clazz);
            RelOptTable relOptTable =
                    RelOptTableImpl.create(
                            null, rowType, udf.getNameAsId().names, table, expressionFunction);
            RelNode converted = toRel(relOptTable, ImmutableList.of());
            bb.setRoot(converted, true);
            return;
        }

        Type elementType;
        if (operator instanceof SqlUserDefinedTableFunction) {
            SqlUserDefinedTableFunction udtf = (SqlUserDefinedTableFunction) operator;
            elementType = udtf.getElementType(callBinding);
        } else {
            elementType = null;
        }

        RexNode rexCall = bb.convertExpression(call);
        final List<RelNode> inputs = bb.retrieveCursors();
        Set<RelColumnMapping> columnMappings = getColumnMappings(operator);

        LogicalTableFunctionScan callRel =
                LogicalTableFunctionScan.create(
                        cluster,
                        inputs,
                        rexCall,
                        elementType,
                        validator().getValidatedNodeType(call),
                        columnMappings);

        bb.setRoot(callRel, true);
        afterTableFunction(bb, call, callRel);
    }

    protected void afterTableFunction(
            SqlToRelConverter.Blackboard bb, SqlCall call, LogicalTableFunctionScan callRel) {}

    private void convertTemporalTable(Blackboard bb, SqlCall call) {
        final SqlSnapshot snapshot = (SqlSnapshot) call;
        final RexNode period = bb.convertExpression(snapshot.getPeriod());

        // convert inner query, could be a table name or a derived table
        SqlNode expr = snapshot.getTableRef();
        // ----- FLINK MODIFICATION BEGIN -----
        SqlNode tableRef = snapshot.getTableRef();
        // since we have reduced the period of SqlSnapshot in the validate phase, we only need to
        // check whether the period is a RexLiteral.
        // in most cases, tableRef is a SqlBasicCall and the first operand is a SqlIdentifier.
        // when using SQL Hints, tableRef will be a SqlTableRef.
        if (((tableRef instanceof SqlBasicCall
                                && ((SqlBasicCall) tableRef).operand(0) instanceof SqlIdentifier)
                        || (tableRef instanceof SqlTableRef))
                && period instanceof RexLiteral) {
            TableConfig tableConfig = ShortcutUtils.unwrapContext(relBuilder).getTableConfig();
            ZoneId zoneId = tableConfig.getLocalTimeZone();
            TimestampString timestampString =
                    ((RexLiteral) period).getValueAs(TimestampString.class);
            checkNotNull(
                    timestampString,
                    "The time travel expression %s can not convert to a valid timestamp string. This is a bug. Please file an issue.",
                    period);

            long timeTravelTimestamp =
                    TimestampData.fromEpochMillis(timestampString.getMillisSinceEpoch())
                            .toLocalDateTime()
                            .atZone(zoneId)
                            .toInstant()
                            .toEpochMilli();
            SqlIdentifier sqlIdentifier =
                    tableRef instanceof SqlBasicCall
                            ? ((SqlBasicCall) tableRef).operand(0)
                            : ((SqlTableRef) tableRef).operand(0);
            SchemaVersion schemaVersion = TimestampSchemaVersion.of(timeTravelTimestamp);
            convertIdentifier(bb, sqlIdentifier, null, null, schemaVersion);
        } else {
            convertFrom(bb, expr);
        }
        // ----- FLINK MODIFICATION END -----

        final RelNode snapshotRel = relBuilder.push(bb.root()).snapshot(period).build();

        bb.setRoot(snapshotRel, false);
    }

    private static @Nullable Set<RelColumnMapping> getColumnMappings(SqlOperator op) {
        SqlReturnTypeInference rti = op.getReturnTypeInference();
        if (rti == null) {
            return null;
        }
        if (rti instanceof TableFunctionReturnTypeInference) {
            TableFunctionReturnTypeInference tfrti = (TableFunctionReturnTypeInference) rti;
            return tfrti.getColumnMappings();
        } else {
            return null;
        }
    }

    /**
     * Shuttle that replace outer {@link RexInputRef} with {@link RexFieldAccess}, and adjust {@code
     * offset} to each inner {@link RexInputRef} in the lateral join condition.
     */
    private static class RexAccessShuttle extends RexShuttle {
        private final RexBuilder builder;
        private final RexCorrelVariable rexCorrel;
        private final BitSet varCols = new BitSet();

        RexAccessShuttle(RexBuilder builder, RexCorrelVariable rexCorrel) {
            this.builder = builder;
            this.rexCorrel = rexCorrel;
        }

        @Override
        public RexNode visitInputRef(RexInputRef input) {
            int i = input.getIndex() - rexCorrel.getType().getFieldCount();
            if (i < 0) {
                varCols.set(input.getIndex());
                return builder.makeFieldAccess(rexCorrel, input.getIndex());
            }
            return builder.makeInputRef(input.getType(), i);
        }
    }

    protected RelNode createJoin(
            Blackboard bb,
            RelNode leftRel,
            RelNode rightRel,
            RexNode joinCond,
            JoinRelType joinType) {
        assert joinCond != null;

        final CorrelationUse p = getCorrelationUse(bb, rightRel);
        if (p != null) {
            RelNode innerRel = p.r;
            ImmutableBitSet requiredCols = p.requiredColumns;

            if (!joinCond.isAlwaysTrue()) {
                final RelFactories.FilterFactory factory = RelFactories.DEFAULT_FILTER_FACTORY;
                final RexCorrelVariable rexCorrel =
                        (RexCorrelVariable) rexBuilder.makeCorrel(leftRel.getRowType(), p.id);
                final RexAccessShuttle shuttle = new RexAccessShuttle(rexBuilder, rexCorrel);

                // Replace outer RexInputRef with RexFieldAccess,
                // and push lateral join predicate into inner child
                final RexNode newCond = joinCond.accept(shuttle);
                innerRel = factory.createFilter(p.r, newCond, ImmutableSet.of());
                requiredCols = ImmutableBitSet.fromBitSet(shuttle.varCols).union(p.requiredColumns);
            }

            return LogicalCorrelate.create(
                    leftRel, innerRel, ImmutableList.of(), p.id, requiredCols, joinType);
        }

        final RelNode node =
                relBuilder.push(leftRel).push(rightRel).join(joinType, joinCond).build();

        // If join conditions are pushed down, update the leaves.
        if (node instanceof Project) {
            final Join newJoin = (Join) node.getInputs().get(0);
            if (leaves.containsKey(leftRel)) {
                leaves.put(newJoin.getLeft(), leaves.get(leftRel));
            }
            if (leaves.containsKey(rightRel)) {
                leaves.put(newJoin.getRight(), leaves.get(rightRel));
            }
        }
        return node;
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 获取一个关系表达式（RelNode）或其子表达式中使用的变量集合。
     */
    private @Nullable CorrelationUse getCorrelationUse(Blackboard bb, final RelNode r0) {
        // 获取在RelNode r0中使用的所有关联变量
        final Set<CorrelationId> correlatedVariables = RelOptUtil.getVariablesUsed(r0);
        // 如果没有使用任何关联变量，则返回null
        if (correlatedVariables.isEmpty()) {
            return null;
        }
        // 用于构建所需列集合的ImmutableBitSet构建器
        final ImmutableBitSet.Builder requiredColumns = ImmutableBitSet.builder();
        // 存储关联变量名称的列表
        final List<CorrelationId> correlNames = new ArrayList<>();

        // All correlations must refer the same namespace since correlation
        // produces exactly one correlation source.
        // The same source might be referenced by different variables since
        // DeferredLookups are not de-duplicated at create time.
        // 所有关联必须引用相同的命名空间，因为关联操作仅产生一个关联源
        // 相同的源可能由不同的变量引用，因为延迟查找在创建时不会被去重
        SqlValidatorNamespace prevNs = null;

        for (CorrelationId correlName : correlatedVariables) {
            // 从映射中获取与关联变量对应的延迟查找对象
            DeferredLookup lookup =
                    requireNonNull(
                            mapCorrelToDeferred.get(correlName),
                            () -> "correlation variable is not found: " + correlName);
            // 获取关联变量对应的字段访问表达式
            RexFieldAccess fieldAccess = lookup.getFieldAccess(correlName);
            // 获取原始关系名（即表名或别名）
            String originalRelName = lookup.getOriginalRelName();
            // 获取字段名
            String originalFieldName = fieldAccess.getField().getName();

            // 获取名称匹配器，用于解析名称
            final SqlNameMatcher nameMatcher = bb.getValidator().getCatalogReader().nameMatcher();
            // 创建一个解析器实例，用于解析命名空间
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            // 使用lookup对象的Blackboard和原始关系名进行解析
            lookup.bb
                    .scope()
                    .resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);
            // 断言解析结果只有一个，因为关联变量应该唯一对应一个命名空间
            assert resolved.count() == 1;
            // 获取解析结果中的命名空间、行类型和路径信息
            final SqlValidatorScope.Resolve resolve = resolved.only();
            final SqlValidatorNamespace foundNs = resolve.namespace;
            final RelDataType rowType = resolve.rowType();
            // 获取在路径中引用的子命名空间的索引
            final int childNamespaceIndex = resolve.path.steps().get(0).i;
            // 获取祖先作用域，这通常用于进一步的上下文或作用域查询
            final SqlValidatorScope ancestorScope = resolve.scope;
            // 检查当前作用域是否包含该关联的祖先作用域
            boolean correlInCurrentScope = bb.scope().isWithin(ancestorScope);
            // 如果关联不在当前作用域内，则跳过当前循环迭代
            if (!correlInCurrentScope) {
                continue;
            }
            // 检查是否已经设置了前一个命名空间，如果没有，则设置它
            if (prevNs == null) {
                prevNs = foundNs;
            } else {
                // 断言所有关联变量都应解析到同一个命名空间
                // 如果不是，则抛出AssertionError
                assert prevNs == foundNs
                        : "All correlation variables should resolve"
                                + " to the same namespace."
                                + " Prev ns="
                                + prevNs
                                + ", new ns="
                                + foundNs;
            }
            // 计算命名空间偏移量，用于处理多层嵌套的情况
            int namespaceOffset = 0;
            if (childNamespaceIndex > 0) {
                // If not the first child, need to figure out the width
                // of output types from all the preceding namespaces
                // 如果不是第一个子命名空间，则需要计算前面所有命名空间的输出类型宽度
                assert ancestorScope instanceof ListScope;
                List<SqlValidatorNamespace> children = ((ListScope) ancestorScope).getChildren();

                // 遍历所有前面的子命名空间，并累加它们的字段数
                for (int i = 0; i < childNamespaceIndex; i++) {
                    SqlValidatorNamespace child = children.get(i);
                    namespaceOffset += child.getRowType().getFieldCount();
                }
            }
            // 找到顶级字段访问表达式，即最外层的字段访问
            RexFieldAccess topLevelFieldAccess = fieldAccess;
            while (topLevelFieldAccess.getReferenceExpr() instanceof RexFieldAccess) {
                topLevelFieldAccess = (RexFieldAccess) topLevelFieldAccess.getReferenceExpr();
            }
            // 根据顶级字段访问的索引和命名空间偏移量，从行类型中获取实际的字段
            final RelDataTypeField field =
                    rowType.getFieldList()
                            .get(topLevelFieldAccess.getField().getIndex() - namespaceOffset);
            // 计算该字段在整个输出行类型中的位置
            int pos = namespaceOffset + field.getIndex();
            // 断言顶级字段访问的字段类型与从行类型中获取的字段类型相同
            assert field.getType() == topLevelFieldAccess.getField().getType();

            // 断言位置pos不应该是-1，这表示我们成功找到了字段
            assert pos != -1;

            // bb.root is an aggregate and only projects group by
            // keys.
            // 假设bb.root是一个聚合操作，并且只投影了GROUP BY的键。
            Map<Integer, Integer> exprProjection = bb.mapRootRelToFieldProjection.get(bb.root);
            if (exprProjection != null) {
                // sub-query can reference group by keys projected from
                // the root of the outer relation.
                // 子查询可以引用从外部关系根节点投影出的GROUP BY键。
                // 尝试从映射中获取当前字段（由pos标识）的投影位置。
                Integer projection = exprProjection.get(pos);
                if (projection != null) {
                    // 如果找到了投影位置，则更新pos为该位置。
                    pos = projection;
                } else {
                    // correl not grouped
                    // 如果没有找到投影位置，说明当前字段不是GROUP BY表达式的一部分。
                    throw new AssertionError(
                            "Identifier '"
                                    + originalRelName
                                    + "."
                                    + originalFieldName
                                    + "' is not a group expr");
                }
            }
            // 将需要的列位置（由pos标识）标记在requiredColumns集合中。
            requiredColumns.set(pos);
            // 将关联变量的名称添加到correlNames列表中。
            correlNames.add(correlName);
        }
        // 检查是否有任何关联变量起源于当前作用域。
        if (correlNames.isEmpty()) {
            // None of the correlating variables originated in this scope.
            // 如果没有关联变量起源于当前作用域，则返回null。
            return null;
        }

        // 初始化r为原始关系节点r0。
        RelNode r = r0;
        // 如果关联变量列表中有多个相同的表被引用（即correlNames大小大于1），
        // 则需要进行去重处理。
        if (correlNames.size() > 1) {
            // The same table was referenced more than once.
            // So we deduplicate.
            // 调用DeduplicateCorrelateVariables工具类的方法来处理去重，
            // 并返回一个新的关系节点r。
            r =
                    DeduplicateCorrelateVariables.go(
                            rexBuilder, correlNames.get(0), Util.skip(correlNames), r0);
            // Add new node to leaves.
            // 将新的关系节点r及其字段数添加到leaves映射中。
            leaves.put(r, r.getRowType().getFieldCount());
        }
        // 返回一个新的CorrelationUse实例，表示当前作用域中关联变量的使用情况。
        return new CorrelationUse(correlNames.get(0), requiredColumns.build(), r);
    }

    /**
     * Determines whether a sub-query is non-correlated. Note that a non-correlated sub-query can
     * contain correlated references, provided those references do not reference select statements
     * that are parents of the sub-query.
     *
     * @param subq the sub-query
     * @param bb blackboard used while converting the sub-query, i.e., the blackboard of the parent
     *     query of this sub-query
     * @return true if the sub-query is non-correlated
     */
    private boolean isSubQueryNonCorrelated(RelNode subq, Blackboard bb) {
        Set<CorrelationId> correlatedVariables = RelOptUtil.getVariablesUsed(subq);
        for (CorrelationId correlName : correlatedVariables) {
            DeferredLookup lookup =
                    requireNonNull(
                            mapCorrelToDeferred.get(correlName),
                            () -> "correlation variable is not found: " + correlName);
            String originalRelName = lookup.getOriginalRelName();

            final SqlNameMatcher nameMatcher =
                    lookup.bb.scope().getValidator().getCatalogReader().nameMatcher();
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            lookup.bb
                    .scope()
                    .resolve(ImmutableList.of(originalRelName), nameMatcher, false, resolved);

            SqlValidatorScope ancestorScope = resolved.only().scope;

            // If the correlated reference is in a scope that's "above" the
            // sub-query, then this is a correlated sub-query.
            SqlValidatorScope parentScope = bb.scope;
            do {
                if (ancestorScope == parentScope) {
                    return false;
                }
                if (parentScope instanceof DelegatingScope) {
                    parentScope = ((DelegatingScope) parentScope).getParent();
                } else {
                    break;
                }
            } while (parentScope != null);
        }
        return true;
    }

    /**
     * Returns a list of fields to be prefixed to each relational expression.
     *
     * @return List of system fields
     */
    protected List<RelDataTypeField> getSystemFields() {
        return Collections.emptyList();
    }

    private void convertJoin(Blackboard bb, SqlJoin join) {
        SqlValidator validator = validator();
        final SqlValidatorScope scope = validator.getJoinScope(join);
        final Blackboard fromBlackboard = createBlackboard(scope, null, false);
        SqlNode left = join.getLeft();
        SqlNode right = join.getRight();
        final SqlValidatorScope leftScope =
                Util.first(
                        validator.getJoinScope(left), ((DelegatingScope) bb.scope()).getParent());
        final Blackboard leftBlackboard = createBlackboard(leftScope, null, false);
        final SqlValidatorScope rightScope =
                Util.first(
                        validator.getJoinScope(right), ((DelegatingScope) bb.scope()).getParent());
        final Blackboard rightBlackboard = createBlackboard(rightScope, null, false);
        convertFrom(leftBlackboard, left);
        final RelNode leftRel = requireNonNull(leftBlackboard.root, "leftBlackboard.root");
        convertFrom(rightBlackboard, right);
        final RelNode tempRightRel = requireNonNull(rightBlackboard.root, "rightBlackboard.root");

        final JoinConditionType conditionType = join.getConditionType();
        final RexNode condition;
        final RelNode rightRel;
        if (join.isNatural()) {
            condition = convertNaturalCondition(getNamespace(left), getNamespace(right));
            rightRel = tempRightRel;
        } else {
            switch (conditionType) {
                case NONE:
                    condition = rexBuilder.makeLiteral(true);
                    rightRel = tempRightRel;
                    break;
                case USING:
                    condition =
                            convertUsingCondition(join, getNamespace(left), getNamespace(right));
                    rightRel = tempRightRel;
                    break;
                case ON:
                    Pair<RexNode, RelNode> conditionAndRightNode =
                            convertOnCondition(fromBlackboard, join, leftRel, tempRightRel);
                    condition = conditionAndRightNode.left;
                    rightRel = conditionAndRightNode.right;
                    break;
                default:
                    throw Util.unexpected(conditionType);
            }
        }
        final RelNode joinRel =
                createJoin(
                        fromBlackboard,
                        leftRel,
                        rightRel,
                        condition,
                        convertJoinType(join.getJoinType()));
        relBuilder.push(joinRel);
        final RelNode newProjectRel = relBuilder.project(relBuilder.fields()).build();
        bb.setRoot(newProjectRel, false);
    }

    private RexNode convertNaturalCondition(
            SqlValidatorNamespace leftNamespace, SqlValidatorNamespace rightNamespace) {
        final List<String> columnList =
                SqlValidatorUtil.deriveNaturalJoinColumnList(
                        catalogReader.nameMatcher(),
                        leftNamespace.getRowType(),
                        rightNamespace.getRowType());
        return convertUsing(leftNamespace, rightNamespace, columnList);
    }

    private RexNode convertUsingCondition(
            SqlJoin join,
            SqlValidatorNamespace leftNamespace,
            SqlValidatorNamespace rightNamespace) {
        final SqlNodeList list =
                (SqlNodeList)
                        requireNonNull(join.getCondition(), () -> "getCondition for join " + join);
        return convertUsing(
                leftNamespace,
                rightNamespace,
                ImmutableList.copyOf(SqlIdentifier.simpleNames(list)));
    }

    /**
     * This currently does not expand correlated full outer joins correctly. Replaying on the right
     * side to correctly support left joins multiplicities.
     *
     * <blockquote>
     *
     * <pre>
     *   SELECT *
     *   FROM t1
     *   LEFT JOIN t2 ON
     *    EXIST(SELECT t3.c3 WHERE t1.c1 = t3.c1 AND t2.c2 = t3.c2)
     *    AND NOT (t2.t2 = 2)
     * </pre>
     *
     * </blockquote>
     *
     * <p>Given the de-correlated query produces:
     *
     * <blockquote>
     *
     * <pre>
     *  t1.c1 | t2.c2
     *  ------+------
     *    1   |  1
     *    1   |  2
     * </pre>
     *
     * </blockquote>
     *
     * <p>If correlated query was replayed on the left side, then an extra rows would be emitted for
     * every {code t1.c1 = 1}, where it failed to join to right side due to {code NOT(t2.t2 = 2)}.
     * However, if the query is joined on the right, side multiplicity is maintained.
     */
    private Pair<RexNode, RelNode> convertOnCondition(
            Blackboard bb, SqlJoin join, RelNode leftRel, RelNode rightRel) {
        SqlNode condition =
                requireNonNull(join.getCondition(), () -> "getCondition for join " + join);

        bb.setRoot(ImmutableList.of(leftRel, rightRel));
        replaceSubQueries(bb, condition, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
        final RelNode newRightRel =
                bb.root == null || bb.registered.size() == 0 ? rightRel : bb.reRegister(rightRel);
        bb.setRoot(ImmutableList.of(leftRel, newRightRel));
        RexNode conditionExp = bb.convertExpression(condition);
        if (conditionExp instanceof RexInputRef && newRightRel != rightRel) {
            int leftFieldCount = leftRel.getRowType().getFieldCount();
            List<RelDataTypeField> rightFieldList = newRightRel.getRowType().getFieldList();
            int rightFieldCount = newRightRel.getRowType().getFieldCount();
            conditionExp =
                    rexBuilder.makeInputRef(
                            rightFieldList.get(rightFieldCount - 1).getType(),
                            leftFieldCount + rightFieldCount - 1);
        }
        return Pair.of(conditionExp, newRightRel);
    }

    /**
     * Returns an expression for matching columns of a USING clause or inferred from NATURAL JOIN.
     * "a JOIN b USING (x, y)" becomes "a.x = b.x AND a.y = b.y". Returns null if the column list is
     * empty.
     *
     * @param leftNamespace Namespace of left input to join
     * @param rightNamespace Namespace of right input to join
     * @param nameList List of column names to join on
     * @return Expression to match columns from name list, or true if name list is empty
     */
    private RexNode convertUsing(
            SqlValidatorNamespace leftNamespace,
            SqlValidatorNamespace rightNamespace,
            List<String> nameList) {
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        final List<RexNode> list = new ArrayList<>();
        for (String name : nameList) {
            List<RexNode> operands = new ArrayList<>();
            int offset = 0;
            for (SqlValidatorNamespace n : ImmutableList.of(leftNamespace, rightNamespace)) {
                final RelDataType rowType = n.getRowType();
                final RelDataTypeField field = nameMatcher.field(rowType, name);
                assert field != null
                        : "field " + name + " is not found in " + rowType + " with " + nameMatcher;
                operands.add(rexBuilder.makeInputRef(field.getType(), offset + field.getIndex()));
                offset += rowType.getFieldList().size();
            }
            list.add(rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, operands));
        }
        return RexUtil.composeConjunction(rexBuilder, list);
    }

    private static JoinRelType convertJoinType(JoinType joinType) {
        switch (joinType) {
            case COMMA:
            case INNER:
            case CROSS:
                return JoinRelType.INNER;
            case FULL:
                return JoinRelType.FULL;
            case LEFT:
                return JoinRelType.LEFT;
            case RIGHT:
                return JoinRelType.RIGHT;
            default:
                throw Util.unexpected(joinType);
        }
    }

    /**
     * Converts the SELECT, GROUP BY and HAVING clauses of an aggregate query.
     *
     * <p>This method extracts SELECT, GROUP BY and HAVING clauses, and creates an {@link
     * AggConverter}, then delegates to {@link #createAggImpl}. Derived class may override this
     * method to change any of those clauses or specify a different {@link AggConverter}.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Query
     * @param orderExprList Additional expressions needed to implement ORDER BY
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 转换聚合查询中的SELECT、GROUP BY和HAVING子句。
     */
    protected void convertAgg(Blackboard bb, SqlSelect select, List<SqlNode> orderExprList) {
        // 断言确保bb.root不为空，这是前置条件
        assert bb.root != null : "precondition: child != null";
        // 获取GROUP BY子句中的列或表达式列表
        SqlNodeList groupList = select.getGroup();
        // 获取SELECT子句中的列或表达式列表
        SqlNodeList selectList = select.getSelectList();
        // 获取HAVING子句（如果有的话）
        SqlNode having = select.getHaving();
        // 创建一个AggConverter对象，用于后续的聚合转换处理
        final AggConverter aggConverter = new AggConverter(bb, select);
        // 调用createAggImpl方法，传入必要的参数以进行实际的聚合转换逻辑
        createAggImpl(bb, aggConverter, selectList, groupList, having, orderExprList);
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 解析聚合操作
     */
    protected final void createAggImpl(
            Blackboard bb,
            final AggConverter aggConverter,
            SqlNodeList selectList,
            @Nullable SqlNodeList groupList,
            @Nullable SqlNode having,
            List<SqlNode> orderExprList) {
        // Find aggregate functions in SELECT and HAVING clause
        // 在SELECT和HAVING子句中查找聚合函数
        final AggregateFinder aggregateFinder = new AggregateFinder();
        // 遍历SELECT子句中的每个节点，查找聚合函数
        selectList.accept(aggregateFinder);
        if (having != null) {
            // 如果存在HAVING子句，则遍历HAVING子句中的每个节点，查找聚合函数
            having.accept(aggregateFinder);
        }

        // first replace the sub-queries inside the aggregates
        // because they will provide input rows to the aggregates.
        // 首先替换聚合函数内部的子查询
        // 因为这些子查询将为聚合函数提供输入行。
        replaceSubQueries(bb, aggregateFinder.list, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // also replace sub-queries inside filters in the aggregates
        // 替换聚合函数过滤器中的子查询
        replaceSubQueries(bb, aggregateFinder.filterList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // also replace sub-queries inside ordering spec in the aggregates
        // 替换聚合函数排序规范中的子查询
        replaceSubQueries(bb, aggregateFinder.orderList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // If group-by clause is missing, pretend that it has zero elements.
        // 如果缺少GROUP BY子句，设置为空元素列表。
        if (groupList == null) {
            groupList = SqlNodeList.EMPTY;
        }
        // 替换GROUP BY列表中的子查询，虽然通常GROUP BY中不包含子查询此调用是为了某种形式上预处理或标准化）
        replaceSubQueries(bb, groupList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        // register the group exprs

        // build a map to remember the projections from the top scope to the
        // output of the current root.
        //
        // Calcite allows expressions, not just column references in
        // group by list. This is not SQL 2003 compliant, but hey.

        // 注册GROUP BY表达式
        // 这可能涉及将GROUP BY表达式添加到某种上下文中，以便在后续的聚合处理中使用
        // 构建一个映射，用于记住从顶层作用域到当前根输出的投影
        // Calcite允许在GROUP BY列表中使用表达式，而不仅仅是列引用，这与SQL 2003标准不完全兼容，但这是Calcite的特性。

        final AggregatingSelectScope scope =
                requireNonNull(aggConverter.aggregatingSelectScope, "aggregatingSelectScope");
        final AggregatingSelectScope.Resolved r = scope.resolved.get();
        for (SqlNode groupExpr : r.groupExprList) {
            // 将GROUP BY表达式添加到聚合转换器中，以便在生成聚合查询时考虑这些表达式
            aggConverter.addGroupExpr(groupExpr);
        }

        // 初始化HAVING表达式的RexNode表示（RexNode是Calcite中用于表示表达式的内部数据结构）
        final RexNode havingExpr;
        // 初始化一个列表，用于存储从原始查询到聚合查询结果的投影对（RexNode和列名）
        final List<Pair<RexNode, String>> projects = new ArrayList<>();

        try {
            // 确保当前不在聚合模式下，以避免状态冲突
            Preconditions.checkArgument(bb.agg == null, "already in agg mode");
            // 设置当前处于聚合模式，并将聚合转换器分配给当前的黑板（Blackboard）对象
            bb.agg = aggConverter;

            // convert the select and having expressions, so that the
            // agg converter knows which aggregations are required
            // 转换SELECT和HAVING表达式，以便聚合转换器知道需要哪些聚合操作
            // 这可能涉及将SQL表达式转换为Calcite可以理解的内部表示（RexNode）
            selectList.accept(aggConverter);
            // Assert we don't have dangling items left in the stack
            // 断言确保在处理SELECT表达式后，没有遗留的未处理项
            assert !aggConverter.inOver;
            // 对ORDER BY列表中的每个表达式执行相同的操作
            for (SqlNode expr : orderExprList) {
                expr.accept(aggConverter);
                // 再次断言，确保没有遗留的未处理项
                assert !aggConverter.inOver;
            }
            // 如果存在HAVING子句，则处理它
            if (having != null) {
                having.accept(aggConverter);
                // 再次断言，确保没有遗留的未处理项
                assert !aggConverter.inOver;
            }

            // compute inputs to the aggregator
            // 计算聚合器所需的输入
            List<Pair<RexNode, @Nullable String>> preExprs = aggConverter.getPreExprs();

            if (preExprs.size() == 0) {
                // Special case for COUNT(*), where we can end up with no inputs
                // at all.  The rest of the system doesn't like 0-tuples, so we
                // select a dummy constant here.
                // 特殊情况处理：当使用COUNT(*)且没有其他输入时，整个系统可能不喜欢处理0元组（即没有字段的元组）。
                // 因此，我们在这里选择一个哑元常量（dummy constant），通常是0，作为输入。
                final RexNode zero = rexBuilder.makeExactLiteral(BigDecimal.ZERO);
                preExprs = ImmutableList.of(Pair.of(zero, null));
            }

            // 获取当前查询的根节点（RelNode），这是构建新查询的基础。
            final RelNode inputRel = bb.root();

            // Project the expressions required by agg and having.
            // 投影出agg和having所需的表达式。这意味着我们将从原始查询中选取一部分表达式，
            bb.setRoot(
                    relBuilder
                            .push(inputRel)
                            .projectNamed(Pair.left(preExprs), Pair.right(preExprs), false)
                            .build(),
                    false);
            // 将新的根节点与其字段投影映射关系存储起来，以便后续使用。
            bb.mapRootRelToFieldProjection.put(bb.root(), r.groupExprProjection);

            // REVIEW jvs 31-Oct-2007:  doesn't the declaration of
            // monotonicity here assume sort-based aggregation at
            // the physical level?

            // Tell bb which of group columns are sorted.
            // 清除现有的列单调性信息，并重新根据GROUP BY列表中的元素设置列单调性。
           // 列单调性可能用于优化查询，比如判断某个列是否在整个数据集上单调递增或递减。
            bb.columnMonotonicities.clear();
            for (SqlNode groupItem : groupList) {
                bb.columnMonotonicities.add(bb.scope().getMonotonicity(groupItem));
            }

            // Add the aggregator
            // 添加聚合器到查询中。这通常意味着在查询中插入一个聚合节点，
           // 该节点将根据GROUP BY子句和聚合函数对输入数据进行分组和聚合操作。
            bb.setRoot(
                    createAggregate(
                            bb, r.groupSet, r.groupSets.asList(), aggConverter.getAggCalls()),
                    false);
            // 更新根节点与字段投影映射关系。
            bb.mapRootRelToFieldProjection.put(bb.root(), r.groupExprProjection);

            // Replace sub-queries in having here and modify having to use
            // the replaced expressions
            // 如果存在HAVING子句，则对其进行处理。
            // 首先，将HAVING子句中的NOT IN子查询下推（如果有的话），然后替换HAVING子句中的子查询。
            // 注意，这里的替换逻辑可能有所不同，具体取决于将UNKNOWN视为FALSE还是其他逻辑。
            if (having != null) {
                // 下推NOT IN子查询
                SqlNode newHaving = pushDownNotForIn(bb.scope(), having);
                // 替换子查询，并将UNKNOWN视为FALSE
                replaceSubQueries(bb, newHaving, RelOptUtil.Logic.UNKNOWN_AS_FALSE);
                // 将更新后的HAVING子句转换为RexNode，以便在查询执行时使用。
                havingExpr = bb.convertExpression(newHaving);
            } else {
                // 如果前面没有设置havingExpr（即没有HAVING子句），则默认将其设置为true的字面量。
                // 这意味着如果没有特定的HAVING条件，查询将不会基于HAVING子句过滤任何分组聚合的结果。
                havingExpr = relBuilder.literal(true);
            }

            // Now convert the other sub-queries in the select list.
            // This needs to be done separately from the sub-query inside
            // any aggregate in the select list, and after the aggregate rel
            // is allocated.
            // 现在转换SELECT列表中的其他子查询。
            // 这需要单独进行，以区别于SELECT列表中任何聚合函数内的子查询，并且应该在聚合关系（rel）分配之后进行。
            // 因为聚合操作可能改变了查询的上下文，使得子查询的解析方式有所不同。
            replaceSubQueries(bb, selectList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

            // Now sub-queries in the entire select list have been converted.
            // Convert the select expressions to get the final list to be
            // projected.
            // 现在，整个SELECT列表中的子查询都已经被转换。
            // 接下来，将SELECT表达式转换为最终要投影的列表。
            int k = 0;

            // For select expressions, use the field names previously assigned
            // by the validator. If we derive afresh, we might generate names
            // like "EXPR$2" that don't match the names generated by the
            // validator. This is especially the case when there are system
            // fields; system fields appear in the relnode's rowtype but do not
            // (yet) appear in the validator type.
            // 对于SELECT表达式，使用之前由验证器分配的字段名。
            // 如果我们重新推导字段名，可能会生成如"EXPR$2"这样的名称，这些名称可能与验证器生成的名称不匹配。
            // 特别是当存在系统字段时，系统字段会出现在关系节点的行类型中，但尚未（或不会）出现在验证器类型中。
            final SelectScope selectScope = SqlValidatorUtil.getEnclosingSelectScope(bb.scope);
            // 确保存在SELECT作用域
            assert selectScope != null;
            final SqlValidatorNamespace selectNamespace = getNamespaceOrNull(selectScope.getNode());
            // 确保SELECT命名空间不为空
            assert selectNamespace != null : "selectNamespace must not be null for " + selectScope;
            // 获取SELECT命名空间的行类型中的字段名列表
            final List<String> names = selectNamespace.getRowType().getFieldNames();
            // 计算系统字段的数量，这些字段在SELECT列表中但可能不在原始查询的字段列表中
            int sysFieldCount = selectList.size() - names.size();
            // 遍历SELECT列表中的每个表达式
            for (SqlNode expr : selectList) {
                // 将每个表达式转换为RexNode，并与对应的字段名（或推导出的别名）配对
                // 如果当前索引k小于系统字段数，则使用推导出的别名（可能是系统字段的别名）
                // 否则，从names列表中获取字段名
                projects.add(
                        Pair.of(
                                bb.convertExpression(expr),
                                k < sysFieldCount
                                        ? castNonNull(validator().deriveAlias(expr, k++))
                                        : names.get(k++ - sysFieldCount)));
            }
            // 遍历ORDER BY子句中的表达式列表
            for (SqlNode expr : orderExprList) {
                // 将每个ORDER BY表达式转换为RexNode，并为其生成一个别名（如果可能）
                // 然后将RexNode和别名作为一对添加到projects列表中
                // 这里projects列表可能用于后续的投影或排序操作
                projects.add(
                        Pair.of(
                                bb.convertExpression(expr),
                                castNonNull(validator().deriveAlias(expr, k++))));
            }
            // 无论前面的操作是否成功，都执行finally块中的代码
            // 这里主要是将bb（可能是某个查询构建器的上下文对象）的agg属性重置为null
            // 这可能是为了确保后续操作不会错误地使用之前设置的聚合上下文
        } finally {
            bb.agg = null;
        }

        // implement HAVING (we have already checked that it is non-trivial)
        // 实现HAVING子句（之前已经检查过HAVING子句不是简单的TRUE）
        // 首先，将根关系（可能是聚合后的结果）推送到relBuilder的栈上
        relBuilder.push(bb.root());
        // 如果havingExpr不为null，则应用HAVING条件作为过滤
        if (havingExpr != null) {
            relBuilder.filter(havingExpr);
        }

        // implement the SELECT list
        // 实现SELECT列表
        // 使用projects列表中的RexNode作为投影的源，并使用别名进行重命名
        // 注意：这里假设projects列表中的RexNode和别名是成对出现的
        relBuilder.project(Pair.left(projects), Pair.right(projects)).rename(Pair.right(projects));
        bb.setRoot(relBuilder.build(), false);

        // Tell bb which of group columns are sorted.
        // 告诉bb哪些分组列是有序的（单调性）
        // 这可能用于优化查询，特别是当分组列已经排序时
        bb.columnMonotonicities.clear();
        // 遍历SELECT列表中的每个项
        for (SqlNode selectItem : selectList) {
            // 为每个SELECT项添加其单调性信息到bb中
            // 单调性信息可能用于后续的查询优化，比如确定是否可以跳过某些排序操作
            bb.columnMonotonicities.add(bb.scope().getMonotonicity(selectItem));
        }
    }

    /**
     * Creates an Aggregate.
     *
     * <p>In case the aggregate rel changes the order in which it projects fields, the <code>
     * groupExprProjection</code> parameter is provided, and the implementation of this method may
     * modify it.
     *
     * <p>The <code>sortedCount</code> parameter is the number of expressions known to be monotonic.
     * These expressions must be on the leading edge of the grouping keys. The default
     * implementation of this method ignores this parameter.
     *
     * @param bb Blackboard
     * @param groupSet Bit set of ordinals of grouping columns
     * @param groupSets Grouping sets
     * @param aggCalls Array of calls to aggregate functions
     * @return LogicalAggregate
     */
    protected RelNode createAggregate(
            Blackboard bb,
            ImmutableBitSet groupSet,
            ImmutableList<ImmutableBitSet> groupSets,
            List<AggregateCall> aggCalls) {
        relBuilder.push(bb.root());
        final RelBuilder.GroupKey groupKey = relBuilder.groupKey(groupSet, groupSets);
        return relBuilder.aggregate(groupKey, aggCalls).build();
    }

    public RexDynamicParam convertDynamicParam(final SqlDynamicParam dynamicParam) {
        // REVIEW jvs 8-Jan-2005:  dynamic params may be encountered out of
        // order.  Should probably cross-check with the count from the parser
        // at the end and make sure they all got filled in.  Why doesn't List
        // have a resize() method?!?  Make this a utility.
        while (dynamicParam.getIndex() >= dynamicParamSqlNodes.size()) {
            dynamicParamSqlNodes.add(null);
        }

        dynamicParamSqlNodes.set(dynamicParam.getIndex(), dynamicParam);
        return rexBuilder.makeDynamicParam(
                getDynamicParamType(dynamicParam.getIndex()), dynamicParam.getIndex());
    }

    /**
     * Creates a list of collations required to implement the ORDER BY clause, if there is one.
     * Populates <code>extraOrderExprs</code> with any sort expressions which are not in the select
     * clause.
     *
     * @param bb Scope within which to resolve identifiers
     * @param select Select clause. Never null, because we invent a dummy SELECT if ORDER BY is
     *     applied to a set operation (UNION etc.)
     * @param orderList Order by clause, may be null
     * @param extraOrderExprs Sort expressions which are not in the select clause (output)
     * @param collationList List of collations (output)
     */
    protected void gatherOrderExprs(
            Blackboard bb,
            SqlSelect select,
            @Nullable SqlNodeList orderList,
            List<SqlNode> extraOrderExprs,
            List<RelFieldCollation> collationList) {
        // TODO:  add validation rules to SqlValidator also
        assert bb.root != null : "precondition: child != null";
        assert select != null;
        if (orderList == null) {
            return;
        }

        if (removeSortInSubQuery(bb.top)) {
            SqlNode offset = select.getOffset();
            if ((offset == null
                            || (offset instanceof SqlLiteral
                                    && Objects.equals(
                                            ((SqlLiteral) offset).bigDecimalValue(),
                                            BigDecimal.ZERO)))
                    && select.getFetch() == null) {
                return;
            }
        }

        for (SqlNode orderItem : orderList) {
            collationList.add(
                    convertOrderItem(
                            select,
                            orderItem,
                            extraOrderExprs,
                            RelFieldCollation.Direction.ASCENDING,
                            RelFieldCollation.NullDirection.UNSPECIFIED));
        }
    }

    protected RelFieldCollation convertOrderItem(
            SqlSelect select,
            SqlNode orderItem,
            List<SqlNode> extraExprs,
            RelFieldCollation.Direction direction,
            RelFieldCollation.NullDirection nullDirection) {
        assert select != null;
        // Handle DESC keyword, e.g. 'select a, b from t order by a desc'.
        switch (orderItem.getKind()) {
            case DESCENDING:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand(0),
                        extraExprs,
                        RelFieldCollation.Direction.DESCENDING,
                        nullDirection);
            case NULLS_FIRST:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand(0),
                        extraExprs,
                        direction,
                        RelFieldCollation.NullDirection.FIRST);
            case NULLS_LAST:
                return convertOrderItem(
                        select,
                        ((SqlCall) orderItem).operand(0),
                        extraExprs,
                        direction,
                        RelFieldCollation.NullDirection.LAST);
            default:
                break;
        }

        SqlNode converted = validator().expandOrderExpr(select, orderItem);

        switch (nullDirection) {
            case UNSPECIFIED:
                nullDirection =
                        validator().config().defaultNullCollation().last(desc(direction))
                                ? RelFieldCollation.NullDirection.LAST
                                : RelFieldCollation.NullDirection.FIRST;
                break;
            default:
                break;
        }

        // Scan the select list and order exprs for an identical expression.
        final SelectScope selectScope =
                requireNonNull(
                        validator().getRawSelectScope(select),
                        () -> "getRawSelectScope is not found for " + select);
        int ordinal = -1;
        List<SqlNode> expandedSelectList = selectScope.getExpandedSelectList();
        for (SqlNode selectItem : requireNonNull(expandedSelectList, "expandedSelectList")) {
            ++ordinal;
            if (converted.equalsDeep(stripAs(selectItem), Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        for (SqlNode extraExpr : extraExprs) {
            ++ordinal;
            if (converted.equalsDeep(extraExpr, Litmus.IGNORE)) {
                return new RelFieldCollation(ordinal, direction, nullDirection);
            }
        }

        // TODO:  handle collation sequence
        // TODO: flag expressions as non-standard

        extraExprs.add(converted);
        return new RelFieldCollation(ordinal + 1, direction, nullDirection);
    }

    private static boolean desc(RelFieldCollation.Direction direction) {
        switch (direction) {
            case DESCENDING:
            case STRICTLY_DESCENDING:
                return true;
            default:
                return false;
        }
    }

    @Deprecated // to be removed before 2.0
    protected boolean enableDecorrelation() {
        // disable sub-query decorrelation when needed.
        // e.g. if outer joins are not supported.
        return config.isDecorrelationEnabled();
    }

    protected RelNode decorrelateQuery(RelNode rootRel) {
        return RelDecorrelator.decorrelateQuery(rootRel, relBuilder);
    }

    /**
     * Returns whether to trim unused fields as part of the conversion process.
     *
     * @return Whether to trim unused fields
     */
    @Deprecated // to be removed before 2.0
    public boolean isTrimUnusedFields() {
        return config.isTrimUnusedFields();
    }

    /**
     * Recursively converts a query to a relational expression.
     *
     * @param query Query
     * @param top Whether this query is the top-level query of the statement
     * @param targetRowType Target row type, or null
     * @return Relational expression
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 递归地将查询转换为关系表达式。
     *
     * @param query Query SQL节点，代表要转换的查询
     * @param top 是否该查询是语句的顶层查询
     * @param targetRowType 目标行类型，或者为null（如果不需要指定）
     * @return 转换后的关系表达式
     */
    protected RelRoot convertQueryRecursive(
            SqlNode query, boolean top, @Nullable RelDataType targetRowType) {
        // 获取查询的类型
        final SqlKind kind = query.getKind();
        // 根据查询类型进行分支处理
        switch (kind) {
            case SELECT:// SELECT查询
                return RelRoot.of(convertSelect((SqlSelect) query, top), kind);// 调用convertSelect方法转换SELECT查询
            case INSERT:// INSERT语句
                return RelRoot.of(convertInsert((SqlInsert) query), kind);// 调用convertInsert方法转换INSERT语句
            case DELETE:// DELETE语句
                return RelRoot.of(convertDelete((SqlDelete) query), kind);// 调用convertDelete方法转换DELETE语句
            case UPDATE:// UPDATE语句
                return RelRoot.of(convertUpdate((SqlUpdate) query), kind);// 调用convertUpdate方法转换UPDATE语句
            case MERGE:// MERGE语句
                return RelRoot.of(convertMerge((SqlMerge) query), kind);// 调用convertMerge方法转换MERGE语句
            case UNION:// UNION操作
            case INTERSECT:// INTERSECT操作
            case EXCEPT:// EXCEPT操作
                return RelRoot.of(convertSetOp((SqlCall) query), kind);// 调用convertSetOp方法转换集合操作
            case WITH:// WITH子句
                return convertWith((SqlWith) query, top);// 调用convertWith方法转换WITH子句
            case VALUES:// VALUES列表
                return RelRoot.of(convertValues((SqlCall) query, targetRowType), kind);// 调用convertValues方法转换VALUES列表
            default:// 其他未识别的查询类型
                throw new AssertionError("not a query: " + query);// 抛出异常，表示传入的不是一个有效的查询
        }
    }

    /**
     * Converts a set operation (UNION, INTERSECT, MINUS) into relational expressions.
     *
     * @param call Call to set operator
     * @return Relational expression
     */
    protected RelNode convertSetOp(SqlCall call) {
        final RelNode left = convertQueryRecursive(call.operand(0), false, null).project();
        final RelNode right = convertQueryRecursive(call.operand(1), false, null).project();
        switch (call.getKind()) {
            case UNION:
                return LogicalUnion.create(ImmutableList.of(left, right), all(call));

            case INTERSECT:
                return LogicalIntersect.create(ImmutableList.of(left, right), all(call));

            case EXCEPT:
                return LogicalMinus.create(ImmutableList.of(left, right), all(call));

            default:
                throw Util.unexpected(call.getKind());
        }
    }

    private static boolean all(SqlCall call) {
        return ((SqlSetOperator) call.getOperator()).isAll();
    }

    protected RelNode convertInsert(SqlInsert call) {
        RelOptTable targetTable = getTargetTable(call);

        final RelDataType targetRowType = validator().getValidatedNodeType(call);
        assert targetRowType != null;
        RelNode sourceRel = convertQueryRecursive(call.getSource(), true, targetRowType).project();
        RelNode massagedRel = convertColumnList(call, sourceRel);

        return createModify(targetTable, massagedRel);
    }

    /** Creates a relational expression to modify a table or modifiable view. */
    private RelNode createModify(RelOptTable targetTable, RelNode source) {
        final ModifiableTable modifiableTable = targetTable.unwrap(ModifiableTable.class);
        if (modifiableTable != null && modifiableTable == targetTable.unwrap(Table.class)) {
            return modifiableTable.toModificationRel(
                    cluster,
                    targetTable,
                    catalogReader,
                    source,
                    LogicalTableModify.Operation.INSERT,
                    null,
                    null,
                    false);
        }
        final ModifiableView modifiableView = targetTable.unwrap(ModifiableView.class);
        if (modifiableView != null) {
            final Table delegateTable = modifiableView.getTable();
            final RelDataType delegateRowType = delegateTable.getRowType(typeFactory);
            final RelOptTable delegateRelOptTable =
                    RelOptTableImpl.create(
                            null, delegateRowType, delegateTable, modifiableView.getTablePath());
            final RelNode newSource =
                    createSource(targetTable, source, modifiableView, delegateRowType);
            return createModify(delegateRelOptTable, newSource);
        }
        return LogicalTableModify.create(
                targetTable,
                catalogReader,
                source,
                LogicalTableModify.Operation.INSERT,
                null,
                null,
                false);
    }

    /**
     * Wraps a relational expression in the projects and filters implied by a {@link
     * ModifiableView}.
     *
     * <p>The input relational expression is suitable for inserting into the view, and the returned
     * relational expression is suitable for inserting into its delegate table.
     *
     * <p>In principle, the delegate table of a view might be another modifiable view, and if so,
     * the process can be repeated.
     */
    private RelNode createSource(
            RelOptTable targetTable,
            RelNode source,
            ModifiableView modifiableView,
            RelDataType delegateRowType) {
        final ImmutableIntList mapping = modifiableView.getColumnMapping();
        assert mapping.size() == targetTable.getRowType().getFieldCount();

        // For columns represented in the mapping, the expression is just a field
        // reference.
        final Map<Integer, RexNode> projectMap = new HashMap<>();
        final List<RexNode> filters = new ArrayList<>();
        for (int i = 0; i < mapping.size(); i++) {
            int target = mapping.get(i);
            if (target >= 0) {
                projectMap.put(target, RexInputRef.of(i, source.getRowType()));
            }
        }

        // For columns that are not in the mapping, and have a constraint of the
        // form "column = value", the expression is the literal "value".
        //
        // If a column has multiple constraints, the extra ones will become a
        // filter.
        final RexNode constraint = modifiableView.getConstraint(rexBuilder, delegateRowType);
        RelOptUtil.inferViewPredicates(projectMap, filters, constraint);
        final List<Pair<RexNode, String>> projects = new ArrayList<>();
        for (RelDataTypeField field : delegateRowType.getFieldList()) {
            RexNode node = projectMap.get(field.getIndex());
            if (node == null) {
                node = rexBuilder.makeNullLiteral(field.getType());
            }
            projects.add(
                    Pair.of(rexBuilder.ensureType(field.getType(), node, false), field.getName()));
        }

        return relBuilder
                .push(source)
                .projectNamed(Pair.left(projects), Pair.right(projects), false)
                .filter(filters)
                .build();
    }

    private RelOptTable.ToRelContext createToRelContext(List<RelHint> hints) {
        return ViewExpanders.toRelContext(viewExpander, cluster, hints);
    }
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将RelOptTable转换为RelNode
     */
    public RelNode toRel(final RelOptTable table, final List<RelHint> hints) {
        // 使用给定的hints创建一个转换上下文，并通过这个上下文将table转换为RelNode
        final RelNode scan = table.toRel(createToRelContext(hints));

        // 尝试从table中解包出一个InitializerExpressionFactory，如果没有，则使用NullInitializerExpressionFactory的实例
        // InitializerExpressionFactory用于生成表达式，特别是在处理虚拟字段时
        final InitializerExpressionFactory ief =
                table.maybeUnwrap(InitializerExpressionFactory.class)
                        .orElse(NullInitializerExpressionFactory.INSTANCE);
        // 检查表的行类型中是否包含任何虚拟字段
        boolean hasVirtualFields =
                table.getRowType().getFieldList().stream()
                        .anyMatch(
                                f ->
                                        ief.generationStrategy(table, f.getIndex())
                                                == ColumnStrategy.VIRTUAL);

        if (hasVirtualFields) {
            // 如果存在虚拟字段，则创建一个表示源数据的RexNode（关系表达式节点）
            final RexNode sourceRef = rexBuilder.makeRangeReference(scan);
            // 创建一个Blackboard，用于在后续操作中存储和访问数据
            final Blackboard bb =
                    createInsertBlackboard(table, sourceRef, table.getRowType().getFieldNames());

            // 初始化一个列表，用于存储转换后的字段表达式
            final List<RexNode> list = new ArrayList<>();

            // 遍历表的行类型中的所有字段
            for (RelDataTypeField f : table.getRowType().getFieldList()) {
                // 根据字段的索引获取其生成策略
                final ColumnStrategy strategy = ief.generationStrategy(table, f.getIndex());
                // 根据字段的生成策略，决定如何构建RexNode
                switch (strategy) {
                    case VIRTUAL:
                        // 如果是虚拟字段，则使用InitializerExpressionFactory为该字段生成一个默认值
                        list.add(ief.newColumnDefaultValue(table, f.getIndex(), bb));
                        break;
                    default:
                        // 对于非虚拟字段，直接引用源数据的相应字段
                        list.add(
                                rexBuilder.makeInputRef(
                                        scan, RelOptTableImpl.realOrdinal(table, f.getIndex())));
                }
            }
            // 将scan节点推入RelBuilder的栈中，作为后续操作的基础
            relBuilder.push(scan);

            // 使用之前构建的RexNode列表（list）作为投影字段，创建一个投影操作
            // 这意味着我们将选择或转换scan节点中的某些列，以生成一个新的RelNode
            relBuilder.project(list);

            // 调用RelBuilder的build方法，将当前栈中的操作转换为一个完整的RelNode
            // 这个RelNode现在包含了scan操作后的投影操作
            final RelNode project = relBuilder.build();

            // 尝试从InitializerExpressionFactory中获取一个后转换钩子（post-conversion hook）
            // 这个钩子可以在投影操作之后，但在返回最终的RelNode之前，对RelNode进行额外的处理
            BiFunction<InitializerContext, RelNode, RelNode> postConversionHook =
                    ief.postExpressionConversionHook();
            // 如果存在后转换钩子，则调用它，并将黑板（Blackboard）和构建的投影RelNode作为参数传递
            // 后转换钩子可以返回一个新的RelNode，该节点可能是对原始投影RelNode的进一步转换
            if (postConversionHook != null) {
                return postConversionHook.apply(bb, project);
            } else {
                // 如果没有后转换钩子，则直接返回构建的投影RelNode
                return project;
            }
        }
         // 则直接返回原始的scan节点
        return scan;
    }

    protected RelOptTable getTargetTable(SqlNode call) {
        final SqlValidatorNamespace targetNs = getNamespace(call);
        SqlValidatorNamespace namespace;
        if (targetNs.isWrapperFor(SqlValidatorImpl.DmlNamespace.class)) {
            namespace = targetNs.unwrap(SqlValidatorImpl.DmlNamespace.class);
        } else {
            namespace = targetNs.resolve();
        }
        RelOptTable table = SqlValidatorUtil.getRelOptTable(namespace, catalogReader, null, null);
        return requireNonNull(table, "no table found for " + call);
    }

    /**
     * Creates a source for an INSERT statement.
     *
     * <p>If the column list is not specified, source expressions match target columns in order.
     *
     * <p>If the column list is specified, Source expressions are mapped to target columns by name
     * via targetColumnList, and may not cover the entire target table. So, we'll make up a full
     * row, using a combination of default values and the source expressions provided.
     *
     * @param call Insert expression
     * @param source Source relational expression
     * @return Converted INSERT statement
     */
    protected RelNode convertColumnList(final SqlInsert call, RelNode source) {
        RelDataType sourceRowType = source.getRowType();
        final RexNode sourceRef = rexBuilder.makeRangeReference(sourceRowType, 0, false);
        final List<String> targetColumnNames = new ArrayList<>();
        final List<RexNode> columnExprs = new ArrayList<>();
        collectInsertTargets(call, sourceRef, targetColumnNames, columnExprs);

        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType targetRowType = RelOptTableImpl.realRowType(targetTable);
        final List<RelDataTypeField> targetFields = targetRowType.getFieldList();
        final List<@Nullable RexNode> sourceExps =
                new ArrayList<>(Collections.nCopies(targetFields.size(), null));
        final List<@Nullable String> fieldNames =
                new ArrayList<>(Collections.nCopies(targetFields.size(), null));

        final InitializerExpressionFactory initializerFactory =
                getInitializerFactory(getNamespace(call).getTable());

        // Walk the name list and place the associated value in the
        // expression list according to the ordinal value returned from
        // the table construct, leaving nulls in the list for columns
        // that are not referenced.
        final SqlNameMatcher nameMatcher = catalogReader.nameMatcher();
        for (Pair<String, RexNode> p : Pair.zip(targetColumnNames, columnExprs)) {
            RelDataTypeField field = nameMatcher.field(targetRowType, p.left);
            assert field != null : "column " + p.left + " not found";
            sourceExps.set(field.getIndex(), p.right);
        }

        // Lazily create a blackboard that contains all non-generated columns.
        final Supplier<Blackboard> bb =
                () -> createInsertBlackboard(targetTable, sourceRef, targetColumnNames);

        // Walk the expression list and get default values for any columns
        // that were not supplied in the statement. Get field names too.
        for (int i = 0; i < targetFields.size(); ++i) {
            final RelDataTypeField field = targetFields.get(i);
            final String fieldName = field.getName();
            fieldNames.set(i, fieldName);
            RexNode sourceExpression = sourceExps.get(i);
            if (sourceExpression == null || sourceExpression.getKind() == SqlKind.DEFAULT) {
                sourceExpression =
                        initializerFactory.newColumnDefaultValue(targetTable, i, bb.get());
                // bare nulls are dangerous in the wrong hands
                sourceExpression = castNullLiteralIfNeeded(sourceExpression, field.getType());

                sourceExps.set(i, sourceExpression);
            }
        }

        // sourceExps should not contain nulls (see the loop above)
        @SuppressWarnings("assignment.type.incompatible")
        List<RexNode> nonNullExprs = sourceExps;

        return relBuilder.push(source).projectNamed(nonNullExprs, fieldNames, false).build();
    }

    /**
     * Creates a blackboard for translating the expressions of generated columns in an INSERT
     * statement.
     */
    private Blackboard createInsertBlackboard(
            RelOptTable targetTable, RexNode sourceRef, List<String> targetColumnNames) {
        final Map<String, RexNode> nameToNodeMap = new HashMap<>();
        int j = 0;

        // Assign expressions for non-generated columns.
        final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
        final List<String> targetFields = targetTable.getRowType().getFieldNames();
        for (String targetColumnName : targetColumnNames) {
            final int i = targetFields.indexOf(targetColumnName);
            switch (strategies.get(i)) {
                case STORED:
                case VIRTUAL:
                    break;
                default:
                    nameToNodeMap.put(targetColumnName, rexBuilder.makeFieldAccess(sourceRef, j++));
            }
        }
        return createBlackboard(null, nameToNodeMap, false);
    }

    private static InitializerExpressionFactory getInitializerFactory(
            @Nullable SqlValidatorTable validatorTable) {
        // We might unwrap a null instead of a InitializerExpressionFactory.
        final Table table = unwrap(validatorTable, Table.class);
        if (table != null) {
            InitializerExpressionFactory f = unwrap(table, InitializerExpressionFactory.class);
            if (f != null) {
                return f;
            }
        }
        return NullInitializerExpressionFactory.INSTANCE;
    }

    private static <T extends Object> @Nullable T unwrap(@Nullable Object o, Class<T> clazz) {
        if (o instanceof Wrapper) {
            return ((Wrapper) o).unwrap(clazz);
        }
        return null;
    }

    private RexNode castNullLiteralIfNeeded(RexNode node, RelDataType type) {
        if (!RexLiteral.isNullLiteral(node)) {
            return node;
        }
        return rexBuilder.makeCast(type, node);
    }

    /**
     * Given an INSERT statement, collects the list of names to be populated and the expressions to
     * put in them.
     *
     * @param call Insert statement
     * @param sourceRef Expression representing a row from the source relational expression
     * @param targetColumnNames List of target column names, to be populated
     * @param columnExprs List of expressions, to be populated
     */
    protected void collectInsertTargets(
            SqlInsert call,
            final RexNode sourceRef,
            final List<String> targetColumnNames,
            List<RexNode> columnExprs) {
        final RelOptTable targetTable = getTargetTable(call);
        final RelDataType tableRowType = targetTable.getRowType();
        SqlNodeList targetColumnList = call.getTargetColumnList();
        if (targetColumnList == null) {
            if (validator().config().conformance().isInsertSubsetColumnsAllowed()) {
                final RelDataType targetRowType =
                        typeFactory.createStructType(
                                tableRowType
                                        .getFieldList()
                                        .subList(0, sourceRef.getType().getFieldCount()));
                targetColumnNames.addAll(targetRowType.getFieldNames());
            } else {
                targetColumnNames.addAll(tableRowType.getFieldNames());
            }
        } else {
            for (int i = 0; i < targetColumnList.size(); i++) {
                SqlIdentifier id = (SqlIdentifier) targetColumnList.get(i);
                RelDataTypeField field =
                        SqlValidatorUtil.getTargetField(
                                tableRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNames.add(field.getName());
            }
        }

        final Blackboard bb = createInsertBlackboard(targetTable, sourceRef, targetColumnNames);

        // Next, assign expressions for generated columns.
        final List<ColumnStrategy> strategies = targetTable.getColumnStrategies();
        for (String columnName : targetColumnNames) {
            final int i = tableRowType.getFieldNames().indexOf(columnName);
            final RexNode expr;
            switch (strategies.get(i)) {
                case STORED:
                    final InitializerExpressionFactory f =
                            targetTable
                                    .maybeUnwrap(InitializerExpressionFactory.class)
                                    .orElse(NullInitializerExpressionFactory.INSTANCE);
                    expr = f.newColumnDefaultValue(targetTable, i, bb);
                    break;
                case VIRTUAL:
                    expr = null;
                    break;
                default:
                    expr = requireNonNull(bb.nameToNodeMap, "nameToNodeMap").get(columnName);
            }
            // expr is nullable, however, all the nulls will be removed in the loop below
            columnExprs.add(castNonNull(expr));
        }

        // Remove virtual columns from the list.
        for (int i = 0; i < targetColumnNames.size(); i++) {
            if (columnExprs.get(i) == null) {
                columnExprs.remove(i);
                targetColumnNames.remove(i);
                --i;
            }
        }
    }

    private RelNode convertDelete(SqlDelete call) {
        RelOptTable targetTable = getTargetTable(call);
        RelNode sourceRel =
                convertSelect(
                        requireNonNull(call.getSourceSelect(), () -> "sourceSelect for " + call),
                        false);
        return LogicalTableModify.create(
                targetTable,
                catalogReader,
                sourceRel,
                LogicalTableModify.Operation.DELETE,
                null,
                null,
                false);
    }

    private RelNode convertUpdate(SqlUpdate call) {
        final SqlValidatorScope scope =
                validator()
                        .getWhereScope(
                                requireNonNull(
                                        call.getSourceSelect(), () -> "sourceSelect for " + call));
        Blackboard bb = createBlackboard(scope, null, false);

        replaceSubQueries(bb, call, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final RelDataType targetRowType = targetTable.getRowType();
        for (SqlNode node : call.getTargetColumnList()) {
            SqlIdentifier id = (SqlIdentifier) node;
            RelDataTypeField field =
                    SqlValidatorUtil.getTargetField(
                            targetRowType, typeFactory, id, catalogReader, targetTable);
            assert field != null : "column " + id.toString() + " not found";
            targetColumnNameList.add(field.getName());
        }

        RelNode sourceRel =
                convertSelect(
                        requireNonNull(call.getSourceSelect(), () -> "sourceSelect for " + call),
                        false);

        bb.setRoot(sourceRel, false);
        ImmutableList.Builder<RexNode> rexNodeSourceExpressionListBuilder = ImmutableList.builder();
        for (SqlNode n : call.getSourceExpressionList()) {
            RexNode rn = bb.convertExpression(n);
            rexNodeSourceExpressionListBuilder.add(rn);
        }

        return LogicalTableModify.create(
                targetTable,
                catalogReader,
                sourceRel,
                LogicalTableModify.Operation.UPDATE,
                targetColumnNameList,
                rexNodeSourceExpressionListBuilder.build(),
                false);
    }

    private RelNode convertMerge(SqlMerge call) {
        RelOptTable targetTable = getTargetTable(call);

        // convert update column list from SqlIdentifier to String
        final List<String> targetColumnNameList = new ArrayList<>();
        final RelDataType targetRowType = targetTable.getRowType();
        SqlUpdate updateCall = call.getUpdateCall();
        if (updateCall != null) {
            for (SqlNode targetColumn : updateCall.getTargetColumnList()) {
                SqlIdentifier id = (SqlIdentifier) targetColumn;
                RelDataTypeField field =
                        SqlValidatorUtil.getTargetField(
                                targetRowType, typeFactory, id, catalogReader, targetTable);
                assert field != null : "column " + id.toString() + " not found";
                targetColumnNameList.add(field.getName());
            }
        }

        // replace the projection of the source select with a
        // projection that contains the following:
        // 1) the expressions corresponding to the new insert row (if there is
        //    an insert)
        // 2) all columns from the target table (if there is an update)
        // 3) the set expressions in the update call (if there is an update)

        // first, convert the merge's source select to construct the columns
        // from the target table and the set expressions in the update call
        RelNode mergeSourceRel =
                convertSelect(
                        requireNonNull(call.getSourceSelect(), () -> "sourceSelect for " + call),
                        false);

        // then, convert the insert statement so we can get the insert
        // values expressions
        SqlInsert insertCall = call.getInsertCall();
        int nLevel1Exprs = 0;
        List<RexNode> level1InsertExprs = null;
        List<RexNode> level2InsertExprs = null;
        if (insertCall != null) {
            RelNode insertRel = convertInsert(insertCall);

            // if there are 2 level of projections in the insert source, combine
            // them into a single project; level1 refers to the topmost project;
            // the level1 projection contains references to the level2
            // expressions, except in the case where no target expression was
            // provided, in which case, the expression is the default value for
            // the column; or if the expressions directly map to the source
            // table
            level1InsertExprs = ((LogicalProject) insertRel.getInput(0)).getProjects();
            if (insertRel.getInput(0).getInput(0) instanceof LogicalProject) {
                level2InsertExprs =
                        ((LogicalProject) insertRel.getInput(0).getInput(0)).getProjects();
            }
            nLevel1Exprs = level1InsertExprs.size();
        }

        LogicalJoin join = (LogicalJoin) mergeSourceRel.getInput(0);
        int nSourceFields = join.getLeft().getRowType().getFieldCount();
        final List<RexNode> projects = new ArrayList<>();
        for (int level1Idx = 0; level1Idx < nLevel1Exprs; level1Idx++) {
            requireNonNull(level1InsertExprs, "level1InsertExprs");
            if ((level2InsertExprs != null)
                    && (level1InsertExprs.get(level1Idx) instanceof RexInputRef)) {
                int level2Idx = ((RexInputRef) level1InsertExprs.get(level1Idx)).getIndex();
                projects.add(level2InsertExprs.get(level2Idx));
            } else {
                projects.add(level1InsertExprs.get(level1Idx));
            }
        }
        if (updateCall != null) {
            final LogicalProject project = (LogicalProject) mergeSourceRel;
            projects.addAll(Util.skip(project.getProjects(), nSourceFields));
        }

        relBuilder.push(join).project(projects);

        return LogicalTableModify.create(
                targetTable,
                catalogReader,
                relBuilder.build(),
                LogicalTableModify.Operation.MERGE,
                targetColumnNameList,
                null,
                false);
    }

    /**
     * Converts an identifier into an expression in a given scope. For example, the "empno" in
     * "select empno from emp join dept" becomes "emp.empno".
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 将标识符转换为给定作用域内的表达式。例如，在 "select empno from emp join dept" 中的 "empno"
     * 将被转换为 "emp.empno"。
     */
    private RexNode convertIdentifier(Blackboard bb, SqlIdentifier identifier) {
        // first check for reserved identifiers like CURRENT_USER
        // 首先检查是否为保留标识符，如 CURRENT_USER
        final SqlCall call = bb.getValidator().makeNullaryCall(identifier);
        if (call != null) {
            // 如果是保留标识符，则直接转换该调用表达式
            return bb.convertExpression(call);
        }
        // 用于存储模式变量引用的前缀
        String pv = null;
        if (bb.isPatternVarRef && identifier.names.size() > 1) {
            // 如果标识符包含多个部分且当前作用域是模式变量引用，则取第一部分作为模式变量名
            pv = identifier.names.get(0);
        }

        // 用于存储完全限定的标识符
        final SqlQualified qualified;
        if (bb.scope != null) {
            // 如果当前有作用域，则使用作用域来完全限定标识符，SqlQualified
            qualified = bb.scope.fullyQualify(identifier);
        } else {
            // 如果没有作用域，则创建一个非限定的标识符
            qualified = SqlQualified.create(null, 1, null, identifier);
        }
        // 查找完全限定标识符对应的 RexNode 表达式，以及一个可能的函数用于后续访问
        final Pair<RexNode, @Nullable BiFunction<RexNode, String, RexNode>> e0 =
                bb.lookupExp(qualified);
        // 初始化表达式
        RexNode e = e0.left;
        // 遍历限定符的后缀部分
        for (String name : qualified.suffix()) {
            if (e == e0.left && e0.right != null) {
                // 如果表达式是初始查找的结果
                e = e0.right.apply(e, name);
                // 否则，使用RexBuilder来创建字段访问表达式
            } else {
                // 名称已经是完全限定的，因此是大小写敏感的
                final boolean caseSensitive = true; // name already fully-qualified
                if (identifier.isStar() && bb.scope instanceof MatchRecognizeScope) {
                    // 如果是星号（*）且作用域是匹配识别作用域，则特殊处理
                    e = rexBuilder.makeFieldAccess(e, 0);
                } else {
                    // 一般的字段访问
                    e = rexBuilder.makeFieldAccess(e, name, caseSensitive);
                }
            }
        }
        // 如果最终表达式是输入引用，可能需要调整其类型以考虑外连接引入的空值
        if (e instanceof RexInputRef) {
            // adjust the type to account for nulls introduced by outer joins
            e = adjustInputRef(bb, (RexInputRef) e);
            if (pv != null) {
                // 如果存在模式变量前缀，则将其包装为模式字段引用
                e = RexPatternFieldRef.of(pv, (RexInputRef) e);
            }
        }

        // 检查e0.left（即最初查找到的表达式）是否是一个相关变量
        if (e0.left instanceof RexCorrelVariable) {
            // 断言e是一个RexFieldAccess实例，这通常意味着我们从一个相关变量中访问了某个字段
            assert e instanceof RexFieldAccess;
            // 从e0.left中获取相关变量的ID
            // 将这个相关变量ID映射到当前的RexFieldAccess表达式上
            final RexNode prev =
                    bb.mapCorrelateToRex.put(((RexCorrelVariable) e0.left).id, (RexFieldAccess) e);
            assert prev == null;
        }
        // 返回最终构建的RexNode表达式
        return e;
    }

    /**
     * Adjusts the type of a reference to an input field to account for nulls introduced by outer
     * joins; and adjusts the offset to match the physical implementation.
     *
     * @param bb Blackboard
     * @param inputRef Input ref
     * @return Adjusted input ref
     */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 调整对输入字段的引用的类型，以考虑外部连接引入的空值；并调整偏移量以匹配物理实现
     */
    protected RexNode adjustInputRef(Blackboard bb, RexInputRef inputRef) {
        // 从Blackboard中获取与输入引用相关联的字段,也就是where语句字段与输入字段的引用关系
        RelDataTypeField field = bb.getRootField(inputRef);
        if (field != null) {
            // 检查字段的类型（忽略空值性）是否与输入引用的类型相同
            // 如果不同，则通常不需要调整，因为这里的调整主要是处理类型因外部连接而可能引入的空值性变化
            if (!SqlTypeUtil.equalSansNullability(
                    typeFactory, field.getType(), inputRef.getType())) {
                return inputRef;
            }
            return rexBuilder.makeInputRef(field.getType(), inputRef.getIndex());
        }
        return inputRef;
    }

    /**
     * Converts a row constructor into a relational expression.
     *
     * @param bb Blackboard
     * @param rowConstructor Row constructor expression
     * @return Relational expression which returns a single row.
     */
    private RelNode convertRowConstructor(Blackboard bb, SqlCall rowConstructor) {
        Preconditions.checkArgument(isRowConstructor(rowConstructor));
        final List<SqlNode> operands = rowConstructor.getOperandList();
        return convertMultisets(operands, bb);
    }

    private RelNode convertCursor(Blackboard bb, SubQuery subQuery) {
        final SqlCall cursorCall = (SqlCall) subQuery.node;
        assert cursorCall.operandCount() == 1;
        SqlNode query = cursorCall.operand(0);
        RelNode converted = convertQuery(query, false, false).rel;
        int iCursor = bb.cursors.size();
        bb.cursors.add(converted);
        subQuery.expr = new RexInputRef(iCursor, converted.getRowType());
        return converted;
    }

    private RelNode convertMultisets(final List<SqlNode> operands, Blackboard bb) {
        // NOTE: Wael 2/04/05: this implementation is not the most efficient in
        // terms of planning since it generates XOs that can be reduced.
        final List<Object> joinList = new ArrayList<>();
        List<SqlNode> lastList = new ArrayList<>();
        for (int i = 0; i < operands.size(); i++) {
            SqlNode operand = operands.get(i);
            if (!(operand instanceof SqlCall)) {
                lastList.add(operand);
                continue;
            }

            final SqlCall call = (SqlCall) operand;
            final RelNode input;
            switch (call.getKind()) {
                case MULTISET_VALUE_CONSTRUCTOR:
                case ARRAY_VALUE_CONSTRUCTOR:
                    final SqlNodeList list =
                            new SqlNodeList(call.getOperandList(), call.getParserPosition());
                    CollectNamespace nss = getNamespaceOrNull(call);
                    Blackboard usedBb;
                    if (null != nss) {
                        usedBb = createBlackboard(nss.getScope(), null, false);
                    } else {
                        usedBb =
                                createBlackboard(
                                        new ListScope(bb.scope()) {
                                            @Override
                                            public SqlNode getNode() {
                                                return call;
                                            }
                                        },
                                        null,
                                        false);
                    }
                    RelDataType multisetType = validator().getValidatedNodeType(call);
                    validator()
                            .setValidatedNodeType(
                                    list,
                                    requireNonNull(
                                            multisetType.getComponentType(),
                                            () ->
                                                    "componentType for multisetType "
                                                            + multisetType));
                    input = convertQueryOrInList(usedBb, list, null);
                    break;
                case MULTISET_QUERY_CONSTRUCTOR:
                case ARRAY_QUERY_CONSTRUCTOR:
                case MAP_QUERY_CONSTRUCTOR:
                    final RelRoot root = convertQuery(call.operand(0), false, true);
                    input = root.rel;
                    break;
                default:
                    lastList.add(operand);
                    continue;
            }

            if (lastList.size() > 0) {
                joinList.add(lastList);
            }
            lastList = new ArrayList<>();
            relBuilder.push(
                    Collect.create(
                            requireNonNull(input, "input"),
                            call.getKind(),
                            castNonNull(validator().deriveAlias(call, i))));
            joinList.add(relBuilder.build());
        }

        if (joinList.size() == 0) {
            joinList.add(lastList);
        }

        for (int i = 0; i < joinList.size(); i++) {
            Object o = joinList.get(i);
            if (o instanceof List) {
                @SuppressWarnings("unchecked")
                List<SqlNode> projectList = (List<SqlNode>) o;
                final List<RexNode> selectList = new ArrayList<>();
                final List<String> fieldNameList = new ArrayList<>();
                for (int j = 0; j < projectList.size(); j++) {
                    SqlNode operand = projectList.get(j);
                    selectList.add(bb.convertExpression(operand));

                    // REVIEW angel 5-June-2005: Use deriveAliasFromOrdinal
                    // instead of deriveAlias to match field names from
                    // SqlRowOperator. Otherwise, get error   Type
                    // 'RecordType(INTEGER EMPNO)' has no field 'EXPR$0' when
                    // doing   select * from unnest(     select multiset[empno]
                    // from sales.emps);

                    fieldNameList.add(SqlUtil.deriveAliasFromOrdinal(j));
                }

                relBuilder
                        .push(LogicalValues.createOneRow(cluster))
                        .projectNamed(selectList, fieldNameList, true);

                joinList.set(i, relBuilder.build());
            }
        }

        RelNode ret = (RelNode) joinList.get(0);
        for (int i = 1; i < joinList.size(); i++) {
            RelNode relNode = (RelNode) joinList.get(i);
            ret =
                    RelFactories.DEFAULT_JOIN_FACTORY.createJoin(
                            ret,
                            relNode,
                            ImmutableList.of(),
                            rexBuilder.makeLiteral(true),
                            ImmutableSet.of(),
                            JoinRelType.INNER,
                            false);
        }
        return ret;
    }

    private void convertSelectList(Blackboard bb, SqlSelect select, List<SqlNode> orderList) {
        SqlNodeList selectList = select.getSelectList();
        selectList = validator().expandStar(selectList, select, false);

        replaceSubQueries(bb, selectList, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);

        List<String> fieldNames = new ArrayList<>();
        final List<RexNode> exprs = new ArrayList<>();
        final Collection<String> aliases = new TreeSet<>();

        // Project any system fields. (Must be done before regular select items,
        // because offsets may be affected.)
        final List<SqlMonotonicity> columnMonotonicityList = new ArrayList<>();
        extraSelectItems(bb, select, exprs, fieldNames, aliases, columnMonotonicityList);

        // Project select clause.
        int i = -1;
        for (SqlNode expr : selectList) {
            ++i;
            exprs.add(bb.convertExpression(expr));
            fieldNames.add(deriveAlias(expr, aliases, i));
        }

        // Project extra fields for sorting.
        for (SqlNode expr : orderList) {
            ++i;
            SqlNode expr2 = validator().expandOrderExpr(select, expr);
            exprs.add(bb.convertExpression(expr2));
            fieldNames.add(deriveAlias(expr, aliases, i));
        }

        fieldNames =
                SqlValidatorUtil.uniquify(
                        fieldNames, catalogReader.nameMatcher().isCaseSensitive());

        relBuilder.push(bb.root()).projectNamed(exprs, fieldNames, true);

        RelNode project = relBuilder.build();

        final RelNode r;
        final CorrelationUse p = getCorrelationUse(bb, project);
        if (p != null) {
            r = p.r;
        } else {
            r = project;
        }

        bb.setRoot(r, false);

        assert bb.columnMonotonicities.isEmpty();
        bb.columnMonotonicities.addAll(columnMonotonicityList);
        for (SqlNode selectItem : selectList) {
            bb.columnMonotonicities.add(selectItem.getMonotonicity(bb.scope));
        }
    }

    /**
     * Adds extra select items. The default implementation adds nothing; derived classes may add
     * columns to exprList, nameList, aliasList and columnMonotonicityList.
     *
     * @param bb Blackboard
     * @param select Select statement being translated
     * @param exprList List of expressions in select clause
     * @param nameList List of names, one per column
     * @param aliasList Collection of aliases that have been used already
     * @param columnMonotonicityList List of monotonicity, one per column
     */
    protected void extraSelectItems(
            Blackboard bb,
            SqlSelect select,
            List<RexNode> exprList,
            List<String> nameList,
            Collection<String> aliasList,
            List<SqlMonotonicity> columnMonotonicityList) {}

    private String deriveAlias(final SqlNode node, Collection<String> aliases, final int ordinal) {
        String alias = validator().deriveAlias(node, ordinal);
        if (alias == null || aliases.contains(alias)) {
            final String aliasBase = Util.first(alias, SqlUtil.GENERATED_EXPR_ALIAS_PREFIX);
            for (int j = 0; ; j++) {
                alias = aliasBase + j;
                if (!aliases.contains(alias)) {
                    break;
                }
            }
        }
        aliases.add(alias);
        return alias;
    }

    /** Converts a WITH sub-query into a relational expression. */
    public RelRoot convertWith(SqlWith with, boolean top) {
        return convertQuery(with.body, false, top);
    }

    /** Converts a SELECT statement's parse tree into a relational expression. */
    public RelNode convertValues(SqlCall values, @Nullable RelDataType targetRowType) {
        final SqlValidatorScope scope = validator().getOverScope(values);
        assert scope != null;
        final Blackboard bb = createBlackboard(scope, null, false);
        convertValuesImpl(bb, values, targetRowType);
        return bb.root();
    }

    /**
     * Converts a values clause (as in "INSERT INTO T(x,y) VALUES (1,2)") into a relational
     * expression.
     *
     * @param bb Blackboard
     * @param values Call to SQL VALUES operator
     * @param targetRowType Target row type
     */
    private void convertValuesImpl(
            Blackboard bb, SqlCall values, @Nullable RelDataType targetRowType) {
        // Attempt direct conversion to LogicalValues; if that fails, deal with
        // fancy stuff like sub-queries below.
        RelNode valuesRel =
                convertRowValues(bb, values, values.getOperandList(), true, targetRowType);
        if (valuesRel != null) {
            bb.setRoot(valuesRel, true);
            return;
        }

        for (SqlNode rowConstructor1 : values.getOperandList()) {
            SqlCall rowConstructor = (SqlCall) rowConstructor1;
            Blackboard tmpBb = createBlackboard(bb.scope, null, false);
            replaceSubQueries(tmpBb, rowConstructor, RelOptUtil.Logic.TRUE_FALSE_UNKNOWN);
            final List<Pair<RexNode, String>> exps = new ArrayList<>();
            for (Ord<SqlNode> operand : Ord.zip(rowConstructor.getOperandList())) {
                exps.add(
                        Pair.of(
                                tmpBb.convertExpression(operand.e),
                                castNonNull(validator().deriveAlias(operand.e, operand.i))));
            }
            RelNode in = (null == tmpBb.root) ? LogicalValues.createOneRow(cluster) : tmpBb.root;
            relBuilder.push(in).project(Pair.left(exps), Pair.right(exps));
        }

        bb.setRoot(relBuilder.union(true, values.getOperandList().size()).build(), true);
    }

    // ~ Inner Classes ----------------------------------------------------------

    /** A Tuple to remember all calls to Blackboard.register */
    private static class RegisterArgs {
        final RelNode rel;
        final JoinRelType joinType;
        final @Nullable List<RexNode> leftKeys;

        RegisterArgs(RelNode rel, JoinRelType joinType, @Nullable List<RexNode> leftKeys) {
            this.rel = rel;
            this.joinType = joinType;
            this.leftKeys = leftKeys;
        }
    }

    /**
     * Function that can convert a sort specification (expression, direction and null direction) to
     * a target format.
     *
     * @param <R> Target format, such as {@link RexFieldCollation} or {@link RexNode}
     */
    @FunctionalInterface
    interface SortExpressionConverter<R> {
        R convert(
                SqlNode node,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection);
    }

    /** Workspace for translating an individual SELECT statement (or sub-SELECT). */
    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 单个语句的工作区
     */
    protected class Blackboard implements SqlRexContext, SqlVisitor<RexNode>, InitializerContext {
        /** Collection of {@link RelNode} objects which correspond to a SELECT statement. */
        public final @Nullable SqlValidatorScope scope;

        private final @Nullable Map<String, RexNode> nameToNodeMap;
        public @Nullable RelNode root;
        private @Nullable List<RelNode> inputs;
        private final Map<CorrelationId, RexFieldAccess> mapCorrelateToRex = new HashMap<>();
        private List<RegisterArgs> registered = new ArrayList<>();

        private boolean isPatternVarRef = false;

        final List<RelNode> cursors = new ArrayList<>();

        /**
         * List of <code>IN</code> and <code>EXISTS</code> nodes inside this <code>SELECT</code>
         * statement (but not inside sub-queries).
         */
        private final List<SubQuery> subQueryList = new ArrayList<>();

        /** Workspace for building aggregates. */
        @Nullable AggConverter agg;

        /**
         * When converting window aggregate, we need to know if the window is guaranteed to be
         * non-empty.
         */
        @Nullable SqlWindow window;

        /**
         * Project the groupby expressions out of the root of this sub-select. Sub-queries can
         * reference group by expressions projected from the "right" to the sub-query.
         */
        private final Map<RelNode, Map<Integer, Integer>> mapRootRelToFieldProjection =
                new HashMap<>();

        private final List<SqlMonotonicity> columnMonotonicities = new ArrayList<>();

        private final List<RelDataTypeField> systemFieldList = new ArrayList<>();
        final boolean top;

        private final InitializerExpressionFactory initializerExpressionFactory =
                new NullInitializerExpressionFactory();

        /**
         * Creates a Blackboard.
         *
         * @param scope Name-resolution scope for expressions validated within this query. Can be
         *     null if this Blackboard is for a leaf node, say
         * @param nameToNodeMap Map which translates the expression to map a given parameter into,
         *     if translating expressions; null otherwise
         * @param top Whether this is the root of the query
         */
        protected Blackboard(
                @Nullable SqlValidatorScope scope,
                @Nullable Map<String, RexNode> nameToNodeMap,
                boolean top) {
            this.scope = scope;
            this.nameToNodeMap = nameToNodeMap;
            this.top = top;
        }

        public RelNode root() {
            return requireNonNull(root, "root");
        }

        public SqlValidatorScope scope() {
            return requireNonNull(scope, "scope");
        }

        public void setPatternVarRef(boolean isVarRef) {
            this.isPatternVarRef = isVarRef;
        }

        public RexNode register(RelNode rel, JoinRelType joinType) {
            return register(rel, joinType, null);
        }

        /**
         * Registers a relational expression.
         *
         * @param rel Relational expression
         * @param joinType Join type
         * @param leftKeys LHS of IN clause, or null for expressions other than IN
         * @return Expression with which to refer to the row (or partial row) coming from this
         *     relational expression's side of the join
         */
        public RexNode register(
                RelNode rel, JoinRelType joinType, @Nullable List<RexNode> leftKeys) {
            requireNonNull(joinType, "joinType");
            registered.add(new RegisterArgs(rel, joinType, leftKeys));
            if (root == null) {
                assert leftKeys == null : "leftKeys must be null";
                setRoot(rel, false);
                return rexBuilder.makeRangeReference(root().getRowType(), 0, false);
            }

            final RexNode joinCond;
            final int origLeftInputCount = root.getRowType().getFieldCount();
            if (leftKeys != null) {
                List<RexNode> newLeftInputExprs = new ArrayList<>();
                for (int i = 0; i < origLeftInputCount; i++) {
                    newLeftInputExprs.add(rexBuilder.makeInputRef(root(), i));
                }

                final List<Integer> leftJoinKeys = new ArrayList<>();
                for (RexNode leftKey : leftKeys) {
                    int index = newLeftInputExprs.indexOf(leftKey);
                    if (index < 0 || joinType == JoinRelType.LEFT) {
                        index = newLeftInputExprs.size();
                        newLeftInputExprs.add(leftKey);
                    }
                    leftJoinKeys.add(index);
                }

                RelNode newLeftInput = relBuilder.push(root()).project(newLeftInputExprs).build();

                // maintain the group by mapping in the new LogicalProject
                Map<Integer, Integer> currentProjection = mapRootRelToFieldProjection.get(root());
                if (currentProjection != null) {
                    mapRootRelToFieldProjection.put(newLeftInput, currentProjection);
                }

                // if the original root rel is a leaf rel, the new root should be a leaf.
                // otherwise the field offset will be wrong.
                setRoot(newLeftInput, leaves.remove(root()) != null);

                // right fields appear after the LHS fields.
                final int rightOffset =
                        root().getRowType().getFieldCount()
                                - newLeftInput.getRowType().getFieldCount();
                final List<Integer> rightKeys =
                        Util.range(rightOffset, rightOffset + leftKeys.size());

                joinCond =
                        RelOptUtil.createEquiJoinCondition(
                                newLeftInput, leftJoinKeys, rel, rightKeys, rexBuilder);
            } else {
                joinCond = rexBuilder.makeLiteral(true);
            }

            int leftFieldCount = root().getRowType().getFieldCount();
            final RelNode join = createJoin(this, root(), rel, joinCond, joinType);

            setRoot(join, false);

            if (leftKeys != null && joinType == JoinRelType.LEFT) {
                final int leftKeyCount = leftKeys.size();
                int rightFieldLength = rel.getRowType().getFieldCount();
                assert leftKeyCount == rightFieldLength - 1;

                final int rexRangeRefLength = leftKeyCount + rightFieldLength;
                RelDataType returnType =
                        typeFactory.createStructType(
                                new AbstractList<Map.Entry<String, RelDataType>>() {
                                    @Override
                                    public Map.Entry<String, RelDataType> get(int index) {
                                        return join.getRowType()
                                                .getFieldList()
                                                .get(origLeftInputCount + index);
                                    }

                                    @Override
                                    public int size() {
                                        return rexRangeRefLength;
                                    }
                                });

                return rexBuilder.makeRangeReference(returnType, origLeftInputCount, false);
            } else {
                return rexBuilder.makeRangeReference(
                        rel.getRowType(), leftFieldCount, joinType.generatesNullsOnRight());
            }
        }

        /**
         * Re-register the {@code registered} with given root node and return the new root node.
         *
         * @param root The given root, never leaf
         * @return new root after the registration
         */
        public RelNode reRegister(RelNode root) {
            setRoot(root, false);
            List<RegisterArgs> registerCopy = registered;
            registered = new ArrayList<>();
            for (RegisterArgs reg : registerCopy) {
                RelNode relNode = reg.rel;
                relBuilder.push(relNode);
                final RelMetadataQuery mq = relBuilder.getCluster().getMetadataQuery();
                final Boolean unique = mq.areColumnsUnique(relBuilder.peek(), ImmutableBitSet.of());
                if (unique == null || !unique) {
                    relBuilder.aggregate(
                            relBuilder.groupKey(),
                            relBuilder.aggregateCall(
                                    SqlStdOperatorTable.SINGLE_VALUE, relBuilder.field(0)));
                }
                register(relBuilder.build(), reg.joinType, reg.leftKeys);
            }
            return requireNonNull(this.root, "root");
        }

        /**
         * Sets a new root relational expression, as the translation process backs its way further
         * up the tree.
         *
         * @param root New root relational expression
         * @param leaf Whether the relational expression is a leaf, that is, derived from an atomic
         *     relational expression such as a table name in the from clause, or the projection on
         *     top of a select-sub-query. In particular, relational expressions derived from JOIN
         *     operators are not leaves, but set expressions are.
         */
        public void setRoot(RelNode root, boolean leaf) {
            setRoot(Collections.singletonList(root), root, root instanceof LogicalJoin);
            if (leaf) {
                leaves.put(root, root.getRowType().getFieldCount());
            }
            this.columnMonotonicities.clear();
        }

        private void setRoot(
                List<RelNode> inputs, @Nullable RelNode root, boolean hasSystemFields) {
            this.inputs = inputs;
            this.root = root;
            this.systemFieldList.clear();
            if (hasSystemFields) {
                this.systemFieldList.addAll(getSystemFields());
            }
        }

        /**
         * Notifies this Blackboard that the root just set using {@link #setRoot(RelNode, boolean)}
         * was derived using dataset substitution.
         *
         * <p>The default implementation is not interested in such notifications, and does nothing.
         *
         * @param datasetName Dataset name
         */
        public void setDataset(@Nullable String datasetName) {}

        void setRoot(List<RelNode> inputs) {
            setRoot(inputs, null, false);
        }

        /**
         * Returns an expression with which to reference a from-list item; throws if not found.
         *
         * @param qualified The alias of the FROM item
         * @return a {@link RexFieldAccess} or {@link RexRangeRef}, never null
         */
        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 根据提供的限定标识符（SqlQualified）查找对应的RexNode，并可能返回一个用于进一步转换的函数。
         */
        Pair<RexNode, @Nullable BiFunction<RexNode, String, RexNode>> lookupExp(
                SqlQualified qualified) {
            // 如果nameToNodeMap不为空且限定标识符只有一个前缀
            if (nameToNodeMap != null && qualified.prefixLength == 1) {
                // 尝试从nameToNodeMap中获取对应的RexNode
                RexNode node = nameToNodeMap.get(qualified.identifier.names.get(0));
                // 如果没有找到对应的RexNode，则抛出断言错误
                if (node == null) {
                    throw new AssertionError(
                            "Unknown identifier '"
                                    + qualified.identifier
                                    + "' encountered while expanding expression");
                }
                // 返回一个包含找到的RexNode和null函数的Pair对象
                return Pair.of(node, null);
            }
            // 使用SQL名称匹配器来解析限定标识符
            final SqlNameMatcher nameMatcher =
                    scope().getValidator().getCatalogReader().nameMatcher();
            // 创建一个解析结果对象
            final SqlValidatorScope.ResolvedImpl resolved = new SqlValidatorScope.ResolvedImpl();
            // 尝试解析限定标识符的前缀
            scope().resolve(qualified.prefix(), nameMatcher, false, resolved);
            // 如果没有找到唯一的解析结果，则抛出断言错误
            if (resolved.count() != 1) {
                throw new AssertionError(
                        "no unique expression found for "
                                + qualified
                                + "; count is "
                                + resolved.count());
            }
            // 获取唯一的解析结果
            final SqlValidatorScope.Resolve resolve = resolved.only();
            // 获取解析结果的行类型  
            final RelDataType rowType = resolve.rowType();

            // Found in current query's from list.  Find which from item.
            // We assume that the order of the from clause items has been
            // preserved.
            // 如果找到的解析结果在当前的查询FROM列表中，确定它是来自哪个FROM项。
            final SqlValidatorScope ancestorScope = resolve.scope;
            // 检查解析结果的作用域是否是当前作用域的直接父级
            boolean isParent = ancestorScope != scope;
            if ((inputs != null) && !isParent) {
                // 如果inputs不为空且不是父级作用域，则尝试在当前作用域内查找
                final LookupContext rels = new LookupContext(this, inputs, systemFieldList.size());
                // 根据解析路径的第一个步骤的索引在rels中查找RexNode
                final RexNode node = lookup(resolve.path.steps().get(0).i, rels);
                // 断言找到的RexNode不为null
                assert node != null;
                // 返回一个Pair对象，其中包含了找到的RexNode和一个用于字段访问的转换函数
                return Pair.of(
                        node,
                        (e, fieldName) -> {
                            // 获取指定字段名的RelDataTypeField，如果不存在则抛出异常
                            final RelDataTypeField field =
                                    requireNonNull(
                                            rowType.getField(fieldName, true, false),
                                            () -> "field " + fieldName);
                            // 使用RexBuilder创建一个字段访问表达式
                            return rexBuilder.makeFieldAccess(e, field.getIndex());
                        });
            } else {
                // We're referencing a relational expression which has not been
                // converted yet. This occurs when from items are correlated,
                // e.g. "select from emp as emp join emp.getDepts() as dept".
                // Create a temporary expression.
                // 我们正在引用一个尚未转换的关系表达式，这通常发生在from项是关联的情况下
                // 例如，"select from emp as emp join emp.getDepts() as dept"
                // 创建一个延迟查找对象
                DeferredLookup lookup = new DeferredLookup(this, qualified.identifier.names.get(0));
                // 创建一个新的关联ID
                final CorrelationId correlId = cluster.createCorrel();
                // 将关联ID映射到延迟查找对象
                mapCorrelToDeferred.put(correlId, lookup);
                // 如果解析路径的第一个步骤的索引小于0，表示这是一个特殊的关联表达式
                if (resolve.path.steps().get(0).i < 0) {
                    // 返回一个包含关联RexNode和null函数的Pair对象
                    return Pair.of(rexBuilder.makeCorrel(rowType, correlId), null);
                } else {
                    // 否则，开始构建一个新的RelDataType，这可能涉及从祖先作用域中提取字段信息
                    final RelDataTypeFactory.Builder builder = typeFactory.builder();
                    // 确保resolve.scope不为null，并转换为ListScope（这里假设了resolve.scope的类型）
                    final ListScope ancestorScope1 =
                            (ListScope) requireNonNull(resolve.scope, "resolve.scope");
                    // 开始构建一个不可变映射，用于存储字段名和索引的映射
                    final ImmutableMap.Builder<String, Integer> fields = ImmutableMap.builder();
                    // 初始化变量
                    int i = 0;// 用于遍历ancestorScope1的子节点的索引
                    int offset = 0;// 用于计算字段的偏移量，以便在最终的字段映射中正确处理索引
                    // 遍历ancestorScope1的所有子节点
                    for (SqlValidatorNamespace c : ancestorScope1.getChildren()) {
                        // 检查当前子节点是否为可空的
                        if (ancestorScope1.isChildNullable(i)) {
                            // 如果是可空的，则遍历该子节点的所有字段
                            // 并为每个字段创建一个新的类型，该类型是可空的，然后将这些字段添加到builder中
                            for (final RelDataTypeField f : c.getRowType().getFieldList()) {
                                builder.add(
                                        f.getName(),
                                        typeFactory.createTypeWithNullability(f.getType(), true));
                            }
                        } else {
                            // 如果子节点不是可空的，则直接将所有字段添加到builder中
                            builder.addAll(c.getRowType().getFieldList());
                        }
                        // 检查当前子节点是否是resolve.path中指定的子节点
                        if (i == resolve.path.steps().get(0).i) {
                            // 如果是，则遍历该子节点的所有字段
                            // 并将字段名和对应的索引（加上之前的偏移量）添加到fields映射中
                            for (RelDataTypeField field : c.getRowType().getFieldList()) {
                                fields.put(field.getName(), field.getIndex() + offset);
                            }
                        }
                        // 更新索引和偏移量
                        ++i;
                        // 将当前子节点的字段数加到偏移量上
                        offset += c.getRowType().getFieldCount();
                    }
                    // 使用builder构建一个新的类型，并创建一个关联表达式RexNode
                    final RexNode c = rexBuilder.makeCorrel(builder.uniquify().build(), correlId);
                    // 构建最终的字段名到索引的映射，并结束fields的构建
                    final ImmutableMap<String, Integer> fieldMap = fields.build();
                    // 返回一个Pair对象，包含了关联表达式RexNode和一个函数
                   // 该函数接受一个表达式e和字段名fieldName，并返回一个新的RexNode，表示对e中指定字段的访问
                    return Pair.of(
                            c,
                            (e, fieldName) -> {
                                // 从fieldMap中获取字段名对应的索引
                                // 如果找不到，则抛出异常
                                final int j =
                                        requireNonNull(
                                                fieldMap.get(fieldName), "field " + fieldName);
                                // 使用RexBuilder创建一个字段访问表达式
                                return rexBuilder.makeFieldAccess(e, j);
                            });
                }
            }
        }

        /**
         * Creates an expression with which to reference the expression whose offset in its
         * from-list is {@code offset}.
         */
        RexNode lookup(int offset, LookupContext lookupContext) {
            Pair<RelNode, Integer> pair = lookupContext.findRel(offset);
            return rexBuilder.makeRangeReference(pair.left.getRowType(), pair.right, false);
        }

        @Nullable
        RelDataTypeField getRootField(RexInputRef inputRef) {
            List<RelNode> inputs = this.inputs;
            if (inputs == null) {
                return null;
            }
            int fieldOffset = inputRef.getIndex();
            for (RelNode input : inputs) {
                RelDataType rowType = input.getRowType();
                if (fieldOffset < rowType.getFieldCount()) {
                    return rowType.getFieldList().get(fieldOffset);
                }
                fieldOffset -= rowType.getFieldCount();
            }
            return null;
        }

        public void flatten(
                List<RelNode> rels,
                int systemFieldCount,
                int[] start,
                List<Pair<RelNode, Integer>> relOffsetList) {
            for (RelNode rel : rels) {
                if (leaves.containsKey(rel)) {
                    relOffsetList.add(Pair.of(rel, start[0]));
                    start[0] += leaves.get(rel);
                } else if (rel instanceof LogicalMatch) {
                    relOffsetList.add(Pair.of(rel, start[0]));
                    start[0] += rel.getRowType().getFieldCount();
                } else {
                    if (rel instanceof LogicalJoin || rel instanceof LogicalAggregate) {
                        start[0] += systemFieldCount;
                    }
                    flatten(rel.getInputs(), systemFieldCount, start, relOffsetList);
                }
            }
        }

        void registerSubQuery(SqlNode node, RelOptUtil.Logic logic) {
            for (SubQuery subQuery : subQueryList) {
                // Compare the reference to make sure the matched node has
                // exact scope where it belongs.
                if (node == subQuery.node) {
                    return;
                }
            }
            subQueryList.add(new SubQuery(node, logic));
        }

        @Nullable
        SubQuery getSubQuery(SqlNode expr) {
            for (SubQuery subQuery : subQueryList) {
                // Compare the reference to make sure the matched node has
                // exact scope where it belongs.
                if (expr == subQuery.node) {
                    return subQuery;
                }
            }

            return null;
        }

        ImmutableList<RelNode> retrieveCursors() {
            try {
                return ImmutableList.copyOf(cursors);
            } finally {
                cursors.clear();
            }
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 将SqlNode表达式转换为RexNode表达式
         */
        @Override
        public RexNode convertExpression(SqlNode expr) {
            // If we're in aggregation mode and this is an expression in the
            // GROUP BY clause, return a reference to the field.
            // 如果我们处于聚合模式，并且当前表达式在GROUP BY子句中，
            // 则直接返回对该字段的引用。
            AggConverter agg = this.agg;// 获取聚合转换器实例
            if (agg != null) {  // 如果聚合转换器存在
                // 展开表达式，处理可能的别名或嵌套表达式
                final SqlNode expandedGroupExpr = validator().expand(expr, scope());
                // 查找展开后的GROUP BY表达式在聚合转换器中的索引
                final int ref = agg.lookupGroupExpr(expandedGroupExpr);
                if (ref >= 0) {
                    // 如果找到了索引，则创建并返回对该字段的输入引用
                    return rexBuilder.makeInputRef(root(), ref);
                }
                // 如果表达式是一个函数调用（SqlCall）
                if (expr instanceof SqlCall) {
                    // 尝试在聚合转换器中查找该函数调用对应的聚合表达式
                    final RexNode rex = agg.lookupAggregates((SqlCall) expr);
                    if (rex != null) {
                        // 如果找到了对应的聚合表达式，则返回它
                        return rex;
                    }
                }
            }

            // Allow the derived class chance to override the standard
            // behavior for special kinds of expressions.
            // 转换非标准表达式，以处理特殊类型的表达式
            RexNode rex = convertExtendedExpression(expr, this);
            if (rex != null) {
                // 如果派生类已经处理了该表达式，并返回了一个RexNode，则直接返回该RexNode
                return rex;
            }

            // Sub-queries and OVER expressions are not like ordinary
            // expressions.
            // 子查询和OVER表达式与普通表达式不同。
            final SqlKind kind = expr.getKind();// 获取表达式的类型
            // 声明一个SubQuery类型的变量
            final SubQuery subQuery;
            if (!config.isExpand()) {// 如果配置不允许展开表达式
                final SqlCall call;// 声明一个SqlCall类型的变量，用于存储函数调用表达式
                final SqlNode query;// 声明一个SqlNode类型的变量，用于存储子查询
                final RelRoot root;// 声明一个RelRoot类型的变量，用于存储转换后的查询关系根
                // 根据表达式的类型（如IN, NOT IN, SOME, ALL）进行不同的处理
                switch (kind) {
                    case IN:
                    case NOT_IN:
                    case SOME:
                    case ALL:
                        // 将表达式转换为SqlCall类型，因为这些类型通常是通过函数调用来表示的
                        call = (SqlCall) expr;
                        // 获取子查询部分，它通常是函数调用的第二个操作数
                        query = call.operand(1);
                        // 如果子查询不是一个SqlNodeList（表示可能是一个更复杂的查询），则需要递归转换
                        if (!(query instanceof SqlNodeList)) {
                            // 递归转换子查询为RelRoot
                            root = convertQueryRecursive(query, false, null);
                            // 获取函数调用的第一个操作数
                            final SqlNode operand = call.operand(0);
                            // 根据第一个操作数的类型
                            List<SqlNode> nodes;
                            switch (operand.getKind()) {
                                case ROW:
                                    // 如果操作数是ROW类型，则获取其操作数列表
                                    nodes = ((SqlCall) operand).getOperandList();
                                    break;
                                default:
                                    // 如果不是ROW类型，则将其视为单个操作数并创建列表
                                    nodes = ImmutableList.of(operand);
                            }
                            final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                            for (SqlNode node : nodes) {
                                builder.add(convertExpression(node));
                            }
                            final ImmutableList<RexNode> list = builder.build();
                            switch (kind) {
                                case IN:
                                    return RexSubQuery.in(root.rel, list);
                                case NOT_IN:
                                    return rexBuilder.makeCall(
                                            SqlStdOperatorTable.NOT,
                                            RexSubQuery.in(root.rel, list));
                                case SOME:
                                    return RexSubQuery.some(
                                            root.rel,
                                            list,
                                            (SqlQuantifyOperator) call.getOperator());
                                case ALL:
                                    return rexBuilder.makeCall(
                                            SqlStdOperatorTable.NOT,
                                            RexSubQuery.some(
                                                    root.rel,
                                                    list,
                                                    negate(
                                                            (SqlQuantifyOperator)
                                                                    call.getOperator())));
                                default:
                                    throw new AssertionError(kind);
                            }
                        }
                        break;

                    case EXISTS:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        RelNode rel = root.rel;
                        while (rel instanceof Project
                                || rel instanceof Sort
                                        && ((Sort) rel).fetch == null
                                        && ((Sort) rel).offset == null) {
                            rel = ((SingleRel) rel).getInput();
                        }
                        return RexSubQuery.exists(rel);

                    case UNIQUE:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        return RexSubQuery.unique(root.rel);

                    case SCALAR_QUERY:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        return RexSubQuery.scalar(root.rel);

                    case ARRAY_QUERY_CONSTRUCTOR:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        return RexSubQuery.array(root.rel);

                    case MAP_QUERY_CONSTRUCTOR:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        return RexSubQuery.map(root.rel);

                    case MULTISET_QUERY_CONSTRUCTOR:
                        call = (SqlCall) expr;
                        query = Iterables.getOnlyElement(call.getOperandList());
                        root = convertQueryRecursive(query, false, null);
                        return RexSubQuery.multiset(root.rel);

                    default:
                        break;
                }
            }

            switch (kind) {
                case SOME:
                case ALL:
                case UNIQUE:
                    if (config.isExpand()) {
                        throw new RuntimeException(kind + " is only supported if expand = false");
                    }
                    // fall through
                case CURSOR:
                case IN:
                case NOT_IN:
                    subQuery = requireNonNull(getSubQuery(expr));
                    rex = requireNonNull(subQuery.expr);
                    return StandardConvertletTable.castToValidatedType(
                            expr, rex, validator(), rexBuilder);

                case SELECT:
                case EXISTS:
                case SCALAR_QUERY:
                case ARRAY_QUERY_CONSTRUCTOR:
                case MAP_QUERY_CONSTRUCTOR:
                case MULTISET_QUERY_CONSTRUCTOR:
                    subQuery = getSubQuery(expr);
                    assert subQuery != null;
                    rex = subQuery.expr;
                    assert rex != null : "rex != null";

                    if (((kind == SqlKind.SCALAR_QUERY) || (kind == SqlKind.EXISTS))
                            && isConvertedSubq(rex)) {
                        // scalar sub-query or EXISTS has been converted to a
                        // constant
                        return rex;
                    }

                    // The indicator column is the last field of the sub-query.
                    RexNode fieldAccess =
                            rexBuilder.makeFieldAccess(rex, rex.getType().getFieldCount() - 1);

                    // The indicator column will be nullable if it comes from
                    // the null-generating side of the join. For EXISTS, add an
                    // "IS TRUE" check so that the result is "BOOLEAN NOT NULL".
                    if (fieldAccess.getType().isNullable() && kind == SqlKind.EXISTS) {
                        fieldAccess =
                                rexBuilder.makeCall(SqlStdOperatorTable.IS_NOT_NULL, fieldAccess);
                    }
                    return fieldAccess;

                case OVER:
                    return convertOver(this, expr);

                default:
                    // fall through
            }

            // Apply standard conversions.
            rex = expr.accept(this);
            return requireNonNull(rex, "rex");
        }

        /**
         * Converts an item in an ORDER BY clause inside a window (OVER) clause, extracting DESC,
         * NULLS LAST and NULLS FIRST flags first.
         */
        public RexFieldCollation convertSortExpression(
                SqlNode expr,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection) {
            return convertSortExpression(
                    expr, direction, nullDirection, this::sortToRexFieldCollation);
        }

        /**
         * Handles an item in an ORDER BY clause, passing using a converter function to produce the
         * final result.
         */
        <R> R convertSortExpression(
                SqlNode expr,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection,
                SortExpressionConverter<R> converter) {
            switch (expr.getKind()) {
                case DESCENDING:
                    return convertSortExpression(
                            ((SqlCall) expr).operand(0),
                            RelFieldCollation.Direction.DESCENDING,
                            nullDirection,
                            converter);
                case NULLS_LAST:
                    return convertSortExpression(
                            ((SqlCall) expr).operand(0),
                            direction,
                            RelFieldCollation.NullDirection.LAST,
                            converter);
                case NULLS_FIRST:
                    return convertSortExpression(
                            ((SqlCall) expr).operand(0),
                            direction,
                            RelFieldCollation.NullDirection.FIRST,
                            converter);
                default:
                    return converter.convert(expr, direction, nullDirection);
            }
        }

        private RexFieldCollation sortToRexFieldCollation(
                SqlNode expr,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection) {
            final Set<SqlKind> flags = EnumSet.noneOf(SqlKind.class);
            if (direction == RelFieldCollation.Direction.DESCENDING) {
                flags.add(SqlKind.DESCENDING);
            }
            switch (nullDirection) {
                case UNSPECIFIED:
                    final RelFieldCollation.NullDirection nullDefaultDirection =
                            validator().config().defaultNullCollation().last(desc(direction))
                                    ? RelFieldCollation.NullDirection.LAST
                                    : RelFieldCollation.NullDirection.FIRST;
                    if (nullDefaultDirection != direction.defaultNullDirection()) {
                        SqlKind nullDirectionSqlKind =
                                validator().config().defaultNullCollation().last(desc(direction))
                                        ? SqlKind.NULLS_LAST
                                        : SqlKind.NULLS_FIRST;
                        flags.add(nullDirectionSqlKind);
                    }
                    break;
                case FIRST:
                    flags.add(SqlKind.NULLS_FIRST);
                    break;
                case LAST:
                    flags.add(SqlKind.NULLS_LAST);
                    break;
                default:
                    break;
            }
            return new RexFieldCollation(convertExpression(expr), flags);
        }

        private RexNode sortToRex(
                SqlNode expr,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection) {
            RexNode node = convertExpression(expr);
            if (direction == RelFieldCollation.Direction.DESCENDING) {
                node = relBuilder.desc(node);
            }
            // ----- FLINK MODIFICATION BEGIN -----
            // if null direction is unspecified then check default
            // to keep same behavior as before Calcite 1.27.0
            if (nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED) {
                nullDirection =
                        validator().config().defaultNullCollation().last(desc(direction))
                                ? RelFieldCollation.NullDirection.LAST
                                : RelFieldCollation.NullDirection.FIRST;
            }
            // ----- FLINK MODIFICATION END -----
            if (nullDirection == RelFieldCollation.NullDirection.FIRST) {
                node = relBuilder.nullsFirst(node);
            }
            if (nullDirection == RelFieldCollation.NullDirection.LAST) {
                node = relBuilder.nullsLast(node);
            }
            return node;
        }

        /**
         * Determines whether a RexNode corresponds to a sub-query that's been converted to a
         * constant.
         *
         * @param rex the expression to be examined
         * @return true if the expression is a dynamic parameter, a literal, or a literal that is
         *     being cast
         */
        private boolean isConvertedSubq(RexNode rex) {
            if ((rex instanceof RexLiteral) || (rex instanceof RexDynamicParam)) {
                return true;
            }
            if (rex instanceof RexCall) {
                RexCall call = (RexCall) rex;
                if (call.getOperator() == SqlStdOperatorTable.CAST) {
                    RexNode operand = call.getOperands().get(0);
                    if (operand instanceof RexLiteral) {
                        return true;
                    }
                }
            }
            return false;
        }

        @Override
        public int getGroupCount() {
            if (agg != null) {
                return agg.groupExprs.size();
            }
            if (window != null) {
                return window.isAlwaysNonEmpty() ? 1 : 0;
            }
            return -1;
        }

        @Override
        public RexBuilder getRexBuilder() {
            return rexBuilder;
        }

        @Override
        public SqlNode validateExpression(RelDataType rowType, SqlNode expr) {
            return SqlValidatorUtil.validateExprWithRowType(
                            catalogReader.nameMatcher().isCaseSensitive(),
                            opTab,
                            typeFactory,
                            rowType,
                            expr)
                    .left;
        }

        @Override
        public RexRangeRef getSubQueryExpr(SqlCall call) {
            final SubQuery subQuery = getSubQuery(call);
            assert subQuery != null;
            return (RexRangeRef) requireNonNull(subQuery.expr, () -> "subQuery.expr for " + call);
        }

        @Override
        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }

        @Override
        public InitializerExpressionFactory getInitializerExpressionFactory() {
            return initializerExpressionFactory;
        }

        @Override
        public SqlValidator getValidator() {
            return validator();
        }

        @Override
        public RexNode convertLiteral(SqlLiteral literal) {
            return exprConverter.convertLiteral(this, literal);
        }

        public RexNode convertInterval(SqlIntervalQualifier intervalQualifier) {
            return exprConverter.convertInterval(this, intervalQualifier);
        }

        @Override
        public RexNode visit(SqlLiteral literal) {
            return exprConverter.convertLiteral(this, literal);
        }

        @Override
        public RexNode visit(SqlCall call) {
            if (agg != null) {
                final SqlOperator op = call.getOperator();
                if (window == null
                        && (op.isAggregator()
                                || op.getKind() == SqlKind.FILTER
                                || op.getKind() == SqlKind.WITHIN_DISTINCT
                                || op.getKind() == SqlKind.WITHIN_GROUP)) {
                    return requireNonNull(
                            agg.lookupAggregates(call),
                            () -> "agg.lookupAggregates for call " + call);
                }
            }
            return exprConverter.convertCall(
                    this, new SqlCallBinding(validator(), scope, call).permutedCall());
        }

        @Override
        public RexNode visit(SqlNodeList nodeList) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RexNode visit(SqlIdentifier id) {
            return convertIdentifier(this, id);
        }

        @Override
        public RexNode visit(SqlDataTypeSpec type) {
            throw new UnsupportedOperationException();
        }

        @Override
        public RexNode visit(SqlDynamicParam param) {
            return convertDynamicParam(param);
        }

        @Override
        public RexNode visit(SqlIntervalQualifier intervalQualifier) {
            return convertInterval(intervalQualifier);
        }

        public List<SqlMonotonicity> getColumnMonotonicities() {
            return columnMonotonicities;
        }
    }

    private static SqlQuantifyOperator negate(SqlQuantifyOperator operator) {
        assert operator.kind == SqlKind.ALL;
        return SqlStdOperatorTable.some(operator.comparisonKind.negateNullSafe());
    }

    /** Deferred lookup. */
    private static class DeferredLookup {
        Blackboard bb;
        String originalRelName;

        DeferredLookup(Blackboard bb, String originalRelName) {
            this.bb = bb;
            this.originalRelName = originalRelName;
        }

        public RexFieldAccess getFieldAccess(CorrelationId name) {
            return (RexFieldAccess)
                    requireNonNull(
                            bb.mapCorrelateToRex.get(name),
                            () -> "Correlation " + name + " is not found");
        }

        public String getOriginalRelName() {
            return originalRelName;
        }
    }

    /** A default implementation of SubQueryConverter that does no conversion. */
    private static class NoOpSubQueryConverter implements SubQueryConverter {
        @Override
        public boolean canConvertSubQuery() {
            return false;
        }

        @Override
        public RexNode convertSubQuery(
                SqlCall subQuery,
                SqlToRelConverter parentConverter,
                boolean isExists,
                boolean isExplain) {
            throw new IllegalArgumentException();
        }
    }

    /**
     * Converts expressions to aggregates.
     *
     * <p>Consider the expression
     *
     * <blockquote>
     *
     * {@code SELECT deptno, SUM(2 * sal) FROM emp GROUP BY deptno}
     *
     * </blockquote>
     *
     * <p>Then:
     *
     * <ul>
     *   <li>groupExprs = {SqlIdentifier(deptno)}
     *   <li>convertedInputExprs = {RexInputRef(deptno), 2 * RefInputRef(sal)}
     *   <li>inputRefs = {RefInputRef(#0), RexInputRef(#1)}
     *   <li>aggCalls = {AggCall(SUM, {1})}
     * </ul>
     */
    protected class AggConverter implements SqlVisitor<Void> {
        private final Blackboard bb;
        public final @Nullable AggregatingSelectScope aggregatingSelectScope;

        private final Map<String, String> nameMap = new HashMap<>();

        /** The group-by expressions, in {@link SqlNode} format. */
        private final SqlNodeList groupExprs = new SqlNodeList(SqlParserPos.ZERO);

        /** The auxiliary group-by expressions. */
        private final Map<SqlNode, Ord<AuxiliaryConverter>> auxiliaryGroupExprs = new HashMap<>();

        /**
         * Input expressions for the group columns and aggregates, in {@link RexNode} format. The
         * first elements of the list correspond to the elements in {@link #groupExprs}; the
         * remaining elements are for aggregates. The right field of each pair is the name of the
         * expression, where the expressions are simple mappings to input fields.
         */
        private final List<Pair<RexNode, @Nullable String>> convertedInputExprs = new ArrayList<>();

        /**
         * Expressions to be evaluated as rows are being placed into the aggregate's hash table.
         * This is when group functions such as TUMBLE cause rows to be expanded.
         */
        private final List<AggregateCall> aggCalls = new ArrayList<>();

        private final Map<SqlNode, RexNode> aggMapping = new HashMap<>();
        private final Map<AggregateCall, RexNode> aggCallMapping = new HashMap<>();

        /** Whether we are directly inside a windowed aggregate. */
        private boolean inOver = false;

        AggConverter(Blackboard bb, @Nullable AggregatingSelectScope aggregatingSelectScope) {
            this.bb = bb;
            this.aggregatingSelectScope = aggregatingSelectScope;
        }

        /**
         * Creates an AggConverter.
         *
         * <p>The <code>select</code> parameter provides enough context to name aggregate calls
         * which are top-level select list items.
         *
         * @param bb Blackboard
         * @param select Query being translated; provides context to give
         */
        public AggConverter(Blackboard bb, SqlSelect select) {
            this(bb, (AggregatingSelectScope) bb.getValidator().getSelectScope(select));

            // Collect all expressions used in the select list so that aggregate
            // calls can be named correctly.
            final SqlNodeList selectList = select.getSelectList();
            for (int i = 0; i < selectList.size(); i++) {
                SqlNode selectItem = selectList.get(i);
                String name = null;
                if (SqlUtil.isCallTo(selectItem, SqlStdOperatorTable.AS)) {
                    final SqlCall call = (SqlCall) selectItem;
                    selectItem = call.operand(0);
                    name = call.operand(1).toString();
                }
                if (name == null) {
                    name = validator().deriveAlias(selectItem, i);
                    assert name != null : "alias must not be null for " + selectItem + ", i=" + i;
                }
                nameMap.put(selectItem.toString(), name);
            }
        }

        public int addGroupExpr(SqlNode expr) {
            // 查找给定的表达式是否已存在于分组表达式列表中
            int ref = lookupGroupExpr(expr);
            // 如果已存在，则返回其索引（或引用）
            if (ref >= 0) {
                return ref;
            }
            // 否则，将表达式添加到分组表达式列表中，并获取其索引
            final int index = groupExprs.size();
            groupExprs.add(expr);
            // 尝试从名称映射中获取表达式名称，但这里直接使用表达式的字符串表示可能不够精确
            // 通常情况下，表达式到名称的映射可能需要更复杂的逻辑
            String name = nameMap.get(expr.toString());
            // 将原始SqlNode表达式转换为RexNode表达式，RexNode是Calcite框架中用于表示表达式的类
            RexNode convExpr = bb.convertExpression(expr);
            // 将转换后的RexNode表达式和名称添加到另一个管理表达式的结构中
            // 这个步骤可能用于优化、查询计划生成等
            addExpr(convExpr, name);
            // 如果传入的expr是一个SqlCall（即一个函数调用），则进行特殊处理
            if (expr instanceof SqlCall) {
                SqlCall call = (SqlCall) expr;
                // 遍历并处理所有需要转换为辅助调用的分组表达式
                for (Pair<SqlNode, AuxiliaryConverter> p :
                        SqlStdOperatorTable.convertGroupToAuxiliaryCalls(call)) {
                    // 为每个转换后的表达式添加辅助分组表达式
                    // 这里的index用于标识原始分组表达式在列表中的位置
                    // AuxiliaryConverter可能包含了转换的详细信息或逻辑
                    addAuxiliaryGroupExpr(p.left, index, p.right);
                }
            }
            // 返回新添加的分组表达式的索引
            return index;
        }

        void addAuxiliaryGroupExpr(SqlNode node, int index, AuxiliaryConverter converter) {
            for (SqlNode node2 : auxiliaryGroupExprs.keySet()) {
                if (node2.equalsDeep(node, Litmus.IGNORE)) {
                    return;
                }
            }
            auxiliaryGroupExprs.put(node, Ord.of(index, converter));
        }

        /**
         * Adds an expression, deducing an appropriate name if possible.
         *
         * @param expr Expression
         * @param name Suggested name
         */
        private void addExpr(RexNode expr, @Nullable String name) {
            if ((name == null) && (expr instanceof RexInputRef)) {
                final int i = ((RexInputRef) expr).getIndex();
                name = bb.root().getRowType().getFieldList().get(i).getName();
            }
            if (Pair.right(convertedInputExprs).contains(name)) {
                // In case like 'SELECT ... GROUP BY x, y, x', don't add
                // name 'x' twice.
                name = null;
            }
            convertedInputExprs.add(Pair.of(expr, name));
        }

        @Override
        public Void visit(SqlIdentifier id) {
            return null;
        }

        @Override
        public Void visit(SqlNodeList nodeList) {
            for (int i = 0; i < nodeList.size(); i++) {
                nodeList.get(i).accept(this);
            }
            return null;
        }

        @Override
        public Void visit(SqlLiteral lit) {
            return null;
        }

        @Override
        public Void visit(SqlDataTypeSpec type) {
            return null;
        }

        @Override
        public Void visit(SqlDynamicParam param) {
            return null;
        }

        @Override
        public Void visit(SqlIntervalQualifier intervalQualifier) {
            return null;
        }

        @Override
        public Void visit(SqlCall call) {
            // 使用switch语句来根据SqlCall的类型执行不同的逻辑
            switch (call.getKind()) {
                // 对于FILTER、IGNORE_NULLS、RESPECT_NULLS、WITHIN_DISTINCT、WITHIN_GROUP等特殊类型的SqlCall，
                // 调用translateAgg方法进行处理，并返回null表示不再继续递归
                case FILTER:
                case IGNORE_NULLS:
                case RESPECT_NULLS:
                case WITHIN_DISTINCT:
                case WITHIN_GROUP:
                    translateAgg(call);
                    return null;
                // 如果是SELECT类型的SqlCall（通常表示一个子查询），则不检测其中的聚合函数，直接返回null
                case SELECT:
                    // rchen 2006-10-17:
                    // for now do not detect aggregates in sub-queries.
                    return null;
                default:
                    break;
            }
            // 标记之前的inOver状态，用于处理与OVER操作符相关的逻辑
            final boolean prevInOver = inOver;
            // Ignore window aggregates and ranking functions (associated with OVER
            // operator). However, do not ignore nested window aggregates.
            // 忽略与OVER操作符关联的窗口聚合和排名函数，但不忽略嵌套的窗口聚合
            if (call.getOperator().getKind() == SqlKind.OVER) {
                // Track aggregate nesting levels only within an OVER operator.
                // 遍历OVER操作符的操作数列表
                List<SqlNode> operandList = call.getOperandList();
                assert operandList.size() == 2;// 通常OVER操作符有两个操作数

                // Ignore the top level window aggregates and ranking functions
                // positioned as the first operand of a OVER operator
                // 忽略OVER操作符的第一个操作数（通常是窗口聚合或排名函数），并设置inOver为true
                inOver = true;
                operandList.get(0).accept(this);// 递归访问第一个操作数，但不做特别处理

                // Normal translation for the second operand of a OVER operator
                // 恢复正常状态，并处理OVER操作符的第二个操作数（如分区键或排序键）
                inOver = false;
                operandList.get(1).accept(this);// 递归访问第二个操作数
                return null;// OVER操作符处理完毕，返回null
            }

            // Do not translate the top level window aggregate. Only do so for
            // nested aggregates, if present
            // 对于聚合函数，仅在嵌套在OVER操作符内部时不做特别处理（即不调用translateAgg），
            // 否则（即在顶层或嵌套在其他非OVER结构中）调用translateAgg进行处理
            if (call.getOperator().isAggregator()) {
                if (inOver) {
                    // Add the parent aggregate level before visiting its children
                    // 如果当前在OVER操作符内部，则不处理该聚合函数，并恢复inOver状态为false
                    // 以供后续可能的嵌套聚合函数处理
                    inOver = false;
                } else {
                    // We're beyond the one ignored level
                    // 不在OVER操作符内部，调用translateAgg处理该聚合函数
                    translateAgg(call);
                    return null;
                }
            }
            // 遍历SqlCall对象的所有操作数
            for (SqlNode operand : call.getOperandList()) {
                // Operands are occasionally null, e.g. switched CASE arg 0.
                // 操作数列表中的元素有时可能是null，例如在某些情况下CASE语句的第一个参数可能被省略。
                // 因此，在尝试访问之前需要检查操作数是否为null。
                if (operand != null) {
                    operand.accept(this);
                }
            }
            // Remove the parent aggregate level after visiting its children
            inOver = prevInOver;
            // 返回null，表示当前SqlCall节点的处理已经完成，且没有特定的返回值。
            return null;
        }

        private void translateAgg(SqlCall call) {
            translateAgg(call, null, null, null, false, call);
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 转换聚合函数
         */
        private void translateAgg(
                SqlCall call,
                @Nullable SqlNode filter,
                @Nullable SqlNodeList distinctList,
                @Nullable SqlNodeList orderList,
                boolean ignoreNulls,
                SqlCall outerCall) {
            // 断言确保当前聚合对象（可能是当前类的实例）是bb.agg
            assert bb.agg == this;
            // 断言确保外部调用对象不为null
            assert outerCall != null;
            // 获取当前聚合调用的操作数列表
            final List<SqlNode> operands = call.getOperandList();
            // 获取当前聚合调用的解析位置
            final SqlParserPos pos = call.getParserPosition();
            // 定义一个临时的SqlCall变量，用于后续可能的替换或操作
            final SqlCall call2;
            // 根据当前聚合调用的种类进行不同的处理
            switch (call.getKind()) {
                case FILTER:// 处理FILTER聚合（如FILTER(SUM(x) FILTER (WHERE y > 0))）
                    assert filter == null;
                    // 递归调用translateAgg处理FILTER内的聚合调用，传入FILTER的过滤条件和其他参数
                    translateAgg(
                            call.operand(0),
                            call.operand(1),
                            distinctList,
                            orderList,
                            ignoreNulls,
                            outerCall);
                    return;
                case WITHIN_DISTINCT:// 处理DISTINCT内的聚合（如SUM(DISTINCT x)）
                    // 断言确保此聚合调用不应有排序列表（因为DISTINCT与ORDER BY不兼容）
                    assert orderList == null;
                    // 递归调用translateAgg处理DISTINCT内的聚合调用，传入DISTINCT的列表和其他参数
                    translateAgg(
                            call.operand(0),
                            filter,
                            call.operand(1),
                            orderList,
                            ignoreNulls,
                            outerCall);
                    return;
                case WITHIN_GROUP:// 处理GROUP内的聚合或窗口函数相关
                    assert orderList == null;
                    // 递归调用translateAgg处理GROUP内的聚合调用或窗口函数相关逻辑，传入分组或窗口的列表和其他参数
                    translateAgg(
                            call.operand(0),
                            filter,
                            distinctList,
                            call.operand(1),
                            ignoreNulls,
                            outerCall);
                    return;
                case IGNORE_NULLS:
                    ignoreNulls = true;
                    // fall through
                case RESPECT_NULLS:
                    translateAgg(
                            call.operand(0),
                            filter,
                            distinctList,
                            orderList,
                            ignoreNulls,
                            outerCall);
                    return;

                case COUNTIF:// 处理COUNTIF聚合函数
                    // COUNTIF(b)  ==> COUNT(*) FILTER (WHERE b)
                    // COUNTIF(b) FILTER (WHERE b2)  ==> COUNT(*) FILTER (WHERE b2 AND b)
                    // COUNTIF(b) 被转换为 COUNT(*) FILTER (WHERE b)
                    // COUNTIF(b) FILTER (WHERE b2) 被转换为 COUNT(*) FILTER (WHERE b2 AND b)
                    // 创建一个COUNT(*)的调用
                    call2 = SqlStdOperatorTable.COUNT.createCall(pos, SqlIdentifier.star(pos));
                    // 如果存在外部过滤器，则将其与COUNTIF的条件组合成一个新的过滤器
                    final SqlNode filter2 = SqlUtil.andExpressions(filter, call.operand(0));
                    // 使用新的过滤器和其他参数调用translateAgg
                    translateAgg(call2, filter2, distinctList, orderList, ignoreNulls, outerCall);
                    return;

                case STRING_AGG:// 处理STRING_AGG聚合函数
                    // Translate "STRING_AGG(s, sep ORDER BY x, y)"
                    // as if it were "LISTAGG(s, sep) WITHIN GROUP (ORDER BY x, y)";
                    // and "STRING_AGG(s, sep)" as "LISTAGG(s, sep)".

                    // 将 "STRING_AGG(s, sep ORDER BY x, y)" 翻译为类似 "LISTAGG(s, sep) WITHIN GROUP (ORDER BY x, y)" 的形式
                    // 并且将 "STRING_AGG(s, sep)" 翻译为 "LISTAGG(s, sep)"
                    // 首先处理ORDER BY子句（如果存在）
                    final List<SqlNode> operands2;
                    if (!operands.isEmpty() && Util.last(operands) instanceof SqlNodeList) {
                        // 如果操作数列表的最后一个元素是SqlNodeList，则假定它是ORDER BY子句
                        orderList = (SqlNodeList) Util.last(operands);
                        // 移除ORDER BY子句，保留其他操作数
                        operands2 = Util.skipLast(operands);
                    } else {
                        // 如果没有ORDER BY子句，则直接使用原始操作数列表
                        operands2 = operands;
                    }
                    // 创建一个LISTAGG的调用，使用修改后的操作数列表
                    call2 =
                            SqlStdOperatorTable.LISTAGG.createCall(
                                    call.getFunctionQuantifier(), pos, operands2);
                    // 使用新的调用和其他参数调用translateAgg
                    translateAgg(call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                    return;

                case GROUP_CONCAT:
                    // Translate "GROUP_CONCAT(s ORDER BY x, y SEPARATOR ',')"
                    // as if it were "LISTAGG(s, ',') WITHIN GROUP (ORDER BY x, y)".
                    // To do this, build a list of operands without ORDER BY with with sep.

                    // 将 "GROUP_CONCAT(s ORDER BY x, y SEPARATOR ',')" 翻译为 "LISTAGG(s, ',') WITHIN GROUP (ORDER BY x, y)" 的形式。
                    // 首先，我们构建一个新的操作数列表，但不包含 ORDER BY 子句和 SEPARATOR（如果它们存在的话）。
                    operands2 = new ArrayList<>(operands);
                    final SqlNode separator;
                    // 检查并提取 ORDER BY 子句，如果存在的话
                    if (!operands2.isEmpty()
                            && Util.last(operands2).getKind() == SqlKind.SEPARATOR) {
                        // 假设最后一个操作数是一个SqlNodeList，表示ORDER BY子句
                        final SqlCall sepCall = (SqlCall) operands2.remove(operands.size() - 1);
                        separator = sepCall.operand(0);
                    } else {
                        separator = null;
                    }

                    if (!operands2.isEmpty() && Util.last(operands2) instanceof SqlNodeList) {
                        orderList = (SqlNodeList) operands2.remove(operands2.size() - 1);
                    }
                    // 如果找到了分隔符，则将其添加回操作数列表（因为我们在前面移除了它）
                    if (separator != null) {
                        operands2.add(separator);// 分隔符应该放在除ORDER BY外的其他操作数之后
                    }

                    // 创建一个LISTAGG的调用，使用修改后的操作数列表（现在包含分隔符但不包含ORDER BY）
                    call2 =
                            SqlStdOperatorTable.LISTAGG.createCall(
                                    call.getFunctionQuantifier(), pos, operands2);
                    // 使用新的LISTAGG调用、过滤器、DISTINCT列表、ORDER BY列表、ignoreNulls标志和外部调用作为参数，调用translateAgg函数
                    translateAgg(call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                    return;
                // 处理ARRAY_AGG和ARRAY_CONCAT_AGG的情况
                case ARRAY_AGG:
                case ARRAY_CONCAT_AGG:
                    // Translate "ARRAY_AGG(s ORDER BY x, y)"
                    // as if it were "ARRAY_AGG(s) WITHIN GROUP (ORDER BY x, y)";
                    // similarly "ARRAY_CONCAT_AGG".

                    // 将 "ARRAY_AGG(s ORDER BY x, y)" 翻译为 "ARRAY_AGG(s) WITHIN GROUP (ORDER BY x, y)" 的形式。
                    // 类似地处理ARRAY_CONCAT_AGG。
                    if (!operands.isEmpty() && Util.last(operands) instanceof SqlNodeList) {
                        // 如果最后一个操作数是SqlNodeList类型，则假设它表示ORDER BY子句
                        orderList = (SqlNodeList) Util.last(operands);
                        // 创建一个新的调用，但去掉最后一个操作数（ORDER BY子句），因为ORDER BY将在translateAgg中单独处理
                        call2 =
                                call.getOperator()
                                        .createCall(
                                                call.getFunctionQuantifier(),
                                                pos,
                                                Util.skipLast(operands));
                        // 调用translateAgg来处理聚合，包括新的调用、过滤器、DISTINCT列表、ORDER BY列表等
                        translateAgg(
                                call2, filter, distinctList, orderList, ignoreNulls, outerCall);
                        return;
                    }
                    // "ARRAY_AGG" and "ARRAY_CONCAT_AGG" without "ORDER BY"
                    // are handled normally; fall through.

                default:
                    break;
            }
            // 以下代码段是处理非聚合函数或不需要特殊处理的情况
            final List<Integer> args = new ArrayList<>(); // 初始化一个参数列表
            int filterArg = -1;// 初始化一个变量来标记过滤器的参数位置，默认为-1表示没有过滤器
            final ImmutableBitSet distinctKeys;// 初始化一个不可变位集，可能用于标记DISTINCT子句中涉及的列
            try {
                // switch out of agg mode
                // 退出聚合模式（如果之前处于该模式）
                bb.agg = null;
                // ----- FLINK MODIFICATION BEGIN -----
                // 创建一个SqlCallBinding对象，用于绑定SQL调用和验证上下文
                SqlCallBinding sqlCallBinding =
                        new SqlCallBinding(validator(), aggregatingSelectScope, call);
                // 通过SqlCallBinding获取操作数列表
                List<SqlNode> sqlNodes = sqlCallBinding.operands();
                // 创建一个FlinkOperatorBinding对象，可能是为了将SQL操作绑定到Flink特有的执行逻辑上
                FlinkOperatorBinding flinkOperatorBinding =
                        new FlinkOperatorBinding(sqlCallBinding);
                // 遍历SQL调用中的所有操作数（节点）
                for (int i = 0; i < sqlNodes.size(); i++) {
                    SqlNode operand = sqlNodes.get(i);// 获取当前操作数
                    // special case for COUNT(*):  delete the *

                    // 特殊情况处理：如果是COUNT(*)函数，则忽略星号(*)
                    if (operand instanceof SqlIdentifier) {
                        SqlIdentifier id = (SqlIdentifier) operand;
                        if (id.isStar()) {// 判断是否是星号(*)
                            assert call.operandCount() == 1;
                            assert args.isEmpty();
                            break;
                        }
                    }
                    // 将SQL节点转换为RexNode表达式
                    RexNode convertedExpr = bb.convertExpression(operand);
                    // 如果转换后的RexNode表达式的种类是DEFAULT（可能表示类型不匹配或需要特殊处理）
                    if (convertedExpr.getKind() == SqlKind.DEFAULT) {
                        // 获取当前操作数的数据类型
                        RelDataType relDataType = flinkOperatorBinding.getOperandType(i);
                        // 克隆原始的RexCall表达式，但使用新的数据类型和相同的操作数
                        convertedExpr =
                                ((RexCall) convertedExpr)
                                        .clone(relDataType, ((RexCall) convertedExpr).operands);
                    }
                    // 将转换后的表达式添加到参数列表中，可能需要查找或创建分组表达式
                    args.add(lookupOrCreateGroupExpr(convertedExpr));
                }
                // ----- FLINK MODIFICATION END -----
                // 如果存在过滤器
                if (filter != null) {
                    // 将过滤器表达式转换为RexNode
                    RexNode convertedExpr = bb.convertExpression(filter);
                    // 如果转换后的过滤器表达式是可空的（即可能为NULL）
                    if (convertedExpr.getType().isNullable()) {
                        // 使用IS_TRUE函数来包装过滤器表达式，以确保NULL值被视为FALSE
                        convertedExpr =
                                rexBuilder.makeCall(SqlStdOperatorTable.IS_TRUE, convertedExpr);
                    }
                    // 将处理后的过滤器表达式添加到参数列表中（或作为特殊参数），可能需要查找或创建分组表达式
                    filterArg = lookupOrCreateGroupExpr(convertedExpr);
                }
                // 检查distinctList是否为null，以决定是否需要构建distinctKeys
                if (distinctList == null) {
                    // 如果distinctList为null，则不设置distinctKeys，保持为null
                    distinctKeys = null;
                } else {
                    // 使用ImmutableBitSet.Builder来构建distinctKeys
                    final ImmutableBitSet.Builder distinctBuilder = ImmutableBitSet.builder();
                    // 遍历distinctList中的每个SqlNode
                    for (SqlNode distinct : distinctList) {
                        // 将SqlNode转换为RexNode表达式
                        RexNode e = bb.convertExpression(distinct);
                        // 断言转换后的RexNode不为null（理论上不会失败，但作为安全检查）
                        assert e != null;
                        // 查找或创建分组表达式，并使用其索引在distinctBuilder中设置位
                        distinctBuilder.set(lookupOrCreateGroupExpr(e));
                    }
                    // 构建并设置distinctKeys
                    distinctKeys = distinctBuilder.build();
                }
            } finally {
                // switch back into agg mode
                // 切换回聚合模式，确保bb的agg属性指向当前聚合函数处理对象
                bb.agg = this;
            }
            // 获取SQL调用中的聚合函数
            SqlAggFunction aggFunction = (SqlAggFunction) call.getOperator();
            // 推导聚合函数的返回类型
            final RelDataType type = validator().deriveType(bb.scope(), call);
            // 初始化distinct标志位
            boolean distinct = false;
            // 检查调用中是否有DISTINCT量词
            SqlLiteral quantifier = call.getFunctionQuantifier();
            if ((null != quantifier) && (quantifier.getValue() == SqlSelectKeyword.DISTINCT)) {
                // 如果有DISTINCT量词，则将distinct标志位设置为true
                distinct = true;
            }
            // 初始化approximate标志位，用于近似聚合函数
            boolean approximate = false;
            // 检查是否使用了APPROX_COUNT_DISTINCT聚合函数
            if (aggFunction == SqlStdOperatorTable.APPROX_COUNT_DISTINCT) {
                // 如果是，则将聚合函数替换为COUNT，并设置distinct和approximate标志位
                aggFunction = SqlStdOperatorTable.COUNT;
                distinct = true;
                approximate = true;
            }
            // 定义排序规则变量
            final RelCollation collation;
            // 检查orderList是否为空或没有元素
            if (orderList == null || orderList.size() == 0) {
                // 如果orderList为空或没有元素，则使用空的排序规则
                collation = RelCollations.EMPTY;
            } else {
                try {
                    // switch out of agg mode
                    // 切换出聚合模式（假设bb是一个SQL构建上下文或类似的对象）
                    bb.agg = null;
                    // 构建排序规则
                    // 使用stream处理orderList，将每个排序项转换为RelFieldCollation，并收集到一个列表中
                    collation =
                            RelCollations.of(
                                    orderList.stream()
                                            .map(
                                                    order ->
                                                            bb.convertSortExpression(
                                                                    order,
                                                                    RelFieldCollation.Direction
                                                                            .ASCENDING,
                                                                    RelFieldCollation.NullDirection
                                                                            .UNSPECIFIED,
                                                                    this::sortToFieldCollation))
                                            .collect(Collectors.toList()));
                } finally {
                    // switch back into agg mode
                    // 无论是否成功构建排序规则，最后都切换回聚合模式
                    bb.agg = this;
                }
            }
            // 创建一个AggregateCall对象
            // AggregateCall用于表示聚合调用，包括聚合函数、是否去重、是否近似、是否忽略空值、参数列表、过滤参数、DISTINCT键、排序规则、返回类型以及可能的别名
            final AggregateCall aggCall =
                    AggregateCall.create(
                            aggFunction,
                            distinct,
                            approximate,
                            ignoreNulls,
                            args,
                            filterArg,
                            distinctKeys,
                            collation,
                            type,
                            nameMap.get(outerCall.toString()));
            // 创建一个RexNode对象，该对象表示一个Rex表达式，这里特指与聚合调用相关的表达式
           // RexNode是Calcite框架中用于表示表达式树的节点
            RexNode rex =
                    rexBuilder.addAggCall(
                            aggCall,
                            groupExprs.size(),
                            aggCalls,
                            aggCallMapping,
                            i -> convertedInputExprs.get(i).left.getType().isNullable());
            // 将外部聚合调用（outerCall）与构建好的RexNode表达式（rex）关联起来
            aggMapping.put(outerCall, rex);
        }

        private RelFieldCollation sortToFieldCollation(
                SqlNode expr,
                RelFieldCollation.Direction direction,
                RelFieldCollation.NullDirection nullDirection) {
            final RexNode node = bb.convertExpression(expr);
            final int fieldIndex = lookupOrCreateGroupExpr(node);
            if (nullDirection == RelFieldCollation.NullDirection.UNSPECIFIED) {
                nullDirection = direction.defaultNullDirection();
            }
            return new RelFieldCollation(fieldIndex, direction, nullDirection);
        }

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 查找给定的RexNode表达式是否已存在于已转换的输入表达式列表中。
         */
        private int lookupOrCreateGroupExpr(RexNode expr) {
            // 初始化索引为0，用于遍历列表
            int index = 0;
            for (RexNode convertedInputExpr : Pair.left(convertedInputExprs)) {
                if (expr.equals(convertedInputExpr)) {
                    // 如果找到了相同的表达式，则返回其索引
                    return index;
                }
                ++index;// 如果没有找到，则递增索引以继续查找
            }

            // not found -- add it
            // 如果遍历完列表都没有找到相同的表达式，则执行添加操作（这里只是简单地增加索引计数）
            addExpr(expr, null);
            // 返回新添加的索引
            return index;
        }

        /**
         * If an expression is structurally identical to one of the group-by expressions, returns a
         * reference to the expression, otherwise returns null.
         */
        public int lookupGroupExpr(SqlNode expr) {
            for (int i = 0; i < groupExprs.size(); i++) {
                SqlNode groupExpr = groupExprs.get(i);
                if (expr.equalsDeep(groupExpr, Litmus.IGNORE)) {
                    return i;
                }
            }
            return -1;
        }

        public @Nullable RexNode lookupAggregates(SqlCall call) {
            // assert call.getOperator().isAggregator();
            assert bb.agg == this;

            for (Map.Entry<SqlNode, Ord<AuxiliaryConverter>> e : auxiliaryGroupExprs.entrySet()) {
                if (call.equalsDeep(e.getKey(), Litmus.IGNORE)) {
                    AuxiliaryConverter converter = e.getValue().e;
                    final int groupOrdinal = e.getValue().i;
                    return converter.convert(
                            rexBuilder,
                            convertedInputExprs.get(groupOrdinal).left,
                            rexBuilder.makeInputRef(castNonNull(bb.root), groupOrdinal));
                }
            }

            return aggMapping.get(call);
        }

        public List<Pair<RexNode, @Nullable String>> getPreExprs() {
            return convertedInputExprs;
        }

        public List<AggregateCall> getAggCalls() {
            return aggCalls;
        }

        public RelDataTypeFactory getTypeFactory() {
            return typeFactory;
        }
    }

    /** Context to find a relational expression to a field offset. */
    private static class LookupContext {
        private final List<Pair<RelNode, Integer>> relOffsetList = new ArrayList<>();

        /**
         * Creates a LookupContext with multiple input relational expressions.
         *
         * @param bb Context for translating this sub-query
         * @param rels Relational expressions
         * @param systemFieldCount Number of system fields
         */
        LookupContext(Blackboard bb, List<RelNode> rels, int systemFieldCount) {
            bb.flatten(rels, systemFieldCount, new int[] {0}, relOffsetList);
        }

        /**
         * Returns the relational expression with a given offset, and the ordinal in the combined
         * row of its first field.
         *
         * <p>For example, in {@code Emp JOIN Dept}, findRel(1) returns the relational expression
         * for {@code Dept} and offset 6 (because {@code Emp} has 6 fields, therefore the first
         * field of {@code Dept} is field 6.
         *
         * @param offset Offset of relational expression in FROM clause
         * @return Relational expression and the ordinal of its first field
         */
        Pair<RelNode, Integer> findRel(int offset) {
            return relOffsetList.get(offset);
        }
    }

    /**
     * Shuttle which walks over a tree of {@link RexNode}s and applies 'over' to all agg functions.
     *
     * <p>This is necessary because the returned expression is not necessarily a call to an agg
     * function. For example,
     *
     * <blockquote>
     *
     * <code>AVG(x)</code>
     *
     * </blockquote>
     *
     * <p>becomes
     *
     * <blockquote>
     *
     * <code>SUM(x) / COUNT(x)</code>
     *
     * </blockquote>
     *
     * <p>Any aggregate functions are converted to calls to the internal <code>
     * $Histogram</code> aggregation function and accessors such as <code>
     * $HistogramMin</code>; for example,
     *
     * <blockquote>
     *
     * <code>MIN(x), MAX(x)</code>
     *
     * </blockquote>
     *
     * <p>are converted to
     *
     * <blockquote>
     *
     * <code>$HistogramMin($Histogram(x)),
     * $HistogramMax($Histogram(x))</code>
     *
     * </blockquote>
     *
     * <p>Common sub-expression elimination will ensure that only one histogram is computed.
     */
    private class HistogramShuttle extends RexShuttle {
        /**
         * Whether to convert calls to MIN(x) to HISTOGRAM_MIN(HISTOGRAM(x)). Histograms allow
         * rolling computation, but require more space.
         */
        static final boolean ENABLE_HISTOGRAM_AGG = false;

        private final ImmutableList<RexNode> partitionKeys;
        private final ImmutableList<RexNode> orderKeys;
        private final RexWindowBound lowerBound;
        private final RexWindowBound upperBound;
        private final boolean rows;
        private final boolean allowPartial;
        private final boolean distinct;
        private final boolean ignoreNulls;

        HistogramShuttle(
                ImmutableList<RexNode> partitionKeys,
                ImmutableList<RexNode> orderKeys,
                boolean rows,
                RexWindowBound lowerBound,
                RexWindowBound upperBound,
                boolean allowPartial,
                boolean distinct,
                boolean ignoreNulls) {
            this.partitionKeys = partitionKeys;
            this.orderKeys = orderKeys;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.rows = rows;
            this.allowPartial = allowPartial;
            this.distinct = distinct;
            this.ignoreNulls = ignoreNulls;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            final SqlOperator op = call.getOperator();
            if (!(op instanceof SqlAggFunction)) {
                return super.visitCall(call);
            }
            final SqlAggFunction aggOp = (SqlAggFunction) op;
            final RelDataType type = call.getType();
            List<RexNode> exprs = call.getOperands();

            SqlFunction histogramOp = !ENABLE_HISTOGRAM_AGG ? null : getHistogramOp(aggOp);

            if (histogramOp != null) {
                final RelDataType histogramType = computeHistogramType(type);

                // For DECIMAL, since it's already represented as a bigint we
                // want to do a reinterpretCast instead of a cast to avoid
                // losing any precision.
                boolean reinterpretCast = type.getSqlTypeName() == SqlTypeName.DECIMAL;

                // Replace original expression with CAST of not one
                // of the supported types
                if (histogramType != type) {
                    exprs = new ArrayList<>(exprs);
                    exprs.set(
                            0,
                            reinterpretCast
                                    ? rexBuilder.makeReinterpretCast(
                                            histogramType,
                                            exprs.get(0),
                                            rexBuilder.makeLiteral(false))
                                    : rexBuilder.makeCast(histogramType, exprs.get(0)));
                }

                RexNode over =
                        relBuilder
                                .aggregateCall(SqlStdOperatorTable.HISTOGRAM_AGG, exprs)
                                .distinct(distinct)
                                .ignoreNulls(ignoreNulls)
                                .over()
                                .partitionBy(partitionKeys)
                                .orderBy(orderKeys)
                                .let(
                                        c ->
                                                rows
                                                        ? c.rowsBetween(lowerBound, upperBound)
                                                        : c.rangeBetween(lowerBound, upperBound))
                                .allowPartial(allowPartial)
                                .toRex();

                RexNode histogramCall =
                        rexBuilder.makeCall(histogramType, histogramOp, ImmutableList.of(over));

                // If needed, post Cast result back to original
                // type.
                if (histogramType != type) {
                    if (reinterpretCast) {
                        histogramCall =
                                rexBuilder.makeReinterpretCast(
                                        type, histogramCall, rexBuilder.makeLiteral(false));
                    } else {
                        histogramCall = rexBuilder.makeCast(type, histogramCall);
                    }
                }

                return histogramCall;
            } else {
                boolean needSum0 = aggOp == SqlStdOperatorTable.SUM && type.isNullable();
                SqlAggFunction aggOpToUse = needSum0 ? SqlStdOperatorTable.SUM0 : aggOp;
                return relBuilder
                        .aggregateCall(aggOpToUse, exprs)
                        .distinct(distinct)
                        .ignoreNulls(ignoreNulls)
                        .over()
                        .partitionBy(partitionKeys)
                        .orderBy(orderKeys)
                        .let(
                                c ->
                                        rows
                                                ? c.rowsBetween(lowerBound, upperBound)
                                                : c.rangeBetween(lowerBound, upperBound))
                        .allowPartial(allowPartial)
                        .nullWhenCountZero(needSum0)
                        .toRex();
            }
        }

        /**
         * Returns the histogram operator corresponding to a given aggregate function.
         *
         * <p>For example, <code>getHistogramOp
         * ({@link SqlStdOperatorTable#MIN}}</code> returns {@link
         * SqlStdOperatorTable#HISTOGRAM_MIN}.
         *
         * @param aggFunction An aggregate function
         * @return Its histogram function, or null
         */
        @Nullable
        SqlFunction getHistogramOp(SqlAggFunction aggFunction) {
            if (aggFunction == SqlStdOperatorTable.MIN) {
                return SqlStdOperatorTable.HISTOGRAM_MIN;
            } else if (aggFunction == SqlStdOperatorTable.MAX) {
                return SqlStdOperatorTable.HISTOGRAM_MAX;
            } else if (aggFunction == SqlStdOperatorTable.FIRST_VALUE) {
                return SqlStdOperatorTable.HISTOGRAM_FIRST_VALUE;
            } else if (aggFunction == SqlStdOperatorTable.LAST_VALUE) {
                return SqlStdOperatorTable.HISTOGRAM_LAST_VALUE;
            } else {
                return null;
            }
        }

        /**
         * Returns the type for a histogram function. It is either the actual type or an an
         * approximation to it.
         */
        private RelDataType computeHistogramType(RelDataType type) {
            if (SqlTypeUtil.isExactNumeric(type) && type.getSqlTypeName() != SqlTypeName.BIGINT) {
                return typeFactory.createSqlType(SqlTypeName.BIGINT);
            } else if (SqlTypeUtil.isApproximateNumeric(type)
                    && type.getSqlTypeName() != SqlTypeName.DOUBLE) {
                return typeFactory.createSqlType(SqlTypeName.DOUBLE);
            } else {
                return type;
            }
        }
    }

    /** A sub-query, whether it needs to be translated using 2- or 3-valued logic. */
    private static class SubQuery {
        final SqlNode node;
        final RelOptUtil.Logic logic;
        @Nullable RexNode expr;

        private SubQuery(SqlNode node, RelOptUtil.Logic logic) {
            this.node = node;
            this.logic = logic;
        }
    }

    /**
     * Visitor that looks for an SqlIdentifier inside a tree of {@link SqlNode} objects and return
     * {@link Boolean#TRUE} when it finds one.
     */
    public static class SqlIdentifierFinder implements SqlVisitor<Boolean> {

        @Override
        public Boolean visit(SqlCall sqlCall) {
            return sqlCall.getOperandList().stream().anyMatch(sqlNode -> sqlNode.accept(this));
        }

        @Override
        public Boolean visit(SqlNodeList nodeList) {
            return nodeList.stream().anyMatch(sqlNode -> sqlNode.accept(this));
        }

        @Override
        public Boolean visit(SqlIdentifier identifier) {
            return true;
        }

        @Override
        public Boolean visit(SqlLiteral literal) {
            return false;
        }

        @Override
        public Boolean visit(SqlDataTypeSpec type) {
            return false;
        }

        @Override
        public Boolean visit(SqlDynamicParam param) {
            return false;
        }

        @Override
        public Boolean visit(SqlIntervalQualifier intervalQualifier) {
            return false;
        }
    }

    /** Visitor that collects all aggregate functions in a {@link SqlNode} tree. */
    private static class AggregateFinder extends SqlBasicVisitor<Void> {
        final SqlNodeList list = new SqlNodeList(SqlParserPos.ZERO);
        final SqlNodeList filterList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlNodeList distinctList = new SqlNodeList(SqlParserPos.ZERO);
        final SqlNodeList orderList = new SqlNodeList(SqlParserPos.ZERO);

        /**
         * @授课老师: 码界探索
         * @微信: 252810631
         * @版权所有: 请尊重劳动成果
         * 查找聚合函数，并添加到List中
         */
        @Override
        public Void visit(SqlCall call) {
            // ignore window aggregates and ranking functions (associated with OVER operator)
            // 忽略窗口聚合和排名函数（与OVER操作符相关）
            if (call.getOperator().getKind() == SqlKind.OVER) {
                return null;
            }

            // 如果遇到FILTER函数（例如，在聚合函数中的FILTER子句），需要特别处理WHERE条件
            if (call.getOperator().getKind() == SqlKind.FILTER) {
                // the WHERE in a FILTER must be tracked too so we can call replaceSubQueries on it.
                // see https://issues.apache.org/jira/browse/CALCITE-1910
                // 聚合函数部分
                final SqlNode aggCall = call.getOperandList().get(0);
                // FILTER子句中的WHERE条件
                final SqlNode whereCall = call.getOperandList().get(1);
                // 将聚合函数添加到列表中
                list.add(aggCall);
                // 将WHERE条件添加到需要特别处理的filter列表中
                filterList.add(whereCall);
                return null;
            }

            // 如果遇到WITHIN DISTINCT函数，处理其聚合函数和DISTINCT列表  
            if (call.getOperator().getKind() == SqlKind.WITHIN_DISTINCT) {
                // 聚合函数部分
                final SqlNode aggCall = call.getOperandList().get(0);
                // DISTINCT列表
                final SqlNodeList distinctList = (SqlNodeList) call.getOperandList().get(1);
                // 将聚合函数添加到列表中
                list.add(aggCall);
                // 遍历DISTINCT列表并添加到distinctList中
                distinctList.getList().forEach(this.distinctList::add);
                return null;
            }
            // 如果遇到WITHIN GROUP函数，处理其聚合函数和ORDER BY列表
            if (call.getOperator().getKind() == SqlKind.WITHIN_GROUP) {
                // 聚合函数部分
                final SqlNode aggCall = call.getOperandList().get(0);
                // ORDER BY列表
                final SqlNodeList orderList = (SqlNodeList) call.getOperandList().get(1);
                // 将聚合函数添加到列表中
                list.add(aggCall);
                // 将ORDER BY列表添加到orderList中
                this.orderList.addAll(orderList);
                return null;
            }
            // 如果是聚合函数，则将其添加到列表中
            if (call.getOperator().isAggregator()) {
                list.add(call);
                return null;
            }

            // Don't traverse into sub-queries, even if they contain aggregate
            // functions.
            // 不遍历子查询，即使它们包含聚合函数
            if (call instanceof SqlSelect) {
                return null;
            }
            // 如果不是上述任何情况，则按照正常的访问者模式递归遍历
            return call.getOperator().acceptCall(this, call);
        }
    }

    /** Use of a row as a correlating variable by a given relational expression. */
    private static class CorrelationUse {
        private final CorrelationId id;
        private final ImmutableBitSet requiredColumns;
        /** The relational expression that uses the variable. */
        private final RelNode r;

        CorrelationUse(CorrelationId id, ImmutableBitSet requiredColumns, RelNode r) {
            this.id = id;
            this.requiredColumns = requiredColumns;
            this.r = r;
        }
    }

    /** Returns a default {@link Config}. */
    public static Config config() {
        return CONFIG;
    }

    /**
     * Interface to define the configuration for a SqlToRelConverter. Provides methods to set each
     * configuration option.
     *
     * @see SqlToRelConverter#CONFIG
     */
    @Value.Immutable(singleton = false)
    public interface Config {
        /**
         * Returns the {@code decorrelationEnabled} option. Controls whether to disable sub-query
         * decorrelation when needed. e.g. if outer joins are not supported.
         */
        @Value.Default
        default boolean isDecorrelationEnabled() {
            return true;
        }

        /** Sets {@link #isDecorrelationEnabled()}. */
        Config withDecorrelationEnabled(boolean decorrelationEnabled);

        /**
         * Returns the {@code trimUnusedFields} option. Controls whether to trim unused fields as
         * part of the conversion process.
         */
        @Value.Default
        default boolean isTrimUnusedFields() {
            return false;
        }

        /** Sets {@link #isTrimUnusedFields()}. */
        Config withTrimUnusedFields(boolean trimUnusedFields);

        /**
         * Returns the {@code createValuesRel} option. Controls whether instances of {@link
         * org.apache.calcite.rel.logical.LogicalValues} are generated. These may not be supported
         * by all physical implementations.
         */
        @Value.Default
        default boolean isCreateValuesRel() {
            return true;
        }

        /** Sets {@link #isCreateValuesRel()}. */
        Config withCreateValuesRel(boolean createValuesRel);

        /**
         * Returns the {@code explain} option. Describes whether the current statement is part of an
         * EXPLAIN PLAN statement.
         */
        @Value.Default
        default boolean isExplain() {
            return false;
        }

        /** Sets {@link #isExplain()}. */
        Config withExplain(boolean explain);

        /**
         * Returns the {@code expand} option. Controls whether to expand sub-queries. If false, each
         * sub-query becomes a {@link org.apache.calcite.rex.RexSubQuery}.
         */
        @Value.Default
        default boolean isExpand() {
            return true;
        }

        /** Sets {@link #isExpand()}. */
        Config withExpand(boolean expand);

        /**
         * Returns the {@code inSubQueryThreshold} option, default {@link
         * #DEFAULT_IN_SUB_QUERY_THRESHOLD}. Controls the list size threshold under which {@link
         * #convertInToOr} is used. Lists of this size or greater will instead be converted to use a
         * join against an inline table ({@link org.apache.calcite.rel.logical.LogicalValues})
         * rather than a predicate. A threshold of 0 forces usage of an inline table in all cases; a
         * threshold of {@link Integer#MAX_VALUE} forces usage of OR in all cases.
         */
        @Value.Default
        default int getInSubQueryThreshold() {
            return DEFAULT_IN_SUB_QUERY_THRESHOLD;
        }

        /** Sets {@link #getInSubQueryThreshold()}. */
        Config withInSubQueryThreshold(int threshold);

        /**
         * Returns whether to remove Sort operator for a sub-query if the Sort has no offset and
         * fetch limit attributes. Because the remove does not change the semantics, in many cases
         * this is a promotion. Default is true.
         */
        @Value.Default
        default boolean isRemoveSortInSubQuery() {
            return true;
        }

        /** Sets {@link #isRemoveSortInSubQuery()}. */
        Config withRemoveSortInSubQuery(boolean removeSortInSubQuery);

        /**
         * Returns the factory to create {@link RelBuilder}, never null. Default is {@link
         * RelFactories#LOGICAL_BUILDER}.
         */
        RelBuilderFactory getRelBuilderFactory();

        /** Sets {@link #getRelBuilderFactory()}. */
        Config withRelBuilderFactory(RelBuilderFactory factory);

        /**
         * Returns a function that takes a {@link RelBuilder.Config} and returns another. Default is
         * the identity function.
         */
        UnaryOperator<RelBuilder.Config> getRelBuilderConfigTransform();

        /**
         * Sets {@link #getRelBuilderConfigTransform()}.
         *
         * @see #addRelBuilderConfigTransform
         */
        Config withRelBuilderConfigTransform(UnaryOperator<RelBuilder.Config> transform);

        /** Adds a transform to {@link #getRelBuilderConfigTransform()}. */
        default Config addRelBuilderConfigTransform(UnaryOperator<RelBuilder.Config> transform) {
            return withRelBuilderConfigTransform(
                    getRelBuilderConfigTransform().andThen(transform)::apply);
        }

        /**
         * Returns the hint strategies used to decide how the hints are propagated to the relational
         * expressions. Default is {@link HintStrategyTable#EMPTY}.
         */
        HintStrategyTable getHintStrategyTable();

        /** Sets {@link #getHintStrategyTable()}. */
        Config withHintStrategyTable(HintStrategyTable hintStrategyTable);

        /**
         * Whether add {@link SqlStdOperatorTable#JSON_TYPE_OPERATOR} for between json functions.
         */
        @Value.Default
        default boolean isAddJsonTypeOperatorEnabled() {
            return true;
        }

        /** Sets {@link #isAddJsonTypeOperatorEnabled()}. */
        Config withAddJsonTypeOperatorEnabled(boolean addJsonTypeOperatorEnabled);
    }

    /**
     * Used to find nested json functions, and add {@link SqlStdOperatorTable#JSON_TYPE_OPERATOR} to
     * nested json output.
     */
    private class NestedJsonFunctionRelRewriter extends RelShuttleImpl {

        @Override
        public RelNode visit(LogicalProject project) {
            final Set<Integer> jsonInputFields = findJsonInputs(project.getInput());
            final Set<Integer> requiredJsonFieldsFromParent =
                    stack.size() > 0
                            ? requiredJsonOutputFromParent(stack.getLast())
                            : Collections.emptySet();

            final List<RexNode> originalProjections = project.getProjects();
            final ImmutableList.Builder<RexNode> newProjections = ImmutableList.builder();
            JsonFunctionRexRewriter rexRewriter = new JsonFunctionRexRewriter(jsonInputFields);
            for (int i = 0; i < originalProjections.size(); ++i) {
                if (requiredJsonFieldsFromParent.contains(i)) {
                    newProjections.add(rexRewriter.forceChildJsonType(originalProjections.get(i)));
                } else {
                    newProjections.add(originalProjections.get(i).accept(rexRewriter));
                }
            }

            RelNode newInput = project.getInput().accept(this);
            return LogicalProject.create(
                    newInput,
                    project.getHints(),
                    newProjections.build(),
                    project.getRowType().getFieldNames());
        }

        private Set<Integer> requiredJsonOutputFromParent(RelNode relNode) {
            if (!(relNode instanceof Aggregate)) {
                return Collections.emptySet();
            }
            final Aggregate aggregate = (Aggregate) relNode;
            final List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
            final ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
            for (final AggregateCall call : aggregateCalls) {
                if (call.getAggregation() == SqlStdOperatorTable.JSON_OBJECTAGG) {
                    result.add(call.getArgList().get(1));
                } else if (call.getAggregation() == SqlStdOperatorTable.JSON_ARRAYAGG) {
                    result.add(call.getArgList().get(0));
                }
            }
            return result.build();
        }

        private Set<Integer> findJsonInputs(RelNode relNode) {
            if (!(relNode instanceof Aggregate)) {
                return Collections.emptySet();
            }
            final Aggregate aggregate = (Aggregate) relNode;
            final List<AggregateCall> aggregateCalls = aggregate.getAggCallList();
            final ImmutableSet.Builder<Integer> result = ImmutableSet.builder();
            for (int i = 0; i < aggregateCalls.size(); ++i) {
                final AggregateCall call = aggregateCalls.get(i);
                if (call.getAggregation() == SqlStdOperatorTable.JSON_OBJECTAGG
                        || call.getAggregation() == SqlStdOperatorTable.JSON_ARRAYAGG) {
                    result.add(aggregate.getGroupCount() + i);
                }
            }
            return result.build();
        }
    }

    /** Used to rewrite json functions which is nested. */
    private class JsonFunctionRexRewriter extends RexShuttle {

        private final Set<Integer> jsonInputFields;

        JsonFunctionRexRewriter(Set<Integer> jsonInputFields) {
            this.jsonInputFields = jsonInputFields;
        }

        @Override
        public RexNode visitCall(RexCall call) {
            if (call.getOperator() == SqlStdOperatorTable.JSON_OBJECT) {
                final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                for (int i = 0; i < call.operands.size(); ++i) {
                    if ((i & 1) == 0 && i != 0) {
                        builder.add(forceChildJsonType(call.operands.get(i)));
                    } else {
                        builder.add(call.operands.get(i));
                    }
                }
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_OBJECT, builder.build());
            }
            if (call.getOperator() == SqlStdOperatorTable.JSON_ARRAY) {
                final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
                builder.add(call.operands.get(0));
                for (int i = 1; i < call.operands.size(); ++i) {
                    builder.add(forceChildJsonType(call.operands.get(i)));
                }
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_ARRAY, builder.build());
            }
            return super.visitCall(call);
        }

        private RexNode forceChildJsonType(RexNode rexNode) {
            final RexNode childResult = rexNode.accept(this);
            if (isJsonResult(rexNode)) {
                return rexBuilder.makeCall(SqlStdOperatorTable.JSON_TYPE_OPERATOR, childResult);
            }
            return childResult;
        }

        private boolean isJsonResult(RexNode rexNode) {
            if (rexNode instanceof RexCall) {
                final RexCall call = (RexCall) rexNode;
                final SqlOperator operator = call.getOperator();
                return operator == SqlStdOperatorTable.JSON_OBJECT
                        || operator == SqlStdOperatorTable.JSON_ARRAY
                        || operator == SqlStdOperatorTable.JSON_VALUE;
            } else if (rexNode instanceof RexInputRef) {
                final RexInputRef inputRef = (RexInputRef) rexNode;
                return jsonInputFields.contains(inputRef.getIndex());
            }
            return false;
        }
    }
}
