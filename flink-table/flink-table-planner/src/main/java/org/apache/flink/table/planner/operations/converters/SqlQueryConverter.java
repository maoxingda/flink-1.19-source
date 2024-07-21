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

package org.apache.flink.table.planner.operations.converters;

import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.planner.operations.PlannerQueryOperation;

import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlTableRef;

import java.util.EnumSet;
import java.util.Optional;

/**
 * A converter for query {@link SqlNode}, e.g., {@link SqlSelect}, {@link SqlOrderBy}, {@link
 * SqlTableRef}.
 */
public class SqlQueryConverter implements SqlNodeConverter<SqlNode> {

    @Override
    public Optional<EnumSet<SqlKind>> supportedSqlKinds() {
        return Optional.of(SqlKind.QUERY);
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 调用ConvertContext将SqlNode转换为Operztion
     */
    @Override
    public Operation convertSqlNode(SqlNode node, ConvertContext context) {
        // transform to a relational tree
        // 使用上下文（ConvertContext）中的方法将SqlNode转换为关系型根节点（RelRoot），
        // 这个过程通常涉及解析SQL节点，并构建出一个内部表示（如Calcite的RelNode），
        // 用于后续的关系操作、优化等。
        RelRoot relational = context.toRelRoot(node);
        // 创建一个PlannerQueryOperation对象，该对象封装了查询操作的细节。
        // 这里主要使用了relational对象的project（投影）部分，
        // 它可能表示了查询结果中应该包含的列或表达式。
        return new PlannerQueryOperation(
                relational.project(), () -> context.toQuotedSqlString(node));
    }
}
