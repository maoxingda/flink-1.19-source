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

package org.apache.flink.sql.parser;

import org.apache.flink.sql.parser.ddl.SqlTableColumn;
import org.apache.flink.sql.parser.ddl.constraint.SqlTableConstraint;
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/** Util to validate {@link SqlTableConstraint}. */
public class SqlConstraintValidator {

    /** Returns the column constraints plus the table constraints. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 返回列约束和表约束的完整列表。
     *
     * @param tableConstraints 表的约束列表
     * @param columnList 列节点列表
     * @return 包含列约束和表约束的完整列表
     */
    public static List<SqlTableConstraint> getFullConstraints(
            List<SqlTableConstraint> tableConstraints, SqlNodeList columnList) {
        // 用于存储完整约束的列表
        List<SqlTableConstraint> ret = new ArrayList<>();
        // 遍历列节点列表
        columnList.forEach(
                column -> {
                    // 将SqlNode转换为SqlTableColumn
                    SqlTableColumn tableColumn = (SqlTableColumn) column;
                    // 如果列是常规列
                    if (tableColumn instanceof SqlTableColumn.SqlRegularColumn) {
                        // 转换为SqlRegularColumn
                        SqlTableColumn.SqlRegularColumn regularColumn =
                                (SqlTableColumn.SqlRegularColumn) tableColumn;
                        // 如果常规列有约束，则添加到完整约束列表中
                        regularColumn.getConstraint().map(ret::add);
                    }
                });
        // 将给定的表约束也添加到完整约束列表中
        ret.addAll(tableConstraints);
        // 返回包含列约束和表约束的完整列表
        return ret;
    }

    /**
     * Check constraints and change the nullability of primary key columns.
     *
     * @throws SqlValidateException if encountered duplicate primary key constraints, or the
     *     constraint is enforced or unique.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 检查约束并更改主键列的可空性。
     *
     * @param tableConstraints 表约束列表
     * @param columnList 列节点列表
     * @throws SqlValidateException 如果遇到重复的主键约束，或者约束是强制的或唯一的，则抛出异常
    */
    public static void validateAndChangeColumnNullability(
            List<SqlTableConstraint> tableConstraints, SqlNodeList columnList)
            throws SqlValidateException {
        // 获取完整的约束列表（包括从列定义中解析出的隐式约束）
        List<SqlTableConstraint> fullConstraints = getFullConstraints(tableConstraints, columnList);
        // 如果存在多个主键约束，则抛出异常
        if (fullConstraints.stream().filter(SqlTableConstraint::isPrimaryKey).count() > 1) {
            throw new SqlValidateException(
                    fullConstraints.get(1).getParserPosition(), "Duplicate primary key definition");
        }
        // 遍历所有约束
        for (SqlTableConstraint constraint : fullConstraints) {
            // 验证约束（可能是检查是否有效或符合某些条件）
            validate(constraint);
            // 收集主键列的名称
            Set<String> primaryKeyColumns =
                    Arrays.stream(constraint.getColumnNames()).collect(Collectors.toSet());

            // rewrite primary key's nullability to false
            // e.g. CREATE TABLE tbl (`a` STRING PRIMARY KEY NOT ENFORCED, ...) or
            // CREATE TABLE tbl (`a` STRING, PRIMARY KEY(`a`) NOT ENFORCED) will change `a`
            // to STRING NOT NULL
            // 重写主键列的可空性为不可空
            for (SqlNode column : columnList) {
                SqlTableColumn tableColumn = (SqlTableColumn) column;
                // 如果列是常规列且是主键的一部分
                if (tableColumn instanceof SqlTableColumn.SqlRegularColumn
                        && primaryKeyColumns.contains(tableColumn.getName().getSimple())) {
                    SqlTableColumn.SqlRegularColumn regularColumn =
                            (SqlTableColumn.SqlRegularColumn) column;
                    // 创建一个新的数据类型，其可空性设置为 false
                    SqlDataTypeSpec notNullType = regularColumn.getType().withNullable(false);
                    // 设置新的数据类型给该列
                    regularColumn.setType(notNullType);
                }
            }
        }
    }

    /** Check table constraint. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 验证给定的 SqlTableConstraint 对象是否满足特定条件。
     * 如果不满足，则抛出 SqlValidateException 异常。
     *
     * @param constraint 要验证的 SqlTableConstraint 对象
     * @throws SqlValidateException 如果验证失败，则抛出此异常
    */
    private static void validate(SqlTableConstraint constraint) throws SqlValidateException {
        // 检查约束是否为 UNIQUE 约束
        // 如果是，则抛出异常，因为 UNIQUE 约束当前还不被支持
        if (constraint.isUnique()) {
            throw new SqlValidateException(
                    constraint.getParserPosition(), "UNIQUE constraint is not supported yet");
        }
        // 检查约束是否是被强制执行的（即 ENFORCED）
        // 如果是，则抛出异常，因为 Flink 不支持 PRIMARY KEY 约束的 ENFORCED 模式
        if (constraint.isEnforced()) {
            throw new SqlValidateException(
                    constraint.getParserPosition(),
                    "Flink doesn't support ENFORCED mode for PRIMARY KEY constraint. ENFORCED/NOT ENFORCED "
                            + "controls if the constraint checks are performed on the incoming/outgoing data. "
                            + "Flink does not own the data therefore the only supported mode is the NOT ENFORCED mode");
        }
    }
}
