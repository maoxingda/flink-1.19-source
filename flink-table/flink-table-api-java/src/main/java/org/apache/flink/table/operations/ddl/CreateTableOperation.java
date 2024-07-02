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

package org.apache.flink.table.operations.ddl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.internal.TableResultImpl;
import org.apache.flink.table.api.internal.TableResultInternal;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/** Operation to describe a CREATE TABLE statement. */
@Internal
public class CreateTableOperation implements CreateOperation {
    private final ObjectIdentifier tableIdentifier;
    private final CatalogTable catalogTable;
    private final boolean ignoreIfExists;
    private final boolean isTemporary;

    public CreateTableOperation(
            ObjectIdentifier tableIdentifier,
            CatalogTable catalogTable,
            boolean ignoreIfExists,
            boolean isTemporary) {
        this.tableIdentifier = tableIdentifier;
        this.catalogTable = catalogTable;
        this.ignoreIfExists = ignoreIfExists;
        this.isTemporary = isTemporary;
    }

    public CatalogTable getCatalogTable() {
        return catalogTable;
    }

    public ObjectIdentifier getTableIdentifier() {
        return tableIdentifier;
    }

    public boolean isIgnoreIfExists() {
        return ignoreIfExists;
    }

    public boolean isTemporary() {
        return isTemporary;
    }

    @Override
    public String asSummaryString() {
        Map<String, Object> params = new LinkedHashMap<>();
        params.put("catalogTable", catalogTable.toProperties());
        params.put("identifier", tableIdentifier);
        params.put("ignoreIfExists", ignoreIfExists);
        params.put("isTemporary", isTemporary);

        return OperationUtils.formatWithChildren(
                "CREATE TABLE", params, Collections.emptyList(), Operation::asSummaryString);
    }

    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 执行创建表的操作，并返回执行结果。
     *
     * @param ctx 上下文对象，用于获取CatalogManager等组件
     * @return 表的执行结果，如果成功则返回TableResultImpl.TABLE_RESULT_OK
    */
    @Override
    public TableResultInternal execute(Context ctx) {
        // 判断是否为临时表
        if (isTemporary) {
            // 如果是临时表，则调用CatalogManager的createTemporaryTable方法来创建
            ctx.getCatalogManager()
                    .createTemporaryTable(catalogTable, tableIdentifier, ignoreIfExists);
        } else {
            // 如果不是临时表，则调用CatalogManager的createTable方法来创建
            ctx.getCatalogManager().createTable(catalogTable, tableIdentifier, ignoreIfExists);
        }
        // 无论是否成功，都返回TableResultImpl.TABLE_RESULT_OK作为执行结果
        return TableResultImpl.TABLE_RESULT_OK;
    }
}
