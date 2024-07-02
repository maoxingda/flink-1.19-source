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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.delegation.Parser;
import org.apache.flink.table.delegation.ParserFactory;

import java.util.Collections;
import java.util.Set;

/** A Parser factory that creates {@link ParserImpl}. */
public class DefaultParserFactory implements ParserFactory {

    @Override
    public String factoryIdentifier() {
        return SqlDialect.DEFAULT.name().toLowerCase();
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Collections.emptySet();
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }

    /**
     * @授课老师: 码界探索
     * @微信: 252810631
     * @版权所有: 请尊重劳动成果
     * 创建Parser接口具体实现类
     */
    @Override
    public Parser create(Context context) {
        // 将传入的Context强制转换为DefaultCalciteContext类型
        DefaultCalciteContext defaultCalciteContext = (DefaultCalciteContext) context;
        // 创建一个新的ParserImpl对象，并传入以下参数：
        // 1. CatalogManager：从DefaultCalciteContext中获取CatalogManager
        // 2. 方法引用：用于创建FlinkPlanner的方法，从PlannerContext中获取
        // 3. 方法引用：用于创建CalciteParser的方法，从PlannerContext中获取
        // 4. RexFactory：从PlannerContext中获取RexFactory
        return new ParserImpl(
                defaultCalciteContext.getCatalogManager(),
                defaultCalciteContext.getPlannerContext()::createFlinkPlanner,
                defaultCalciteContext.getPlannerContext()::createCalciteParser,
                defaultCalciteContext.getPlannerContext().getRexFactory());
    }
}
