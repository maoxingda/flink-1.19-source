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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel

/** Base class for stream physical relational expression. */
/**
 * @授课老师: 码界探索
 * @微信: 252810631
 * @版权所有: 请尊重劳动成果
 * 流物理关系表达式的基类。
 */
trait StreamPhysicalRel extends FlinkPhysicalRel {

  /** Whether the [[StreamPhysicalRel]] requires rowtime watermark in processing logic. */
  /**
   * @授课老师: 码界探索
   * @微信: 252810631
   * @版权所有: 请尊重劳动成果
   * [[StreamPhysicalRel]]在处理逻辑中是否需要行时水印
   */
  def requireWatermark: Boolean
}
