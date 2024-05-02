/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.jobmaster;

/** Base interface for managers of services that are explicitly connected to / disconnected from. */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 连接到/断开连接的服务的管理器的基本接口。
*/
public interface ServiceConnectionManager<S> {

    /**
     * Connect to the given service.
     *
     * @param service service to connect to
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 连接到给定的服务。
    */
    void connect(S service);

    /** Disconnect from the current service. */
    void disconnect();

    /** Close the service connection manager. A closed manager must not be used again. */
    void close();
}
