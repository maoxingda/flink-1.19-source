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

package org.apache.flink.runtime.heartbeat;

import org.apache.flink.runtime.clusterframework.types.ResourceID;

/**
 * A heartbeat manager has to be able to start/stop monitoring a {@link HeartbeatTarget}, and report
 * heartbeat timeouts for this target.
 *
 * @param <I> Type of the incoming payload
 * @param <O> Type of the outgoing payload
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 心跳管理器用来启动或停止监视HeartbeatTarget，并报告该目标心跳超时事件。通过monitorTarget来传递并监控HeartbeatTarget，
 * 这个方法可以看做是整个服务的输入，告诉心跳服务去管理哪些目标。
*/
public interface HeartbeatManager<I, O> extends HeartbeatTarget<I> {

    /**
     * Start monitoring a {@link HeartbeatTarget}. Heartbeat timeouts for this target are reported
     * to the {@link HeartbeatListener} associated with this heartbeat manager.
     *
     * @param resourceID Resource ID identifying the heartbeat target
     * @param heartbeatTarget Interface to send heartbeat requests and responses to the heartbeat
     *     target
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 开始监控心跳目标，当目标心跳超时，会报告给与HeartbeatManager关联的HeartbeatListener
    */
    void monitorTarget(ResourceID resourceID, HeartbeatTarget<O> heartbeatTarget);

    /**
     * Stops monitoring the heartbeat target with the associated resource ID.
     *
     * @param resourceID Resource ID of the heartbeat target which shall no longer be monitored
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 取消监控心跳目标，ResourceID是心跳目标的标识
    */
    void unmonitorTarget(ResourceID resourceID);

    /** Stops the heartbeat manager. */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 停止当前心跳管理器
    */
    void stop();

    /**
     * Returns the last received heartbeat from the given target.
     *
     * @param resourceId for which to return the last heartbeat
     * @return Last heartbeat received from the given target or -1 if the target is not being
     *     monitored.
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 返回最近一次心跳时间，如果心跳目标被移除了则返回-1
    */
    long getLastHeartbeatFrom(ResourceID resourceId);
}
