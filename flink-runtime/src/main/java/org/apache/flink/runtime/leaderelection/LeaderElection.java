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

package org.apache.flink.runtime.leaderelection;

import java.util.UUID;

/**
 * {@code LeaderElection} serves as a proxy between {@code LeaderElectionService} and {@link
 * LeaderContender}.
 */
public interface LeaderElection extends AutoCloseable {

    /** Registers the passed {@link LeaderContender} with the leader election process. */
    void startLeaderElection(LeaderContender contender) throws Exception;

    /**
     * Confirms that the {@link LeaderContender} has accepted the leadership identified by the given
     * leader session id. It also publishes the leader address under which the leader is reachable.
     *
     * <p>The intention of this method is to establish an order between setting the new leader
     * session ID in the {@link LeaderContender} and publishing the new leader session ID and the
     * related leader address to the leader retrieval services.
     *
     * @param leaderSessionID The new leader session ID
     * @param leaderAddress The address of the new leader
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 确认LeaderContender已接受由给定的LeaderId标识的领导者。
    */
    void confirmLeadership(UUID leaderSessionID, String leaderAddress);

    /**
     * Returns {@code true} if the service's {@link LeaderContender} has the leadership under the
     * given leader session ID acquired.
     *
     * @param leaderSessionId identifying the current leader
     * @return true if the associated {@link LeaderContender} is the leader, otherwise false
     */
    /**
     * @授课老师(微信): yi_locus
     * email: 156184212@qq.com
     * 如果通过leaderSessionId获取的LeaderContender(leader)是当前会话霞的Leader，则返回true。
    */
    boolean hasLeadership(UUID leaderSessionId);

    /**
     * Closes the {@code LeaderElection} by deregistering the {@link LeaderContender} from the
     * underlying leader election. {@link LeaderContender#revokeLeadership()} will be called if the
     * service still holds the leadership.
     */
    void close() throws Exception;
}
