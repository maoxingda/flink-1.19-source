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

package org.apache.flink.yarn.cli;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.yarn.configuration.YarnDeploymentTarget;

import org.apache.commons.cli.CommandLine;

/**
 * A stub Yarn Command Line to throw an exception with the correct message when the {@code
 * HADOOP_CLASSPATH} is not set.
 */
/**
 * @授课老师(微信): yi_locus
 * email: 156184212@qq.com
 * 一个stub Yarn命令行，用于在未设置 HADOOP_CLASSPATH 时引发带有正确消息的异常。
*/
@Internal
public class FallbackYarnSessionCli extends AbstractYarnCli {

    public FallbackYarnSessionCli(Configuration configuration) {
        super(configuration, "y", "yarn");
    }

    @Override
    public boolean isActive(CommandLine commandLine) {
        if (super.isActive(commandLine)) {
            throw new IllegalStateException(YarnDeploymentTarget.ERROR_MESSAGE);
        }
        return false;
    }
}
