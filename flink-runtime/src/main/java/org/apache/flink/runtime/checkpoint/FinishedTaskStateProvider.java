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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.executiongraph.ExecutionVertex;
import org.apache.flink.runtime.jobgraph.OperatorID;

import java.util.Map;

/** Collects and fulfills the finished state for the subtasks or operators. */
public interface FinishedTaskStateProvider {

    /** Reports the {@code task} is finished on restoring. */
    void reportTaskFinishedOnRestore(ExecutionVertex task);

    /** Reports the {@code task} has finished all the operators. */
    void reportTaskHasFinishedOperators(ExecutionVertex task);

    /** Fulfills the state for the finished subtasks and operators to indicate they are finished. */
    void fulfillFinishedTaskStatus(Map<OperatorID, OperatorState> operatorStates);
}
