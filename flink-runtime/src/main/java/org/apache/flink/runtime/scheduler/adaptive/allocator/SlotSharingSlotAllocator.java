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

package org.apache.flink.runtime.scheduler.adaptive.allocator;

import org.apache.flink.runtime.clusterframework.types.AllocationID;
import org.apache.flink.runtime.clusterframework.types.ResourceProfile;
import org.apache.flink.runtime.instance.SlotSharingGroupId;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobmanager.scheduler.SlotSharingGroup;
import org.apache.flink.runtime.jobmaster.LogicalSlot;
import org.apache.flink.runtime.jobmaster.SlotInfo;
import org.apache.flink.runtime.jobmaster.SlotRequestId;
import org.apache.flink.runtime.jobmaster.slotpool.PhysicalSlot;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.util.ResourceCounter;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/** {@link SlotAllocator} implementation that supports slot sharing. */
public class SlotSharingSlotAllocator implements SlotAllocator {

    private final ReserveSlotFunction reserveSlotFunction;
    private final FreeSlotFunction freeSlotFunction;
    private final IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction;

    private SlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        this.reserveSlotFunction = reserveSlot;
        this.freeSlotFunction = freeSlotFunction;
        this.isSlotAvailableAndFreeFunction = isSlotAvailableAndFreeFunction;
    }

    public static SlotSharingSlotAllocator createSlotSharingSlotAllocator(
            ReserveSlotFunction reserveSlot,
            FreeSlotFunction freeSlotFunction,
            IsSlotAvailableAndFreeFunction isSlotAvailableAndFreeFunction) {
        return new SlotSharingSlotAllocator(
                reserveSlot, freeSlotFunction, isSlotAvailableAndFreeFunction);
    }

    @Override
    public ResourceCounter calculateRequiredSlots(
            Iterable<JobInformation.VertexInformation> vertices) {
        int numTotalRequiredSlots = 0;
        for (Integer requiredSlots : getMaxParallelismForSlotSharingGroups(vertices).values()) {
            numTotalRequiredSlots += requiredSlots;
        }
        return ResourceCounter.withResource(ResourceProfile.UNKNOWN, numTotalRequiredSlots);
    }

    private static Map<SlotSharingGroupId, Integer> getMaxParallelismForSlotSharingGroups(
            Iterable<JobInformation.VertexInformation> vertices) {
        final Map<SlotSharingGroupId, Integer> maxParallelismForSlotSharingGroups = new HashMap<>();
        for (JobInformation.VertexInformation vertex : vertices) {
            maxParallelismForSlotSharingGroups.compute(
                    vertex.getSlotSharingGroup().getSlotSharingGroupId(),
                    (slotSharingGroupId, currentMaxParallelism) ->
                            currentMaxParallelism == null
                                    ? vertex.getParallelism()
                                    : Math.max(currentMaxParallelism, vertex.getParallelism()));
        }
        return maxParallelismForSlotSharingGroups;
    }

    /**
     * 给ExecutionVertex分配slot的逻辑
     */
    @Override
    public Optional<VertexParallelismWithSlotSharing> determineParallelism(
            JobInformation jobInformation, Collection<? extends SlotInfo> freeSlots) {
        // TODO: This can waste slots if the max parallelism for slot sharing groups is not equal
        // availableSlots
        final int slotsPerSlotSharingGroup =
                freeSlots.size() / jobInformation.getSlotSharingGroups().size();

        if (slotsPerSlotSharingGroup == 0) {
            // => less slots than slot-sharing groups
            return Optional.empty();
        }
        // 所有空闲的Slot
        final Iterator<? extends SlotInfo> slotIterator = freeSlots.iterator();

        // ExecutionSlot分配
        final Collection<ExecutionSlotSharingGroupAndSlot> assignments = new ArrayList<>();
        final Map<JobVertexID, Integer> allVertexParallelism = new HashMap<>();

        // 所有 SlotSharingGroup => ExecutionVertex
        for (SlotSharingGroup slotSharingGroup : jobInformation.getSlotSharingGroups()) {
            final List<JobInformation.VertexInformation> containedJobVertices =
                    slotSharingGroup.getJobVertexIds().stream()
                            .map(jobInformation::getVertexInformation)
                            .collect(Collectors.toList());

            final Map<JobVertexID, Integer> vertexParallelism =
                    determineParallelism(containedJobVertices, slotsPerSlotSharingGroup);

            // 按照subTaskIndex维度，将每个Slot下的ExecutionVertex放到一起 Map<Integer,Set<ExecutionVertex>>
            final Iterable<ExecutionSlotSharingGroup> sharedSlotToVertexAssignment =
                    createExecutionSlotSharingGroups(vertexParallelism);
            /*
             * 顺序分配 FreeSlot 给 ExecutionVertex => OperatorSubTask
             */
            for (ExecutionSlotSharingGroup executionSlotSharingGroup :
                    sharedSlotToVertexAssignment) {
                // 关联 Slot 和 ExecutionSlotSharingGroup
                final SlotInfo slotInfo = slotIterator.next();
                assignments.add(
                        new ExecutionSlotSharingGroupAndSlot(executionSlotSharingGroup, slotInfo));
            }
            allVertexParallelism.putAll(vertexParallelism);
        }

        return Optional.of(new VertexParallelismWithSlotSharing(allVertexParallelism, assignments));
    }

    private static Map<JobVertexID, Integer> determineParallelism(
            Collection<JobInformation.VertexInformation> containedJobVertices, int availableSlots) {
        final Map<JobVertexID, Integer> vertexParallelism = new HashMap<>();
        for (JobInformation.VertexInformation jobVertex : containedJobVertices) {
            final int parallelism = Math.min(jobVertex.getParallelism(), availableSlots);

            vertexParallelism.put(jobVertex.getJobVertexID(), parallelism);
        }

        return vertexParallelism;
    }

    /**
     * 按照subTaskIndex维度，将每个Slot下的ExecutionVertex放到一起 Map<Integer,Set<ExecutionVertex>>
     */
    private static Iterable<ExecutionSlotSharingGroup> createExecutionSlotSharingGroups(
            Map<JobVertexID, Integer> containedJobVertices) {
        final Map<Integer, Set<ExecutionVertexID>> sharedSlotToVertexAssignment = new HashMap<>();
        // 将JobVertex 转化为 ExecutionVertex
        for (Map.Entry<JobVertexID, Integer> jobVertex : containedJobVertices.entrySet()) {
            for (int i = 0; i < jobVertex.getValue(); i++) {
                sharedSlotToVertexAssignment
                        .computeIfAbsent(i, ignored -> new HashSet<>())
                        .add(new ExecutionVertexID(jobVertex.getKey(), i));
            }
        }

        return sharedSlotToVertexAssignment.values().stream()
                .map(ExecutionSlotSharingGroup::new)
                .collect(Collectors.toList());
    }

    @Override
    public Optional<ReservedSlots> tryReserveResources(VertexParallelism vertexParallelism) {
        Preconditions.checkArgument(
                vertexParallelism instanceof VertexParallelismWithSlotSharing,
                String.format(
                        "%s expects %s as argument.",
                        SlotSharingSlotAllocator.class.getSimpleName(),
                        VertexParallelismWithSlotSharing.class.getSimpleName()));

        final VertexParallelismWithSlotSharing vertexParallelismWithSlotSharing =
                (VertexParallelismWithSlotSharing) vertexParallelism;

        // 收集Slot AllocationID
        final Collection<AllocationID> expectedSlots =
                calculateExpectedSlots(vertexParallelismWithSlotSharing.getAssignments());

        if (areAllExpectedSlotsAvailableAndFree(expectedSlots)) {
            final Map<ExecutionVertexID, LogicalSlot> assignedSlots = new HashMap<>();

            // 【Slot分配】分配 Slot
            for (ExecutionSlotSharingGroupAndSlot executionSlotSharingGroup :
                    vertexParallelismWithSlotSharing.getAssignments()) {
                // 从 Slot Pool 中获取 Slot
                final SharedSlot sharedSlot =
                        reserveSharedSlot(executionSlotSharingGroup.getSlotInfo());

                for (ExecutionVertexID executionVertexId :
                        executionSlotSharingGroup
                                .getExecutionSlotSharingGroup()
                                .getContainedExecutionVertices()) {
                    final LogicalSlot logicalSlot = sharedSlot.allocateLogicalSlot();
                    assignedSlots.put(executionVertexId, logicalSlot);
                }
            }

            return Optional.of(ReservedSlots.create(assignedSlots));
        } else {
            return Optional.empty();
        }
    }

    @Nonnull
    private Collection<AllocationID> calculateExpectedSlots(
            Iterable<? extends ExecutionSlotSharingGroupAndSlot> assignments) {
        final Collection<AllocationID> requiredSlots = new ArrayList<>();

        for (ExecutionSlotSharingGroupAndSlot assignment : assignments) {
            requiredSlots.add(assignment.getSlotInfo().getAllocationId());
        }
        return requiredSlots;
    }

    private boolean areAllExpectedSlotsAvailableAndFree(
            Iterable<? extends AllocationID> requiredSlots) {
        for (AllocationID requiredSlot : requiredSlots) {
            if (!isSlotAvailableAndFreeFunction.isSlotAvailableAndFree(requiredSlot)) {
                return false;
            }
        }

        return true;
    }

    private SharedSlot reserveSharedSlot(SlotInfo slotInfo) {
        final PhysicalSlot physicalSlot =
                reserveSlotFunction.reserveSlot(
                        slotInfo.getAllocationId(), ResourceProfile.UNKNOWN);

        return new SharedSlot(
                new SlotRequestId(),
                physicalSlot,
                slotInfo.willBeOccupiedIndefinitely(),
                () ->
                        freeSlotFunction.freeSlot(
                                slotInfo.getAllocationId(), null, System.currentTimeMillis()));
    }

    static class ExecutionSlotSharingGroup {
        private final Set<ExecutionVertexID> containedExecutionVertices;

        public ExecutionSlotSharingGroup(Set<ExecutionVertexID> containedExecutionVertices) {
            this.containedExecutionVertices = containedExecutionVertices;
        }

        public Collection<ExecutionVertexID> getContainedExecutionVertices() {
            return containedExecutionVertices;
        }
    }

    static class ExecutionSlotSharingGroupAndSlot {
        private final ExecutionSlotSharingGroup executionSlotSharingGroup;
        private final SlotInfo slotInfo;

        public ExecutionSlotSharingGroupAndSlot(
                ExecutionSlotSharingGroup executionSlotSharingGroup, SlotInfo slotInfo) {
            this.executionSlotSharingGroup = executionSlotSharingGroup;
            this.slotInfo = slotInfo;
        }

        public ExecutionSlotSharingGroup getExecutionSlotSharingGroup() {
            return executionSlotSharingGroup;
        }

        public SlotInfo getSlotInfo() {
            return slotInfo;
        }
    }
}
