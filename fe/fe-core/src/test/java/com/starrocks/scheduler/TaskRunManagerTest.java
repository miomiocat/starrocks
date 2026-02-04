// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.starrocks.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.common.Config;
import com.starrocks.common.FeConstants;
import com.starrocks.common.util.PropertyAnalyzer;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.utframe.UtFrameUtils;
import com.starrocks.warehouse.DefaultWarehouse;
import org.apache.commons.collections4.CollectionUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.starrocks.scheduler.MVRefreshTestBase.createAndRefreshMv;

public class TaskRunManagerTest {

    private static final int N = 100;
    private static ConnectContext connectContext;

    @BeforeClass
    public static void beforeClass() throws Exception {
        FeConstants.runningUnitTest = true;
        UtFrameUtils.createMinStarRocksCluster();

        connectContext = UtFrameUtils.createDefaultCtx();
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(1, "w1"));
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(2, "w2"));
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority) {
        return makeExecuteOption(isMergeRedundant, isSync, priority, Maps.newHashMap());
    }

    private static ExecuteOption makeExecuteOption(boolean isMergeRedundant, boolean isSync, int priority,
                                                   Map<String, String> properties) {
        ExecuteOption executeOption = new ExecuteOption(Constants.TaskRunPriority.LOWEST.value(), isMergeRedundant,
                properties);
        executeOption.setSync(isSync);
        executeOption.setPriority(priority);
        return executeOption;
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption) {
        return makeTaskRun(taskId, task, executeOption, -1);
    }

    private TaskRun makeTaskRun(long taskId, Task task, ExecuteOption executeOption, long createTime) {
        TaskRun taskRun = TaskRunBuilder
                .newBuilder(task)
                .setExecuteOption(executeOption)
                .build();
        taskRun.setTaskId(taskId);
        // submitTaskRun needs task run status is empty
        if (createTime >= 0) {
            taskRun.initStatus("1", createTime);
            taskRun.getStatus().setPriority(executeOption.getPriority());
        }
        return taskRun;
    }

    @Test
    public void testKillTaskRun() {
        Task task = new Task("test");
        task.setDefinition("select 1");
        List<TaskRun> taskRuns = Lists.newArrayList();
        long taskId = 100;

        TaskRunScheduler scheduler = new TaskRunScheduler();
        TaskRunManager taskRunManager = new TaskRunManager(scheduler);

        boolean[] forces = {false, true};
        for (boolean force : forces) {
            for (int i = 0; i < N; i++) {
                TaskRun taskRun = makeTaskRun(taskId, task, makeExecuteOption(true, false, 1));
                taskRuns.add(taskRun);
                scheduler.addPendingTaskRun(taskRun);
            }

            scheduler.scheduledPendingTaskRun(taskRun -> {
                Assert.assertTrue(taskRun.getTaskId() == taskId);
            });

            Assert.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
            Assert.assertTrue(scheduler.getRunnableTaskRun(taskId) != null);
            Assert.assertTrue(scheduler.getPendingTaskRunsByTaskId(taskId).size() == N - 1);

            // no matter whether force is true or not, we always clear running and pending task run
            taskRunManager.killTaskRun(taskId, force);

            System.out.println("force:" + force);
            Assert.assertTrue(CollectionUtils.isEmpty(scheduler.getPendingTaskRunsByTaskId(taskId)));
            if (force) {
                Assert.assertTrue(scheduler.getRunningTaskRun(taskId) == null);
            } else {
                Assert.assertTrue(scheduler.getRunningTaskRun(taskId) != null);
                scheduler.removeRunningTask(taskId);
            }
        }
    }

    private Map<String, String> makeTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    private Map<String, String> makeMVTaskRunProperties(String partitionStart,
                                                      String partitionEnd,
                                                      boolean isForce) {
        Map<String, String> result = Maps.newHashMap();
        result.put(TaskRun.PARTITION_START, partitionStart);
        result.put(TaskRun.PARTITION_END, partitionEnd);
        result.put(TaskRun.MV_ID, "1");
        result.put(TaskRun.FORCE, String.valueOf(isForce));
        return result;
    }

    @Test
    public void testExecutionOption() {
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(true, false, 10);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            ExecuteOption option1 = makeExecuteOption(true, false, 1);
            ExecuteOption option2 = makeExecuteOption(false, false, 10);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", true);
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertFalse(option1.isMergeableWith(option2));
        }
        {
            Map<String, String> prop1 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop1.put("a", "a");
            ExecuteOption option1 = makeExecuteOption(true, false, 1, prop1);
            Map<String, String> prop11 = option1.getTaskRunComparableProperties();
            Assert.assertTrue(prop11.size() == 4);

            Map<String, String> prop2 = makeMVTaskRunProperties("2023-01-01", "2023-01-02", false);
            prop2.put("a", "b");
            ExecuteOption option2 = makeExecuteOption(true, false, 2, prop2);
            Assert.assertTrue(option1.isMergeableWith(option2));
        }
    }

    /**
     * Test warehouse selection logic when refreshing materialized view
     * Covers the new feature: enable_mv_manual_refresh_use_context_warehouse
     */
    @Test
    public void testManualRefreshMVWarehouseSelection() throws Exception {

        createAndRefreshMv("create materialized view `test_mv_wh`" +
                " partition by id_date" +
                " distributed by hash(`t1a`)" +
                " as" +
                " select t1a, id_date, t1b from table_with_partition");
        // Test warehouses
        final long MV_WAREHOUSE_ID = 1001L;
        final String MV_WAREHOUSE_NAME = "mv_warehouse";
        final long CONTEXT_WAREHOUSE_ID = 1002L;
        final String CONTEXT_WAREHOUSE_NAME = "context_warehouse";
        String dbName = "test";
        GlobalStateMgr globalStateMgr = connectContext.getGlobalStateMgr();
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(MV_WAREHOUSE_ID, MV_WAREHOUSE_NAME));
        globalStateMgr.getWarehouseMgr().addWarehouse(new DefaultWarehouse(CONTEXT_WAREHOUSE_ID, CONTEXT_WAREHOUSE_NAME));

        boolean originalConfigValue = Config.enable_mv_manual_refresh_use_context_warehouse;
        // Test case 1: Config enabled + manual refresh + has parentRunCtx
        // Expected: Use warehouse from connection context
        {
            Config.enable_mv_manual_refresh_use_context_warehouse = true;

            // Create parent context with CONTEXT_WAREHOUSE
            ConnectContext parentCtx = new ConnectContext(null);
            parentCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            parentCtx.setCurrentWarehouse(CONTEXT_WAREHOUSE_NAME);

            // Create TaskRun with manual refresh
            Task task = new Task("test_mv_task");
            task.setSource(Constants.TaskSource.MV);
            task.setDbName(dbName);
            task.setDefinition("SELECT 1");
            Map<String, String> taskProperties = Maps.newHashMap();
            taskProperties.put(TaskRun.MV_ID, "10001");
            task.setProperties(taskProperties);

            ExecuteOption executeOption = new ExecuteOption(task);
            executeOption.setManual(true);

            TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                    .setExecuteOption(executeOption)
                    .setConnectContext(parentCtx)
                    .build();

            // Execute refreshTaskProperties
            ConnectContext runCtx = new ConnectContext(null);
            runCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            runCtx.setDatabase("test");

            Map<String, String> properties = taskRun.refreshTaskProperties(runCtx);

            // Verify: should use CONTEXT_WAREHOUSE
            Assert.assertEquals("Should use warehouse from connection context",
                    CONTEXT_WAREHOUSE_NAME, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
            Assert.assertEquals("Context warehouse should be set in runCtx",
                    CONTEXT_WAREHOUSE_NAME, runCtx.getCurrentWarehouseName());
        }

        // Test case 2: Config disabled
        // Expected: Use warehouse from MV property
        {
            Config.enable_mv_manual_refresh_use_context_warehouse = false;

            ConnectContext parentCtx = new ConnectContext(null);
            parentCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            parentCtx.setCurrentWarehouse(CONTEXT_WAREHOUSE_NAME);

            Task task = new Task("test_mv_task");
            task.setSource(Constants.TaskSource.MV);
            task.setDbName("test");
            task.setDefinition("SELECT 1");
            Map<String, String> taskProperties = Maps.newHashMap();
            taskProperties.put(TaskRun.MV_ID, "10001");
            task.setProperties(taskProperties);

            ExecuteOption executeOption = new ExecuteOption(task);
            executeOption.setManual(true);

            TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                    .setExecuteOption(executeOption)
                    .setConnectContext(parentCtx)
                    .build();

            ConnectContext runCtx = new ConnectContext(null);
            runCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            runCtx.setDatabase("test");

            Map<String, String> properties = taskRun.refreshTaskProperties(runCtx);

            // Verify: should use MV_WAREHOUSE
            Assert.assertEquals("Should use warehouse from MV property when config disabled",
                    MV_WAREHOUSE_NAME, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }

        // Test case 3: Config enabled but not manual refresh (automatic refresh)
        // Expected: Use warehouse from MV property
        {
            Config.enable_mv_manual_refresh_use_context_warehouse = true;

            ConnectContext parentCtx = new ConnectContext(null);
            parentCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            parentCtx.setCurrentWarehouse(CONTEXT_WAREHOUSE_NAME);

            Task task = new Task("test_mv_task");
            task.setSource(Constants.TaskSource.MV);
            task.setDbName("test");
            task.setDefinition("SELECT 1");
            Map<String, String> taskProperties = Maps.newHashMap();
            taskProperties.put(TaskRun.MV_ID, "10001");
            task.setProperties(taskProperties);

            ExecuteOption executeOption = new ExecuteOption(task);
            executeOption.setManual(false);  // Automatic refresh

            TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                    .setExecuteOption(executeOption)
                    .setConnectContext(parentCtx)
                    .build();

            ConnectContext runCtx = new ConnectContext(null);
            runCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            runCtx.setDatabase("test");

            Map<String, String> properties = taskRun.refreshTaskProperties(runCtx);

            // Verify: should use MV_WAREHOUSE for automatic refresh
            Assert.assertEquals("Should use warehouse from MV property for automatic refresh",
                    MV_WAREHOUSE_NAME, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }

        // Test case 4: Config enabled + manual refresh but no parentRunCtx
        // Expected: Use warehouse from MV property (fallback)
        {
            Config.enable_mv_manual_refresh_use_context_warehouse = true;

            Task task = new Task("test_mv_task");
            task.setSource(Constants.TaskSource.MV);
            task.setDbName("test");
            task.setDefinition("SELECT 1");
            Map<String, String> taskProperties = Maps.newHashMap();
            taskProperties.put(TaskRun.MV_ID, "10001");
            task.setProperties(taskProperties);

            ExecuteOption executeOption = new ExecuteOption(task);
            executeOption.setManual(true);

            TaskRun taskRun = TaskRunBuilder.newBuilder(task)
                    .setExecuteOption(executeOption)
                    .setConnectContext(null)  // No parent context
                    .build();

            ConnectContext runCtx = new ConnectContext(null);
            runCtx.setGlobalStateMgr(GlobalStateMgr.getCurrentState());
            runCtx.setDatabase("test");

            Map<String, String> properties = taskRun.refreshTaskProperties(runCtx);

            // Verify: should fallback to MV_WAREHOUSE
            Assert.assertEquals("Should fallback to MV warehouse when no parent context",
                    MV_WAREHOUSE_NAME, properties.get(PropertyAnalyzer.PROPERTIES_WAREHOUSE));
        }
        Config.enable_mv_manual_refresh_use_context_warehouse = originalConfigValue;
    }
}
