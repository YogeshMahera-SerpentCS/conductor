/*
 * Copyright 2020 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.netflix.conductor.dao.cassandra;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.workflow.WorkflowDef;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.core.execution.ApplicationException;
import com.netflix.conductor.util.EmbeddedCassandra;
import com.netflix.conductor.util.Statements;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CassandraMetadataDAOTest {

    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();

    private EmbeddedCassandra embeddedCassandra;
    private CassandraMetadataDAO metadataDAO;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        embeddedCassandra = new EmbeddedCassandra();
        Session session = embeddedCassandra.getSession();
        Statements statements = new Statements(testConfiguration);
        metadataDAO = new CassandraMetadataDAO(session, objectMapper, testConfiguration, statements);
    }

    @After
    public void teardown() {
        embeddedCassandra.cleanupData();
    }

    @Test
    public void testWorkflowDefCRUD() {
        String name = "workflow_def_1";
        int version = 1;

        WorkflowDef workflowDef = new WorkflowDef();
        workflowDef.setName(name);
        workflowDef.setVersion(version);
        workflowDef.setOwnerEmail("test@junit.com");

        // register the workflow definition
        metadataDAO.createWorkflowDef(workflowDef);

        // check if workflow definition exists
        assertTrue(metadataDAO.workflowDefExists(workflowDef));

        // fetch the workflow definition
        Optional<WorkflowDef> defOptional = metadataDAO.getWorkflowDef(name, version);
        assertTrue(defOptional.isPresent());
        assertEquals(workflowDef, defOptional.get());

        // get all workflow definitions
        List<WorkflowDef> workflowDefs = metadataDAO.getAllWorkflowDefs();
        assertNotNull(workflowDefs);
        assertEquals(1, workflowDefs.size());
        assertEquals(workflowDef, workflowDefs.get(0));

        // register a higher version
        int higherVersion = 2;
        workflowDef.setVersion(higherVersion);
        workflowDef.setDescription("higher version");

        // register the higher version definition
        metadataDAO.createWorkflowDef(workflowDef);

        // check if workflow definition exists
        assertTrue(metadataDAO.workflowDefExists(workflowDef));

        // fetch the higher version
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion);
        assertTrue(defOptional.isPresent());
        assertEquals(workflowDef, defOptional.get());

        // fetch latest version
        defOptional = metadataDAO.getLatestWorkflowDef(name);
        assertTrue(defOptional.isPresent());
        assertEquals(workflowDef, defOptional.get());

        // get all workflow definitions
        workflowDefs = metadataDAO.getAllWorkflowDefs();
        assertNotNull(workflowDefs);
        assertEquals(2, workflowDefs.size());

        // modify the definition
        workflowDef.setOwnerEmail("junit@test.com");
        metadataDAO.updateWorkflowDef(workflowDef);

        // fetch the workflow definition
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion);
        assertTrue(defOptional.isPresent());
        assertEquals(workflowDef, defOptional.get());

        // register same definition again
        expectedException.expect(ApplicationException.class);
        expectedException.expectMessage("Workflow: workflow_def_1, version: 2 already exists!");
        metadataDAO.createWorkflowDef(workflowDef);

        // delete workflow def
        metadataDAO.removeWorkflowDef(name, higherVersion);
        defOptional = metadataDAO.getWorkflowDef(name, higherVersion);
        assertFalse(defOptional.isPresent());

        // get all workflow definitions
        workflowDefs = metadataDAO.getAllWorkflowDefs();
        assertNotNull(workflowDefs);
        assertEquals(1, workflowDefs.size());

        // check if workflow definition exists
        assertFalse(metadataDAO.workflowDefExists(workflowDef));
    }

    @Test
    public void testTaskDefCrud() {
        String task1Name = "task1";
        String task2Name = "task2";

        // fetch all task defs
        List<TaskDef> taskDefList = metadataDAO.getAllTaskDefs();
        assertNotNull(taskDefList);
        assertEquals(0, taskDefList.size());

        TaskDef taskDef = new TaskDef();
        taskDef.setName(task1Name);

        // register a task definition
        metadataDAO.createTaskDef(taskDef);

        // fetch all task defs
        taskDefList = metadataDAO.getAllTaskDefs();
        assertNotNull(taskDefList);
        assertEquals(1, taskDefList.size());

        // fetch the task def
        TaskDef def = metadataDAO.getTaskDef(task1Name);
        assertEquals(taskDef, def);

        // register another task definition
        TaskDef taskDef1 = new TaskDef();
        taskDef1.setName(task2Name);
        metadataDAO.createTaskDef(taskDef1);

        // fetch all task defs
        taskDefList = metadataDAO.getAllTaskDefs();
        assertNotNull(taskDefList);
        assertEquals(2, taskDefList.size());

        // update task def
        taskDef.setOwnerEmail("juni@test.com");
        metadataDAO.updateTaskDef(taskDef);
        def = metadataDAO.getTaskDef(task1Name);
        assertEquals(taskDef, def);

        // delete task def
        metadataDAO.removeTaskDef(task2Name);
        taskDefList = metadataDAO.getAllTaskDefs();
        assertNotNull(taskDefList);
        assertEquals(1, taskDefList.size());

        // fetch deleted task def
        def = metadataDAO.getTaskDef(task2Name);
        assertNull(def);
    }
}