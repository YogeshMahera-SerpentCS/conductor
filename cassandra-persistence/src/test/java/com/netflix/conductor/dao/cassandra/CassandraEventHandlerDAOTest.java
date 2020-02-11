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
import static org.junit.Assert.assertTrue;

import com.datastax.driver.core.Session;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.netflix.conductor.common.metadata.events.EventHandler;
import com.netflix.conductor.common.utils.JsonMapperProvider;
import com.netflix.conductor.config.TestConfiguration;
import com.netflix.conductor.util.EmbeddedCassandra;
import com.netflix.conductor.util.Statements;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class CassandraEventHandlerDAOTest {

    private final TestConfiguration testConfiguration = new TestConfiguration();
    private final ObjectMapper objectMapper = new JsonMapperProvider().get();

    private EmbeddedCassandra embeddedCassandra;
    private CassandraEventHandlerDAO eventHandlerDAO;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        embeddedCassandra = new EmbeddedCassandra();
        Session session = embeddedCassandra.getSession();
        Statements statements = new Statements(testConfiguration);
        eventHandlerDAO = new CassandraEventHandlerDAO(session, objectMapper, testConfiguration, statements);
    }

    @After
    public void teardown() {
        embeddedCassandra.cleanupData();
    }

    @Test
    public void testEventHandlerCRUD() {
        String event = "event";
        String eventHandlerName1 = "event_handler1";
        String eventHandlerName2 = "event_handler2";

        EventHandler eventHandler = new EventHandler();
        eventHandler.setName(eventHandlerName1);
        eventHandler.setEvent(event);

        // create event handler
        eventHandlerDAO.addEventHandler(eventHandler);

        // fetch all event handlers for event
        List<EventHandler> handlers = eventHandlerDAO.getEventHandlersForEvent(event, false);
        assertNotNull(handlers);
        assertEquals(1, handlers.size());
        assertEquals(eventHandler.getName(), handlers.get(0).getName());
        assertEquals(eventHandler.getEvent(), handlers.get(0).getEvent());
        assertFalse(handlers.get(0).isActive());

        // add an active event handler for the same event
        EventHandler eventHandler1 = new EventHandler();
        eventHandler1.setName(eventHandlerName2);
        eventHandler1.setEvent(event);
        eventHandler1.setActive(true);
        eventHandlerDAO.addEventHandler(eventHandler1);

        // fetch all event handlers
        handlers = eventHandlerDAO.getAllEventHandlers();
        assertNotNull(handlers);
        assertEquals(2, handlers.size());

        // fetch all event handlers for event
        handlers = eventHandlerDAO.getEventHandlersForEvent(event, false);
        assertNotNull(handlers);
        assertEquals(2, handlers.size());

        // fetch only active handlers for event
        handlers = eventHandlerDAO.getEventHandlersForEvent(event, true);
        assertNotNull(handlers);
        assertEquals(1, handlers.size());
        assertEquals(eventHandler1.getName(), handlers.get(0).getName());
        assertEquals(eventHandler1.getEvent(), handlers.get(0).getEvent());
        assertTrue(handlers.get(0).isActive());

        // remove event handler
        eventHandlerDAO.removeEventHandler(eventHandlerName1);
        handlers = eventHandlerDAO.getAllEventHandlers();
        assertNotNull(handlers);
        assertEquals(1, handlers.size());
    }
}