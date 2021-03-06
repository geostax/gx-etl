/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.geostax.etl.client.avro;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;

import com.geostax.etl.annotations.InterfaceAudience;
import com.geostax.etl.annotations.InterfaceStability;
import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

/**
 * A {@link EventReader} implementation which delegates to a
 * {@link BufferedReader}.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class SimpleTextLineEventReader implements EventReader {

  private final BufferedReader reader;

  public SimpleTextLineEventReader(Reader in) {
    reader = new BufferedReader(in);
  }

  @Override
  public Event readEvent() throws IOException {
    String line = reader.readLine();
    if (line != null) {
      return EventBuilder.withBody(line, Charsets.UTF_8);
    } else {
      return null;
    }
  }

  @Override
  public List<Event> readEvents(int n) throws IOException {
    List<Event> events = Lists.newLinkedList();
    while (events.size() < n) {
      Event event = readEvent();
      if (event != null) {
        events.add(event);
      } else {
        break;
      }
    }
    return events;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
