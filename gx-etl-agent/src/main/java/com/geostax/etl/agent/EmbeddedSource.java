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
package com.geostax.etl.agent;

import java.util.List;

import org.apache.flume.Event;

import com.geostax.etl.ChannelException;
import com.geostax.etl.Context;
import com.geostax.etl.EventDrivenSource;
import com.geostax.etl.annotations.InterfaceAudience;
import com.geostax.etl.annotations.InterfaceStability;
import com.geostax.etl.conf.Configurable;
import com.geostax.etl.source.AbstractSource;

/**
 * Simple source used to allow direct access to the channel for the Embedded
 * Agent.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
public class EmbeddedSource extends AbstractSource implements EventDrivenSource, Configurable {

  @Override
  public void configure(Context context) {
  }

  public void put(Event event) throws ChannelException {
    getChannelProcessor().processEvent(event);
  }

  public void putAll(List<Event> events) throws ChannelException {
    getChannelProcessor().processEventBatch(events);
  }

}
