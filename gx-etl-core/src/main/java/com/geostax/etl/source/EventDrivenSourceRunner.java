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

package com.geostax.etl.source;

import com.geostax.etl.EventDrivenSource;
import com.geostax.etl.Source;
import com.geostax.etl.SourceRunner;
import com.geostax.etl.channel.ChannelProcessor;
import com.geostax.etl.lifecycle.LifecycleState;

/**
 * Starts, stops, and manages {@linkplain EventDrivenSource event-driven
 * sources}.
 */
public class EventDrivenSourceRunner extends SourceRunner {

	private LifecycleState lifecycleState;

	public EventDrivenSourceRunner() {
		lifecycleState = LifecycleState.IDLE;
	}

	@Override
	public void start() {
		Source source = getSource();
		ChannelProcessor cp = source.getChannelProcessor();
		cp.initialize();
		source.start();
		lifecycleState = LifecycleState.START;
	}

	@Override
	public void stop() {
		Source source = getSource();
		source.stop();
		ChannelProcessor cp = source.getChannelProcessor();
		cp.close();
		lifecycleState = LifecycleState.STOP;
	}

	@Override
	public String toString() {
		return "EventDrivenSourceRunner: { source:" + getSource() + " }";
	}

	@Override
	public LifecycleState getLifecycleState() {
		return lifecycleState;
	}

}
