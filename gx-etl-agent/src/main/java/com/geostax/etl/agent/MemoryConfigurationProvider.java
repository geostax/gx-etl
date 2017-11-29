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

import java.util.Map;

import com.geostax.etl.annotations.InterfaceAudience;
import com.geostax.etl.annotations.InterfaceStability;
import com.geostax.etl.configuration.FlumeConfiguration;
import com.geostax.etl.node.AbstractConfigurationProvider;

/**
 * MemoryConfigurationProvider is the simplest possible
 * AbstractConfigurationProvider simply turning a give properties file and
 * agent name into a FlumeConfiguration object.
 */
@InterfaceAudience.Private
@InterfaceStability.Unstable
class MemoryConfigurationProvider extends AbstractConfigurationProvider {
  private final Map<String, String> properties;

  MemoryConfigurationProvider(String name, Map<String, String> properties) {
    super(name);
    this.properties = properties;
  }

  @Override
  protected FlumeConfiguration getFlumeConfiguration() {
    return new FlumeConfiguration(properties);
  }

}
