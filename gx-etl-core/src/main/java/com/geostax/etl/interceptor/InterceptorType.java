/**
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

package com.geostax.etl.interceptor;

public enum InterceptorType {

  TIMESTAMP(com.geostax.etl.interceptor.TimestampInterceptor.Builder.class),
  HOST(com.geostax.etl.interceptor.HostInterceptor.Builder.class),
  STATIC(com.geostax.etl.interceptor.StaticInterceptor.Builder.class),
  REGEX_FILTER(
      com.geostax.etl.interceptor.RegexFilteringInterceptor.Builder.class),
  REGEX_EXTRACTOR(com.geostax.etl.interceptor.RegexExtractorInterceptor.Builder.class),
  REMOVE_HEADER(com.geostax.etl.interceptor.RemoveHeaderInterceptor.Builder.class),
  SEARCH_REPLACE(com.geostax.etl.interceptor.SearchAndReplaceInterceptor.Builder.class),
  PRINT(com.geostax.etl.interceptor.PrintInterceptor.Builder.class);

  private final Class<? extends Interceptor.Builder> builderClass;

  InterceptorType(Class<? extends Interceptor.Builder> builderClass) {
    this.builderClass = builderClass;
  }

  public Class<? extends Interceptor.Builder> getBuilderClass() {
    return builderClass;
  }

}
