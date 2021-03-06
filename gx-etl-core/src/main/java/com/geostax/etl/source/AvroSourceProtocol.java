/**
 * Autogenerated by Avro
 * 
 * DO NOT EDIT DIRECTLY
 */
package com.geostax.etl.source;

@SuppressWarnings("all")
/**
 * * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
@org.apache.avro.specific.AvroGenerated
public interface AvroSourceProtocol {
	public static final org.apache.avro.Protocol PROTOCOL = org.apache.avro.Protocol.parse(
			"{\"protocol\":\"AvroSourceProtocol\",\"namespace\":\"org.apache.flume.source.avro\",\"doc\":\"* Licensed to the Apache Software Foundation (ASF) under one\\n * or more contributor license agreements.  See the NOTICE file\\n * distributed with this work for additional information\\n * regarding copyright ownership.  The ASF licenses this file\\n * to you under the Apache License, Version 2.0 (the\\n * \\\"License\\\"); you may not use this file except in compliance\\n * with the License.  You may obtain a copy of the License at\\n *\\n * http://www.apache.org/licenses/LICENSE-2.0\\n *\\n * Unless required by applicable law or agreed to in writing,\\n * software distributed under the License is distributed on an\\n * \\\"AS IS\\\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY\\n * KIND, either express or implied.  See the License for the\\n * specific language governing permissions and limitations\\n * under the License.\",\"types\":[{\"type\":\"enum\",\"name\":\"Status\",\"symbols\":[\"OK\",\"FAILED\",\"UNKNOWN\"]},{\"type\":\"record\",\"name\":\"AvroFlumeEvent\",\"fields\":[{\"name\":\"headers\",\"type\":{\"type\":\"map\",\"values\":\"string\"}},{\"name\":\"body\",\"type\":\"bytes\"}]}],\"messages\":{\"append\":{\"request\":[{\"name\":\"event\",\"type\":\"AvroFlumeEvent\"}],\"response\":\"Status\"},\"appendBatch\":{\"request\":[{\"name\":\"events\",\"type\":{\"type\":\"array\",\"items\":\"AvroFlumeEvent\"}}],\"response\":\"Status\"}}}");

	org.apache.flume.source.avro.Status append(org.apache.flume.source.avro.AvroFlumeEvent event)
			throws org.apache.avro.AvroRemoteException;

	org.apache.flume.source.avro.Status appendBatch(java.util.List<org.apache.flume.source.avro.AvroFlumeEvent> events)
			throws org.apache.avro.AvroRemoteException;

	@SuppressWarnings("all")
	/**
	 * * Licensed to the Apache Software Foundation (ASF) under one or more
	 * contributor license agreements. See the NOTICE file distributed with this
	 * work for additional information regarding copyright ownership. The ASF
	 * licenses this file to you under the Apache License, Version 2.0 (the
	 * "License"); you may not use this file except in compliance with the
	 * License. You may obtain a copy of the License at
	 *
	 * http://www.apache.org/licenses/LICENSE-2.0
	 *
	 * Unless required by applicable law or agreed to in writing, software
	 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
	 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
	 * License for the specific language governing permissions and limitations
	 * under the License.
	 */
	public interface Callback extends AvroSourceProtocol {
		public static final org.apache.avro.Protocol PROTOCOL = org.apache.flume.source.avro.AvroSourceProtocol.PROTOCOL;

		void append(org.apache.flume.source.avro.AvroFlumeEvent event,
				org.apache.avro.ipc.Callback<org.apache.flume.source.avro.Status> callback) throws java.io.IOException;

		void appendBatch(java.util.List<org.apache.flume.source.avro.AvroFlumeEvent> events,
				org.apache.avro.ipc.Callback<org.apache.flume.source.avro.Status> callback) throws java.io.IOException;
	}
}