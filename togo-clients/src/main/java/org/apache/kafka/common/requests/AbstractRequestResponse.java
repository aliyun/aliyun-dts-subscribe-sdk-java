/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.requests;

import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.Struct;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public abstract class AbstractRequestResponse {
    /**
     * Visible for testing.
     */
    public static ByteBuffer serialize(Struct headerStruct, Struct bodyStruct) {
        ByteBuffer buffer = ByteBuffer.allocate(headerStruct.sizeOf() + bodyStruct.sizeOf());
        headerStruct.writeTo(buffer);
        bodyStruct.writeTo(buffer);
        buffer.rewind();
        return buffer;
    }
    public static final Schema GAP_REQUEST_RESPONSE = null;

    public static Schema[] fillSchemaWithGap(Schema[] originSchema, Schema gapSchema, int gapToSize, Schema endSchema) {
        List<Schema> retSchema = new ArrayList<>();
        retSchema.addAll(Arrays.asList(originSchema));
        while (retSchema.size() < gapToSize) {
            retSchema.add(gapSchema);
        }
        retSchema.add(endSchema);
        return retSchema.toArray(new Schema[retSchema.size()]);
    }
}
