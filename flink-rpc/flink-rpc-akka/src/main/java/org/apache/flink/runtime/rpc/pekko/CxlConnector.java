/*
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

package org.apache.flink.runtime.rpc.pekko;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

class CxlConnector {
    static {
        System.loadLibrary("cxlconnector");
    }

    private final long cxlConnector;

    CxlConnector() {
        cxlConnector = create();
    }

    public int getPosition() {
        return get_position(cxlConnector);
    }

    public void invoke(Object invocation, int position) throws IOException {
        byte[] payload;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(invocation);
            oos.flush();
            payload = baos.toByteArray();
        }
        ByteBuffer direct =
                ByteBuffer.allocateDirect(payload.length).order(ByteOrder.nativeOrder());
        direct.put(payload).flip();
        write_buf(cxlConnector, direct, direct.remaining(), position);
    }

    public Object parseInvocation(int position) throws IOException, ClassNotFoundException {
        int size = read_size(cxlConnector, position);
        ByteBuffer direct = ByteBuffer.allocateDirect(size).order(ByteOrder.nativeOrder());
        read_buf(cxlConnector, direct, size, position);
        byte[] payload = new byte[size];
        direct.get(payload);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            return ois.readObject();
        }
    }

    private static native long create();

    private static native void destroy(long obj);

    private static native int get_position(long obj);

    private static native void write_buf(long obj, ByteBuffer buf, int len, int position);

    private static native int read_size(long obj, int position);

    private static native void read_buf(long obj, ByteBuffer buf, int len, int position);
}
