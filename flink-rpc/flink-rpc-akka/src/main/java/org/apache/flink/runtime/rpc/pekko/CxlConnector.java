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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

class CxlConnector {
    private static final String CXLPATH = "/mnt/cxl/shm";
    private static final int SHMSIZE = 1024 * 1024; // Default SHM size 1MB
    private final MappedByteBuffer shm;

    CxlConnector() throws IOException {
        Path file = Path.of(System.getProperty("file", CXLPATH));

        try (FileChannel ch =
                FileChannel.open(
                        file,
                        StandardOpenOption.CREATE,
                        StandardOpenOption.READ,
                        StandardOpenOption.WRITE)) {
            if (ch.size() != SHMSIZE) {
                ch.truncate(SHMSIZE);
            }
            this.shm = ch.map(FileChannel.MapMode.READ_WRITE, 0, SHMSIZE);
        }
    }

    public void invoke(Object invocation) throws IOException {
        byte[] payload;
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(invocation);
            oos.flush();
            payload = baos.toByteArray();
        }
        shm.position(0);
        shm.putInt(payload.length);
        shm.put(payload);
    }

    public Object parseInvocation() throws IOException, ClassNotFoundException {
        shm.position(0);
        int len = shm.getInt();
        byte[] payload = new byte[len];
        shm.get(payload);

        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(payload))) {
            return ois.readObject();
        }
    }
}
