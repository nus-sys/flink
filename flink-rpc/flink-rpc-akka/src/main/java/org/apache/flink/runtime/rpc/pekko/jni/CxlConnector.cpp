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

#include <jni.h>
#include <string.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <stdexcept>
#include <random>
#include <immintrin.h>
#include "org_apache_flink_runtime_rpc_pekko_CxlConnector.h"

#define DAX_DEVICE_SIZE 8ULL * 1024 * 1024 * 1024

static inline void sync_write(void *addr, size_t len) {
    const size_t cacheline = 64;
    uintptr_t p = (uintptr_t)addr & ~(cacheline - 1);
    uintptr_t end = (uintptr_t)addr + len;
    for (; p < end; p += cacheline) {
        _mm_clwb((void*)p);
    }
    _mm_sfence();
}

static inline void sync_read(const void *addr, size_t len) {
    const size_t cacheline = 64;
    uintptr_t p = (uintptr_t)addr & ~(cacheline - 1);
    uintptr_t end = (uintptr_t)addr + len;
    for (; p < end; p += cacheline) {
        _mm_clflush((void*)p);
    }
    _mm_mfence();
}

class CxlConnector {
public:
  CxlConnector() {
    dev_fd = open("/dev/dax0.0", O_RDWR);
    if (dev_fd == -1) {
      throw std::runtime_error("Could not open DAX device");
    }

    base = (uint8_t*)mmap(NULL, DAX_DEVICE_SIZE, PROT_READ | PROT_WRITE, MAP_SHARED, dev_fd, 0);
    if (base == MAP_FAILED) {
      close(dev_fd);
      throw std::runtime_error("Could not map DAX device");
    }
  }

  void write_buf(void *buf, int size, int pos) {
    int *p = (int*)(base + pos);
    *p = size;
    p++;
    memcpy(p, buf, size);
    sync_write(base + pos, sizeof(int) + size);
  }

  int read_size(int pos) {
    sync_read(base + pos, sizeof(int));
    int *p = (int*)(base + pos);
    return *p;
  }

  void read_buf(void *buf, int size, int pos) {
    sync_read(base + pos + sizeof(int), size);
    int *p = (int*)(base + pos);
    p++;
    memcpy(buf, p, size);
  }

private:
  int dev_fd;
  uint8_t *base;
};

extern "C" {

JNIEXPORT jlong JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_create(JNIEnv *, jclass) {
  return reinterpret_cast<jlong>(new CxlConnector());
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_destroy (JNIEnv *, jclass, jlong obj) {
  delete reinterpret_cast<CxlConnector*>(obj);
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_get_1position(JNIEnv *, jclass, jlong obj) {
  static thread_local std::mt19937 gen(std::random_device{}());
  static thread_local std::uniform_int_distribution<int> D(0, 1024);
  return D(gen) * 128 * 1024;
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_write_1buf(JNIEnv *env, jclass, jlong obj, jobject buf, jint size, jint position) {
  auto *cxlConnector = reinterpret_cast<CxlConnector*>(obj);
  void *base = env->GetDirectBufferAddress(buf);
  cxlConnector->write_buf(base, size, position);
}

JNIEXPORT jint JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_read_1size(JNIEnv *env, jclass, jlong obj, jint position) {
  auto *cxlConnector = reinterpret_cast<CxlConnector*>(obj);
  return cxlConnector->read_size(position);
}

JNIEXPORT void JNICALL
Java_org_apache_flink_runtime_rpc_pekko_CxlConnector_read_1buf(JNIEnv *env, jclass, jlong obj, jobject buf, jint size, jint position) {
  auto *cxlConnector = reinterpret_cast<CxlConnector*>(obj);
  void *base = env->GetDirectBufferAddress(buf);
  cxlConnector->read_buf(base, size, position);
}

}
