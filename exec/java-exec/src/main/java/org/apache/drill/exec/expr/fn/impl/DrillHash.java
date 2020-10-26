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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.exec.expr.fn.impl;

import io.netty.util.internal.PlatformDependent;
import java.nio.ByteOrder;

/**
 * The base class of hash classes used in Drill.
 */
public class DrillHash {
  private static final boolean reverse = ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN;

  public static final long getLongLittleEndian(long offset) {
    long v = PlatformDependent.getLong(offset);
    if (reverse) {
      return Long.reverseBytes(v);
    }
    return v;
  }

  public static final long getIntLittleEndian(long offset) {
    int v = PlatformDependent.getInt(offset);
    if (reverse) {
      return Integer.reverseBytes(v);
    }
    return v;
  }

}