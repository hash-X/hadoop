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
package org.apache.hadoop.io.erasurecode.rawcoder;

import org.junit.Before;
import org.junit.Test;

/**
 * Test raw Reed-solomon coder implemented in Java.
 */
public class TestRSRawCoderNative extends TestRSRawCoderBase {

  @Before
  public void setup() {
    this.encoderClass = NativeRSRawEncoder.class;
    this.decoderClass = NativeRSRawDecoder.class;
    setAllowDump(true); // Change to true to allow verbose dump for debugging
  }

  @Test
  public void testCoding_6x3_erasing_d0() {
    prepare(null, 6, 3, new int[]{0}, new int[0], true);
    testCoding(false);
  }
}
