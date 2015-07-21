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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.ErasureCodeUtil;
import org.junit.Test;

public class TestErasureCodeUtil {

  @Test
  public void testGenCauchyMatrix() {
    int numDataUnits = 6;
    int numParityUnits = 3;
    int numAllUnits = numDataUnits + numParityUnits;

    byte[] matrix = new byte[numDataUnits * numAllUnits];
    ErasureCodeUtil.genCauchyMatrix(matrix, numAllUnits, numDataUnits);
    DumpUtil.dumpMatrix(matrix, numDataUnits, numAllUnits);
  }

  @Test
  public void testGenCauchyMatrix_JE() {
    int numDataUnits = 6;
    int numParityUnits = 3;

    byte[] matrix = new byte[numDataUnits * numParityUnits];
    ErasureCodeUtil.genCauchyMatrix_JE(matrix, numDataUnits, numParityUnits);
    DumpUtil.dumpMatrix_JE(matrix, numDataUnits, numParityUnits);
  }
}
