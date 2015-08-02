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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.io.erasurecode.rawcoder.util.DumpUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.ErasureCodeUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;

/**
 * A raw erasure decoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
public class RSRawDecoder2 extends AbstractRawErasureDecoder {
  private byte[] encodeMatrix;

  public RSRawDecoder2(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
    if (numDataUnits + numParityUnits >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
              "Invalid numDataUnits and numParityUnits");
    }
    encodeMatrix = new byte[numDataUnits * numParityUnits];
    ErasureCodeUtil.genCauchyMatrix_JE(encodeMatrix, numDataUnits, numParityUnits);
    DumpUtil.dumpMatrix_JE(encodeMatrix, numDataUnits, numParityUnits);
  }

  @Override
  protected void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    //TODO
  }

  @Override
  protected void doDecode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    ErasureCodeUtil.decodeData_JE(getNumDataUnits(), getNumParityUnits(),
        encodeMatrix, erasedIndexes, inputs, outputs);
  }
}
