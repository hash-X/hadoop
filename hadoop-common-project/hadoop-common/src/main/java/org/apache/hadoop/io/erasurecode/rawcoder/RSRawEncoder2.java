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
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
public class RSRawEncoder2 extends AbstractRawErasureEncoder {
  private byte[] encodeMatrix;
  private byte[] gftbls;

  public RSRawEncoder2(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);

    if (getNumDataUnits() + getNumParityUnits() >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
          "Invalid numDataUnits and numParityUnits");
    }

    int numAllUnits = numDataUnits + numParityUnits;
    encodeMatrix = new byte[numAllUnits * numDataUnits];
    ErasureCodeUtil.genCauchyMatrix(encodeMatrix, numAllUnits, numDataUnits);
    //DumpUtil.dumpMatrix(encodeMatrix, numDataUnits, numAllUnits);
    gftbls = new byte[numAllUnits * numDataUnits * 32];
    ErasureCodeUtil.initTables(numDataUnits, numParityUnits, encodeMatrix,
        numDataUnits * numDataUnits, gftbls);
    //System.out.println(DumpUtil.bytesToHex(gftbls, 9999999));
  }

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    ErasureCodeUtil.encodeData(gftbls, inputs, outputs);
  }

  @Override
  protected void doEncode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, byte[][] outputs, int[] outputOffsets) {
    ErasureCodeUtil.encodeData(gftbls, dataLen, inputs, inputOffsets,
        outputs, outputOffsets);
  }
}
