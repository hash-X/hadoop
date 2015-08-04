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
import org.apache.hadoop.io.erasurecode.rawcoder.util.GaloisFieldUtil;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;

/**
 * A raw erasure decoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible.
 */
public class RSRawDecoder3 extends AbstractRawErasureDecoder {
  private byte[] encodeMatrix;

  private byte[] decodeMatrix;
  private int[] validIndexes;
  private int numErasedDataUnits;
  private boolean[] erasureFlags;

  public RSRawDecoder3(int numDataUnits, int numParityUnits) {
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
    prepareDecoding(inputs, erasedIndexes);

    int outputIdx = 0;

    // Decode erased data units
    for (int i = 0; i < numDataUnits; i++) {
      if (erasureFlags[i]) {
        //ErasureCodeUtil.decodeDotprod(numDataUnits, decodeMatrix, i *
        //    numDataUnits, validIndexes, inputs, outputs[outputIdx++]);
      }
    }

    // Decode erased parity units by re-encoding
    for (int i = 0; i < numParityUnits; i++) {
      if (erasureFlags[numDataUnits + i]) {
        //ErasureCodeUtil.encodeDotprod(numDataUnits, encodeMatrix, i * numDataUnits,
        //    inputs, outputs[outputIdx++]);
      }
    }
  }

  @Override
  protected void doDecode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    prepareDecoding(inputs, erasedIndexes);

    int outputIdx = 0;

    // Decode erased data units
    for (int i = 0; i < numDataUnits; i++) {
      if (erasureFlags[i]) {
        ErasureCodeUtil.decodeDotprod(numDataUnits, decodeMatrix, i *
            numDataUnits, validIndexes, inputs, outputs[outputIdx++]);
      }
    }

    // Decode erased parity units by re-encoding
    for (int i = 0; i < numParityUnits; i++) {
      if (erasureFlags[numDataUnits + i]) {
        ErasureCodeUtil.encodeDotprod(numDataUnits, encodeMatrix, i *
            numDataUnits, inputs, outputs[outputIdx++]);
      }
    }
  }

  private <T> void prepareDecoding(T[] inputs, int[] erasedIndexes) {
    decodeMatrix = new byte[numDataUnits * numDataUnits];
    validIndexes = new int[numDataUnits];
    erasureFlags = new boolean[numAllUnits];
    numErasedDataUnits = 0;
    erasureFlags = erasures2erased(erasedIndexes);
    makeValidIndexes(inputs, validIndexes);
    makeDecodingMatrix(encodeMatrix, validIndexes);
    //DumpUtil.dumpMatrix_JE(decodeMatrix, numDataUnits, numDataUnits);
  }

  public void makeDecodingMatrix(byte[] matrix, int[] validIndexes) {
    byte[] tmpMatrix = new byte[numDataUnits * numDataUnits];

    for (int i = 0; i < numDataUnits; i++) {
      if (validIndexes[i] < numDataUnits) {
        for (int j = 0; j < numDataUnits; j++) {
          tmpMatrix[i * numDataUnits + j] = 0;
        }
        tmpMatrix[i * numDataUnits + validIndexes[i]] = 1;
      } else {
        for (int j = 0; j < numDataUnits; j++) {
          tmpMatrix[i * numDataUnits + j] =
              matrix[(validIndexes[i] - numDataUnits) * numDataUnits + j];
        }
      }
    }
    DumpUtil.dumpMatrix_JE(tmpMatrix, numDataUnits, numDataUnits);

    GaloisFieldUtil.gfInvertMatrix_JE(tmpMatrix, decodeMatrix, numDataUnits);
  }

}
