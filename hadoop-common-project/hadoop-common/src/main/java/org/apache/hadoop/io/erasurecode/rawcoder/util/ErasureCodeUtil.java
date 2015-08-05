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
package org.apache.hadoop.io.erasurecode.rawcoder.util;

import java.nio.ByteBuffer;

/**
 * An erasure code util class.
 */
public final class ErasureCodeUtil {

  private ErasureCodeUtil() { }

  public static void initTables(int k, int rows, byte[] codingMatrix,
                                int matrixOffset, byte[] gftbls) {
    int i, j;

    int offset = 0, idx = matrixOffset;
    for (i = 0; i < rows; i++) {
      for (j = 0; j < k; j++) {
        GaloisFieldUtil.gfVectMulInit(codingMatrix[idx++], gftbls, offset);
        offset += 32;
      }
    }
  }

  public static void genRSMatrix(byte[] a, int m, int k) {
    int i, j;
    byte p, gen = 1;

    for (i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    for (i = k; i < m; i++) {
      p = 1;
      for (j = 0; j < k; j++) {
        a[k * i + j] = p;
        p = GaloisFieldUtil.gfMul(p, gen);
      }
      gen = GaloisFieldUtil.gfMul(gen, (byte) 0x10);
    }
  }

  public static void genCauchyMatrix(byte[] a, int m, int k) {
    int i, j;
    byte[] p;

    // Identity matrix in high position
    for (i = 0; i < k; i++) {
      a[k * i + i] = 1;
    }

    // For the rest choose 1/(i + j) | i != j
    int pos = k * k;
    for (i = k; i < m; i++) {
      for (j = 0; j < k; j++) {
        a[pos++] = GaloisFieldUtil.gfInv((byte) (i ^ j));
      }
    }
  }

  /**
   * Generate Cauchy matrix.
   * @param matrix
   * @param numParityUnits
   * @param numDataUnits
   */
  public static void genCauchyMatrix_JE(byte[] matrix,
                                        int numDataUnits, int numParityUnits) {
    for (int i = 0; i < numParityUnits; i++) {
      for (int j = 0; j < numDataUnits; j++) {
        matrix[i * numDataUnits + j] =
                GaloisFieldUtil.gfInv((byte) (i ^ (numParityUnits + j)));
      }
    }
  }

  public static void encodeDataOld(byte[] gftbls, int dataLen, byte[][] inputs,
                                int[] inputOffsets, byte[][] outputs,
                                int[] outputOffsets) {
    int numInputs = inputs.length;
    int numOutputs = outputs.length;
    int l, i, j, oPos;
    byte s;
    for (l = 0; l < numOutputs; l++) {
      for (i = 0; i < dataLen; i++) {
        oPos = outputOffsets[l] + i;
        s = 0;
        for (j = 0; j < numInputs; j++) {
          s ^= GaloisFieldUtil.gfMul(inputs[j][inputOffsets[j] + i],
              gftbls[j * 32 + l * numInputs * 32 + 1]);
        }
        outputs[l][oPos] = s;
      }
    }
  }

  public static void encodeData(byte[] gftbls, int dataLen, byte[][] inputs,
                                int[] inputOffsets, byte[][] outputs,
                                int[] outputOffsets) {
    int numInputs = inputs.length;
    int numOutputs = outputs.length;
    int l, i, j, iPos, oPos;
    byte[] input, output;
    byte s;
    for (l = 0; l < numOutputs; l++) {
      output = outputs[l];

      for (j = 0; j < numInputs; j++) {
        input = inputs[j];
        iPos = inputOffsets[j];
        oPos = outputOffsets[l];

        s = gftbls[j * 32 + l * numInputs * 32 + 1];
        for (i = 0; i < dataLen / 4; i += 4, iPos += 4, oPos += 4) {
          output[oPos + 0] ^= GaloisFieldUtil.gfMul(input[iPos + 0], s);
          output[oPos + 1] ^= GaloisFieldUtil.gfMul(input[iPos + 1], s);
          output[oPos + 2] ^= GaloisFieldUtil.gfMul(input[iPos + 2], s);
          output[oPos + 3] ^= GaloisFieldUtil.gfMul(input[iPos + 3], s);
        }
      }
    }
  }

  public static void encodeData(byte[] gftbls, ByteBuffer[] inputs,
                                ByteBuffer[] outputs) {
    int[] inputOffsets = new int[inputs.length];
    for (int i = 0; i < inputs.length; i++) {
      inputOffsets[i] = inputs[i].position();
    }

    int[] outputOffsets = new int[outputs.length];
    for (int i = 0; i < outputs.length; i++) {
      outputOffsets[i] = outputs[i].position();
    }

    int dataLen = inputs[0].remaining();
    for (int l = 0; l < outputs.length; l++) {
      for (int i = 0; i < dataLen; i++) {
        byte s = 0;
        for (int j = 0; j < inputs.length; j++) {
          s ^= GaloisFieldUtil.gfMul(inputs[j].get(inputOffsets[j] + i),
              gftbls[j * 32 + l * inputs.length * 32 + 1]);
        }
        outputs[l].put(outputOffsets[l] + i, s);
      }
    }
  }

  public static void encodeDotprod(byte[] matrix, int matrixOffset, byte[][] inputs,
                                   int[] inputOffsets, int dataLen,
                                   byte[] output, int outputOffset) {
    //First copy or xor any data that does not need to be multiplied by a factor
    boolean init = true;
    for (int i = 0; i < inputs.length; i++) {
      if (matrix[matrixOffset + i] == 1) {
        if (init) {
          System.arraycopy(inputs[i], inputOffsets[i], output, 0, dataLen);
          init = false;
        } else {
          for (int j = 0; j < dataLen; j++) {
            output[outputOffset + j] ^= inputs[i][inputOffsets[i] + j];
          }
        }
      }
    }

    //Now do the data that needs to be multiplied by a factor
    for (int i = 0; i < inputs.length; i++) {
      if (matrix[matrixOffset + i] != 0 && matrix[matrixOffset + i] != 1) {
        regionMultiply(inputs[i], inputOffsets[i], dataLen, matrix[matrixOffset + i],
            output, outputOffset, init);
        init = false;
      }
    }
  }

  public static void encodeDotprod(byte[] matrix, int matrixOffset,
                                   ByteBuffer[] inputs, ByteBuffer output) {
    ByteBuffer input;
    //First copy or xor any data that does not need to be multiplied by a factor
    boolean init = true;
    for (int i = 0; i < inputs.length; i++) {
      if (matrix[matrixOffset + i] == 1) {
        input = inputs[i];
        if (init) {
          output.put(input);
          init = false;
        } else {
          for (int j = 0; j < input.remaining(); j++) {
            output.put(j, (byte) ((output.get(j) ^ input.get(j)) & 0xff));
          }
        }
      }
    }

    //Now do the data that needs to be multiplied by a factor
    for (int i = 0; i < inputs.length; i++) {
      if (matrix[matrixOffset + i] != 0 && matrix[matrixOffset + i] != 1) {
        input = inputs[i];
        regionMultiply(input, matrix[matrixOffset + i], output, init);
        init = false;
      }
    }
  }

  /*
  public static void decodeDotprod(int numDataUnits, byte[] matrix, int matrixOffset,
                                   int[] validIndexes, byte[][] inputs, byte[] output) {
    byte[] input;
    int size = 16; //inputs[0].length;
    int i;

    //First copy or xor any data that does not need to be multiplied by a factor
    boolean init = true;
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[matrixOffset + i] == 1) {
        input = inputs[validIndexes[i]];
        if (init) {
          System.arraycopy(input, 0, output, 0, size);
          init = false;
        } else {
          for (int j = 0; j < size; j++) {
            output[j] ^= input[j];
          }
        }
      }
    }

    //Now do the data that needs to be multiplied by a factor
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[matrixOffset + i] != 0 && matrix[matrixOffset + i] != 1) {
        input = inputs[validIndexes[i]];
        regionMultiply(input, matrix[matrixOffset + i], output, init);
        init = false;
      }
    }
  }*/

  public static void regionMultiply(byte[] input, int inputOffset, int dataLen,
                                    byte multiply, byte[] output, int outputOffset,
                                    boolean init) {
    if (init) {
      for (int i = 0; i < dataLen; i++) {
        output[outputOffset + i] = GaloisFieldUtil.gfMul(input[inputOffset + i], multiply);
      }
    } else {
      for (int i = 0; i < dataLen; i++) {
        byte tmp = GaloisFieldUtil.gfMul(input[inputOffset + i], multiply);
        output[outputOffset + i] = (byte) ((output[outputOffset + i] ^ tmp) & 0xff);
      }
    }
  }

  public static void regionMultiply(ByteBuffer input, byte multiply,
                                    ByteBuffer output, boolean init) {
    if (init) {
      for (int i = 0; i < input.limit(); i++) {
        output.put(i, GaloisFieldUtil.gfMul(input.get(i), multiply));
      }
    } else {
      for (int i = 0; i < input.limit(); i++) {
        byte tmp = GaloisFieldUtil.gfMul(input.get(i), multiply);
        output.put(i, (byte) ((output.get(i) ^ tmp) & 0xff));
      }
    }
  }
}
