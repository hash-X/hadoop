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

  public static void encodeData(int numInputs, int numOutputs, byte[] gftbls,
                                int dataLen, byte[][] inputs, int[] inputOffsets,
                                 byte[][] outputs, int[] outputOffsets) {
    byte s;

    for (int l = 0; l < numOutputs; l++) {
      for (int i = 0; i < dataLen; i++) {
        s = 0;
        for (int j = 0; j < numInputs; j++) {
          s ^= GaloisFieldUtil.gfMul(inputs[j][inputOffsets[j] + i],
              gftbls[j * 32 + l * numInputs * 32 + 1]);
        }
        outputs[l][outputOffsets[l] + i] = s;
      }
    }
  }

  public static void encodeData(int numInputs, int numOutputs, byte[] gftbls,
                                 ByteBuffer[] inputs, ByteBuffer[] outputs) {
    byte s;
    int dataLen = inputs[0].remaining();
    for (int l = 0; l < numOutputs; l++) {
      for (int i = 0; i < dataLen; i++) {
        s = 0;
        for (int j = 0; j < numInputs; j++) {
          s ^= GaloisFieldUtil.gfMul(inputs[j].get(i),
              gftbls[j * 32 + l * numInputs * 32 + 1]);
        }
        outputs[l].put(i, s);
      }
    }
  }

  public static void encodeDotprod(int numDataUnits, byte[] matrix,
                                   int matrixOffset, byte[][] inputs, byte[] output) {
    byte[] input;
    int size = 16; //inputs[0].length;
    int i;

    //First copy or xor any data that does not need to be multiplied by a factor
    boolean init = true;
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[matrixOffset + i] == 1) {
        input = inputs[i];
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
        input = inputs[i];
        regionMultiply(input, matrix[matrixOffset + i], output, init);
        init = false;
      }
    }
  }

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
  }

  public static void regionMultiply(byte[] input, byte multiply,
                                    byte[] output, boolean init) {
    if (init) {
      for (int i = 0; i < input.length; i++) {
        output[i] = GaloisFieldUtil.gfMul(input[i], multiply);
      }
    } else {
      for (int i = 0; i < input.length; i++) {
        byte tmp = GaloisFieldUtil.gfMul(input[i], multiply);
        output[i] = (byte) ((output[i] ^ tmp) & 0xff);
      }
    }
  }

  public static void decodeData_JE(int numDataUnits, int numParityUnits,
                                byte[] matrix, int[] erasures,
                                byte[][] inputs, byte[][] outputs) {

  }

  // Generate decode matrix from encode matrix
  public static void makeDecodingMatrix(int numDataUnits, byte[] encodeMatrix,
                           byte[] decodingMatrix, int[] erasedIndexes,
                           int[] validIndexes, int numErasedDataUnits) {
    byte[] tmpMatrix = new byte[9 * numDataUnits];
    byte[] invertMatrix = new byte[9 * numDataUnits];

    // Construct matrix b by removing error rows
    for (int i = 0; i < numDataUnits; i++) {
      for (int j = 0; j < numDataUnits; j++) {
        tmpMatrix[numDataUnits * i + j] =
            encodeMatrix[numDataUnits * validIndexes[i] + j];
      }
    }

    GaloisFieldUtil.gfInvertMatrix(tmpMatrix, invertMatrix, numDataUnits);

    for (int i = 0; i < numErasedDataUnits; i++) {
      for (int j = 0; j < numDataUnits; j++) {
        decodingMatrix[numDataUnits * i + j] =
            invertMatrix[numDataUnits * erasedIndexes[i] + j];
      }
    }
  }
}
