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

  public static void encodeData(int numDataUnits, int numParityUnits, byte[] gftbls,
                                byte[][] inputs, byte[][] outputs) {
    ec_encode_data(16, numDataUnits, numParityUnits, gftbls, inputs, outputs);
  }

  public static void encodeData_JE(int numDataUnits, int numParityUnits, byte[] matrix,
                                byte[][] inputs, byte[][] outputs) {
    for (int i = 0; i < numParityUnits; i++) {
      encodeDotprod(numDataUnits, matrix, i * numDataUnits, inputs, outputs[i]);
    }
  }

  private static void ec_encode_data(int len, int srcs, int dests, byte[] v,
                           byte[][] src, byte[][] dest) {
    int i, j, l;
    byte s;

    for (l = 0; l < dests; l++) {
      for (i = 0; i < len; i++) {
        s = 0;
        for (j = 0; j < srcs; j++)
          s ^= GaloisFieldUtil.gfMul(src[j][i], v[j * 32 + l * srcs * 32 + 1]);

        dest[l][i] = s;
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
    boolean[] erased = erasures2erased(numDataUnits, numParityUnits, erasures);

    int[] validSourceIndexes = new int[numDataUnits];
    makeValidIndexes(inputs, validSourceIndexes);

    byte[] decodingMatrix = new byte[numDataUnits * numDataUnits];
    makeDecodingMatrix_JE(numDataUnits, matrix, decodingMatrix,
        validSourceIndexes);
    DumpUtil.dumpMatrix_JE(decodingMatrix, numDataUnits, numDataUnits);

    int outputIdx = 0;

    // Decode erased data units
    for (int i = 0; i < numDataUnits; i++) {
      if (erased[i]) {
        decodeDotprod(numDataUnits, decodingMatrix, i * numDataUnits,
            validSourceIndexes, inputs, outputs[outputIdx++]);
      }
    }

    // Decode erased parity units by re-encoding
    for (int i = 0; i < numParityUnits; i++) {
      if (erased[numDataUnits + i]) {
        encodeDotprod(numDataUnits, matrix, i * numDataUnits,
            inputs, outputs[outputIdx++]);
      }
    }
  }

  public static void decodeData(int numDataUnits, int numParityUnits,
                                   byte[] matrix, int[] erasures,
                                   byte[][] inputs, byte[][] outputs) {
    boolean[] erased = erasures2erased(numDataUnits, numParityUnits, erasures);

    int[] validSourceIndexes = new int[numDataUnits];
    makeValidIndexes(inputs, validSourceIndexes);

    int numErasedDataUnits = 0;
    for (int i = 0; i < numDataUnits; i++) {
      if (erased[i]) {
        numErasedDataUnits++;
      }
    }

    byte[] decodingMatrix = new byte[numDataUnits * numDataUnits];
    makeDecodingMatrix(numDataUnits, matrix, decodingMatrix, erasures,
        validSourceIndexes, numErasedDataUnits);
    DumpUtil.dumpMatrix_JE(decodingMatrix, numDataUnits, numParityUnits);

    int outputIdx = 0;

    // Decode erased data units
    for (int i = 0; i < numDataUnits; i++) {
      if (erased[i]) {
        decodeDotprod(numDataUnits, decodingMatrix, i * numDataUnits,
            validSourceIndexes, inputs, outputs[outputIdx++]);
      }
    }

    // Decode erased parity units by re-encoding
    for (int i = 0; i < numParityUnits; i++) {
      if (erased[numDataUnits + i]) {
        encodeDotprod(numDataUnits, matrix, i * numDataUnits,
            inputs, outputs[outputIdx++]);
      }
    }
  }

  private static void makeValidIndexes(byte[][] inputs, int[] validIndexes) {
    int idx = 0;
    for (int i = 0; i < inputs.length; i++) {
      if (inputs[i] != null) {
        validIndexes[idx++] = i;
      }
    }
  }

  private static boolean[] erasures2erased(int numDataUnits,
                             int numParityUnits, int[] erasures) {
    int td;
    boolean[] erased;
    int i;

    td = numDataUnits+numParityUnits;
    erased = new boolean[td];

    for (i = 0; i < td; i++) {
      erased[i] = false;
    }

    for (i = 0; i < erasures.length; i++) {
      erased[erasures[i]] = true;
    }

    return erased;
  }

  public static void makeDecodingMatrix_JE(int numDataUnits, byte[] matrix,
                                  byte[] decodingMatrix, int[] validIndexes) {
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

    GaloisFieldUtil.gfInvertMatrix_JE(tmpMatrix, decodingMatrix, numDataUnits);
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
