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

  public static void initTables(int k, int rows, byte[] a, byte[] gftbls) {
    int i, j;

    for (i = 0; i < rows; i++) {
      for (j = 0; j < k; j++) {
        //GaloisFieldUtil.gfVectMulInit(*a++, gftbls);
        //gftbls += 32;
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

  public static void encodeData(int numDataUnits, int numParityUnits, byte[] matrix,
                                byte[][] inputs, byte[][] outputs) {
    for (int i = 0; i < numParityUnits; i++) {
      encodeDotprod(numDataUnits, matrix, i * numDataUnits, inputs, outputs[i]);
    }
  }

  public static void dotprod(int numDataUnits, byte[] matrix, int rowIdx, int[] srcIds,
                             int destId, byte[][] inputs, byte[][] outputs) {
    byte[] output, input;
    int size = 513; //inputs[0].length;
    int i;

    output = (destId < numDataUnits) ? inputs[destId] : outputs[destId - numDataUnits];

    //First copy or xor any data that does not need to be multiplied by a factor
    int init = 0;
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[rowIdx + i] == 1) {
        if (srcIds == null) {
          input = inputs[i];
        } else if (srcIds[i] < numDataUnits) {
          input = inputs[srcIds[i]];
        } else {
          input = outputs[srcIds[i]-numDataUnits];
        }
        if (init == 0) {
          System.arraycopy(input, 0, output, 0, size);
          init = 1;
        } else {
          for (int j = 0; j < size; j++) {
            output[j] ^= input[j];
          }
        }
      }
    }

    //Now do the data that needs to be multiplied by a factor
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[rowIdx + i] != 0 && matrix[rowIdx + i] != 1) {
        if (srcIds == null) {
          input = inputs[i];
        } else if (srcIds[i] < numDataUnits) {
          input = inputs[srcIds[i]];
        } else {
          input = outputs[srcIds[i]-numDataUnits];
        }
        regionMultiply(input, matrix[rowIdx + i], output, false);
        init = 1;
      }
    }
  }

  public static void encodeDotprod(int numDataUnits, byte[] matrix,
                                   int matrixOffset, byte[][] inputs, byte[] output) {
    byte[] input;
    int size = 513; //inputs[0].length;
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

  public static void decodeDotprod(int numDataUnits, byte[] matrix, int rowIdx, int[] srcIds,
                             byte[][] inputs, byte[] output) {
    byte[] input;
    int size = 513; //inputs[0].length;
    int i;

    //First copy or xor any data that does not need to be multiplied by a factor
    int init = 0;
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[rowIdx + i] == 1) {
        input = inputs[srcIds[i]];
        if (init == 0) {
          System.arraycopy(input, 0, output, 0, size);
          init = 1;
        } else {
          for (int j = 0; j < size; j++) {
            output[j] ^= input[j];
          }
        }
      }
    }

    //Now do the data that needs to be multiplied by a factor
    for (i = 0; i < numDataUnits; i++) {
      if (matrix[rowIdx + i] != 0 && matrix[rowIdx + i] != 1) {
        input = inputs[srcIds[i]];
        regionMultiply(input, matrix[rowIdx + i], output, false);
      }
    }
  }

  public static void regionMultiply(byte[] input, int multiply,
                                    byte[] output, boolean init) {
    if (init) {
      for (int i = 0; i < input.length; i++) {
        output[i] = GaloisFieldUtil.gfMul(input[i], (byte) multiply);
      }
    } else {
      for (int i = 0; i < input.length; i++) {
        output[i] = (byte) (((output[i] ^ GaloisFieldUtil.gfMul(input[i],
            (byte) multiply))) & 0xff);
      }
    }
  }

  public static void decodeData(int numDataunits, int numParityUnits,
                                byte[] matrix, int row_k_ones, int[] erasures,
                                byte[][] inputs, byte[][] outputs) {
    int i, numErasedDataUnits;
    boolean[] erased;

    erased = erasures2erased(numDataunits, numParityUnits, erasures);

    int[] validSourceIndexes = new int[numDataunits];
    makeValidIndexes(inputs, validSourceIndexes);

    //Find the number of data units failed

    numErasedDataUnits = 0;
    for (i = 0; i < numDataunits; i++) {
      if (erased[i]) {
        numErasedDataUnits++;
      }
    }

    byte[] decodingMatrix = new byte[numDataunits * numDataunits];
    makeDecodingMatrix(numDataunits, numParityUnits, matrix, erased, decodingMatrix, validSourceIndexes);

    int idx = 0; // For output

    // Decode erased data units
    for (i = 0; numErasedDataUnits > 0; i++) {
      if (erased[i]) {
        decodeDotprod(numDataunits, decodingMatrix, i * numDataunits, validSourceIndexes, inputs, outputs[idx++]);
        numErasedDataUnits--;
      }
    }

    // Finally, re-encode any erased coding devices
    for (i = 0; i < numParityUnits; i++) {
      if (erased[numDataunits+i]) {
        encodeDotprod(numDataunits, matrix, i * numDataunits, inputs, outputs[idx++]);
      }
    }
  }

  public static void decodeDataOrg(int numDataunits, int numParityUnits,
                                   byte[] matrix, int row_k_ones, int[] erasures,
                                   byte[][] inputs, byte[][] outputs) {
    int i, numErasedDataUnits, lastdrive;
    int[] tmpIds;
    boolean[] erased;
    int[] dmIds;

    erased = erasures2erased(numDataunits, numParityUnits, erasures);

    //Find the number of data units failed

    lastdrive = numDataunits;

    numErasedDataUnits = 0;
    for (i = 0; i < numDataunits; i++) {
      if (erased[i]) {
        numErasedDataUnits++;
        lastdrive = i;
      }
    }

  /* You only need to create the decoding matrix in the following cases:

      1. numErasedDataUnits > 0 and row_k_ones is false.
      2. numErasedDataUnits > 0 and row_k_ones is true and coding device 0 has been erased.
      3. numErasedDataUnits > 1

      We're going to use lastdrive to denote when to stop decoding data.
      At this point in the code, it is equal to the last erased data device.
      However, if we can't use the parity row to decode it (i.e. row_k_ones=0
         or erased[numDataunits] = 1, we're going to set it to numDataunits so that the decoding
         pass will decode all data.
   */

    if (row_k_ones == 0 || erased[numDataunits]) {
      lastdrive = numDataunits;
    }

    dmIds = null;
    byte[] decodingMatrix = null;

    if (numErasedDataUnits > 1 || (numErasedDataUnits > 0 && (row_k_ones ==0 || erased[numDataunits]))) {
      dmIds = new int[numDataunits];
      decodingMatrix = new byte[numDataunits * numDataunits];
      makeDecodingMatrix(numDataunits, numParityUnits, matrix, erased, decodingMatrix, dmIds);
    }

    /*
    Decode the data units.
     If row_k_ones is true and coding device 0 is intact, then only decode numErasedDataUnits-1 units.
     This is done by stopping at lastdrive.
     We test whether numErasedDataUnits > 0 so that we can exit the loop early if we're done.
   */

    for (i = 0; numErasedDataUnits > 0 && i < lastdrive; i++) {
      if (erased[i]) {
        dotprod(numDataunits, decodingMatrix, i * numDataunits, dmIds, i, inputs, outputs);
        numErasedDataUnits--;
      }
    }

    // Then if necessary, decode drive lastdrive

    if (numErasedDataUnits > 0) {
      tmpIds = new int[numDataunits];
      for (i = 0; i < numDataunits; i++) {
        tmpIds[i] = (i < lastdrive) ? i : i+1;
      }
      dotprod(numDataunits, matrix, 0, tmpIds, lastdrive, inputs, outputs);
    }

    // Finally, re-encode any erased coding devices

    for (i = 0; i < numParityUnits; i++) {
      if (erased[numDataunits+i]) {
        dotprod(numDataunits, matrix, i * numDataunits, null, i + numDataunits, inputs, outputs);
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

  private static boolean[] erasures2erased(int numDataUnits, int numParityUnits, int[] erasures) {
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

  public static void makeDecodingMatrix(int numDataUnits, int numParityUnits, byte[] matrix,
                                        boolean[] erased, byte[] decodingMatrix,
                                        int[] dmIds) {
    byte[] tmpMatrix = new byte[numDataUnits * numDataUnits];

    for (int i = 0; i < numDataUnits; i++) {
      if (dmIds[i] < numDataUnits) {
        for (int j = 0; j < numDataUnits; j++) {
          tmpMatrix[i * numDataUnits + j] = 0;
        }
        tmpMatrix[i * numDataUnits + dmIds[i]] = 1;
      } else {
        for (int j = 0; j < numDataUnits; j++) {
          tmpMatrix[i * numDataUnits + j] =
                  matrix[(dmIds[i] - numDataUnits) * numDataUnits + j];
        }
      }
    }

    GaloisFieldUtil.gfInvertMatrix_JE(tmpMatrix, decodingMatrix, numDataUnits);
  }
}
