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
 * A GaloisField util class.
 */
public final class GaloisFieldUtil {

    private GaloisFieldUtil() { }

    private static byte[] gfBase = new byte[] {
            (byte) 0x01, (byte) 0x02, (byte) 0x04, (byte) 0x08, (byte) 0x10,
            (byte) 0x20, (byte) 0x40, (byte) 0x80, (byte) 0x1d, (byte) 0x3a,
            (byte) 0x74, (byte) 0xe8, (byte) 0xcd, (byte) 0x87, (byte) 0x13,
            (byte) 0x26, (byte) 0x4c, (byte) 0x98, (byte) 0x2d, (byte) 0x5a,
            (byte) 0xb4, (byte) 0x75, (byte) 0xea, (byte) 0xc9, (byte) 0x8f,
            (byte) 0x03, (byte) 0x06, (byte) 0x0c, (byte) 0x18, (byte) 0x30,
            (byte) 0x60, (byte) 0xc0, (byte) 0x9d, (byte) 0x27, (byte) 0x4e,
            (byte) 0x9c, (byte) 0x25, (byte) 0x4a, (byte) 0x94, (byte) 0x35,
            (byte) 0x6a, (byte) 0xd4, (byte) 0xb5, (byte) 0x77, (byte) 0xee,
            (byte) 0xc1, (byte) 0x9f, (byte) 0x23, (byte) 0x46, (byte) 0x8c,
            (byte) 0x05, (byte) 0x0a, (byte) 0x14, (byte) 0x28, (byte) 0x50,
            (byte) 0xa0, (byte) 0x5d, (byte) 0xba, (byte) 0x69, (byte) 0xd2,
            (byte) 0xb9, (byte) 0x6f, (byte) 0xde, (byte) 0xa1, (byte) 0x5f,
            (byte) 0xbe, (byte) 0x61, (byte) 0xc2, (byte) 0x99, (byte) 0x2f,
            (byte) 0x5e, (byte) 0xbc, (byte) 0x65, (byte) 0xca, (byte) 0x89,
            (byte) 0x0f, (byte) 0x1e, (byte) 0x3c, (byte) 0x78, (byte) 0xf0,
            (byte) 0xfd, (byte) 0xe7, (byte) 0xd3, (byte) 0xbb, (byte) 0x6b,
            (byte) 0xd6, (byte) 0xb1, (byte) 0x7f, (byte) 0xfe, (byte) 0xe1,
            (byte) 0xdf, (byte) 0xa3, (byte) 0x5b, (byte) 0xb6, (byte) 0x71,
            (byte) 0xe2, (byte) 0xd9, (byte) 0xaf, (byte) 0x43, (byte) 0x86,
            (byte) 0x11, (byte) 0x22, (byte) 0x44, (byte) 0x88, (byte) 0x0d,
            (byte) 0x1a, (byte) 0x34, (byte) 0x68, (byte) 0xd0, (byte) 0xbd,
            (byte) 0x67, (byte) 0xce, (byte) 0x81, (byte) 0x1f, (byte) 0x3e,
            (byte) 0x7c, (byte) 0xf8, (byte) 0xed, (byte) 0xc7, (byte) 0x93,
            (byte) 0x3b, (byte) 0x76, (byte) 0xec, (byte) 0xc5, (byte) 0x97,
            (byte) 0x33, (byte) 0x66, (byte) 0xcc, (byte) 0x85, (byte) 0x17,
            (byte) 0x2e, (byte) 0x5c, (byte) 0xb8, (byte) 0x6d, (byte) 0xda,
            (byte) 0xa9, (byte) 0x4f, (byte) 0x9e, (byte) 0x21, (byte) 0x42,
            (byte) 0x84, (byte) 0x15, (byte) 0x2a, (byte) 0x54, (byte) 0xa8,
            (byte) 0x4d, (byte) 0x9a, (byte) 0x29, (byte) 0x52, (byte) 0xa4,
            (byte) 0x55, (byte) 0xaa, (byte) 0x49, (byte) 0x92, (byte) 0x39,
            (byte) 0x72, (byte) 0xe4, (byte) 0xd5, (byte) 0xb7, (byte) 0x73,
            (byte) 0xe6, (byte) 0xd1, (byte) 0xbf, (byte) 0x63, (byte) 0xc6,
            (byte) 0x91, (byte) 0x3f, (byte) 0x7e, (byte) 0xfc, (byte) 0xe5,
            (byte) 0xd7, (byte) 0xb3, (byte) 0x7b, (byte) 0xf6, (byte) 0xf1,
            (byte) 0xff, (byte) 0xe3, (byte) 0xdb, (byte) 0xab, (byte) 0x4b,
            (byte) 0x96, (byte) 0x31, (byte) 0x62, (byte) 0xc4, (byte) 0x95,
            (byte) 0x37, (byte) 0x6e, (byte) 0xdc, (byte) 0xa5, (byte) 0x57,
            (byte) 0xae, (byte) 0x41, (byte) 0x82, (byte) 0x19, (byte) 0x32,
            (byte) 0x64, (byte) 0xc8, (byte) 0x8d, (byte) 0x07, (byte) 0x0e,
            (byte) 0x1c, (byte) 0x38, (byte) 0x70, (byte) 0xe0, (byte) 0xdd,
            (byte) 0xa7, (byte) 0x53, (byte) 0xa6, (byte) 0x51, (byte) 0xa2,
            (byte) 0x59, (byte) 0xb2, (byte) 0x79, (byte) 0xf2, (byte) 0xf9,
            (byte) 0xef, (byte) 0xc3, (byte) 0x9b, (byte) 0x2b, (byte) 0x56,
            (byte) 0xac, (byte) 0x45, (byte) 0x8a, (byte) 0x09, (byte) 0x12,
            (byte) 0x24, (byte) 0x48, (byte) 0x90, (byte) 0x3d, (byte) 0x7a,
            (byte) 0xf4, (byte) 0xf5, (byte) 0xf7, (byte) 0xf3, (byte) 0xfb,
            (byte) 0xeb, (byte) 0xcb, (byte) 0x8b, (byte) 0x0b, (byte) 0x16,
            (byte) 0x2c, (byte) 0x58, (byte) 0xb0, (byte) 0x7d, (byte) 0xfa,
            (byte) 0xe9, (byte) 0xcf, (byte) 0x83, (byte) 0x1b, (byte) 0x36,
            (byte) 0x6c, (byte) 0xd8, (byte) 0xad, (byte) 0x47, (byte) 0x8e,
            (byte) 0x01
    };

    private static byte[] gfLogBase = new byte[] {
            (byte) 0x00, (byte) 0xff, (byte) 0x01, (byte) 0x19, (byte) 0x02,
            (byte) 0x32, (byte) 0x1a, (byte) 0xc6, (byte) 0x03, (byte) 0xdf,
            (byte) 0x33, (byte) 0xee, (byte) 0x1b, (byte) 0x68, (byte) 0xc7,
            (byte) 0x4b, (byte) 0x04, (byte) 0x64, (byte) 0xe0, (byte) 0x0e,
            (byte) 0x34, (byte) 0x8d, (byte) 0xef, (byte) 0x81, (byte) 0x1c,
            (byte) 0xc1, (byte) 0x69, (byte) 0xf8, (byte) 0xc8, (byte) 0x08,
            (byte) 0x4c, (byte) 0x71, (byte) 0x05, (byte) 0x8a, (byte) 0x65,
            (byte) 0x2f, (byte) 0xe1, (byte) 0x24, (byte) 0x0f, (byte) 0x21,
            (byte) 0x35, (byte) 0x93, (byte) 0x8e, (byte) 0xda, (byte) 0xf0,
            (byte) 0x12, (byte) 0x82, (byte) 0x45, (byte) 0x1d, (byte) 0xb5,
            (byte) 0xc2, (byte) 0x7d, (byte) 0x6a, (byte) 0x27, (byte) 0xf9,
            (byte) 0xb9, (byte) 0xc9, (byte) 0x9a, (byte) 0x09, (byte) 0x78,
            (byte) 0x4d, (byte) 0xe4, (byte) 0x72, (byte) 0xa6, (byte) 0x06,
            (byte) 0xbf, (byte) 0x8b, (byte) 0x62, (byte) 0x66, (byte) 0xdd,
            (byte) 0x30, (byte) 0xfd, (byte) 0xe2, (byte) 0x98, (byte) 0x25,
            (byte) 0xb3, (byte) 0x10, (byte) 0x91, (byte) 0x22, (byte) 0x88,
            (byte) 0x36, (byte) 0xd0, (byte) 0x94, (byte) 0xce, (byte) 0x8f,
            (byte) 0x96, (byte) 0xdb, (byte) 0xbd, (byte) 0xf1, (byte) 0xd2,
            (byte) 0x13, (byte) 0x5c, (byte) 0x83, (byte) 0x38, (byte) 0x46,
            (byte) 0x40, (byte) 0x1e, (byte) 0x42, (byte) 0xb6, (byte) 0xa3,
            (byte) 0xc3, (byte) 0x48, (byte) 0x7e, (byte) 0x6e, (byte) 0x6b,
            (byte) 0x3a, (byte) 0x28, (byte) 0x54, (byte) 0xfa, (byte) 0x85,
            (byte) 0xba, (byte) 0x3d, (byte) 0xca, (byte) 0x5e, (byte) 0x9b,
            (byte) 0x9f, (byte) 0x0a, (byte) 0x15, (byte) 0x79, (byte) 0x2b,
            (byte) 0x4e, (byte) 0xd4, (byte) 0xe5, (byte) 0xac, (byte) 0x73,
            (byte) 0xf3, (byte) 0xa7, (byte) 0x57, (byte) 0x07, (byte) 0x70,
            (byte) 0xc0, (byte) 0xf7, (byte) 0x8c, (byte) 0x80, (byte) 0x63,
            (byte) 0x0d, (byte) 0x67, (byte) 0x4a, (byte) 0xde, (byte) 0xed,
            (byte) 0x31, (byte) 0xc5, (byte) 0xfe, (byte) 0x18, (byte) 0xe3,
            (byte) 0xa5, (byte) 0x99, (byte) 0x77, (byte) 0x26, (byte) 0xb8,
            (byte) 0xb4, (byte) 0x7c, (byte) 0x11, (byte) 0x44, (byte) 0x92,
            (byte) 0xd9, (byte) 0x23, (byte) 0x20, (byte) 0x89, (byte) 0x2e,
            (byte) 0x37, (byte) 0x3f, (byte) 0xd1, (byte) 0x5b, (byte) 0x95,
            (byte) 0xbc, (byte) 0xcf, (byte) 0xcd, (byte) 0x90, (byte) 0x87,
            (byte) 0x97, (byte) 0xb2, (byte) 0xdc, (byte) 0xfc, (byte) 0xbe,
            (byte) 0x61, (byte) 0xf2, (byte) 0x56, (byte) 0xd3, (byte) 0xab,
            (byte) 0x14, (byte) 0x2a, (byte) 0x5d, (byte) 0x9e, (byte) 0x84,
            (byte) 0x3c, (byte) 0x39, (byte) 0x53, (byte) 0x47, (byte) 0x6d,
            (byte) 0x41, (byte) 0xa2, (byte) 0x1f, (byte) 0x2d, (byte) 0x43,
            (byte) 0xd8, (byte) 0xb7, (byte) 0x7b, (byte) 0xa4, (byte) 0x76,
            (byte) 0xc4, (byte) 0x17, (byte) 0x49, (byte) 0xec, (byte) 0x7f,
            (byte) 0x0c, (byte) 0x6f, (byte) 0xf6, (byte) 0x6c, (byte) 0xa1,
            (byte) 0x3b, (byte) 0x52, (byte) 0x29, (byte) 0x9d, (byte) 0x55,
            (byte) 0xaa, (byte) 0xfb, (byte) 0x60, (byte) 0x86, (byte) 0xb1,
            (byte) 0xbb, (byte) 0xcc, (byte) 0x3e, (byte) 0x5a, (byte) 0xcb,
            (byte) 0x59, (byte) 0x5f, (byte) 0xb0, (byte) 0x9c, (byte) 0xa9,
            (byte) 0xa0, (byte) 0x51, (byte) 0x0b, (byte) 0xf5, (byte) 0x16,
            (byte) 0xeb, (byte) 0x7a, (byte) 0x75, (byte) 0x2c, (byte) 0xd7,
            (byte) 0x4f, (byte) 0xae, (byte) 0xd5, (byte) 0xe9, (byte) 0xe6,
            (byte) 0xe7, (byte) 0xad, (byte) 0xe8, (byte) 0x74, (byte) 0xd6,
            (byte) 0xf4, (byte) 0xea, (byte) 0xa8, (byte) 0x50, (byte) 0x58,
            (byte) 0xaf
    };

    public static byte gfMul(byte a, byte b) {
        int i;

        if ((a == 0) || (b == 0)) {
            return 0;
        }
        byte tmp = (byte) ((i = gfLogBase[a & 0xff] + gfLogBase[b & 0xff]) > 254 ? i - 255 : i);
        return gfBase[tmp & 0xff];
    }

    public static byte gfInv(byte a) {
        if (a == 0)
            return 0;

        return gfBase[(255 - gfLogBase[a & 0xff]) & 0xff];
    }

    public static void gfInvertMatrix(byte[] inMatrix, byte[] outMatrix, int n) {
        int i, j, k;
        byte temp;

        // Set outMatrix[] to the identity matrix
        for (i = 0; i < n * n; i++) {
            // memset(outMatrix, 0, n*n)
            outMatrix[i] = 0;
        }

        for (i = 0; i < n; i++) {
            outMatrix[i * n + i] = 1;
        }

        // Inverse
        for (i = 0; i < n; i++) {
            // Check for 0 in pivot element
            if (inMatrix[i * n + i] == 0) {
                // Find a row with non-zero in current column and swap
                for (j = i + 1; j < n; j++) {
                    if (inMatrix[j * n + i] != 0) {
                        break;
                    }
                }

                if (j == n) {
                    // Couldn't find means it's singular
                    throw new RuntimeException("Not invertble");
                }

                for (k = 0; k < n; k++) {
                    // Swap rows i,j
                    temp = inMatrix[i * n + k];
                    inMatrix[i * n + k] = inMatrix[j * n + k];
                    inMatrix[j * n + k] = temp;

                    temp = outMatrix[i * n + k];
                    outMatrix[i * n + k] = outMatrix[j * n + k];
                    outMatrix[j * n + k] = temp;
                }
            }

            temp = gfInv(inMatrix[i * n + i]); // 1/pivot
            for (j = 0; j < n; j++) {
                // Scale row i by 1/pivot
                inMatrix[i * n + j] = gfMul(inMatrix[i * n + j], temp);
                outMatrix[i * n + j] = gfMul(outMatrix[i * n + j], temp);
            }

            for (j = 0; j < n; j++) {
                if (j == i) {
                    continue;
                }

                temp = inMatrix[j * n + i];
                for (k = 0; k < n; k++) {
                    outMatrix[j * n + k] ^= gfMul(temp, outMatrix[i * n + k]);
                    inMatrix[j * n + k] ^= gfMul(temp, inMatrix[i * n + k]);
                }
            }
        }
    }

  public static void gfInvertMatrix_JE(byte[] inMatrix, byte[] outMatrix, int rows) {
    int cols, i, j, k, x, rs2;
    int row_start;
    byte tmp, inverse;

    cols = rows;

    k = 0;
    for (i = 0; i < rows; i++) {
      for (j = 0; j < cols; j++) {
        outMatrix[k] = (byte) ((i == j) ? 1 : 0);
        k++;
      }
    }

  /* First -- convert into upper triangular  */
    for (i = 0; i < cols; i++) {
      row_start = cols*i;

    /* Swap rows if we ave a zero i,i element.  If we can't swap, then the
       matrix was not invertible  */

      if (inMatrix[row_start+i] == 0) {
        for (j = i+1; j < rows && inMatrix[cols*j+i] == 0; j++) ;
        if (j == rows) {
          throw new RuntimeException("Not invertible");
        }
        rs2 = j*cols;
        for (k = 0; k < cols; k++) {
          tmp = inMatrix[row_start+k];
          inMatrix[row_start+k] = inMatrix[rs2+k];
          inMatrix[rs2+k] = tmp;
          tmp = outMatrix[row_start+k];
          outMatrix[row_start+k] = outMatrix[rs2+k];
          outMatrix[rs2+k] = tmp;
        }
      }

    /* Multiply the row by 1/element i,i  */
      tmp = inMatrix[row_start+i];
      if (tmp != 1) {
        inverse = gfInv(tmp);
        for (j = 0; j < cols; j++) {
          inMatrix[row_start+j] = gfMul(inMatrix[row_start + j], inverse);
          outMatrix[row_start+j] = gfMul(outMatrix[row_start+j], inverse);
        }
      }

    /* Now for each j>i, add A_ji*Ai to Aj  */
      k = row_start+i;
      for (j = i+1; j != cols; j++) {
        k += cols;
        if (inMatrix[k] != 0) {
          if (inMatrix[k] == 1) {
            rs2 = cols*j;
            for (x = 0; x < cols; x++) {
              inMatrix[rs2+x] ^= inMatrix[row_start+x];
              outMatrix[rs2+x] ^= outMatrix[row_start+x];
            }
          } else {
            tmp = inMatrix[k];
            rs2 = cols*j;
            for (x = 0; x < cols; x++) {
              inMatrix[rs2+x] ^= gfMul(tmp, inMatrix[row_start+x]);
              outMatrix[rs2+x] ^= gfMul(tmp, outMatrix[row_start+x]);
            }
          }
        }
      }
    }
    DumpUtil.dumpMatrix_JE(outMatrix, rows, rows);

  /* Now the matrix is upper triangular.  Start at the top and multiply down  */

    for (i = rows-1; i >= 0; i--) {
      row_start = i*cols;
      for (j = 0; j < i; j++) {
        rs2 = j*cols;
        if (inMatrix[rs2+i] != 0) {
          tmp = inMatrix[rs2+i];
          inMatrix[rs2+i] = 0;
          for (k = 0; k < cols; k++) {
            outMatrix[rs2+k] ^= gfMul(tmp, outMatrix[row_start+k]);
          }
        }
      }
    }
  }

    /*
  public static void ec_init_tables(int k, int rows, byte[] a, byte[] gf_tbls) {
    int i, j;

    for (i = 0; i < rows; i++) {
      for (j = 0; j < k; j++) {
        gfVectMulInit(*a++, gf_tbls);
        g_tbls += 32;
      }
    }
  }

  // Calculates const table gftbl in GF(2^8) from single input A
  // gftbl(A) = {A{00}, A{01}, A{02}, ... , A{0f} }, {A{00}, A{10}, A{20}, ... , A{f0} }

  public static void gfVectMulInit(byte c, byte[] tbl) {
    byte c2 = (c << 1) ^ ((c & 0x80) ? 0x1d : 0); //Mult by GF{2}
    byte c4 = (c2 << 1) ^ ((c2 & 0x80) ? 0x1d : 0); //Mult by GF{2}
    byte c8 = (c4 << 1) ^ ((c4 & 0x80) ? 0x1d : 0); //Mult by GF{2}

    long v1, v2, v4, v8;
    long v10, v20, v40, v80;
    byte c17, c18, c20, c24;
    long[] t = new long[] {0}; // Converted from tbl

    v1 = c * 0x0100010001000100L;
    v2 = c2 * 0x0101000001010000L;
    v4 = c4 * 0x0101010100000000L;
    v8 = c8 * 0x0101010101010101L;

    v4 = v1 ^ v2 ^ v4;
    t[0] = v4;
    t[1] = v8 ^ v4;

    c17 = (c8 << 1) ^ ((c8 & 0x80) ? 0x1d : 0); //Mult by GF{2}
    c18 = (c17 << 1) ^ ((c17 & 0x80) ? 0x1d : 0); //Mult by GF{2}
    c20 = (c18 << 1) ^ ((c18 & 0x80) ? 0x1d : 0); //Mult by GF{2}
    c24 = (c20 << 1) ^ ((c20 & 0x80) ? 0x1d : 0); //Mult by GF{2}

    v10 = c17 * 0x0100010001000100L;
    v20 = c18 * 0x0101000001010000L;
    v40 = c20 * 0x0101010100000000L;
    v80 = c24 * 0x0101010101010101L;

    v40 = v10 ^ v20 ^ v40;
    t[2] = v40;
    t[3] = v80 ^ v40;

  }
    */
}
