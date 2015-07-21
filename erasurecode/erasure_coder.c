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

#include "erasure_code.h"
#include "gf_util.h"
#include "erasure_coder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

void initCoder(CoderState* pCoderState, int numDataUnits, int numParityUnits) {
  pCoderState->verbose = 0;
  pCoderState->numParityUnits = numParityUnits;
  pCoderState->numDataUnits = numDataUnits;
  pCoderState->numAllUnits = numDataUnits + numParityUnits;
}

// 0 not to verbose, 1 to verbose
void allowVerbose(CoderState* pCoderState, int flag) {
  pCoderState->verbose = flag;
}

static void initEncodeMatrix(int numDataUnits, int numParityUnits,
                                                unsigned char* encodeMatrix) {
  // Generate encode matrix, always invertible
  h_gf_gen_cauchy_matrix(encodeMatrix,
                          numDataUnits + numParityUnits, numDataUnits);
}

void initEncoder(EncoderState* pCoderState, int numDataUnits,
                            int numParityUnits) {
  initCoder((CoderState*)pCoderState, numDataUnits, numParityUnits);

  initEncodeMatrix(numDataUnits, numParityUnits, pCoderState->encodeMatrix);

  // Generate gftbls from encode matrix
  h_ec_init_tables(numDataUnits, numParityUnits,
               &pCoderState->encodeMatrix[numDataUnits * numDataUnits],
               pCoderState->gftbls);
}

void initDecoder(DecoderState* pCoderState, int numDataUnits,
                                                       int numParityUnits) {
  initCoder((CoderState*)pCoderState, numDataUnits, numParityUnits);

  initEncodeMatrix(numDataUnits, numParityUnits, pCoderState->encodeMatrix);
}

int encode(EncoderState* pCoderState, unsigned char** dataUnits,
    unsigned char** parityUnits, int chunkSize) {
  int numDataUnits = ((CoderState*)pCoderState)->numDataUnits;
  int numParityUnits = ((CoderState*)pCoderState)->numParityUnits;

  if (((CoderState*)pCoderState)->verbose > 0) {
    dumpEncoder(pCoderState);
  }

  h_ec_encode_data(chunkSize, numDataUnits, numParityUnits,
                         pCoderState->gftbls, dataUnits, parityUnits);

  return 0;
}

static void processErasures(DecoderState* pCoderState,
                                    int* erasedIndexes, int numErased) {
  int i, index;
  int numDataUnits = ((CoderState*)pCoderState)->numDataUnits;

  for (i = 0; i < numErased; i++) {
    index = erasedIndexes[i];
    pCoderState->erasedIndexes[i] = index;
    pCoderState->erasureFlags[index] = 1;
    if (index < numDataUnits) {
      pCoderState->numErasedDataUnits++;
    }
  }

  pCoderState->numErased = numErased;
}

int decode(DecoderState* pCoderState, unsigned char** inputs,
                  int* erasedIndexes, int numErased,
                   unsigned char** outputs, int chunkSize) {
  int numDataUnits, i, r, ret;

  clearDecoder(pCoderState);
  numDataUnits = ((CoderState*)pCoderState)->numDataUnits;

  for (i = 0, r = 0; i < numDataUnits; i++, r++) {
    while (inputs[r] == NULL) {
      r++;
    }
    pCoderState->decodeIndex[i] = r;
  }

  processErasures(pCoderState, erasedIndexes, numErased);

  // Generate decode matrix
  ret = generateDecodeMatrix(pCoderState);
  if (ret != 0) {
    printf("Fail to gf_gen_decode_matrix\n");
    return -1;
  }

  // Pack recovery array as list of valid sources
  // Its order must be the same as the order
  // to generate matrix b in gf_gen_decode_matrix
  for (i = 0; i < numDataUnits; i++) {
    pCoderState->realInputs[i] = inputs[pCoderState->decodeIndex[i]];
  }

  // Recover data
  h_ec_init_tables(numDataUnits, pCoderState->numErased,
                      pCoderState->decodeMatrix, pCoderState->gftbls);

  if (((CoderState*)pCoderState)->verbose > 0) {
    dumpDecoder(pCoderState);
  }

  h_ec_encode_data(chunkSize, numDataUnits, pCoderState->numErased,
      pCoderState->gftbls, pCoderState->realInputs, outputs);

  return 0;
}

// Clear variables used per decode call
void clearDecoder(DecoderState* decoder) {
  memset(decoder->gftbls, 0, sizeof(decoder->gftbls));
  memset(decoder->decodeIndex, 0, sizeof(decoder->decodeIndex));
  memset(decoder->b, 0, sizeof(decoder->b));
  memset(decoder->invertMatrix, 0, sizeof(decoder->invertMatrix));
  memset(decoder->decodeMatrix, 0, sizeof(decoder->decodeMatrix));
  memset(decoder->erasureFlags, 0, sizeof(decoder->erasureFlags));
  memset(decoder->erasedIndexes, 0, sizeof(decoder->erasedIndexes));
  memset(decoder->realInputs, 0, sizeof(decoder->realInputs));
  decoder->numErased = 0;
  decoder->numErasedDataUnits = 0;
}

// Generate decode matrix from encode matrix
int generateDecodeMatrix(DecoderState* pCoderState) {
  int i, j, r, p;
  unsigned char s;
  int numDataUnits;

   numDataUnits = ((CoderState*)pCoderState)->numDataUnits;

  // Construct matrix b by removing error rows
  for (i = 0; i < numDataUnits; i++) {
    r = pCoderState->decodeIndex[i];
    for (j = 0; j < numDataUnits; j++) {
      pCoderState->b[numDataUnits * i + j] =
                pCoderState->encodeMatrix[numDataUnits * r + j];
    }
  }

  h_gf_invert_matrix(pCoderState->b, pCoderState->invertMatrix, numDataUnits);

  for (i = 0; i < pCoderState->numErasedDataUnits; i++) {
    for (j = 0; j < numDataUnits; j++) {
      pCoderState->decodeMatrix[numDataUnits * i + j] =
                      pCoderState->invertMatrix[numDataUnits *
                      pCoderState->erasedIndexes[i] + j];
    }
  }

  for (p = pCoderState->numErasedDataUnits; p < pCoderState->numErased; p++) {
    for (i = 0; i < numDataUnits; i++) {
      s = 0;
      for (j = 0; j < numDataUnits; j++)
        s ^= h_gf_mul(pCoderState->invertMatrix[j * numDataUnits + i],
          pCoderState->encodeMatrix[numDataUnits *
                                        pCoderState->erasedIndexes[p] + j]);

      pCoderState->decodeMatrix[numDataUnits * p + i] = s;
    }
  }

  return 0;
}

void dumpEncoder(EncoderState* pCoderState) {
  int numDataUnits = ((CoderState*)pCoderState)->numDataUnits;
  int numParityUnits = ((CoderState*)pCoderState)->numDataUnits;
  int numAllUnits = ((CoderState*)pCoderState)->numAllUnits;

  printf("Encoding (numAlnumParityUnitslUnits = %d, numDataUnits = %d)\n",
                                    numParityUnits, numDataUnits);

  printf("\n\nEncodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoderState->encodeMatrix,
                                           numDataUnits, numAllUnits);
}

void dumpDecoder(DecoderState* pCoderState) {
  int i, j;
  int numDataUnits = ((CoderState*)pCoderState)->numDataUnits;
  int numParityUnits = ((CoderState*)pCoderState)->numParityUnits;
  int numAllUnits = ((CoderState*)pCoderState)->numAllUnits;

  printf("Recovering (numAllUnits = %d, numDataUnits = %d, numErased = %d)\n",
                       numAllUnits, numDataUnits, pCoderState->numErased);

  printf(" - ErasedIndexes = ");
  for (j = 0; j < pCoderState->numErased; j++) {
    printf(" %d", pCoderState->erasedIndexes[j]);
  }
  printf("       - DecodeIndex = ");
  for (i = 0; i < numDataUnits; i++) {
    printf(" %d", pCoderState->decodeIndex[i]);
  }

  printf("\n\nEncodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoderState->encodeMatrix,
                                    numDataUnits, numAllUnits);

  printf("InvertMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoderState->invertMatrix,
                                   numDataUnits, numDataUnits);

  printf("DecodeMatrix:\n");
  dumpCodingMatrix((unsigned char*) pCoderState->decodeMatrix,
                                    numDataUnits, numAllUnits);
}

void dump(unsigned char* buf, int len) {
  int i;
  for (i = 0; i < len;) {
    printf(" %2x", 0xff & buf[i++]);
    if (i % 32 == 0)
      printf("\n");
  }
}

void dumpMatrix(unsigned char** buf, int n1, int n2) {
  int i, j;
  for (i = 0; i < n1; i++) {
    for (j = 0; j < n2; j++) {
      printf(" %2x", buf[i][j]);
    }
    printf("\n");
  }
  printf("\n");
}

void dumpCodingMatrix(unsigned char* buf, int n1, int n2) {
  int i, j;
  for (i = 0; i < n1; i++) {
    for (j = 0; j < n2; j++) {
      printf(" %d", 0xff & buf[j + (i * n2)]);
    }
    printf("\n");
  }
  printf("\n");
}
