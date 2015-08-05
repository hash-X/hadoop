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

/**
 * This is a sample program illustrating how to use the Intel ISA-L library.
 * Note it's adapted from erasure_code_test.c test program, but trying to use
 * variable names and styles we're more familiar with already similar to Java
 * coders.
 */

#include "erasure_code.h"
#include "gf_util.h"
#include "erasure_coder.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static void usage(char* errorMsg) {
  fprintf(stderr, "usage: sample <numDataUnits> <numParityUnits>\n");
  fprintf(stderr, "A Reed-Solomon coding sample.\n");
  if (errorMsg != NULL) fprintf(stderr, "\n%s\n\n", errorMsg);
  exit(1);
}

int main(int argc, char *argv[]) {
  int i, j;
  int chunkSize = 1024;
  int numDataUnits;
  int numParityUnits;
  char errMsg[256];
  unsigned char** dataUnits;
  unsigned char** parityUnits;
  EncoderState* pEncoder;
  int erasedIndexes[2];
  unsigned char* allUnits[MMAX];
  DecoderState* pDecoder;
  unsigned char* decodingOutput[2];
  unsigned char** backupUnits;

  load_erasurecode_lib(errMsg, sizeof(errMsg));
  if (strlen(errMsg) > 0) {
    // TODO: this may indicate s severe error instead, failing the test.
    fprintf(stderr,
      "loading erasurecode library failed: %s, skipping this\n", errMsg);
    return 0;
  }

  if (argc != 3) usage(NULL);
  if (sscanf(argv[1], "%d", &numDataUnits) == 0 || numDataUnits <= 0) {
    usage("Invalid numDataUnits");
  }
  if (sscanf(argv[2], "%d", &numParityUnits) == 0 || numParityUnits <= 0) {
    usage("Invalid numParityUnits");
  }

  dataUnits = (unsigned char**)malloc(sizeof(unsigned char*) * numDataUnits);
  parityUnits = (unsigned char**)malloc(sizeof(unsigned char*) * numParityUnits);
  backupUnits = (unsigned char**)malloc(sizeof(unsigned char*) * numParityUnits);

  // Allocate and generate data units
  srand(135);
  for (i = 0; i < numDataUnits; i++) {
    dataUnits[i] = malloc(chunkSize);
    for (j = 0; j < chunkSize; j++) {
      dataUnits[i][j] = rand();
    }
  }

  // Allocate and initialize parity units
  for (i = 0; i < numParityUnits; i++) {
    parityUnits[i] = malloc(chunkSize);
    for (j = 0; j < chunkSize; j++) {
      parityUnits[i][j] = 0;
    }
  }

  pEncoder = (EncoderState*)malloc(sizeof(EncoderState));
  memset(pEncoder, 0, sizeof(*pEncoder));
  initEncoder(pEncoder, numDataUnits, numParityUnits);
  encode(pEncoder, dataUnits, parityUnits, chunkSize);

  pDecoder = (DecoderState*)malloc(sizeof(DecoderState));
  memset(pDecoder, 0, sizeof(*pDecoder));
  initDecoder(pDecoder, numDataUnits, numParityUnits);

  memcpy(allUnits, dataUnits, numDataUnits * (sizeof (unsigned char*)));
  memcpy(allUnits + numDataUnits, parityUnits,
                            numParityUnits * (sizeof (unsigned char*)));

  erasedIndexes[0] = 1;
  erasedIndexes[1] = 7;

  backupUnits[0] = allUnits[0];
  backupUnits[1] = allUnits[1];
  backupUnits[2] = allUnits[7];

  allUnits[0] = NULL; // Not to read
  allUnits[1] = NULL;
  allUnits[7] = NULL;

  decodingOutput[0] = malloc(chunkSize);
  decodingOutput[1] = malloc(chunkSize);

  for (i = 0; i < 100; i++) {
    decode(pDecoder, allUnits, erasedIndexes, 2, decodingOutput, chunkSize);
  }

  for (i = 0; i < pDecoder->numErased; i++) {
    if (0 != memcmp(decodingOutput[i], backupUnits[i], chunkSize)) {
      dumpDecoder(pDecoder);
      return -1;
    }
  }

  dumpDecoder(pDecoder);
  printf("done EC tests: Pass\n");

  return 0;
}
