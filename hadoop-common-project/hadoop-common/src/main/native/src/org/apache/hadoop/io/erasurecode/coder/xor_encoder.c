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

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "org_apache_hadoop.h"
#include "../include/erasure_code.h"
#include "../include/gf_util.h"
#include "coder_util.h"
#include "org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder.h"

typedef struct _XOREncoderState {
  CoderState coderStates;
  unsigned char* inputs[MMAX];
  unsigned char* outputs[1];
} XOREncoderState;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_initImpl
(JNIEnv *env, jobject thiz, jint numDataUnits, jint numParityUnits) {
  XOREncoderState* pCoderState =
                           (XOREncoderState*)malloc(sizeof(XOREncoderState));
  memset(pCoderState, 0, sizeof(*pCoderState));
  initCoder((CoderState*)pCoderState, numDataUnits, numParityUnits);

  setCoderState(env, thiz, (CoderState*)pCoderState);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_encodeImpl(
JNIEnv *env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
jint dataLen, jobjectArray outputs, jintArray outputOffsets) {
  int i, j;
  XOREncoderState* xorEncoder = (XOREncoderState*)getCoderState(env, thiz);
  
  int numDataUnits = ((CoderState*)xorEncoder)->numDataUnits;
  int numParityUnits = ((CoderState*)xorEncoder)->numParityUnits;
  int chunkSize = (int)dataLen;

  getInputs(env, inputs, inputOffsets, xorEncoder->inputs, numDataUnits);
  getOutputs(env, outputs, outputOffsets, xorEncoder->outputs, numParityUnits);

  // Get the first buffer's data.
  for (j = 0; j < chunkSize; j++) {
    xorEncoder->outputs[0][j] = xorEncoder->inputs[0][j];
  }

  // XOR with everything else.
  for (i = 1; i < numDataUnits; i++) {
    for (j = 0; j < chunkSize; j++) {
      xorEncoder->outputs[0][j] ^= xorEncoder->inputs[i][j];
    }
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawEncoder_destroyImpl
(JNIEnv *env, jobject thiz) {
  XOREncoderState* xorEncoder = (XOREncoderState*)getCoderState(env, thiz);
  free(xorEncoder);
}

