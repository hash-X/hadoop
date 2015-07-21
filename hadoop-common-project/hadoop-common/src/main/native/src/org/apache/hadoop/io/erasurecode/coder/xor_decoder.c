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
#include "org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawDecoder.h"

typedef struct _XOREncoderState {
  CoderState coderStates;
  unsigned char* inputs[MMAX];
  unsigned char* outputs[1];
} XORDecoderState;

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawDecoder_initImpl(
JNIEnv *env, jobject thiz, jint numDataUnits, jint numParityUnits) {
  XORDecoderState* pCoderState =
                           (XORDecoderState*)malloc(sizeof(XORDecoderState));
  memset(pCoderState, 0, sizeof(*pCoderState));
  initCoder((CoderState*)pCoderState, numDataUnits, numParityUnits);

  setCoderState(env, thiz, (CoderState*)pCoderState);
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawDecoder_decodeImpl(
JNIEnv *env, jobject thiz, jobjectArray inputs, jintArray inputOffsets,
jint dataLen, jintArray erasedIndexes, jobjectArray outputs,
jintArray outputOffsets) {
  XORDecoderState* xorDecoder = (XORDecoderState*)getCoderState(env, thiz);

  int numDataUnits = ((CoderState*)xorDecoder)->numDataUnits;
  int numParityUnits = ((CoderState*)xorDecoder)->numParityUnits;
  int chunkSize = (int)dataLen;
  int i, j;

  getInputs(env, inputs, inputOffsets, xorDecoder->inputs,
                                               numDataUnits + numParityUnits);
  getOutputs(env, outputs, outputOffsets, xorDecoder->outputs, numParityUnits);

  for (i = 0; i < numDataUnits + numParityUnits; i++) {
    if (xorDecoder->inputs[i] == NULL) {
      continue;
    }
    for (j = 0; j < chunkSize; j++) {
      xorDecoder->outputs[0][j] ^= xorDecoder->inputs[i][j];
    }
  }
}

JNIEXPORT void JNICALL
Java_org_apache_hadoop_io_erasurecode_rawcoder_NativeXORRawDecoder_destroyImpl
(JNIEnv *env, jobject thiz){
  XORDecoderState* xorDecoder = (XORDecoderState*)getCoderState(env, thiz);
  free(xorDecoder);
}
