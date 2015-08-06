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

import java.nio.ByteBuffer;

public abstract class AbstractNativeRawDecoder
    extends AbstractRawErasureDecoder {

  public AbstractNativeRawDecoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
  }

  @Override
  protected void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    int[] inputOffsets = new int[inputs.length];
    int[] outputOffsets = new int[outputs.length];
    ByteBuffer validInput = findFirstValidInput(inputs);
    int dataLen = validInput.remaining();

    ByteBuffer buffer;
    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      if (buffer != null) {
        inputOffsets[i] = buffer.position();
      }
    }

    for (int i = 0; i < outputs.length; ++i) {
      buffer = outputs[i];
      outputOffsets[i] = buffer.position();
      outputs[i] = resetBuffer(buffer);
    }

    performDecodeImpl(inputs, inputOffsets, dataLen, erasedIndexes, outputs,
        outputOffsets);

    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      if (buffer != null) {
        buffer.position(inputOffsets[i] + dataLen); // dataLen bytes consumed
      }
    }
  }

  protected abstract void performDecodeImpl(
          ByteBuffer[] inputs, int[] inputOffsets, int dataLen, int[] erased,
          ByteBuffer[] outputs, int[] outputOffsets);

  @Override
  protected void doDecode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    System.out.println("WARNING: doDecodeByConvertingToDirectBuffers is used, not efficiently!!");
    doDecodeByConvertingToDirectBuffers(inputs, inputOffsets, dataLen,
            erasedIndexes, outputs, outputOffsets);
  }

  @Override
  public boolean preferDirectBuffer() {
    return true;
  }

  private long __native_coder;
  private long __native_verbose = 0;
}
