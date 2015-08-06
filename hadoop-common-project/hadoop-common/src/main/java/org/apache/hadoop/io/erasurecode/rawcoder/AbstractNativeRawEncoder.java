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

public abstract class AbstractNativeRawEncoder
    extends AbstractRawErasureEncoder {

  public AbstractNativeRawEncoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
  }

  @Override
  protected void doEncode(ByteBuffer[] inputs, ByteBuffer[] outputs) {
    int[] inputOffsets = new int[inputs.length];
    int[] outputOffsets = new int[outputs.length];
    int dataLen = inputs[0].remaining();

    ByteBuffer buffer;
    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      inputOffsets[i] = buffer.position();
    }

    for (int i = 0; i < outputs.length; ++i) {
      buffer = outputs[i];
      outputOffsets[i] = buffer.position();
      outputs[i] = resetBuffer(buffer);
    }

    performEncodeImpl(inputs, inputOffsets, dataLen, outputs, outputOffsets);

    for (int i = 0; i < inputs.length; ++i) {
      buffer = inputs[i];
      buffer.position(inputOffsets[i] + dataLen); // dataLen bytes consumed
    }
  }

  protected abstract void performEncodeImpl(
          ByteBuffer[] inputs, int[] inputOffsets,
          int dataLen, ByteBuffer[] outputs, int[] outputOffsets);

  @Override
  protected void doEncode(byte[][] inputs, int[] inputOffsets, int dataLen,
                          byte[][] outputs, int[] outputOffsets) {
    System.out.println("WARNING: doEncodeByConvertingToDirectBuffers is used, not efficiently!!");
    doEncodeByConvertingToDirectBuffers(inputs, inputOffsets, dataLen, outputs, outputOffsets);
  }

  @Override
  public boolean preferDirectBuffer() {
    return true;
  }

  private long __native_coder;
  private long __native_verbose = 0;
}
