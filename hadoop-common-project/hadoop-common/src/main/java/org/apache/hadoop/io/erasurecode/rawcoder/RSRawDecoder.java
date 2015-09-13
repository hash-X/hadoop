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

import org.apache.hadoop.HadoopIllegalArgumentException;
import org.apache.hadoop.io.erasurecode.rawcoder.util.RSUtil;

import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A raw erasure encoder in RS code scheme in pure Java in case native one
 * isn't available in some environment. Please always use native implementations
 * when possible. This one originated from HDFS-RAID in its core algorithm. Note
 * it's not compatible with the native/ISA-L coder.
 *
 * Currently this implementation will compute and decode not to read units
 * unnecessarily due to the underlying implementation limit in GF. This will be
 * addressed in HADOOP-11871.
 */
public class RSRawDecoder extends AbstractRawErasureDecoder {
  // To describe and calculate the needed Vandermonde matrix
  private int[] errSignature;
  private int[] primitivePower;

  public RSRawDecoder(int numDataUnits, int numParityUnits) {
    super(numDataUnits, numParityUnits);
    if (numDataUnits + numParityUnits >= RSUtil.GF.getFieldSize()) {
      throw new HadoopIllegalArgumentException(
              "Invalid numDataUnits and numParityUnits");
    }

    this.errSignature = new int[numParityUnits];
    this.primitivePower = RSUtil.getPrimitivePower(numDataUnits,
        numParityUnits);
  }

  @Override
  public void decode(ByteBuffer[] inputs, int[] erasedIndexes,
                     ByteBuffer[] outputs) {
    // Make copies avoiding affecting original ones;
    ByteBuffer[] newInputs = new ByteBuffer[inputs.length];
    int[] newErasedIndexes = new int[erasedIndexes.length];
    ByteBuffer[] newOutputs = new ByteBuffer[outputs.length];

    // Adjust the order to match with underlying requirements.
    adjustOrder(inputs, newInputs,
        erasedIndexes, newErasedIndexes, outputs, newOutputs);

    super.decode(newInputs, newErasedIndexes, newOutputs);
  }

  @Override
  public void decode(byte[][] inputs, int[] erasedIndexes, byte[][] outputs) {
    // Make copies avoiding affecting original ones;
    byte[][] newInputs = new byte[inputs.length][];
    int[] newErasedIndexes = new int[erasedIndexes.length];
    byte[][] newOutputs = new byte[outputs.length][];

    // Adjust the order to match with underlying requirements.
    adjustOrder(inputs, newInputs,
        erasedIndexes, newErasedIndexes, outputs, newOutputs);

    super.decode(newInputs, newErasedIndexes, newOutputs);
  }

  private void doDecodeImpl(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    ByteBuffer valid = findFirstValidInput(inputs);
    int dataLen = valid.remaining();
    for (int i = 0; i < erasedIndexes.length; i++) {
      errSignature[i] = primitivePower[erasedIndexes[i]];
      RSUtil.GF.substitute(inputs, dataLen, outputs[i], primitivePower[i]);
    }

    RSUtil.GF.solveVandermondeSystem(errSignature,
        outputs, erasedIndexes.length);
  }

  private void doDecodeImpl(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    for (int i = 0; i < erasedIndexes.length; i++) {
      errSignature[i] = primitivePower[erasedIndexes[i]];
      RSUtil.GF.substitute(inputs, inputOffsets, dataLen, outputs[i],
          outputOffsets[i], primitivePower[i]);
    }

    RSUtil.GF.solveVandermondeSystem(errSignature, outputs, outputOffsets,
            erasedIndexes.length, dataLen);
  }

  @Override
  protected void doDecode(byte[][] inputs, int[] inputOffsets,
                          int dataLen, int[] erasedIndexes,
                          byte[][] outputs, int[] outputOffsets) {
    /**
     * As passed parameters are friendly to callers but not to the underlying
     * implementations, so we have to adjust them before calling doDecodeImpl.
     */

    byte[][] bytesArrayBuffers = new byte[getNumParityUnits()][];
    byte[][] adjustedByteArrayOutputsParameter =
        new byte[getNumParityUnits()][];
    int[] adjustedOutputOffsets = new int[getNumParityUnits()];

    int[] erasedOrNotToReadIndexes = getErasedOrNotToReadIndexes(inputs);

    // Prepare for adjustedOutputsParameter

    // First reset the positions needed this time
    for (int i = 0; i < erasedOrNotToReadIndexes.length; i++) {
      adjustedByteArrayOutputsParameter[i] = null;
      adjustedOutputOffsets[i] = 0;
    }
    // Use the caller passed buffers in erasedIndexes positions
    for (int outputIdx = 0, i = 0; i < erasedIndexes.length; i++) {
      boolean found = false;
      for (int j = 0; j < erasedOrNotToReadIndexes.length; j++) {
        // If this index is one requested by the caller via erasedIndexes, then
        // we use the passed output buffer to avoid copying data thereafter.
        if (erasedIndexes[i] == erasedOrNotToReadIndexes[j]) {
          found = true;
          adjustedByteArrayOutputsParameter[j] = resetBuffer(
                  outputs[outputIdx], outputOffsets[outputIdx], dataLen);
          adjustedOutputOffsets[j] = outputOffsets[outputIdx];
          outputIdx++;
        }
      }
      if (!found) {
        throw new HadoopIllegalArgumentException(
            "Inputs not fully corresponding to erasedIndexes in null places");
      }
    }
    // Use shared buffers for other positions (not set yet)
    for (int bufferIdx = 0, i = 0; i < erasedOrNotToReadIndexes.length; i++) {
      if (adjustedByteArrayOutputsParameter[i] == null) {
        adjustedByteArrayOutputsParameter[i] = resetBuffer(
            checkGetBytesArrayBuffer(bytesArrayBuffers,
                bufferIdx, dataLen), 0, dataLen);
        adjustedOutputOffsets[i] = 0; // Always 0 for such temp output
        bufferIdx++;
      }
    }

    doDecodeImpl(inputs, inputOffsets, dataLen, erasedOrNotToReadIndexes,
            adjustedByteArrayOutputsParameter, adjustedOutputOffsets);
  }

  @Override
  protected void doDecode(ByteBuffer[] inputs, int[] erasedIndexes,
                          ByteBuffer[] outputs) {
    ByteBuffer validInput = findFirstValidInput(inputs);
    int dataLen = validInput.remaining();

    /**
     * As passed parameters are friendly to callers but not to the underlying
     * implementations, so we have to adjust them before calling doDecodeImpl.
     */

    ByteBuffer[] directBuffers = new ByteBuffer[getNumParityUnits()];
    ByteBuffer[] adjustedDirectBufferOutputsParameter =
        new ByteBuffer[getNumParityUnits()];

    int[] erasedOrNotToReadIndexes = getErasedOrNotToReadIndexes(inputs);

    // Prepare for adjustedDirectBufferOutputsParameter

    // First reset the positions needed this time
    for (int i = 0; i < erasedOrNotToReadIndexes.length; i++) {
      adjustedDirectBufferOutputsParameter[i] = null;
    }
    // Use the caller passed buffers in erasedIndexes positions
    for (int outputIdx = 0, i = 0; i < erasedIndexes.length; i++) {
      boolean found = false;
      for (int j = 0; j < erasedOrNotToReadIndexes.length; j++) {
        // If this index is one requested by the caller via erasedIndexes, then
        // we use the passed output buffer to avoid copying data thereafter.
        if (erasedIndexes[i] == erasedOrNotToReadIndexes[j]) {
          found = true;
          adjustedDirectBufferOutputsParameter[j] =
              resetBuffer(outputs[outputIdx++], dataLen);
        }
      }
      if (!found) {
        throw new HadoopIllegalArgumentException(
            "Inputs not fully corresponding to erasedIndexes in null places");
      }
    }
    // Use shared buffers for other positions (not set yet)
    for (int bufferIdx = 0, i = 0; i < erasedOrNotToReadIndexes.length; i++) {
      if (adjustedDirectBufferOutputsParameter[i] == null) {
        ByteBuffer buffer = checkGetDirectBuffer(directBuffers, bufferIdx, dataLen);
        buffer.position(0);
        buffer.limit(dataLen);
        adjustedDirectBufferOutputsParameter[i] = resetBuffer(buffer, dataLen);
        bufferIdx++;
      }
    }

    doDecodeImpl(inputs, erasedOrNotToReadIndexes,
        adjustedDirectBufferOutputsParameter);
  }

  /*
   * Convert data units first order to parity units first order.
   */
  private <T> void adjustOrder(T[] inputs, T[] inputs2,
                               int[] erasedIndexes, int[] erasedIndexes2,
                               T[] outputs, T[] outputs2) {
    // Example:
    // d0 d1 d2 d3 d4 d5 : p0 p1 p2 => p0 p1 p2 : d0 d1 d2 d3 d4 d5
    System.arraycopy(inputs, numDataUnits, inputs2, 0, numParityUnits);
    System.arraycopy(inputs, 0, inputs2, numParityUnits, numDataUnits);

    int numErasedDataUnits = 0, numErasedParityUnits = 0;
    int idx = 0;
    for (int i = 0; i < erasedIndexes.length; i++) {
      if (erasedIndexes[i] >= numDataUnits) {
        erasedIndexes2[idx++] = erasedIndexes[i] - numDataUnits;
        numErasedParityUnits++;
      }
    }
    for (int i = 0; i < erasedIndexes.length; i++) {
      if (erasedIndexes[i] < numDataUnits) {
        erasedIndexes2[idx++] = erasedIndexes[i] + numParityUnits;
        numErasedDataUnits++;
      }
    }

    // Copy for data units
    System.arraycopy(outputs, numErasedDataUnits, outputs2,
        0, numErasedParityUnits);
    // Copy for parity units
    System.arraycopy(outputs, 0, outputs2,
        numErasedParityUnits, numErasedDataUnits);
  }

  private byte[] checkGetBytesArrayBuffer(byte[][] bytesArrayBuffers,
                                          int idx, int bufferLen) {
    if (bytesArrayBuffers[idx] == null ||
            bytesArrayBuffers[idx].length < bufferLen) {
      bytesArrayBuffers[idx] = new byte[bufferLen];
    }
    return bytesArrayBuffers[idx];
  }

  private ByteBuffer checkGetDirectBuffer(ByteBuffer[] directBuffers,
                                          int idx, int bufferLen) {
    if (directBuffers[idx] == null ||
        directBuffers[idx].capacity() < bufferLen) {
      directBuffers[idx] = ByteBuffer.allocateDirect(bufferLen);
    }
    return directBuffers[idx];
  }
}
