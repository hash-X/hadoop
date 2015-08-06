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
package org.apache.hadoop.io.erasurecode;

import org.apache.hadoop.io.erasurecode.rawcoder.*;

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Random;

public class BenchmarkTool {
  private static RawErasureCoderFactory[] coderMakers =
      new RawErasureCoderFactory[] {
          new RSRawErasureCoderFactory(),
          new RSRawErasureCoderFactory2(),
          new NativeRSRawErasureCoderFactory()
      };

  private static String[] coderNames = new String[] {
      "Reed-Solomon coder in Java (originated from HDFS-RAID)",
      "Reed-Solomon coder in Java (interoperable with ISA-L)",
      "Reed-Solomon coder in native backed by ISA-L",
  };

  private static void usage(String message) {
    if (message != null) {
      System.out.println(message);
    }
    System.out.println("BenchmarkTool [-list list coder indexes]");
    System.out.println("              [-bench <coder-index>]");
    System.exit(1);
  }

  public static void main(String[] args) {
    boolean wantList = false;
    boolean wantBench = false;
    int coderIndex = -1;

    if (args.length == 1 && args[0].equals("-list")) {
      wantList = true;
    } else if (args.length == 2 && args[0].equals("-bench")) {
      wantBench = true;
      coderIndex = Integer.valueOf(args[1]);
      if (coderIndex < 0 || coderIndex > coderNames.length - 1) {
        usage("Invalid coder index");
      }
    } else if (args.length > 0) {
      usage("Invalid option");
    } else {
      //usage(null);
      performBench(0, 1);
    }

    if (wantList) {
      printCoders();
    } else if (wantBench) {
      performBench(coderIndex);
    }
  }

  public static void performBench(int... coderIndexes) {
    for (int coderIndex : coderIndexes) {
      System.out.println("Performing benchmark test for "
          + coderNames[coderIndex]);

      RawErasureCoderFactory maker = coderMakers[coderIndex];
      CoderBench bench = new CoderBench(maker, coderIndex == 2 ? 1000 : 50);
      bench.performEncode();
      bench.performDecode();
    }
  }

  private static void printCoders() {
    StringBuilder sb = new StringBuilder("Available coders:\n");
    for (int i = 0; i < coderNames.length; i++) {
      sb.append(i).append(":").append(coderNames[i]).append("\n");
    }
    System.out.println(sb.toString());
  }

  static class BenchData {
    final static Random rand = new Random();
    final static int numDataUnits = 6;
    final static int numParityUnits = 3;

    final boolean useDirectBuffer;
    final int numAllUnits = numDataUnits + numParityUnits;
    final int chunkSize = 16 * 1024 * 1024; // MB
    final byte[][] inputs = new byte[numDataUnits][];
    final byte[][] outputs = new byte[numParityUnits][];
    final byte[][] decodeInputs = new byte[numAllUnits][];
    final int[] erasedIndexes = new int[]{0, 5, 8};
    final byte[][] decodeOutputs = new byte[erasedIndexes.length][];

    final ByteBuffer[] inputs2 = new ByteBuffer[numDataUnits];
    final ByteBuffer[] outputs2 = new ByteBuffer[numParityUnits];
    final ByteBuffer[] decodeInputs2 = new ByteBuffer[numAllUnits];
    final ByteBuffer[] decodeOutputs2 = new ByteBuffer[erasedIndexes.length];

    BenchData(boolean useDirectBuffer) {
      this.useDirectBuffer = useDirectBuffer;
      if (useDirectBuffer) {
        initWithDirectByteBuffer();
      } else {
        initWithBytesArrayBuffer();
      }
    }

    private void initWithBytesArrayBuffer() {
      for (int i = 0; i < inputs.length; i++) {
        inputs[i] = new byte[chunkSize];
        rand.nextBytes(inputs[i]);
      }

      for (int i = 0; i < outputs.length; i++) {
        outputs[i] = new byte[chunkSize];
      }

      System.arraycopy(inputs, 0, decodeInputs, 0, numDataUnits);
      System.arraycopy(outputs, 0, decodeInputs, numDataUnits, numParityUnits);
      for (int i = 0; i < erasedIndexes.length; i++) {
        decodeInputs[erasedIndexes[i]] = null;
      }

      for (int i = 0; i < decodeOutputs.length; i++) {
        decodeOutputs[i] = new byte[chunkSize];
      }
    }

    private void initWithDirectByteBuffer() {
      byte[] tmpBuf = new byte[chunkSize];
      ByteBuffer tmp;
      for (int i = 0; i < inputs2.length; i++) {
        rand.nextBytes(tmpBuf);
        tmp = ByteBuffer.allocateDirect(chunkSize);
        tmp.put(tmpBuf);
        tmp.flip();
        inputs2[i] = tmp;
      }

      for (int i = 0; i < outputs.length; i++) {
        outputs2[i] = ByteBuffer.allocateDirect(chunkSize);;
      }

      System.arraycopy(inputs2, 0, decodeInputs2, 0, numDataUnits);
      System.arraycopy(outputs2, 0, decodeInputs2, numDataUnits, numParityUnits);
      for (int i = 0; i < erasedIndexes.length; i++) {
        decodeInputs2[erasedIndexes[i]] = null;
      }

      for (int i = 0; i < decodeOutputs2.length; i++) {
        decodeOutputs2[i] = ByteBuffer.allocateDirect(chunkSize);
      }
    }

    void encodeOnce(RawErasureEncoder encoder) {
      if (useDirectBuffer) {
        encoder.encode(inputs2, outputs2);
      } else {
        encoder.encode(inputs, outputs);
      }
    }

    void decodeOnce(RawErasureDecoder decoder) {
      if (useDirectBuffer) {
        decoder.decode(decodeInputs2, erasedIndexes, decodeOutputs2);
      } else {
        decoder.decode(decodeInputs, erasedIndexes, decodeOutputs);
      }
    }
  }

  static class CoderBench {
    static BenchData bytesArrayBufferBenchData;
    static BenchData directByteBufferBenchData;
    final RawErasureEncoder encoder;
    final RawErasureDecoder decoder;
    final int testTimes;
    BenchData benchData;


    CoderBench(RawErasureCoderFactory maker, int testTimes) {
      this.testTimes = testTimes;
      encoder = maker.createEncoder(benchData.numDataUnits, benchData.numParityUnits);
      decoder = maker.createDecoder(benchData.numDataUnits,
          benchData.numParityUnits);
      if (encoder.preferDirectBuffer()) {
        if (directByteBufferBenchData == null) {
          directByteBufferBenchData = new BenchData(true);
        }
        benchData = directByteBufferBenchData;
      } else {
        if (bytesArrayBufferBenchData == null) {
          bytesArrayBufferBenchData = new BenchData(false);
        }
        benchData = bytesArrayBufferBenchData;
      }
    }

    private void warmUp(boolean isEncode) {
      int times = 3;
      for (int i = 0; i < times; i++) {
        if (isEncode) {
          benchData.encodeOnce(encoder);
        } else {
          benchData.decodeOnce(decoder);
        }
      }
    }

    void performEncode() {
      performCoding(true);
    }

    void performDecode() {
      performCoding(false);
    }

    private void performCoding(boolean isEncode) {
      warmUp(isEncode);

      long begin = System.currentTimeMillis();
      for (int i = 0; i < testTimes; i++) {
        if (isEncode) {
          benchData.encodeOnce(encoder);
        } else {
          benchData.decodeOnce(decoder);
        }
      }
      long end = System.currentTimeMillis();

      double usedTime = end - begin;
      long usedData = (testTimes * benchData.numDataUnits *
          benchData.chunkSize) / (1024 * 1024);
      double throughput = (usedData * 1000) / usedTime;

      DecimalFormat df = new DecimalFormat("#.##");
      String text = isEncode ? "Encode " : "Decode ";
      text += usedData + "MB data takes " + usedTime
          + " milliseconds, throughput:" + df.format(throughput) + "MB/s";

      System.out.println(text);
    }
  }
}
