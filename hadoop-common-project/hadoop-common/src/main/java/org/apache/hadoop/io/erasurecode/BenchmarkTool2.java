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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.Random;

public class BenchmarkTool2 {

  final static int DATA_BUFFER_SIZE = 126; //MB

  private static RawErasureCoderFactory[] coderMakers =
      new RawErasureCoderFactory[] {
          new RSRawErasureCoderFactory(),
          new RSRawErasureCoderFactory2(),
          new NativeRSRawErasureCoderFactory()
      };

  private static String[] coderNames = new String[] {
      "hdfs-raid coder",
      "new Java coder",
      "isa-l coder",
  };

  private static void usage(String message) {
    if (message != null) {
      System.out.println(message);
    }
    System.out.println("BenchmarkTool <encode/decode> <coderIndex> " +
        "[dataSize-in-MB] [chunkSize-in-KB]");
    printAvailableCoders();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    String type = null;
    int coderIndex = 0;
    int dataSize = 1024; //MB
    int chunkSize = 1024; //KB

    if (args.length > 1) {
      type = args[0];
      if (!"encode".equals(type) && !"decode".equals(type)) {
        usage("Invalid type, either 'encode' or 'decode'");
      }

      int tmp = Integer.parseInt(args[1]);
      if (tmp >= 0 && tmp <= 2) {
        coderIndex = tmp;
      } else {
        usage("Invalid coder index, should be in the list");
      }
    } else {
      usage(null);
    }

    if (args.length > 2) {
      int tmp = Integer.parseInt(args[2]);
      if (tmp > 0 ) {
        dataSize = tmp;
      } else {
        usage("Invalid dataSize, should be valid integer");
      }
    }

    if (args.length > 3) {
      int tmp = Integer.parseInt(args[3]);
      if (tmp > 0 ) {
        chunkSize = tmp;
      } else {
        usage("Invalid chunkSize, should be valid integer");
      }
    }

    performBench(type, coderIndex, dataSize, chunkSize);
  }

  private static void printAvailableCoders() {
    StringBuilder sb = new StringBuilder("Available coders with coderIndex:\n");
    for (int i = 0; i < coderNames.length; i++) {
      if (i == 2 && !ErasureCodeNative.isNativeCodeLoaded()) {
        continue; // Skip the native one if not loaded successfully
      }
      sb.append(i).append(":").append(coderNames[i]).append("\n");
    }
    System.out.println(sb.toString());
  }

  public static void performBench(String type, int coderIndex, int dataSize,
                                  int chunkSize) throws Exception {
    BenchData.configure(dataSize, chunkSize);
    performBench(type, coderIndex);
  }

  private static void performBench(String type,
                                   int coderIndex) throws Exception {
    System.out.println("Performing " + type +
        " benchmark test for " + coderNames[coderIndex]);

    long runningStart = System.currentTimeMillis();
    CoderBench.codingTime = CoderBench.datagenTime = 0;
    RawErasureCoderFactory coderMaker = coderMakers[coderIndex];
    CoderBench bench = new CoderBench(coderMaker);

    int times = (int) (BenchData.dataSize / DATA_BUFFER_SIZE);
    boolean isEncode = "encode".equals(type);
    for (int i = 0; i < times; i++) {
      if (isEncode) {
        bench.performEncode();
      } else {
        bench.performDecode();
      }
    }
    long runningFinish = System.currentTimeMillis();

    bench.printResult(isEncode, runningFinish - runningStart);
  }

  static class BenchData {
    static int numDataUnits = 6;
    static int numParityUnits = 3;
    static int chunkSize;
    static long groupSize;
    static long dataSize; //MB
    static byte[] emptyChunk;

    final boolean useDirectBuffer;
    final int numAllUnits = numDataUnits + numParityUnits;
    final int[] erasedIndexes = new int[]{6, 7, 8};
    final ByteBuffer[] inputs = new ByteBuffer[numDataUnits];
    final ByteBuffer[] outputs = new ByteBuffer[numParityUnits];
    final ByteBuffer[] decodeInputs = new ByteBuffer[numAllUnits];

    static void configure(int desiredDataSize, int desiredChunkSize) {
      chunkSize = desiredChunkSize * 1024;
      groupSize = chunkSize * numDataUnits;
      dataSize = desiredDataSize;
      int times = (int) Math.round((dataSize * 1.0) / DATA_BUFFER_SIZE);
      times = times == 0 ? 1 : times;
      dataSize = times * DATA_BUFFER_SIZE;
    }

    BenchData(boolean useDirectBuffer) {
      this.useDirectBuffer = useDirectBuffer;

      emptyChunk = new byte[chunkSize];

      for (int i = 0; i < inputs.length; i++) {
        inputs[i] = useDirectBuffer ? ByteBuffer.allocateDirect(chunkSize) :
            ByteBuffer.allocate(chunkSize);
      }

      for (int i = 0; i < outputs.length; i++) {
        outputs[i] = useDirectBuffer ? ByteBuffer.allocateDirect(chunkSize) :
            ByteBuffer.allocate(chunkSize);
      }

      System.arraycopy(inputs, 0, decodeInputs, 0, numDataUnits);
    }

    void encode(RawErasureEncoder encoder) {
      encoder.encode(inputs, outputs);
    }

    void decode(RawErasureDecoder decoder) {
      decoder.decode(decodeInputs, erasedIndexes, outputs);
    }
  }

  static class CoderBench {
    static Random random = new Random();
    static byte[] testData = new byte[DATA_BUFFER_SIZE * 1024 * 1024];
    static BenchData heapBufferBenchData;
    static BenchData directBufferBenchData;
    final RawErasureEncoder encoder;
    final RawErasureDecoder decoder;
    BenchData benchData;

    static long codingTime = 0;
    static long datagenTime = 0;

    CoderBench(RawErasureCoderFactory coderMaker) throws IOException {
      encoder = coderMaker.createEncoder(benchData.numDataUnits,
          benchData.numParityUnits);
      decoder = coderMaker.createDecoder(benchData.numDataUnits,
          benchData.numParityUnits);
      if ((boolean)encoder.getCoderOption(CoderOption.PREFER_DIRECT_BUFFER)) {
        if (directBufferBenchData == null) {
          directBufferBenchData = new BenchData(true);
        }
        benchData = directBufferBenchData;
      } else {
        if (heapBufferBenchData == null) {
          heapBufferBenchData = new BenchData(false);
        }
        benchData = heapBufferBenchData;
      }
    }

    void performEncode() throws Exception {
      performCoding(true);
    }

    void performDecode() throws Exception {

      performCoding(false);
    }

    private void performCoding(boolean isEncode) {
      long codingStart, codingFinish;
      long datagenStart, datagenFinish;

      if (datagenTime < 1) {
        datagenStart = System.currentTimeMillis();
        random.nextBytes(testData);
        datagenFinish = System.currentTimeMillis();
        datagenTime = datagenFinish - datagenStart;
      }

      ByteBuffer dataBuffer = ByteBuffer.wrap(testData);

      ByteBuffer tmp;
      while (dataBuffer.remaining() > 0) {
        for (ByteBuffer input : benchData.inputs) {
          input.clear();
          tmp = dataBuffer.duplicate();
          tmp.limit(dataBuffer.position() + benchData.chunkSize);
          input.put(tmp);
          input.flip();
          dataBuffer.position(dataBuffer.position() + benchData.chunkSize);
        }

        for (ByteBuffer output : benchData.outputs) {
          output.clear();
        }

        codingStart = System.currentTimeMillis();
        if (isEncode) {
          benchData.encode(encoder);
        } else {
          benchData.decode(decoder);
        }
        codingFinish = System.currentTimeMillis();
        codingTime += codingFinish - codingStart;
      }
    }

    void printResult(boolean isEncode, long runningTime) {
      long usedData = BenchData.dataSize; // in MB

      double throughput = (usedData * 1000) / codingTime;
      DecimalFormat df = new DecimalFormat("#.##");

      String text = "";
      text += "RunningTime:" + df.format(runningTime / 1000.0f) + " seconds\n";
      text += "DataSize:" + usedData + " MB\n";
      text += "DataGenTime:" + df.format(datagenTime / 1000.0f) + " seconds\n";
      text += (isEncode ? "EncodeTime:" : "DecodeTime:") +
          df.format(codingTime / 1000.0f) + " seconds\n";
      text += "Throughput:" + df.format(throughput) + " MB/s";

      System.out.println(text);
    }
  }
}
