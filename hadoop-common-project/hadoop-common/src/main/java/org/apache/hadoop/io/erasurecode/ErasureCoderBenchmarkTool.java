/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * A micro-benchmark tool to test the performance of different erasure coders
 */
public class ErasureCoderBenchmarkTool {
  // use a fix-size buffer as input data
  private static final int DATA_BUFFER_SIZE_IN_MB = 126;

  private static List<RawErasureCoderFactory> coderMakers =
      Collections.unmodifiableList(Arrays.asList(
          new RSRawErasureCoderFactory(), new RSRawErasureCoderFactory2(),
          new NativeRSRawErasureCoderFactory()));

  private static List<String> coderNames = Collections.unmodifiableList(
      Arrays.asList("hdfs-raid coder", "new Java coder", "isa-l coder"));

  static {
    assert coderMakers.size() == coderNames.size();
  }

  private static void printAvailableCoders() {
    StringBuilder sb = new StringBuilder("Available coders with coderIndex:\n");
    for (int i = 0; i < coderNames.size(); i++) {
      if (isNativeCoder(i) && !ErasureCodeNative.isNativeCodeLoaded()) {
        continue; // Skip the native one if not loaded successfully
      }
      sb.append(i).append(":").append(coderNames.get(i)).append("\n");
    }
    System.out.println(sb.toString());
  }

  private static void usage(String message) {
    if (message != null) {
      System.out.println(message);
    }
    System.out.println(
        "BenchmarkTool <encode/decode> <coderIndex> [numClients]" +
            "[dataSize-in-MB] [chunkSize-in-KB]");
    printAvailableCoders();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    String opType = null;
    int coderIndex = 0;
    int dataSizeMB = 10240;
    int chunkSizeKB = 1024;
    int numClients = 1;

    if (args.length > 1) {
      opType = args[0];
      if (!"encode".equals(opType) && !"decode".equals(opType)) {
        usage("Invalid type, either 'encode' or 'decode'");
      }

      try {
        coderIndex = Integer.parseInt(args[1]);
        if (coderIndex < 0 || coderIndex >= coderNames.size()) {
          usage("Invalid coder index, should be [0-" +
              (coderNames.size() - 1) + "]");
        }
      } catch (NumberFormatException e) {
        usage("Malformed coder index, " + e.getMessage());
      }
    } else {
      usage(null);
    }

    if (args.length > 2) {
      try {
        numClients = Integer.parseInt(args[2]);
        if (numClients <= 0) {
          usage("Invalid number of clients.");
        }
      } catch (NumberFormatException e) {
        usage("Malformed number of clients, " + e.getMessage());
      }
    }

    if (args.length > 3) {
      try {
        dataSizeMB = Integer.parseInt(args[3]);
        if (dataSizeMB <= 0) {
          usage("Invalid data size.");
        }
      } catch (NumberFormatException e) {
        usage("Malformed data size, " + e.getMessage());
      }
    }

    if (args.length > 4) {
      try {
        chunkSizeKB = Integer.parseInt(args[4]);
        if (chunkSizeKB <= 0) {
          usage("Invalid chunk size.");
        }
      } catch (NumberFormatException e) {
        usage("Malformed chunk size, " + e.getMessage());
      }
    }

    performBench(opType, coderIndex, numClients, dataSizeMB, chunkSizeKB);
  }

  /**
   * Performs benchmark
   *
   * @param opType      The operation to perform. Can be encode or decode
   * @param coderIndex  An index into the coder array
   * @param numClients  Number of threads to launch concurrently
   * @param dataSizeMB  Total test data size in MB
   * @param chunkSizeKB Chunk size in KB
   */
  public static void performBench(String opType, int coderIndex, int numClients,
                                  int dataSizeMB, int chunkSizeKB) {
    BenchData.configure(dataSizeMB, chunkSizeKB);
    ByteBuffer testData = genTestData(coderIndex);

    RawErasureCoder coder;
    boolean isEncode = opType.equalsIgnoreCase("encode");
    // TODO: We perform an encode/decode here to initialize the coder. Ideally
    // this work should be encapsulated.
    if (isEncode) {
      RawErasureEncoder encoder = coderMakers.get(coderIndex).createEncoder(
          BenchData.numDataUnits, BenchData.numParityUnits);
      encoder.encode(new byte[BenchData.numDataUnits][1],
          new byte[BenchData.numParityUnits][1]);
      coder = encoder;
    } else {
      RawErasureDecoder decoder = coderMakers.get(coderIndex).createDecoder(
          BenchData.numDataUnits, BenchData.numParityUnits);
      byte[][] inputs = new byte[BenchData.numAllUnits][1];
      inputs[6] = inputs[7] = inputs[8] = null;
      decoder.decode(inputs, BenchData.erasedIndexes, new byte[3][1]);
      coder = decoder;
    }

    ExecutorService executor = Executors.newFixedThreadPool(numClients);
    List<Future<Long>> futures = new ArrayList<>(numClients);
    long start = System.currentTimeMillis();
    for (int i = 0; i < numClients; i++) {
      futures.add(executor.submit(new CoderBenchCallable(
          coder, testData.duplicate())));
    }
    List<Long> durations = new ArrayList<>(numClients);
    try {
      for (Future<Long> future : futures) {
        durations.add(future.get());
      }
      long duration = System.currentTimeMillis() - start;
      long totalDataSize = BenchData.totalDataSizeMB * numClients;
      DecimalFormat df = new DecimalFormat("#.##");
      System.out.println(coderNames.get(coderIndex) + " " + opType + " " +
          totalDataSize + "MB data, with chunk size " +
          BenchData.chunkSize / 1024 / 1024 + "MB");
      System.out.println("Total time: " + df.format(duration / 1000.0) + " s.");
      System.out.println("Total throughput: " + df.format(
          totalDataSize * 1.0 / duration * 1000.0) + " MB/s");
      printClientStatistics(durations, df);
    } catch (Exception e) {
      System.out.println("Error waiting for client to finish.");
      e.printStackTrace();
    } finally {
      executor.shutdown();
    }
  }

  private static void printClientStatistics(
      List<Long> durations, DecimalFormat df) {
    Collections.sort(durations);
    System.out.println("Clients statistics: ");
    Double min = durations.get(0) / 1000.0;
    Double max = durations.get(durations.size() - 1) / 1000.0;
    Long sum = 0L;
    for (Long duration : durations) {
      sum += duration;
    }
    Double avg = sum.doubleValue() / durations.size() / 1000.0;
    Double percentile = durations.get(
        (int) Math.ceil(durations.size() * 0.9) - 1) / 1000.0;
    System.out.println(durations.size() + " clients in total.");
    System.out.println("Min: " + df.format(min) + " s, Max: " +
        df.format(max) + " s, Avg: " + df.format(avg) +
        " s, 90th Percentile: " + df.format(percentile) + " s.");
  }

  private static boolean isNativeCoder(int coderIndex) {
    return coderIndex == 2;
  }

  private static ByteBuffer genTestData(int coderIndex) {
    Random random = new Random();
    int bufferSize = DATA_BUFFER_SIZE_IN_MB * 1024 * 1024;
    byte tmp[] = new byte[bufferSize];
    random.nextBytes(tmp);
    ByteBuffer data = isNativeCoder(coderIndex) ?
        ByteBuffer.allocateDirect(bufferSize) :
        ByteBuffer.allocate(bufferSize);
    data.put(tmp);
    data.flip();
    return data;
  }

  private static class BenchData {
    public static final int numDataUnits = 6;
    public static final int numParityUnits = 3;
    public static final int numAllUnits = numDataUnits + numParityUnits;
    private static int chunkSize;
    private static int totalDataSizeMB;

    private static final int[] erasedIndexes = new int[]{6, 7, 8};
    private final ByteBuffer[] inputs = new ByteBuffer[numDataUnits];
    private ByteBuffer[] outputs = new ByteBuffer[numParityUnits];
    private ByteBuffer[] decodeInputs = new ByteBuffer[numAllUnits];

    public static void configure(int dataSizeMB, int chunkSizeKB) {
      chunkSize = chunkSizeKB * 1024;
      int rounds = (int) Math.round(
          (dataSizeMB * 1.0) / DATA_BUFFER_SIZE_IN_MB);
      rounds = rounds == 0 ? 1 : rounds;
      totalDataSizeMB = rounds * DATA_BUFFER_SIZE_IN_MB;
    }

    public BenchData(boolean useDirectBuffer) {
      for (int i = 0; i < outputs.length; i++) {
        outputs[i] = useDirectBuffer ? ByteBuffer.allocateDirect(chunkSize) :
            ByteBuffer.allocate(chunkSize);
      }
    }

    public void prepareDecInput() {
      System.arraycopy(inputs, 0, decodeInputs, 0, numDataUnits);
    }

    public void encode(RawErasureEncoder encoder) {
      encoder.encode(inputs, outputs);
    }

    public void decode(RawErasureDecoder decoder) {
      decoder.decode(decodeInputs, erasedIndexes, outputs);
    }
  }

  private static class CoderBenchCallable implements Callable<Long> {
    private final boolean isEncode;
    private RawErasureEncoder encoder;
    private RawErasureDecoder decoder;
    private final BenchData benchData;
    private final ByteBuffer testData;

    public CoderBenchCallable(RawErasureCoder coder, ByteBuffer testData) {
      this.isEncode = (coder instanceof RawErasureEncoder);
      if (isEncode) {
        encoder = (RawErasureEncoder) coder;
      } else {
        decoder = (RawErasureDecoder) coder;
      }
      boolean needDirectBuffer =
          (boolean) coder.getCoderOption(CoderOption.PREFER_DIRECT_BUFFER);
      benchData = new BenchData(needDirectBuffer);
      this.testData = testData;
    }

    @Override
    public Long call() throws Exception {
      int rounds = BenchData.totalDataSizeMB / DATA_BUFFER_SIZE_IN_MB;

      long start = System.currentTimeMillis();
      for (int i = 0; i < rounds; i++) {
        while (testData.remaining() > 0) {
          for (ByteBuffer output : benchData.outputs) {
            output.clear();
          }

          for (int j = 0; j < benchData.inputs.length; j++) {
            benchData.inputs[j] = testData.duplicate();
            benchData.inputs[j].limit(
                testData.position() + BenchData.chunkSize);
            benchData.inputs[j] = benchData.inputs[j].slice();
            testData.position(testData.position() + BenchData.chunkSize);
          }

          if (!isEncode) {
            benchData.prepareDecInput();
          }

          if (isEncode) {
            benchData.encode(encoder);
          } else {
            benchData.decode(decoder);
          }
        }
        testData.clear();
      }
      return System.currentTimeMillis() - start;
    }
  }
}
