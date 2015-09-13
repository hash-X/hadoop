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

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.erasurecode.rawcoder.*;
import org.apache.hadoop.io.erasurecode.rawcoder.util.GaloisField;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
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
    System.out.println("BenchmarkTool <testDir> <coderIndex> " +
        "[dataSize-in-MB] [chunkSize-in-KB]");
    printAvailableCoders();
    System.exit(1);
  }

  public static void main(String[] args) throws Exception {
    File testDir = null;
    int coderIndex = 0;
    int dataSize = 1024; //MB
    int chunkSize = 1024; //KB

    if (args.length > 1) {
      testDir = new File(args[0]);
      if (!testDir.exists() || !testDir.isDirectory()) {
        usage("Invalid testDir");
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

    performBench(testDir, coderIndex, dataSize, chunkSize);
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

  public static void performBench(File testDir, int coderIndex, int dataSize,
                                  int chunkSize) throws Exception {
    BenchData.configure(dataSize, chunkSize);
    performBench(testDir, coderIndex);
  }

  private static void performBench(File testDir, int coderIndex) throws Exception {
    File testDataFile = new File(testDir, "generated-benchtest-data.dat");
    generateTestData(testDataFile);

    System.out.println("Performing benchmark test for " + coderNames[coderIndex]);

    File encodedDataFile = File.createTempFile(
        "encoded-benchtest-data-coder" + coderIndex, ".dat", testDir);
    File decodedDataFile = File.createTempFile(
        "decoded-benchtest-data-coder" + coderIndex, ".dat", testDir);

    RawErasureCoderFactory coderMaker = coderMakers[coderIndex];
    CoderBench bench = new CoderBench(coderMaker);
    bench.performEncode(testDataFile, encodedDataFile);
    bench.performDecode(encodedDataFile, decodedDataFile, testDataFile);

    if (!FileUtils.contentEquals(decodedDataFile, testDataFile)) {
      throw new RuntimeException("Decoding verifying failed, " +
          "not the same with the original file");
    }

    try {
      FileUtils.forceDelete(encodedDataFile);
      FileUtils.forceDelete(decodedDataFile);
    } catch (IOException e) {
      // Ignore
    }
  }

  static void generateTestData(File testDataFile) throws IOException {
    if (testDataFile.exists()) {
      if (testDataFile.length() / (1024 * 1024) == BenchData.dataSize) {
        System.out.println("Reuse existing test data file");
        return;
      }
      FileUtils.forceDelete(testDataFile);
    }

    System.out.println("Generating test data file");
    FileOutputStream out = new FileOutputStream(testDataFile);
    Random random = new Random();
    byte buf[] = new byte[BenchData.chunkSize];

    try {
      for (int i = 0; i < BenchData.dataChunks; i++) {
        random.nextBytes(buf);
        out.write(buf);
      }
    } finally {
      out.close();
    }
  }

  static class BenchData {
    static int numDataUnits = 6;
    static int numParityUnits = 3;
    static int chunkSize;
    static long groupSize;
    static int dataChunks;
    static long dataSize; //MB
    static byte[] emptyChunk;

    final boolean useDirectBuffer;
    final int numAllUnits = numDataUnits + numParityUnits;
    final int[] erasedIndexes = new int[]{0, 5, 8};
    final ByteBuffer[] inputs = new ByteBuffer[numDataUnits];
    final ByteBuffer[] outputs = new ByteBuffer[numParityUnits];
    final ByteBuffer[] decodeInputs = new ByteBuffer[numAllUnits];
    final ByteBuffer[] decodeOutputs = new ByteBuffer[erasedIndexes.length];
    final ByteBuffer[] inputsWithRecovered = new ByteBuffer[numDataUnits];

    static void configure(int desiredDataSize, int desiredChunkSize) {
      chunkSize = desiredChunkSize * 1024;
      groupSize = chunkSize * numDataUnits;
      dataSize = desiredDataSize;
      int numGroups = (int) ((dataSize * 1024) / (groupSize / 1024));
      if (numGroups < 1) {
        numGroups = 1;
      }
      dataSize = (numGroups * groupSize) / (1024 * 1024);
      dataChunks = numGroups * numDataUnits;
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
      System.arraycopy(outputs, 0, decodeInputs, numDataUnits, numParityUnits);
      for (int i = 0; i < erasedIndexes.length; i++) {
        decodeInputs[erasedIndexes[i]] = null;
      }

      for (int i = 0; i < decodeOutputs.length; i++) {
        decodeOutputs[i] = useDirectBuffer ?
            ByteBuffer.allocateDirect(chunkSize) :
            ByteBuffer.allocate(chunkSize);
      }

      System.arraycopy(inputs, 0, inputsWithRecovered, 0, numDataUnits);
      for (int i = 0, idx = 0; i < erasedIndexes.length; i++) {
        if (erasedIndexes[i] < numDataUnits) {
          inputsWithRecovered[erasedIndexes[i]] = decodeOutputs[idx++];
        }
      }
    }

    void encode(RawErasureEncoder encoder) {
      encoder.encode(inputs, outputs);
    }

    void decode(RawErasureDecoder decoder) {
      decoder.decode(decodeInputs, erasedIndexes, decodeOutputs);
    }
  }

  static class CoderBench {
    static BenchData heapBufferBenchData;
    static BenchData directBufferBenchData;
    final RawErasureEncoder encoder;
    final RawErasureDecoder decoder;
    BenchData benchData;

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

    void performEncode(File testDataFile, File resultDataFile) throws Exception {
      FileChannel inputChannel = new FileInputStream((testDataFile)).getChannel();
      FileChannel outputChannel = new FileOutputStream(resultDataFile).getChannel();

      long codingStart, codingFinish, codingTime = 0;

      long got, written;
      while (true) {
        for (ByteBuffer input : benchData.inputs) {
          input.clear();
        }

        got = inputChannel.read(benchData.inputs);
        if (got < 1) {
          break;
        }
        if (got != BenchData.groupSize) {
          throw new RuntimeException("Invalid read, less than expected");
        }

        for (ByteBuffer input : benchData.inputs) {
          input.flip();
        }

        written = outputChannel.write(benchData.inputs);
        if (written != BenchData.groupSize) {
          throw new RuntimeException("Invalid write, less than expected");
        }

        for (ByteBuffer input : benchData.inputs) {
          input.flip();
        }

        for (ByteBuffer output : benchData.outputs) {
          output.clear();
          output.put(benchData.emptyChunk);
          output.clear();
        }

        codingStart = System.currentTimeMillis();
        benchData.encode(encoder);
        codingFinish = System.currentTimeMillis();
        codingTime += codingFinish - codingStart;

        for (ByteBuffer input : benchData.inputs) {
          input.flip();
        }

        written = outputChannel.write(benchData.outputs);
        if (written != BenchData.numParityUnits * BenchData.chunkSize) {
          throw new RuntimeException("Invalid write, less than expected");
        }
      }

      inputChannel.close();
      outputChannel.close();

      printResult(true, codingTime);
    }

    void performDecode(File encodedDataFile, File resultDataFile,
                       File originalDataFile) throws IOException {
      FileChannel inputChannel = new FileInputStream((encodedDataFile)).getChannel();
      FileChannel outputChannel = new FileOutputStream(resultDataFile).getChannel();

      long codingStart, codingFinish, codingTime = 0;

      long got, written;
      while (true) {
        for (ByteBuffer input : benchData.inputs) {
          input.clear();
        }
        for (ByteBuffer output : benchData.outputs) {
          output.clear();
        }

        got = inputChannel.read(benchData.inputs);
        if (got < 1) { //Normal, EOF
          break;
        }
        if (got != BenchData.groupSize) {
          throw new RuntimeException("Invalid read, less than expected");
        }
        got = inputChannel.read(benchData.outputs);
        if (got != BenchData.numParityUnits * BenchData.chunkSize) {
          throw new RuntimeException("Invalid read, less than expected");
        }

        for (ByteBuffer input : benchData.inputs) {
          input.flip();
        }
        for (ByteBuffer output : benchData.outputs) {
          output.flip();
        }

        for (ByteBuffer output : benchData.decodeOutputs) {
          output.clear();
          output.put(benchData.emptyChunk);
          output.clear();
        }

        codingStart = System.currentTimeMillis();
        benchData.decode(decoder);
        codingFinish = System.currentTimeMillis();
        codingTime += codingFinish - codingStart;

        for (ByteBuffer input : benchData.decodeInputs) {
          if (input != null) {
            input.flip();
          }
        }

        written = outputChannel.write(benchData.inputsWithRecovered);
        if (written != BenchData.numDataUnits * BenchData.chunkSize) {
          throw new RuntimeException("Invalid write, less than expected");
        }
      }

      inputChannel.close();
      outputChannel.close();

      printResult(false, codingTime);
    }

    private void printResult(boolean isEncode, long codingTime) {
      long usedData = BenchData.dataSize; // in MB
      double throughput = (usedData * 1000) / codingTime;
      String prefix = isEncode ? "Encode " : "Decode ";
      DecimalFormat df = new DecimalFormat("#.##");

      // Print throughput
      String text = prefix + usedData + " MB data takes " +
          df.format(codingTime / 1000.0f) + " seconds, throughput:" +
          df.format(throughput) + " MB/s";

      System.out.println(text);
    }
  }
}
