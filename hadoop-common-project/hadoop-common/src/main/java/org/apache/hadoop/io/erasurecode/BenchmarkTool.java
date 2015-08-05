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

  private static void performBench(int... coderIndexes) {
    BenchData benchData = new BenchData();
    for (int coderIndex : coderIndexes) {
      performBench(benchData, coderIndex);
    }
  }

  private static void performBench(BenchData benchData, int coderIndex) {
    System.out.println("Performing benchmark test for "
        + coderNames[coderIndex]);

    RawErasureCoderFactory maker = coderMakers[coderIndex];
    CoderBench bench = new CoderBench(benchData, maker);
    bench.performEncode();
    bench.performDecode();
  }

  private static void printCoders() {
    StringBuilder sb = new StringBuilder("Available coders:\n");
    for (int i = 0; i < coderNames.length; i++) {
      sb.append(i).append(":").append(coderNames[i]).append("\n");
    }
    System.out.println(sb.toString());
  }

  static class BenchData {
    Random rand = new Random();
    final int numDataUnits = 6;
    final int numParityUnits = 3;
    final int numAllUnits = numDataUnits + numParityUnits;
    final int chunkSize = 16 * 1024 * 1024; // MB
    final byte[][] inputs = new byte[numDataUnits][];
    final byte[][] outputs = new byte[numParityUnits][];
    final byte[][] decodeInputs = new byte[numAllUnits][];
    final int[] erasedIndexes = new int[]{0, 5, 8};
    final byte[][] decodeOutputs = new byte[erasedIndexes.length][];

    BenchData() {
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
  }

  static class CoderBench {
    final RawErasureEncoder encoder;
    final RawErasureDecoder decoder;
    final BenchData benchData;

    CoderBench(BenchData benchData, RawErasureCoderFactory maker) {
      this.benchData = benchData;
      encoder = maker.createEncoder(benchData.numDataUnits,
          benchData.numParityUnits);
      decoder = maker.createDecoder(benchData.numDataUnits,
          benchData.numParityUnits);
    }

    void encodeOnce() {
      encoder.encode(benchData.inputs, benchData.outputs);
    }

    void decodeOnce() {
      decoder.decode(benchData.decodeInputs, benchData.erasedIndexes,
          benchData.decodeOutputs);
    }

    private void warmup(boolean isEncode) {
      int times = 3;
      for (int i = 0; i < times; i++) {
        if (isEncode) {
          encodeOnce();
        } else {
          decodeOnce();
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
      warmup(isEncode);

      int times = 5;
      long begin = System.currentTimeMillis();
      for (int i = 0; i < times; i++) {
        if (isEncode) {
          encodeOnce();
        } else {
          decodeOnce();
        }
      }
      long end = System.currentTimeMillis();

      double usedTime = ((float)(end - begin) / times) / 1000.00f;
      long usedData = (times * benchData.numDataUnits *
          benchData.chunkSize) / (1024 * 1024);
      double throughput = usedData / usedTime;

      DecimalFormat df = new DecimalFormat("#.##");
      String text = isEncode ? "Encode " : "Decode ";
      text += usedData + "MB data takes " + df.format(usedTime)
          + " seconds, throughput:" + df.format(throughput) + "MB/s";

      System.out.println(text);
    }
  }
}
