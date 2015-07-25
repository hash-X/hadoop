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

package org.apache.hadoop.hdfs.util;

import com.google.common.annotations.VisibleForTesting;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.fs.StorageType;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSStripedOutputStream;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedStripedBlock;

import com.google.common.base.Preconditions;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.io.erasurecode.ECSchema;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;
import org.apache.hadoop.security.token.Token;
import org.apache.hadoop.util.DirectBufferPool;

import java.nio.ByteBuffer;
import java.util.*;
import java.io.IOException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * When accessing a file in striped layout, operations on logical byte ranges
 * in the file need to be mapped to physical byte ranges on block files stored
 * on DataNodes. This utility class facilities this mapping by defining and
 * exposing a number of striping-related concepts. The most basic ones are
 * illustrated in the following diagram. Unless otherwise specified, all
 * range-related calculations are inclusive (the end offset of the previous
 * range should be 1 byte lower than the start offset of the next one).
 *
 *  | <----  Block Group ----> |   <- Block Group: logical unit composing
 *  |                          |        striped HDFS files.
 *  blk_0      blk_1       blk_2   <- Internal Blocks: each internal block
 *    |          |           |          represents a physically stored local
 *    v          v           v          block file
 * +------+   +------+   +------+
 * |cell_0|   |cell_1|   |cell_2|  <- {@link StripingCell} represents the
 * +------+   +------+   +------+       logical order that a Block Group should
 * |cell_3|   |cell_4|   |cell_5|       be accessed: cell_0, cell_1, ...
 * +------+   +------+   +------+
 * |cell_6|   |cell_7|   |cell_8|
 * +------+   +------+   +------+
 * |cell_9|
 * +------+  <- A cell contains cellSize bytes of data
 */
@InterfaceAudience.Private
public class StripedBlockUtil {

  private static DirectBufferPool bufferPool = new DirectBufferPool();

  /**
   * This method parses a striped block group into individual blocks.
   *
   * @param bg The striped block group
   * @param cellSize The size of a striping cell
   * @param dataBlkNum The number of data blocks
   * @return An array containing the blocks in the group
   */
  public static LocatedBlock[] parseStripedBlockGroup(LocatedStripedBlock bg,
      int cellSize, int dataBlkNum, int parityBlkNum) {
    int locatedBGSize = bg.getBlockIndices().length;
    LocatedBlock[] lbs = new LocatedBlock[dataBlkNum + parityBlkNum];
    for (short i = 0; i < locatedBGSize; i++) {
      final int idx = bg.getBlockIndices()[i];
      // for now we do not use redundant replica of an internal block
      if (idx < (dataBlkNum + parityBlkNum) && lbs[idx] == null) {
        lbs[idx] = constructInternalBlock(bg, i, cellSize,
            dataBlkNum, idx);
      }
    }
    return lbs;
  }

  /**
   * This method creates an internal block at the given index of a block group
   *
   * @param idxInReturnedLocs The index in the stored locations in the
   *                          {@link LocatedStripedBlock} object
   * @param idxInBlockGroup The logical index in the striped block group
   * @return The constructed internal block
   */
  public static LocatedBlock constructInternalBlock(LocatedStripedBlock bg,
      int idxInReturnedLocs, int cellSize, int dataBlkNum,
      int idxInBlockGroup) {
    final ExtendedBlock blk = constructInternalBlock(
        bg.getBlock(), cellSize, dataBlkNum, idxInBlockGroup);
    final LocatedBlock locatedBlock;
    if (idxInReturnedLocs < bg.getLocations().length) {
      locatedBlock = new LocatedBlock(blk,
          new DatanodeInfo[]{bg.getLocations()[idxInReturnedLocs]},
          new String[]{bg.getStorageIDs()[idxInReturnedLocs]},
          new StorageType[]{bg.getStorageTypes()[idxInReturnedLocs]},
          bg.getStartOffset(), bg.isCorrupt(), null);
    } else {
      locatedBlock = new LocatedBlock(blk, null, null, null,
          bg.getStartOffset(), bg.isCorrupt(), null);
    }
    Token<BlockTokenIdentifier>[] blockTokens = bg.getBlockTokens();
    if (idxInBlockGroup < blockTokens.length) {
      locatedBlock.setBlockToken(blockTokens[idxInBlockGroup]);
    }
    return locatedBlock;
  }

  /**
   * This method creates an internal {@link ExtendedBlock} at the given index
   * of a block group.
   */
  public static ExtendedBlock constructInternalBlock(ExtendedBlock blockGroup,
      int cellSize, int dataBlkNum, int idxInBlockGroup) {
    ExtendedBlock block = new ExtendedBlock(blockGroup);
    block.setBlockId(blockGroup.getBlockId() + idxInBlockGroup);
    block.setNumBytes(getInternalBlockLength(blockGroup.getNumBytes(),
        cellSize, dataBlkNum, idxInBlockGroup));
    return block;
  }

  /**
   * Get the size of an internal block at the given index of a block group
   *
   * @param dataSize Size of the block group only counting data blocks
   * @param cellSize The size of a striping cell
   * @param numDataBlocks The number of data blocks
   * @param i The logical index in the striped block group
   * @return The size of the internal block at the specified index
   */
  public static long getInternalBlockLength(long dataSize,
      int cellSize, int numDataBlocks, int i) {
    Preconditions.checkArgument(dataSize >= 0);
    Preconditions.checkArgument(cellSize > 0);
    Preconditions.checkArgument(numDataBlocks > 0);
    Preconditions.checkArgument(i >= 0);
    // Size of each stripe (only counting data blocks)
    final int stripeSize = cellSize * numDataBlocks;
    // If block group ends at stripe boundary, each internal block has an equal
    // share of the group
    final int lastStripeDataLen = (int)(dataSize % stripeSize);
    if (lastStripeDataLen == 0) {
      return dataSize / numDataBlocks;
    }

    final int numStripes = (int) ((dataSize - 1) / stripeSize + 1);
    return (numStripes - 1L)*cellSize
        + lastCellSize(lastStripeDataLen, cellSize, numDataBlocks, i);
  }

  private static int lastCellSize(int size, int cellSize, int numDataBlocks,
      int i) {
    if (i < numDataBlocks) {
      // parity block size (i.e. i >= numDataBlocks) is the same as 
      // the first data block size (i.e. i = 0).
      size -= i*cellSize;
      if (size < 0) {
        size = 0;
      }
    }
    return size > cellSize? cellSize: size;
  }

  /**
   * Given a byte's offset in an internal block, calculate the offset in
   * the block group
   */
  public static long offsetInBlkToOffsetInBG(int cellSize, int dataBlkNum,
      long offsetInBlk, int idxInBlockGroup) {
    int cellIdxInBlk = (int) (offsetInBlk / cellSize);
    return cellIdxInBlk * cellSize * dataBlkNum // n full stripes before offset
        + idxInBlockGroup * cellSize // m full cells before offset
        + offsetInBlk % cellSize; // partial cell
  }

  /**
   * Get the next completed striped read task
   *
   * @return {@link StripingChunkReadResult} indicating the status of the read task
   *          succeeded, and the block index of the task. If the method times
   *          out without getting any completed read tasks, -1 is returned as
   *          block index.
   * @throws InterruptedException
   */
  public static StripingChunkReadResult getNextCompletedStripedRead(
      CompletionService<Void> readService, Map<Future<Void>, Integer> futures,
      final long threshold) throws InterruptedException {
    Preconditions.checkArgument(!futures.isEmpty());
    Future<Void> future = null;
    try {
      if (threshold > 0) {
        future = readService.poll(threshold, TimeUnit.MILLISECONDS);
      } else {
        future = readService.take();
      }
      if (future != null) {
        future.get();
        return new StripingChunkReadResult(futures.remove(future),
            StripingChunkReadResult.SUCCESSFUL);
      } else {
        return new StripingChunkReadResult(StripingChunkReadResult.TIMEOUT);
      }
    } catch (ExecutionException e) {
      if (DFSClient.LOG.isDebugEnabled()) {
        DFSClient.LOG.debug("ExecutionException " + e);
      }
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.FAILED);
    } catch (CancellationException e) {
      return new StripingChunkReadResult(futures.remove(future),
          StripingChunkReadResult.CANCELLED);
    }
  }

  /**
   * Get the total usage of the striped blocks, which is the total of data
   * blocks and parity blocks
   *
   * @param numDataBlkBytes
   *          Size of the block group only counting data blocks
   * @param dataBlkNum
   *          The number of data blocks
   * @param parityBlkNum
   *          The number of parity blocks
   * @param cellSize
   *          The size of a striping cell
   * @return The total usage of data blocks and parity blocks
   */
  public static long spaceConsumedByStripedBlock(long numDataBlkBytes,
      int dataBlkNum, int parityBlkNum, int cellSize) {
    int parityIndex = dataBlkNum + 1;
    long numParityBlkBytes = getInternalBlockLength(numDataBlkBytes, cellSize,
        dataBlkNum, parityIndex) * parityBlkNum;
    return numDataBlkBytes + numParityBlkBytes;
  }

  /**
   * Initialize the decoding input buffers based on the chunk states in an
   * {@link AlignedStripe}. For each chunk that was not initially requested,
   * schedule a new fetch request with the decoding input buffer as transfer
   * destination.
   */
  public static ByteBuffer[] initDecodeInputs(AlignedStripe alignedStripe,
      int dataBlkNum, int parityBlkNum) {
    //byte[][] decodeInputs =
    //    new byte[dataBlkNum + parityBlkNum][(int) alignedStripe.getSpanInBlock()];
    // read the full data aligned stripe
    ByteBuffer[] decodeInputs = new ByteBuffer[dataBlkNum + parityBlkNum];
    for (int i = 0; i < decodeInputs.length; i++) {
      decodeInputs[i] = bufferPool.getBuffer((int) alignedStripe.getSpanInBlock());
    }
    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] == null) {
        final int decodeIndex = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
        alignedStripe.chunks[i] = new StripingChunk(decodeInputs[decodeIndex]);
        alignedStripe.chunks[i].addByteArraySlice(0,
            (int) alignedStripe.getSpanInBlock());
      }
    }
    return decodeInputs;
  }

  /**
   * Some fetched {@link StripingChunk} might be stored in original application
   * buffer instead of prepared decode input buffers. Some others are beyond
   * the range of the internal blocks and should correspond to all zero bytes.
   * When all pending requests have returned, this method should be called to
   * finalize decode input buffers.
   */
  public static void finalizeDecodeInputs(final ByteBuffer[] decodeInputs,
      int dataBlkNum, int parityBlkNum, AlignedStripe alignedStripe) {
    for (int i = 0; i < alignedStripe.chunks.length; i++) {
      final StripingChunk chunk = alignedStripe.chunks[i];
      final int decodeIndex = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
      if (chunk != null && chunk.state == StripingChunk.FETCHED) {
        chunk.copyTo(decodeInputs[decodeIndex]);
      } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
        //Arrays.fill(decodeInputs[decodeIndex], (byte) 0);
      } else {
        decodeInputs[decodeIndex] = null;
      }
    }
  }

  /**
   * Currently decoding requires parity chunks are before data chunks.
   * The indices are opposite to what we store in NN. In future we may
   * improve the decoding to make the indices order the same as in NN.
   *
   * @param index The index to convert
   * @param dataBlkNum The number of data blocks
   * @param parityBlkNum The number of parity blocks
   * @return converted index
   */
  public static int convertIndex4Decode(int index, int dataBlkNum,
      int parityBlkNum) {
    //index < dataBlkNum ? index + parityBlkNum : index - dataBlkNum;
    return index;
  }

  public static int convertDecodeIndexBack(int index, int dataBlkNum,
      int parityBlkNum) {
    //index < parityBlkNum ? index + dataBlkNum : index - parityBlkNum;
    return index;
  }

  /**
   * Decode based on the given input buffers and schema.
   */
  public static void decodeAndFillBuffer(final ByteBuffer[] decodeInputs,
      AlignedStripe alignedStripe, int dataBlkNum, int parityBlkNum,
      RawErasureDecoder decoder) {
    // Step 1: prepare indices and output buffers for missing data units
    int[] decodeIndices = new int[parityBlkNum];
    int pos = 0;
    for (int i = 0; i < dataBlkNum; i++) {
      if (alignedStripe.chunks[i] != null &&
          alignedStripe.chunks[i].state == StripingChunk.MISSING){
        decodeIndices[pos++] = convertIndex4Decode(i, dataBlkNum, parityBlkNum);
      }
    }
    decodeIndices = Arrays.copyOf(decodeIndices, pos);
    //byte[][] decodeOutputs =
    //    new byte[decodeIndices.length][(int) alignedStripe.getSpanInBlock()];
    ByteBuffer[] decodeOutputs = new ByteBuffer[decodeIndices.length];
    for (int i = 0; i < decodeOutputs.length; i++) {
      decodeOutputs[i] = bufferPool.getBuffer((int) alignedStripe.getSpanInBlock());
    }
    // Step 2: decode into prepared output buffers
    decoder.decode(decodeInputs, decodeIndices, decodeOutputs);

    // Step 3: fill original application buffer with decoded data
    for (int i = 0; i < decodeIndices.length; i++) {
      int missingBlkIdx = convertDecodeIndexBack(decodeIndices[i],
          dataBlkNum, parityBlkNum);
      StripingChunk chunk = alignedStripe.chunks[missingBlkIdx];
      if (chunk.state == StripingChunk.MISSING) {
        chunk.copyFrom(decodeOutputs[i]);
      }
    }
  }

  /**
   * Similar functionality with {@link #divideByteRangeIntoStripes}, but is used
   * by stateful read and uses ByteBuffer as reading target buffer. Besides the
   * read range is within a single stripe thus the calculation logic is simpler.
   */
  public static AlignedStripe[] divideOneStripe(ECSchema ecSchema,
      int cellSize, LocatedStripedBlock blockGroup, long rangeStartInBlockGroup,
      long rangeEndInBlockGroup, ByteBuffer buf) {
    final int dataBlkNum = ecSchema.getNumDataUnits();
    // Step 1: map the byte range to StripingCells
    StripingCell[] cells = getStripingCellsOfByteRange(ecSchema, cellSize,
        blockGroup, rangeStartInBlockGroup, rangeEndInBlockGroup);

    // Step 2: get the unmerged ranges on each internal block
    VerticalRange[] ranges = getRangesForInternalBlocks(ecSchema, cellSize,
        cells);

    // Step 3: merge into stripes
    AlignedStripe[] stripes = mergeRangesForInternalBlocks(ecSchema, ranges);

    // Step 4: calculate each chunk's position in destination buffer. Since the
    // whole read range is within a single stripe, the logic is simpler here.
    int bufOffset = (int) (rangeStartInBlockGroup % (cellSize * dataBlkNum));
    for (StripingCell cell : cells) {
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      for (AlignedStripe s : stripes) {
        long stripeEnd = s.getOffsetInBlock() + s.getSpanInBlock() - 1;
        long overlapStart = Math.max(cellStart, s.getOffsetInBlock());
        long overlapEnd = Math.min(cellEnd, stripeEnd);
        int overLapLen = (int) (overlapEnd - overlapStart + 1);
        if (overLapLen > 0) {
          Preconditions.checkState(s.chunks[cell.idxInStripe] == null);
          final int pos = (int) (bufOffset + overlapStart - cellStart);
          buf.position(pos);
          buf.limit(pos + overLapLen);
          s.chunks[cell.idxInStripe] = new StripingChunk(buf.slice());
        }
      }
      bufOffset += cell.size;
    }

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, stripes, cellSize, dataBlkNum);
    return stripes;
  }

  /**
   * This method divides a requested byte range into an array of inclusive
   * {@link AlignedStripe}.
   * @param ecSchema The codec schema for the file, which carries the numbers
   *                 of data / parity blocks
   * @param cellSize Cell size of stripe
   * @param blockGroup The striped block group
   * @param rangeStartInBlockGroup The byte range's start offset in block group
   * @param rangeEndInBlockGroup The byte range's end offset in block group
   * @param buf Destination buffer of the read operation for the byte range
   *
   * At most 5 stripes will be generated from each logical range, as
   * demonstrated in the header of {@link AlignedStripe}.
   */
  public static AlignedStripe[] divideByteRangeIntoStripes(ECSchema ecSchema,
      int cellSize, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup, ByteBuffer buf) {

    // Step 0: analyze range and calculate basic parameters
    final int dataBlkNum = ecSchema.getNumDataUnits();

    // Step 1: map the byte range to StripingCells
    StripingCell[] cells = getStripingCellsOfByteRange(ecSchema, cellSize,
        blockGroup, rangeStartInBlockGroup, rangeEndInBlockGroup);

    // Step 2: get the unmerged ranges on each internal block
    VerticalRange[] ranges = getRangesForInternalBlocks(ecSchema, cellSize,
        cells);

    // Step 3: merge into at most 5 stripes
    AlignedStripe[] stripes = mergeRangesForInternalBlocks(ecSchema, ranges);

    // Step 4: calculate each chunk's position in destination buffer
    calcualteChunkPositionsInBuf(cellSize, stripes, cells, buf);

    // Step 5: prepare ALLZERO blocks
    prepareAllZeroChunks(blockGroup, stripes, cellSize, dataBlkNum);

    return stripes;
  }

  /**
   * Map the logical byte range to a set of inclusive {@link StripingCell}
   * instances, each representing the overlap of the byte range to a cell
   * used by {@link DFSStripedOutputStream} in encoding
   */
  @VisibleForTesting
  private static StripingCell[] getStripingCellsOfByteRange(ECSchema ecSchema,
      int cellSize, LocatedStripedBlock blockGroup,
      long rangeStartInBlockGroup, long rangeEndInBlockGroup) {
    Preconditions.checkArgument(
        rangeStartInBlockGroup <= rangeEndInBlockGroup &&
            rangeEndInBlockGroup < blockGroup.getBlockSize());
    long len = rangeEndInBlockGroup - rangeStartInBlockGroup + 1;
    int firstCellIdxInBG = (int) (rangeStartInBlockGroup / cellSize);
    int lastCellIdxInBG = (int) (rangeEndInBlockGroup / cellSize);
    int numCells = lastCellIdxInBG - firstCellIdxInBG + 1;
    StripingCell[] cells = new StripingCell[numCells];

    final int firstCellOffset = (int) (rangeStartInBlockGroup % cellSize);
    final int firstCellSize =
        (int) Math.min(cellSize - (rangeStartInBlockGroup % cellSize), len);
    cells[0] = new StripingCell(ecSchema, firstCellSize, firstCellIdxInBG,
        firstCellOffset);
    if (lastCellIdxInBG != firstCellIdxInBG) {
      final int lastCellSize = (int) (rangeEndInBlockGroup % cellSize) + 1;
      cells[numCells - 1] = new StripingCell(ecSchema, lastCellSize,
          lastCellIdxInBG, 0);
    }

    for (int i = 1; i < numCells - 1; i++) {
      cells[i] = new StripingCell(ecSchema, cellSize, i + firstCellIdxInBG, 0);
    }

    return cells;
  }

  /**
   * Given a logical byte range, mapped to each {@link StripingCell}, calculate
   * the physical byte range (inclusive) on each stored internal block.
   */
  @VisibleForTesting
  private static VerticalRange[] getRangesForInternalBlocks(ECSchema ecSchema,
      int cellSize, StripingCell[] cells) {
    int dataBlkNum = ecSchema.getNumDataUnits();
    int parityBlkNum = ecSchema.getNumParityUnits();

    VerticalRange ranges[] = new VerticalRange[dataBlkNum + parityBlkNum];

    long earliestStart = Long.MAX_VALUE;
    long latestEnd = -1;
    for (StripingCell cell : cells) {
      // iterate through all cells and update the list of StripeRanges
      if (ranges[cell.idxInStripe] == null) {
        ranges[cell.idxInStripe] = new VerticalRange(
            cell.idxInInternalBlk * cellSize + cell.offset, cell.size);
      } else {
        ranges[cell.idxInStripe].spanInBlock += cell.size;
      }
      VerticalRange range = ranges[cell.idxInStripe];
      if (range.offsetInBlock < earliestStart) {
        earliestStart = range.offsetInBlock;
      }
      if (range.offsetInBlock + range.spanInBlock - 1 > latestEnd) {
        latestEnd = range.offsetInBlock + range.spanInBlock - 1;
      }
    }

    // Each parity block should be fetched at maximum range of all data blocks
    for (int i = dataBlkNum; i < dataBlkNum + parityBlkNum; i++) {
      ranges[i] = new VerticalRange(earliestStart,
          latestEnd - earliestStart + 1);
    }

    return ranges;
  }

  /**
   * Merge byte ranges on each internal block into a set of inclusive
   * {@link AlignedStripe} instances.
   */
  private static AlignedStripe[] mergeRangesForInternalBlocks(
      ECSchema ecSchema, VerticalRange[] ranges) {
    int dataBlkNum = ecSchema.getNumDataUnits();
    int parityBlkNum = ecSchema.getNumParityUnits();
    List<AlignedStripe> stripes = new ArrayList<>();
    SortedSet<Long> stripePoints = new TreeSet<>();
    for (VerticalRange r : ranges) {
      if (r != null) {
        stripePoints.add(r.offsetInBlock);
        stripePoints.add(r.offsetInBlock + r.spanInBlock);
      }
    }

    long prev = -1;
    for (long point : stripePoints) {
      if (prev >= 0) {
        stripes.add(new AlignedStripe(prev, point - prev,
            dataBlkNum + parityBlkNum));
      }
      prev = point;
    }
    return stripes.toArray(new AlignedStripe[stripes.size()]);
  }

  private static void calcualteChunkPositionsInBuf(int cellSize,
      AlignedStripe[] stripes, StripingCell[] cells, ByteBuffer buf) {
    /**
     *     | <--------------- AlignedStripe --------------->|
     *
     *     |<- length_0 ->|<--  length_1  -->|<- length_2 ->|
     * +------------------+------------------+----------------+
     * |    cell_0_0_0    |    cell_3_1_0    |   cell_6_2_0   |  <- blk_0
     * +------------------+------------------+----------------+
     *   _/                \_______________________
     *  |                                          |
     *  v offset_0                                 v offset_1
     * +----------------------------------------------------------+
     * |  cell_0_0_0 |  cell_1_0_1 and cell_2_0_2  |cell_3_1_0 ...|   <- buf
     * |  (partial)  |    (from blk_1 and blk_2)   |              |
     * +----------------------------------------------------------+
     *
     * Cell indexing convention defined in {@link StripingCell}
     */
    int done = 0;
    for (StripingCell cell : cells) {
      long cellStart = cell.idxInInternalBlk * cellSize + cell.offset;
      long cellEnd = cellStart + cell.size - 1;
      for (AlignedStripe s : stripes) {
        long stripeEnd = s.getOffsetInBlock() + s.getSpanInBlock() - 1;
        long overlapStart = Math.max(cellStart, s.getOffsetInBlock());
        long overlapEnd = Math.min(cellEnd, stripeEnd);
        int overLapLen = (int) (overlapEnd - overlapStart + 1);
        if (overLapLen <= 0) {
          continue;
        }
        if (s.chunks[cell.idxInStripe] == null) {
          s.chunks[cell.idxInStripe] = new StripingChunk(buf);
        }
        s.chunks[cell.idxInStripe].addByteBufferSlice((int) (done +
            overlapStart - cellStart), overLapLen);
      }
      done += cell.size;
    }
  }

  /**
   * If a {@link StripingChunk} maps to a byte range beyond an internal block's
   * size, the chunk should be treated as zero bytes in decoding.
   */
  private static void prepareAllZeroChunks(LocatedStripedBlock blockGroup,
      AlignedStripe[] stripes, int cellSize, int dataBlkNum) {
    for (AlignedStripe s : stripes) {
      for (int i = 0; i < dataBlkNum; i++) {
        long internalBlkLen = getInternalBlockLength(blockGroup.getBlockSize(),
            cellSize, dataBlkNum, i);
        if (internalBlkLen <= s.getOffsetInBlock()) {
          Preconditions.checkState(s.chunks[i] == null);
          s.chunks[i] = new StripingChunk(StripingChunk.ALLZERO);
        }
      }
    }
  }

  /**
   * Cell is the unit of encoding used in {@link DFSStripedOutputStream}. This
   * size impacts how a logical offset in the file or block group translates
   * to physical byte offset in a stored internal block. The StripingCell util
   * class facilitates this calculation. Each StripingCell is inclusive with
   * its start and end offsets -- e.g., the end logical offset of cell_0_0_0
   * should be 1 byte lower than the start logical offset of cell_1_0_1.
   *
   *  | <------- Striped Block Group -------> |
   *    blk_0          blk_1          blk_2
   *      |              |              |
   *      v              v              v
   * +----------+   +----------+   +----------+
   * |cell_0_0_0|   |cell_1_0_1|   |cell_2_0_2|
   * +----------+   +----------+   +----------+
   * |cell_3_1_0|   |cell_4_1_1|   |cell_5_1_2| <- {@link #idxInBlkGroup} = 5
   * +----------+   +----------+   +----------+    {@link #idxInInternalBlk} = 1
   *                                               {@link #idxInStripe} = 2
   * A StripingCell is a special instance of {@link StripingChunk} whose offset
   * and size align with the cell used when writing data.
   * TODO: consider parity cells
   */
  @VisibleForTesting
  static class StripingCell {
    final ECSchema schema;
    /** Logical order in a block group, used when doing I/O to a block group */
    final int idxInBlkGroup;
    final int idxInInternalBlk;
    final int idxInStripe;
    /**
     * When a logical byte range is mapped to a set of cells, it might
     * partially overlap with the first and last cells. This field and the
     * {@link #size} variable represent the start offset and size of the
     * overlap.
     */
    final int offset;
    final int size;

    StripingCell(ECSchema ecSchema, int cellSize, int idxInBlkGroup,
        int offset) {
      this.schema = ecSchema;
      this.idxInBlkGroup = idxInBlkGroup;
      this.idxInInternalBlk = idxInBlkGroup / ecSchema.getNumDataUnits();
      this.idxInStripe = idxInBlkGroup -
          this.idxInInternalBlk * ecSchema.getNumDataUnits();
      this.offset = offset;
      this.size = cellSize;
    }
  }

  /**
   * Given a requested byte range on a striped block group, an AlignedStripe
   * represents an inclusive {@link VerticalRange} that is aligned with both
   * the byte range and boundaries of all internal blocks. As illustrated in
   * the diagram, any given byte range on a block group leads to 1~5
   * AlignedStripe's.
   *
   * |<-------- Striped Block Group -------->|
   * blk_0   blk_1   blk_2      blk_3   blk_4
   *                 +----+  |  +----+  +----+
   *                 |full|  |  |    |  |    | <- AlignedStripe0:
   *         +----+  |~~~~|  |  |~~~~|  |~~~~|      1st cell is partial
   *         |part|  |    |  |  |    |  |    | <- AlignedStripe1: byte range
   * +----+  +----+  +----+  |  |~~~~|  |~~~~|      doesn't start at 1st block
   * |full|  |full|  |full|  |  |    |  |    |
   * |cell|  |cell|  |cell|  |  |    |  |    | <- AlignedStripe2 (full stripe)
   * |    |  |    |  |    |  |  |    |  |    |
   * +----+  +----+  +----+  |  |~~~~|  |~~~~|
   * |full|  |part|          |  |    |  |    | <- AlignedStripe3: byte range
   * |~~~~|  +----+          |  |~~~~|  |~~~~|      doesn't end at last block
   * |    |                  |  |    |  |    | <- AlignedStripe4:
   * +----+                  |  +----+  +----+      last cell is partial
   *                         |
   * <---- data blocks ----> | <--- parity --->
   *
   * An AlignedStripe is the basic unit of reading from a striped block group,
   * because within the AlignedStripe, all internal blocks can be processed in
   * a uniform manner.
   *
   * The coverage of an AlignedStripe on an internal block is represented as a
   * {@link StripingChunk}.
   *
   * To simplify the logic of reading a logical byte range from a block group,
   * a StripingChunk is either completely in the requested byte range or
   * completely outside the requested byte range.
   */
  public static class AlignedStripe {
    public VerticalRange range;
    /** status of each chunk in the stripe */
    public final StripingChunk[] chunks;
    public int fetchedChunksNum = 0;
    public int missingChunksNum = 0;

    public AlignedStripe(long offsetInBlock, long length, int width) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0);
      this.range = new VerticalRange(offsetInBlock, length);
      this.chunks = new StripingChunk[width];
    }

    public boolean include(long pos) {
      return range.include(pos);
    }

    public long getOffsetInBlock() {
      return range.offsetInBlock;
    }

    public long getSpanInBlock() {
      return range.spanInBlock;
    }

    @Override
    public String toString() {
      return "Offset=" + range.offsetInBlock + ", length=" + range.spanInBlock +
          ", fetchedChunksNum=" + fetchedChunksNum +
          ", missingChunksNum=" + missingChunksNum;
    }
  }

  /**
   * A simple utility class representing an arbitrary vertical inclusive range
   * starting at {@link #offsetInBlock} and lasting for {@link #spanInBlock}
   * bytes in an internal block. Note that VerticalRange doesn't necessarily
   * align with {@link StripingCell}.
   *
   * |<- Striped Block Group ->|
   *  blk_0
   *    |
   *    v
   * +-----+
   * |~~~~~| <-- {@link #offsetInBlock}
   * |     |  ^
   * |     |  |
   * |     |  | {@link #spanInBlock}
   * |     |  v
   * |~~~~~| ---
   * |     |
   * +-----+
   */
  public static class VerticalRange {
    /** start offset in the block group (inclusive) */
    public long offsetInBlock;
    /** length of the stripe range */
    public long spanInBlock;

    public VerticalRange(long offsetInBlock, long length) {
      Preconditions.checkArgument(offsetInBlock >= 0 && length >= 0);
      this.offsetInBlock = offsetInBlock;
      this.spanInBlock = length;
    }

    /** whether a position is in the range */
    public boolean include(long pos) {
      return pos >= offsetInBlock && pos < offsetInBlock + spanInBlock;
    }
  }

  /**
   * Indicates the coverage of an {@link AlignedStripe} on an internal block,
   * and the state of the chunk in the context of the read request.
   *
   * |<---------------- Striped Block Group --------------->|
   *   blk_0        blk_1        blk_2          blk_3   blk_4
   *                           +---------+  |  +----+  +----+
   *     null         null     |REQUESTED|  |  |null|  |null| <- AlignedStripe0
   *              +---------+  |---------|  |  |----|  |----|
   *     null     |REQUESTED|  |REQUESTED|  |  |null|  |null| <- AlignedStripe1
   * +---------+  +---------+  +---------+  |  +----+  +----+
   * |REQUESTED|  |REQUESTED|    ALLZERO    |  |null|  |null| <- AlignedStripe2
   * +---------+  +---------+               |  +----+  +----+
   * <----------- data blocks ------------> | <--- parity --->
   */
  public static class StripingChunk {
    /** Chunk has been successfully fetched */
    public static final int FETCHED = 0x01;
    /** Chunk has encountered failed when being fetched */
    public static final int MISSING = 0x02;
    /** Chunk being fetched (fetching task is in-flight) */
    public static final int PENDING = 0x04;
    /**
     * Chunk is requested either by application or for decoding, need to
     * schedule read task
     */
    public static final int REQUESTED = 0X08;
    /**
     * Internal block is short and has no overlap with chunk. Chunk considered
     * all-zero bytes in codec calculations.
     */
    public static final int ALLZERO = 0X0f;

    /**
     * If a chunk is completely in requested range, the state transition is:
     * REQUESTED (when AlignedStripe created) -> PENDING -> {FETCHED | MISSING}
     * If a chunk is completely outside requested range (including parity
     * chunks), state transition is:
     * null (AlignedStripe created) -> REQUESTED (upon failure) -> PENDING ...
     */
    public int state = REQUESTED;

    public final ChunkByteArray byteArray;
    public final ChunkByteBuffer byteBuffer;

    public StripingChunk(byte[] buf) {
      this.byteArray = new ChunkByteArray(buf);
      byteBuffer = null;
    }

    public StripingChunk(ByteBuffer buf) {
      this.byteArray = null;
      this.byteBuffer = new ChunkByteBuffer(buf);
    }

    public StripingChunk(int state) {
      this.byteArray = null;
      this.byteBuffer = null;
      this.state = state;
    }

    public void addByteArraySlice(int offset, int length) {
      assert byteArray != null;
      byteArray.offsetsInBuf.add(offset);
      byteArray.lengthsInBuf.add(length);
    }

    public void addByteBufferSlice(int offset, int length) {
      assert byteBuffer != null;
      byteBuffer.offsetsInBuf.add(offset);
      byteBuffer.lengthsInBuf.add(length);
    }

    void copyTo(byte[] target) {
      assert byteArray != null;
      byteArray.copyTo(target);
    }

    void copyTo(ByteBuffer target) {
      assert byteBuffer != null;
      byteBuffer.copyTo(target);
    }

    void copyFrom(byte[] src) {
      assert byteArray != null;
      byteArray.copyFrom(src);
    }

    void copyFrom(ByteBuffer src) {
      assert byteBuffer != null;
      byteBuffer.copyFrom(src);
    }
  }

  public static class ChunkByteBuffer {
    private final ByteBuffer buf;
    private final List<Integer> offsetsInBuf;
    private final List<Integer> lengthsInBuf;

    ChunkByteBuffer(ByteBuffer buf) {
      this.buf = buf;
      this.offsetsInBuf = new ArrayList<>();
      this.lengthsInBuf = new ArrayList<>();
    }

    public ByteBuffer getSegment(int i) {
      int off = offsetsInBuf.get(i);
      int len = lengthsInBuf.get(i);
      ByteBuffer bb = buf.duplicate();
      bb.position(off);
      bb.limit(off + len);
      return bb.slice();
    }

    public int[] getOffsets() {
      int[] offsets = new int[offsetsInBuf.size()];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = offsetsInBuf.get(i);
      }
      return offsets;
    }

    public int[] getLengths() {
      int[] lens = new int[this.lengthsInBuf.size()];
      for (int i = 0; i < lens.length; i++) {
        lens[i] = this.lengthsInBuf.get(i);
      }
      return lens;
    }

    public ByteBuffer buf() {
      return buf;
    }

    void copyTo(ByteBuffer target) {
      for (int i = 0; i < offsetsInBuf.size(); i++) {
        //System.arraycopy(buf, offsetsInBuf.get(i),
        //    target, posInBuf, lengthsInBuf.get(i));
        ByteBuffer tmp = buf.duplicate();
        tmp.position(offsetsInBuf.get(i));
        tmp.limit(lengthsInBuf.get(i));
        target.put(tmp);
      }
    }

    void copyFrom(ByteBuffer src) {
      for (int i = 0; i < offsetsInBuf.size(); i++) {
        //System.arraycopy(src, srcPos, buf, offsetsInBuf.get(j),
        //    lengthsInBuf.get(j));
        ByteBuffer tmp = buf.duplicate();
        tmp.position(offsetsInBuf.get(i));
        tmp.limit(lengthsInBuf.get(i));
        tmp.put(src);
      }
    }
  }

  public static class ChunkByteArray {
    private final byte[] buf;
    private final List<Integer> offsetsInBuf;
    private final List<Integer> lengthsInBuf;

    ChunkByteArray(byte[] buf) {
      this.buf = buf;
      this.offsetsInBuf = new ArrayList<>();
      this.lengthsInBuf = new ArrayList<>();
    }

    public int[] getOffsets() {
      int[] offsets = new int[offsetsInBuf.size()];
      for (int i = 0; i < offsets.length; i++) {
        offsets[i] = offsetsInBuf.get(i);
      }
      return offsets;
    }

    public int[] getLengths() {
      int[] lens = new int[this.lengthsInBuf.size()];
      for (int i = 0; i < lens.length; i++) {
        lens[i] = this.lengthsInBuf.get(i);
      }
      return lens;
    }

    public byte[] buf() {
      return buf;
    }

    void copyTo(byte[] target) {
      int posInBuf = 0;
      for (int i = 0; i < offsetsInBuf.size(); i++) {
        System.arraycopy(buf, offsetsInBuf.get(i),
            target, posInBuf, lengthsInBuf.get(i));
        posInBuf += lengthsInBuf.get(i);
      }
    }

    void copyFrom(byte[] src) {
      int srcPos = 0;
      for (int j = 0; j < offsetsInBuf.size(); j++) {
        System.arraycopy(src, srcPos, buf, offsetsInBuf.get(j),
            lengthsInBuf.get(j));
        srcPos += lengthsInBuf.get(j);
      }
    }
  }

  /**
   * This class represents result from a striped read request.
   * If the task was successful or the internal computation failed,
   * an index is also returned.
   */
  public static class StripingChunkReadResult {
    public static final int SUCCESSFUL = 0x01;
    public static final int FAILED = 0x02;
    public static final int TIMEOUT = 0x04;
    public static final int CANCELLED = 0x08;

    public final int index;
    public final int state;

    public StripingChunkReadResult(int state) {
      Preconditions.checkArgument(state == TIMEOUT,
          "Only timeout result should return negative index.");
      this.index = -1;
      this.state = state;
    }

    public StripingChunkReadResult(int index, int state) {
      Preconditions.checkArgument(state != TIMEOUT,
          "Timeout result should return negative index.");
      this.index = index;
      this.state = state;
    }

    @Override
    public String toString() {
      return "(index=" + index + ", state =" + state + ")";
    }
  }

  /**
   * Check if the information such as IDs and generation stamps in block-i
   * match block-j, where block-i and block-j are in the same group.
   */
  public static void checkBlocks(int j, ExtendedBlock blockj,
      int i, ExtendedBlock blocki) throws IOException {

    if (!blocki.getBlockPoolId().equals(blockj.getBlockPoolId())) {
      throw new IOException("Block pool IDs mismatched: block" + j + "="
          + blockj + ", block" + i + "=" + blocki);
    }
    if (blocki.getBlockId() - i != blockj.getBlockId() - j) {
      throw new IOException("Block IDs mismatched: block" + j + "="
          + blockj + ", block" + i + "=" + blocki);
    }
    if (blocki.getGenerationStamp() != blockj.getGenerationStamp()) {
      throw new IOException("Generation stamps mismatched: block" + j + "="
          + blockj + ", block" + i + "=" + blocki);
    }
  }

}
