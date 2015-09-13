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
package org.apache.hadoop.hdfs;

import com.google.common.base.Preconditions;
import org.apache.hadoop.fs.ChecksumException;
import org.apache.hadoop.fs.ReadOption;
import org.apache.hadoop.hdfs.protocol.*;
import org.apache.hadoop.hdfs.protocol.datatransfer.InvalidEncryptionKeyException;
import org.apache.hadoop.hdfs.server.blockmanagement.BlockIdManager;
import org.apache.hadoop.hdfs.util.StripedBlockUtil;
import org.apache.hadoop.io.ByteBufferPool;
import org.apache.hadoop.io.ElasticByteBufferPool;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.erasurecode.CodecUtil;
import org.apache.hadoop.io.erasurecode.ECChunk;
import org.apache.hadoop.io.erasurecode.rawcoder.RawErasureDecoder;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;

import static org.apache.hadoop.hdfs.util.StripedBlockUtil.*;

/**
 * DFSStripedInputStream reads from striped block groups
 */
public class DFSStripedInputStream extends DFSInputStream {

  private static class ReaderRetryPolicy {
    private int fetchEncryptionKeyTimes = 1;
    private int fetchTokenTimes = 1;

    void refetchEncryptionKey() {
      fetchEncryptionKeyTimes--;
    }

    void refetchToken() {
      fetchTokenTimes--;
    }

    boolean shouldRefetchEncryptionKey() {
      return fetchEncryptionKeyTimes > 0;
    }

    boolean shouldRefetchToken() {
      return fetchTokenTimes > 0;
    }
  }

  private static class BlockReaderInfo {
    final BlockReader reader;
    final DatanodeInfo datanode;
    /**
     * when initializing block readers, their starting offsets are set to the same
     * number: the smallest internal block offsets among all the readers. This is
     * because it is possible that for some internal blocks we have to read
     * "backwards" for decoding purpose. We thus use this offset array to track
     * offsets for all the block readers so that we can skip data if necessary.
     */
    long blockReaderOffset;
    LocatedBlock targetBlock;
    /**
     * We use this field to indicate whether we should use this reader. In case
     * we hit any issue with this reader, we set this field to true and avoid
     * using it for the next stripe.
     */
    boolean shouldSkip = false;

    BlockReaderInfo(BlockReader reader, LocatedBlock targetBlock,
        DatanodeInfo dn, long offset) {
      this.reader = reader;
      this.targetBlock = targetBlock;
      this.datanode = dn;
      this.blockReaderOffset = offset;
    }

    void setOffset(long offset) {
      this.blockReaderOffset = offset;
    }

    void skip() {
      this.shouldSkip = true;
    }
  }

  private static final ByteBufferPool bufferPool = new ElasticByteBufferPool();

  private final BlockReaderInfo[] blockReaders;
  private final int cellSize;
  private final short dataBlkNum;
  private final short parityBlkNum;
  private final int groupSize;
  private final ErasureCodingPolicy ecPolicy;
  private final RawErasureDecoder decoder;

  /**
   * indicate the start/end offset of the current buffered stripe in the
   * block group
   */
  //private StripeRange curStripeRange;
  private final CompletionService<Void> readingService;

  DFSStripedInputStream(DFSClient dfsClient, String src,
      boolean verifyChecksum, ErasureCodingPolicy ecPolicy,
      LocatedBlocks locatedBlocks) throws IOException {
    super(dfsClient, src, verifyChecksum, locatedBlocks);

    assert ecPolicy != null;
    this.ecPolicy = ecPolicy;
    this.cellSize = ecPolicy.getCellSize();
    dataBlkNum = (short) ecPolicy.getNumDataUnits();
    parityBlkNum = (short) ecPolicy.getNumParityUnits();
    groupSize = dataBlkNum + parityBlkNum;
    blockReaders = new BlockReaderInfo[groupSize];
    readingService =
        new ExecutorCompletionService<>(dfsClient.getStripedReadsThreadPool());
    decoder = CodecUtil.createRSRawDecoder(dfsClient.getConfiguration(),
        dataBlkNum, parityBlkNum);
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("Creating an striped input stream for file " + src);
    }
  }

  private boolean useDirectBuffer() {
    return CodecUtil.preferDirectBuffer(decoder);
  }

  /**
   * When seeking into a new block group, create blockReader for each internal
   * block in the group.
   */
  private synchronized void blockSeekTo(long target) throws IOException {
    if (target >= getFileLength()) {
      throw new IOException("Attempted to read past end of file");
    }

    // Will be getting a new BlockReader.
    closeCurrentBlockReaders();

    // Compute desired striped block group
    LocatedStripedBlock targetBlockGroup = getBlockGroupAt(target);
    // Update current position
    this.pos = target;
    this.blockEnd = targetBlockGroup.getStartOffset() +
        targetBlockGroup.getBlockSize() - 1;
    currentLocatedBlock = targetBlockGroup;
  }

  /**
   * Extend the super method with the logic of switching between cells.
   * When reaching the end of a cell, proceed to the next cell and read it
   * with the next blockReader.
   */
  @Override
  protected void closeCurrentBlockReaders() {
    if (blockReaders ==  null || blockReaders.length == 0) {
      return;
    }
    for (int i = 0; i < groupSize; i++) {
      closeReader(blockReaders[i]);
      blockReaders[i] = null;
    }
    blockEnd = -1;
  }

  private void closeReader(BlockReaderInfo readerInfo) {
    if (readerInfo != null) {
      IOUtils.cleanup(DFSClient.LOG, readerInfo.reader);
      readerInfo.skip();
    }
  }

  private long getOffsetInBlockGroup() {
    return getOffsetInBlockGroup(pos);
  }

  private long getOffsetInBlockGroup(long pos) {
    return pos - currentLocatedBlock.getStartOffset();
  }

  /**
   * Read a new stripe covering the current position
   */
  private void readStripes(ByteBuffer buffer,
                   Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
                                                          throws IOException {
    // compute stripe range based on pos
    final long targetStart = getOffsetInBlockGroup();
    final long bytesToRead = buffer.remaining();
    fetchBlockByteRange(currentLocatedBlock, targetStart,
        targetStart + bytesToRead - 1, buffer, corruptedBlockMap);
  }

  private Callable<Void> readCells(final BlockReader reader,
      final DatanodeInfo datanode, final long currentReaderOffset,
      final long targetReaderOffset, final ByteBufferStrategy[] strategies,
      final ExtendedBlock currentBlock,
      final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
    return new Callable<Void>() {
      @Override
      public Void call() throws Exception {
        // reader can be null if getBlockReaderWithRetry failed or
        // the reader hit exception before
        if (reader == null) {
          throw new IOException("The BlockReader is null. " +
              "The BlockReader creation failed or the reader hit exception.");
        }
        Preconditions.checkState(currentReaderOffset <= targetReaderOffset);
        if (currentReaderOffset < targetReaderOffset) {
          long skipped = reader.skip(targetReaderOffset - currentReaderOffset);
          Preconditions.checkState(
              skipped == targetReaderOffset - currentReaderOffset);
        }
        int result = 0;
        for (ByteBufferStrategy strategy : strategies) {
          result += readToBuffer(reader, datanode, strategy, currentBlock,
              corruptedBlockMap);
          strategy.getReadBuffer().flip();
        }
        return null;
      }
    };
  }

  private int readToBuffer(BlockReader blockReader,
      DatanodeInfo currentNode, ByteBufferStrategy strategy,
      ExtendedBlock currentBlock,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    final int targetLength = strategy.getTargetLength();
    int gotLen = 0;
    try {
      while (gotLen < targetLength) {
        int ret = strategy.read(blockReader);
        if (ret < 0) {
          throw new IOException("Unexpected EOS from the reader");
        }
        gotLen += ret;
      }
      return gotLen;
    } catch (ChecksumException ce) {
      DFSClient.LOG.warn("Found Checksum error for "
          + currentBlock + " from " + currentNode
          + " at " + ce.getPos());
      // we want to remember which block replicas we have tried
      addIntoCorruptedBlockMap(currentBlock, currentNode,
          corruptedBlockMap);
      throw ce;
    } catch (IOException e) {
      DFSClient.LOG.warn("Exception while reading from "
          + currentBlock + " of " + src + " from "
          + currentNode, e);
      throw e;
    }
  }

  /**
   * Seek to a new arbitrary location
   */
  @Override
  public synchronized void seek(long targetPos) throws IOException {
    if (targetPos > getFileLength()) {
      throw new EOFException("Cannot seek after EOF");
    }
    if (targetPos < 0) {
      throw new EOFException("Cannot seek to negative offset");
    }
    if (closed.get()) {
      throw new IOException("Stream is closed!");
    }

    pos = targetPos;
    blockEnd = -1;
  }

  @Override
  public synchronized boolean seekToNewSource(long targetPos)
      throws IOException {
    return false;
  }

  @Override
  protected synchronized int readWithStrategy(
      ReaderStrategy strategy) throws IOException {
    dfsClient.checkOpen();
    if (closed.get()) {
      throw new IOException("Stream closed");
    }

    if (pos >= getFileLength()) {
      return -1;
    }

    Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap =
        new ConcurrentHashMap<>();

    try {
      if (pos > blockEnd) {
        blockSeekTo(pos);
      }
      int realLen = (int) Math.min(strategy.getTargetLength(),
          (blockEnd - pos + 1L));
      synchronized (infoLock) {
        if (locatedBlocks.isLastBlockComplete()) {
          realLen = (int) Math.min(realLen,
              locatedBlocks.getFileLength() - pos);
        }
      }

      /** Number of bytes already read into buffer */
      int result = 0;
      ByteBuffer targetBuffer = strategy.getReadBuffer();
      ByteBuffer tmpBuffer = targetBuffer.duplicate();
      tmpBuffer.limit(tmpBuffer.position() + realLen);
      readStripes(tmpBuffer.slice(), corruptedBlockMap);
      result = realLen;
      targetBuffer.position(targetBuffer.position() + realLen);
      pos += realLen;

      if (dfsClient.stats != null) {
        dfsClient.stats.incrementBytesRead(result);
      }
      return result;
    } finally {
      // Check if need to report block replicas corruption either read
      // was successful or ChecksumException occured.
      reportCheckSumFailure(corruptedBlockMap,
          currentLocatedBlock.getLocations().length);
    }
  }

  /**
   * The super method {@link DFSInputStream#refreshLocatedBlock} refreshes
   * cached LocatedBlock by executing {@link DFSInputStream#getBlockAt} again.
   * This method extends the logic by first remembering the index of the
   * internal block, and re-parsing the refreshed block group with the same
   * index.
   */
  @Override
  protected LocatedBlock refreshLocatedBlock(LocatedBlock block)
      throws IOException {
    int idx = BlockIdManager.getBlockIndex(block.getBlock().getLocalBlock());
    LocatedBlock lb = getBlockGroupAt(block.getStartOffset());
    // If indexing information is returned, iterate through the index array
    // to find the entry for position idx in the group
    LocatedStripedBlock lsb = (LocatedStripedBlock) lb;
    int i = 0;
    for (; i < lsb.getBlockIndices().length; i++) {
      if (lsb.getBlockIndices()[i] == idx) {
        break;
      }
    }
    if (DFSClient.LOG.isDebugEnabled()) {
      DFSClient.LOG.debug("refreshLocatedBlock for striped blocks, offset="
          + block.getStartOffset() + ". Obtained block " + lb + ", idx=" + idx);
    }
    return StripedBlockUtil.constructInternalBlock(
        lsb, i, cellSize, dataBlkNum, idx);
  }

  private LocatedStripedBlock getBlockGroupAt(long offset) throws IOException {
    LocatedBlock lb = super.getBlockAt(offset);
    assert lb instanceof LocatedStripedBlock : "NameNode" +
        " should return a LocatedStripedBlock for a striped file";
    return (LocatedStripedBlock)lb;
  }

  /**
   * Real implementation of pread.
   */
  @Override
  protected void fetchBlockByteRange(LocatedBlock block, long start,
      long end, ByteBuffer buf,
      Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap)
      throws IOException {
    // Refresh the striped block group
    LocatedStripedBlock blockGroup = getBlockGroupAt(block.getStartOffset());

    AlignedStripe[] stripes = StripedBlockUtil.divideByteRangeIntoStripes(
        ecPolicy, cellSize, blockGroup, start, end, buf);

    CompletionService<Void> readService = new ExecutorCompletionService<>(
        dfsClient.getStripedReadsThreadPool());

    StripeReader stripeReader = new StripeReader(readService,
        blockGroup, corruptedBlockMap);
    try {
      for (AlignedStripe stripe : stripes) {
        try {
          stripeReader.readStripe(stripe);
        } finally {
          stripeReader.reset();
        }
      }
    } finally {
      stripeReader.release();
    }
  }

  /**
   * The reader for reading a complete {@link AlignedStripe}. Note that an
   * {@link AlignedStripe} may cross multiple stripes with cellSize width.
   */
  private class StripeReader {
    final Map<Future<Void>, Integer> futures = new HashMap<>();
    final CompletionService<Void> service;
    final LocatedBlock[] targetBlocks;
    final Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap;
    final BlockReaderInfo[] readerInfos;
    final ECChunk[] decodeInputs;
    final ECChunk[] decodeOutputs;
    final int[] decodeIndices;
    ByteBuffer codingBuffer;
    AlignedStripe alignedStripe;

    StripeReader(CompletionService<Void> service,
                 LocatedStripedBlock blockGroup,
        Map<ExtendedBlock, Set<DatanodeInfo>> corruptedBlockMap) {
      this.service = service;
      this.targetBlocks = StripedBlockUtil.parseStripedBlockGroup(
          blockGroup, cellSize, dataBlkNum, parityBlkNum);
      this.readerInfos = new BlockReaderInfo[groupSize];
      this.corruptedBlockMap = corruptedBlockMap;
      this.decodeInputs = new ECChunk[dataBlkNum + parityBlkNum];
      this.decodeOutputs = new ECChunk[parityBlkNum];
      this.decodeIndices = new int[parityBlkNum];
    }

    void updateState4SuccessRead(StripingChunkReadResult result) {
      Preconditions.checkArgument(
          result.state == StripingChunkReadResult.SUCCESSFUL);
      readerInfos[result.index].setOffset(alignedStripe.getOffsetInBlock()
          + alignedStripe.getSpanInBlock());
    }

    void checkMissingBlocks() throws IOException {
      if (alignedStripe.missingChunksNum > parityBlkNum) {
        clearFutures(futures.keySet());
        throw new IOException(alignedStripe.missingChunksNum
            + " missing blocks, the stripe is: " + alignedStripe);
      }
    }

    /**
     * We need decoding. Thus go through all the data chunks and make sure we
     * submit read requests for all of them.
     */
    void readDataForDecoding() throws IOException {
      prepareDecodeInputs();
      for (int i = 0; i < dataBlkNum; i++) {
        Preconditions.checkNotNull(alignedStripe.chunks[i]);
        if (alignedStripe.chunks[i].state == StripingChunk.REQUESTED) {
          if (!readChunk(targetBlocks[i], i)) {
            alignedStripe.missingChunksNum++;
          }
        }
      }
      checkMissingBlocks();
    }

    void readParityChunks(int num) throws IOException {
      for (int i = dataBlkNum, j = 0; i < dataBlkNum + parityBlkNum && j < num;
           i++) {
        if (alignedStripe.chunks[i] == null) {
          if (prepareParityChunk(i) && readChunk(targetBlocks[i], i)) {
            j++;
          } else {
            alignedStripe.missingChunksNum++;
          }
        }
      }
      checkMissingBlocks();
    }

    boolean createBlockReader(LocatedBlock block, int chunkIndex)
        throws IOException {
      BlockReader reader = null;
      final ReaderRetryPolicy retry = new ReaderRetryPolicy();
      DNAddrPair dnInfo = new DNAddrPair(null, null, null);

      while(true) {
        try {
          // the cached block location might have been re-fetched, so always
          // get it from cache.
          block = refreshLocatedBlock(block);
          targetBlocks[chunkIndex] = block;

          // internal block has one location, just rule out the deadNodes
          dnInfo = getBestNodeDNAddrPair(block, null);
          if (dnInfo == null) {
            break;
          }
          reader = getBlockReader(block, alignedStripe.getOffsetInBlock(),
              block.getBlockSize() - alignedStripe.getOffsetInBlock(),
              dnInfo.addr, dnInfo.storageType, dnInfo.info);
        } catch (IOException e) {
          if (e instanceof InvalidEncryptionKeyException &&
              retry.shouldRefetchEncryptionKey()) {
            DFSClient.LOG.info("Will fetch a new encryption key and retry, "
                + "encryption key was invalid when connecting to " + dnInfo.addr
                + " : " + e);
            dfsClient.clearDataEncryptionKey();
            retry.refetchEncryptionKey();
          } else if (retry.shouldRefetchToken() &&
              tokenRefetchNeeded(e, dnInfo.addr)) {
            fetchBlockAt(block.getStartOffset());
            retry.refetchToken();
          } else {
            //TODO: handles connection issues
            DFSClient.LOG.warn("Failed to connect to " + dnInfo.addr + " for " +
                "block" + block.getBlock(), e);
            // re-fetch the block in case the block has been moved
            fetchBlockAt(block.getStartOffset());
            addToDeadNodes(dnInfo.info);
          }
        }
        if (reader != null) {
          readerInfos[chunkIndex] = new BlockReaderInfo(reader, block,
              dnInfo.info, alignedStripe.getOffsetInBlock());
          return true;
        }
      }
      return false;
    }

    ByteBufferStrategy[] getReadStrategies(StripingChunk chunk) {
      if (chunk.useByteBuffer()) {
        ByteBufferStrategy strategy = new ByteBufferStrategy(chunk.getByteBuffer());
        return new ByteBufferStrategy[]{strategy};
      } else {
        ByteBufferStrategy[] strategies =
            new ByteBufferStrategy[chunk.getChunkBuffer().getSlices().size()];
        for (int i = 0; i < strategies.length; i++) {
          ByteBuffer buffer = chunk.getChunkBuffer().getSlice(i);
          strategies[i] = new ByteBufferStrategy(buffer);
        }
        return strategies;
      }
    }

    boolean readChunk(final LocatedBlock block, int chunkIndex)
        throws IOException {
      final StripingChunk chunk = alignedStripe.chunks[chunkIndex];
      if (block == null) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }
      if (readerInfos[chunkIndex] == null) {
        if (!createBlockReader(block, chunkIndex)) {
          chunk.state = StripingChunk.MISSING;
          return false;
        }
      } else if (readerInfos[chunkIndex].shouldSkip) {
        chunk.state = StripingChunk.MISSING;
        return false;
      }

      chunk.state = StripingChunk.PENDING;
      Callable<Void> readCallable = readCells(readerInfos[chunkIndex].reader,
          readerInfos[chunkIndex].datanode,
          readerInfos[chunkIndex].blockReaderOffset,
          alignedStripe.getOffsetInBlock(), getReadStrategies(chunk),
          block.getBlock(), corruptedBlockMap);

      Future<Void> request = service.submit(readCallable);
      futures.put(request, chunkIndex);
      return true;
    }

    /** read the whole stripe. do decoding if necessary */
    void readStripe(AlignedStripe stripe) throws IOException {
      this.alignedStripe = stripe;

      for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state != StripingChunk.ALLZERO) {
          if (!readChunk(targetBlocks[i], i)) {
            alignedStripe.missingChunksNum++;
          }
        }
      }

      // There are missing block locations at this stage. Thus we need to read
      // the full stripe and one more parity block.
      if (alignedStripe.missingChunksNum > 0) {
        checkMissingBlocks();
        readDataForDecoding();
        // read parity chunks
        readParityChunks(alignedStripe.missingChunksNum);
      }
      // TODO: for a full stripe we can start reading (dataBlkNum + 1) chunks

      // Input buffers for potential decode operation, which remains null until
      // first read failure
      while (!futures.isEmpty()) {
        try {
          StripingChunkReadResult r = StripedBlockUtil
              .getNextCompletedStripedRead(service, futures, 0);
          if (DFSClient.LOG.isDebugEnabled()) {
            DFSClient.LOG.debug("Read task returned: " + r + ", for stripe "
                + alignedStripe);
          }
          StripingChunk returnedChunk = alignedStripe.chunks[r.index];
          Preconditions.checkNotNull(returnedChunk);
          Preconditions.checkState(returnedChunk.state == StripingChunk.PENDING);

          if (r.state == StripingChunkReadResult.SUCCESSFUL) {
            returnedChunk.state = StripingChunk.FETCHED;
            alignedStripe.fetchedChunksNum++;
            updateState4SuccessRead(r);
            if (alignedStripe.fetchedChunksNum == dataBlkNum) {
              clearFutures(futures.keySet());
              break;
            }
          } else {
            returnedChunk.state = StripingChunk.MISSING;
            // close the corresponding reader
            closeReader(readerInfos[r.index]);

            final int missing = alignedStripe.missingChunksNum;
            alignedStripe.missingChunksNum++;
            checkMissingBlocks();

            readDataForDecoding();
            readParityChunks(alignedStripe.missingChunksNum - missing);
          }
        } catch (InterruptedException ie) {
          String err = "Read request interrupted";
          DFSClient.LOG.error(err);
          clearFutures(futures.keySet());
          // Don't decode if read interrupted
          throw new InterruptedIOException(err);
        }
      }

      if (alignedStripe.missingChunksNum > 0) {
        decode();
      }
    }

    void prepareDecodeInputs() {
      if (codingBuffer == null) {
        initDecodeInputs(alignedStripe);
      }
    }

    boolean prepareParityChunk(int index) {
      Preconditions.checkState(index >= dataBlkNum &&
          alignedStripe.chunks[index] == null);
      alignedStripe.chunks[index] = new StripingChunk(decodeInputs[index].getBuffer());
      return true;
    }

    void decode() {
      finalizeDecodeInputs(alignedStripe);
      decodeAndFillBuffer(alignedStripe);
    }

    /**
     * Some fetched {@link StripingChunk} might be stored in original application
     * buffer instead of prepared decode input buffers. Some others are beyond
     * the range of the internal blocks and should correspond to all zero bytes.
     * When all pending requests have returned, this method should be called to
     * finalize decode input buffers.
     */
    void finalizeDecodeInputs(AlignedStripe alignedStripe) {
      for (int i = 0; i < alignedStripe.chunks.length; i++) {
        final StripingChunk chunk = alignedStripe.chunks[i];
        if (chunk != null && chunk.state == StripingChunk.FETCHED) {
          if (chunk.useChunkBuffer()) {
            chunk.getChunkBuffer().copyTo(decodeInputs[i].getBuffer());
          }
        } else if (chunk != null && chunk.state == StripingChunk.ALLZERO) {
            decodeInputs[i].setAllZero(true);
        } else {
          decodeInputs[i] = null;
        }
      }
    }

    /**
     * Initialize the decoding input buffers based on the chunk states in an
     * {@link AlignedStripe}. For each chunk that was not initially requested,
     * schedule a new fetch request with the decoding input buffer as transfer
     * destination.
     */
    void initDecodeInputs(AlignedStripe alignedStripe) {
      int bufLen = (int) alignedStripe.getSpanInBlock();
      int bufCount = decodeInputs.length + decodeOutputs.length;
      codingBuffer = bufferPool.getBuffer(useDirectBuffer(), bufLen * bufCount);

      ByteBuffer buffer;
      int idx = 0;
      for (int i = 0; i < decodeInputs.length; i++, idx++) {
        buffer = codingBuffer.duplicate();
        decodeInputs[i] = new ECChunk(buffer, idx * bufLen, bufLen);
      }
      for (int i = 0; i < decodeOutputs.length; i++, idx++) {
        buffer = codingBuffer.duplicate();
        decodeOutputs[i] = new ECChunk(buffer, idx * bufLen, bufLen);
      }

      for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] == null) {
          alignedStripe.chunks[i] =
              new StripingChunk(decodeInputs[i].getBuffer());
        }
      }
    }

    /**
     * Decode based on the given input buffers and erasure coding policy.
     */
    void decodeAndFillBuffer(AlignedStripe alignedStripe) {
      // Step 1: prepare indices and output buffers for missing data units
      int pos = 0;
      for (int i = 0; i < dataBlkNum; i++) {
        if (alignedStripe.chunks[i] != null &&
            alignedStripe.chunks[i].state == StripingChunk.MISSING) {
          decodeIndices[pos++] = i;
        }
      }

      int[] erasedIndexes = Arrays.copyOf(decodeIndices, pos);
      ECChunk[] outputs = Arrays.copyOf(decodeOutputs, pos);

      // Step 2: decode into prepared output buffers
      decoder.decode(decodeInputs, erasedIndexes, outputs);

      // Step 3: fill original application buffer with decoded data
      for (int i = 0; i < erasedIndexes.length; i++) {
        int missingBlkIdx = erasedIndexes[i];
        StripingChunk chunk = alignedStripe.chunks[missingBlkIdx];
        if (chunk.state == StripingChunk.MISSING && chunk.useChunkBuffer()) {
          chunk.getChunkBuffer().copyFrom(decodeOutputs[i].getBuffer());
        }
      }
    }

    void reset() {
      for (int i = 0; i < decodeInputs.length; i++) {
        decodeInputs[i] = null;
      }

      for (int i = 0; i < decodeOutputs.length; i++) {
        decodeOutputs[i] = null;
      }

      if (codingBuffer != null) {
        bufferPool.putBuffer(codingBuffer);
        codingBuffer = null;
      }
    }

    void release() {
      for (BlockReaderInfo preaderInfo : readerInfos) {
        closeReader(preaderInfo);
      }
    }
  }

  /**
   * May need online read recovery, zero-copy read doesn't make
   * sense, so don't support it.
   */
  @Override
  public synchronized ByteBuffer read(ByteBufferPool bufferPool,
      int maxLength, EnumSet<ReadOption> opts)
          throws IOException, UnsupportedOperationException {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  @Override
  public synchronized void releaseBuffer(ByteBuffer buffer) {
    throw new UnsupportedOperationException(
        "Not support enhanced byte buffer access.");
  }

  /** A variation to {@link DFSInputStream#cancelAll} */
  private void clearFutures(Collection<Future<Void>> futures) {
    for (Future<Void> future : futures) {
      future.cancel(false);
    }
    futures.clear();
  }
}
