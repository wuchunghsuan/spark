/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.shuffle.sort;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.LinkedList;

import scala.Tuple2;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.executor.ShuffleWriteMetrics;
import org.apache.spark.internal.config.package$;
import org.apache.spark.memory.MemoryConsumer;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.memory.TaskMemoryManager;
import org.apache.spark.memory.TooLargePageException;
import org.apache.spark.serializer.DummySerializerInstance;
import org.apache.spark.serializer.SerializerInstance;
import org.apache.spark.storage.BlockManager;
import org.apache.spark.storage.DiskBlockObjectWriter;
import org.apache.spark.storage.FileSegment;
import org.apache.spark.storage.TempShuffleBlockId;
import org.apache.spark.unsafe.Platform;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.array.LongArray;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.OpsPointer;
import org.apache.spark.util.Utils;

/**
 * An external sorter that is specialized for sort-based shuffle.
 * <p>
 * Incoming records are appended to data pages. When all records have been inserted (or when the
 * current thread's shuffle memory limit is reached), the in-memory records are sorted according to
 * their partition ids (using a {@link ShuffleInMemorySorter}). The sorted records are then
 * written to a single output file (or multiple files, if we've spilled). The format of the output
 * files is the same as the format of the final output file written by
 * {@link org.apache.spark.shuffle.sort.SortShuffleWriter}: each output partition's records are
 * written as a single serialized, compressed stream that can be read with a new decompression and
 * deserialization stream.
 * <p>
 * Unlike {@link org.apache.spark.util.collection.ExternalSorter}, this sorter does not merge its
 * spill files. Instead, this merging is performed in {@link UnsafeShuffleWriter}, which uses a
 * specialized merge procedure that avoids extra serialization/deserialization.
 */
final class OpsSharedManager extends MemoryConsumer {

  private static final Logger logger = LoggerFactory.getLogger(ShuffleExternalSorter.class);

  @VisibleForTesting
  static final int DISK_WRITE_BUFFER_SIZE = 1024 * 1024;

  private final int numPartitions;
  private final TaskMemoryManager taskMemoryManager;
  private final BlockManager blockManager;
  private final TaskContext taskContext;
  private final ShuffleWriteMetrics writeMetrics;

  /**
   * Force this sorter to spill when there are this many elements in memory.
   */
  private final int numElementsForSpillThreshold;

  /** The buffer size to use when writing spills using DiskBlockObjectWriter */
  private final int fileBufferSizeBytes;

  /** The buffer size to use when writing the sorted records to an on-disk file */
  private final int diskWriteBufferSize;

  /**
   * Memory pages that hold the records being sorted. The pages in this list are freed when
   * spilling, although in principle we could recycle these pages across spills (on the other hand,
   * this might not be necessary if we maintained a pool of re-usable pages in the TaskMemoryManager
   * itself).
   */
  // private final LinkedList<MemoryBlock> allocatedPages = new LinkedList<>();

  private final LinkedList<SpillInfo> spills = new LinkedList<>();

  // private final LinkedList<Long> addressList = new LinkedList<>();

  /** Peak memory used by this sorter so far, in bytes. **/
  private long peakMemoryUsedBytes;

  // These variables are reset after spilling:
  // @Nullable private ShuffleInMemorySorter inMemSorter;
  @Nullable private MemoryBlock currentPage = null;
  private long pageCursor = -1;

  OpsSharedManager(
      TaskMemoryManager memoryManager,
      BlockManager blockManager,
      TaskContext taskContext,
      int initialSize,
      int numPartitions,
      SparkConf conf,
      ShuffleWriteMetrics writeMetrics) {
    super(memoryManager,
      (int) Math.min(PackedRecordPointer.MAXIMUM_PAGE_SIZE_BYTES, memoryManager.pageSizeBytes()),
      memoryManager.getTungstenMemoryMode());
    this.taskMemoryManager = memoryManager;
    this.blockManager = blockManager;
    this.taskContext = taskContext;
    this.numPartitions = numPartitions;
    // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
    this.fileBufferSizeBytes =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_FILE_BUFFER_SIZE()) * 1024;
    this.numElementsForSpillThreshold =
        (int) conf.get(package$.MODULE$.SHUFFLE_SPILL_NUM_ELEMENTS_FORCE_SPILL_THRESHOLD());
    this.writeMetrics = writeMetrics;
    // this.inMemSorter = new ShuffleInMemorySorter(
    //   this, initialSize, conf.getBoolean("spark.shuffle.sort.useRadixSort", true));
    // this.peakMemoryUsedBytes = getMemoryUsage();
    this.diskWriteBufferSize =
        (int) (long) conf.get(package$.MODULE$.SHUFFLE_DISK_WRITE_BUFFER_SIZE());
  }
  
  /**
   * Sort and spill the current records in response to memory pressure.
   */
  @Override
  public long spill(long size, MemoryConsumer trigger) throws IOException {
    // if (trigger != this || inMemSorter == null || inMemSorter.numRecords() == 0) {
    //   return 0L;
    // }

    // logger.info("Thread {} spilling sort data of {} to disk ({} {} so far)",
    //   Thread.currentThread().getId(),
    //   Utils.bytesToString(getMemoryUsage()),
    //   spills.size(),
    //   spills.size() > 1 ? " times" : " time");

    // writeSortedFile(false);
    // final long spillSize = freeMemory();
    // inMemSorter.reset();
    // // Reset the in-memory sorter's pointer array only after freeing up the memory pages holding the
    // // records. Otherwise, if the task is over allocated memory, then without freeing the memory
    // // pages, we might not be able to get memory for the pointer array.
    // taskContext.taskMetrics().incMemoryBytesSpilled(spillSize);
    // return spillSize;
    return 0;
  }

  // private long getMemoryUsage() {
  //   // long totalPageSize = 0;
  //   // for (MemoryBlock page : allocatedPages) {
  //   //   totalPageSize += page.size();
  //   // }
  //   // return ((inMemSorter == null) ? 0 : inMemSorter.getMemoryUsage()) + totalPageSize;
  //   return 0;
  // }

  private void updatePeakMemoryUsed() {
  //   long mem = getMemoryUsage();
  //   if (mem > peakMemoryUsedBytes) {
  //     peakMemoryUsedBytes = mem;
  //   }
  }

  /**
   * Return the peak memory used so far, in bytes.
   */
  // long getPeakMemoryUsedBytes() {
  //   updatePeakMemoryUsed();
  //   return peakMemoryUsedBytes;
  // }

  // private long freeMemory() {
  //   updatePeakMemoryUsed();
  //   long memoryFreed = 0;
  //   for (MemoryBlock block : allocatedPages) {
  //     memoryFreed += block.size();
  //     freePage(block);
  //   }
  //   allocatedPages.clear();
  //   currentPage = null;
  //   pageCursor = 0;
  //   return memoryFreed;
  // }

  /**
   * Force all memory and spill files to be deleted; called by shuffle error-handling code.
   */
  public void cleanupResources() {
    // freeMemory();
    // if (inMemSorter != null) {
    //   inMemSorter.free();
    //   inMemSorter = null;
    // }
    // for (SpillInfo spill : spills) {
    //   if (spill.file.exists() && !spill.file.delete()) {
    //     logger.error("Unable to delete spill file {}", spill.file.getPath());
    //   }
    // }
  }

  /**
   * Checks whether there is enough space to insert an additional record in to the sort pointer
   * array and grows the array if additional space is required. If the required space cannot be
   * obtained, then the in-memory data will be spilled to disk.
   */
  // private void growPointerArrayIfNecessary() throws IOException {
  //   assert(inMemSorter != null);
  //   if (!inMemSorter.hasSpaceForAnotherRecord()) {
  //     long used = inMemSorter.getMemoryUsage();
  //     LongArray array;
  //     try {
  //       // could trigger spilling
  //       array = allocateArray(used / 8 * 2);
  //     } catch (TooLargePageException e) {
  //       // The pointer array is too big to fix in a single page, spill.
  //       spill();
  //       return;
  //     } catch (SparkOutOfMemoryError e) {
  //       // should have trigger spilling
  //       if (!inMemSorter.hasSpaceForAnotherRecord()) {
  //         logger.error("Unable to grow the pointer array");
  //         throw e;
  //       }
  //       return;
  //     }
  //     // check if spilling is triggered or not
  //     if (inMemSorter.hasSpaceForAnotherRecord()) {
  //       freeArray(array);
  //     } else {
  //       inMemSorter.expandPointerArray(array);
  //     }
  //   }
  // }

  /**
   * Allocates more memory in order to insert an additional record. This will request additional
   * memory from the memory manager and spill if the requested memory can not be obtained.
   *
   * @param required the required space in the data page, in bytes, including space for storing
   *                      the record size. This must be less than or equal to the page size (records
   *                      that exceed the page size are handled via a different code path which uses
   *                      special overflow pages).
   */
  private boolean acquireNewPageIfNecessary(int required) {
    if (currentPage == null ||
      pageCursor + required > currentPage.getBaseOffset() + currentPage.size() ) {
        // TODO: try to find space in previous pages
        currentPage = allocatePage(required);
        pageCursor = currentPage.getBaseOffset();
        System.out.println("OpsSharedManager: allocate new page!!! pageCursor: " + pageCursor);
      // allocatedPages.add(currentPage);
      return true;
    }
    return false;
  }

  /**
   * Write a record to the shuffle sorter.
   */
  public void insertRecord(Object recordBase, long recordOffset, int length, int partitionId)
    throws IOException {

    // for tests
    // assert(inMemSorter != null);
    // if (inMemSorter.numRecords() >= numElementsForSpillThreshold) {
    //   logger.info("Spilling data because number of spilledRecords crossed the threshold " +
    //     numElementsForSpillThreshold);
    //   spill();
    // }

    // growPointerArrayIfNecessary();
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // Need 4 or 8 bytes to store the record length.
    final int required = length + uaoSize;
    boolean isFirst = acquireNewPageIfNecessary(required);

    assert(currentPage != null);
    final Object base = currentPage.getBaseObject();
    // final long recordAddress = taskMemoryManager.encodePageNumberAndOffset(currentPage, pageCursor);

    currentPage.pointers.add(new OpsPointer(pageCursor, partitionId));

    UnsafeAlignedOffset.putSize(base, pageCursor, length);
    // long tmp = pageCursor;
    pageCursor += uaoSize;
    Platform.copyMemory(recordBase, recordOffset, base, pageCursor, length);
    pageCursor += length;
    if(isFirst) {
      // System.out.println("putSize: " + length + "getSize: " + UnsafeAlignedOffset.getSize(base, tmp));
      System.out.println("Add first record length: " + length + " size: " + currentPage.size());
    }
    // addressList.add(recordAddress);
    // inMemSorter.insertRecord(recordAddress, partitionId);
  }

  /**
   * Close the sorter, causing any buffered data to be sorted and written out to disk.
   *
   * @return metadata for the spill files written by this sorter. If no records were ever inserted
   *         into this sorter, then this will return an empty array.
   * @throws IOException
   */
  // public SpillInfo[] closeAndGetSpills() throws IOException {
  //   // if (inMemSorter != null) {
  //   //   // Do not count the final file towards the spill count.
  //   //   writeSortedFile(true);
  //   //   freeMemory();
  //   //   inMemSorter.free();
  //   //   inMemSorter = null;
  //   // }
  //   return spills.toArray(new SpillInfo[spills.size()]);
  // }

}
