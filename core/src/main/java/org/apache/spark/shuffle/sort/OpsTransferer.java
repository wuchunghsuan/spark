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

import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.OpsPointer;

final class OpsTransferer<K, V> extends Thread {

  private boolean stopped = false;

  private final OpsPreShuffleWriter<K, V> masterWriter;
  private final int id;
  private final SparkConf conf;

  public OpsTransferer(OpsPreShuffleWriter<K, V> master, int id, SparkConf conf) {
    this.masterWriter = master;
    this.id = id;
    this.conf = conf;
  }

  @Override
  public void run() {
    try {
      while (!this.stopped && !Thread.currentThread().isInterrupted()) {
        MemoryBlock page = null;
        try {
          // Get a page
          page = this.masterWriter.getPage(id);
          // Shuffle
          shuffle(page);
          this.masterWriter.freeSharedPage(page);  
          page = null;    
        } finally {
          if (page != null) {
            this.masterWriter.freeSharedPage(page);      
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  public void shuffle(MemoryBlock page) {
    final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    final int diskWriteBufferSize = 1024 * 1024;
    final byte[] writeBuffer = new byte[diskWriteBufferSize];

    for (OpsPointer pointer : page.pointers) {
      // DiskBlockObjectWriter writer = partitionWriters[pointer.partitionId];
      final Object recordPage = page.getBaseObject();
      final long recordOffsetInPage = pointer.pageOffset;
      // int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
      long dataRemaining = pointer.length;
      long recordReadPosition = recordOffsetInPage;
      while (dataRemaining > 0) {
        final int toTransfer = (int)Math.min(diskWriteBufferSize, dataRemaining);
        // Platform.copyMemory(
        //   recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
        // writer.write(writeBuffer, 0, toTransfer);
        recordReadPosition += toTransfer;
        dataRemaining -= toTransfer;
      }
      // writer.recordWritten();
    }
  }

  private void transfer() throws IllegalArgumentException {

    // logger.info("getPendingShuffle: task " + shuffle.getTask().getTaskId() + " to node "
    //         + shuffle.getDstNode().getIp());

    // HashMap<String, IndexReader> irMap = this.shuffleHandler.getIndexReaderMap(shuffle.getTask().getJobId());
    // IndexReader indexReader = irMap.get(shuffle.getTask().getTaskId());
    // IndexRecord record = indexReader.getIndex(shuffle.getNum());
        
    // ManagedChannel channel = ManagedChannelBuilder
    // .forAddress(shuffle.getDstNode().getIp(), opsConf.getPortWorkerGRPC()).usePlaintext().build();
    // OpsInternalGrpc.OpsInternalStub asyncStub = OpsInternalGrpc.newStub(channel);

    // String path = OpsUtils.getMapOutputPath(shuffle.getTask().getJobId(), shuffle.getTask().getTaskId(),
    // shuffle.getNum());

    // long start = System.currentTimeMillis();

    // StreamObserver<Chunk> requestObserver = asyncStub.transfer(new StreamObserver<ParentPath>() {
    //     String parentPath = "";
        
    //     @Override
    //     public void onNext(ParentPath path) {
    //         logger.debug("ParentPath: " + path.getPath());
    //         parentPath = path.getPath();
    //     }
        
    //     @Override
    //     public void onError(Throwable t) {
    //         logger.error("gRPC error.", t.getMessage());
    //         logger.info("gRPC channel break down. Re-addPendingShuffle.");
    //         shuffleHandler.addPendingShuffles(shuffle);
    //         try {
    //             channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
    //         } catch (Exception e) {
    //             e.printStackTrace();
    //             //TODO: handle exception
    //         }
    //     }
        
    //     @Override
    //     public void onCompleted() {
    //         logger.debug("Transfer completed.");

    //         long duration = System.currentTimeMillis() - start;
    //         logger.info("[OPS]-" + shuffle.getTask().getJobId() + "-" + start + "-" + duration + "-" + shuffle.getData().length);
        
    //         HadoopPath hadoopPath = new HadoopPath(new File(parentPath, path).toString(), record.getPartLength(),
    //         record.getRawLength());
    //         ShuffleCompletedConf shuffleC = new ShuffleCompletedConf(new ShuffleConf(shuffle.getTask(), shuffle.getDstNode(), shuffle.getNum()), hadoopPath);
    //         shuffleHandler.addPendingShuffleHandlerTask(new ShuffleHandlerTask(shuffleC));
    //         try {
    //             channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    //         } catch (Exception e) {
    //             e.printStackTrace();
    //             //TODO: handle exception
    //         }
    //     }
    // });
        
    // try {
    //     Chunk chunk = Chunk.newBuilder().setIsFirstChunk(true).setPath(path)
    //             .setContent(ByteString.copyFrom(shuffle.getData(), 0, shuffle.getData().length)).build();
    //     logger.debug("Transfer data. Length: " + shuffle.getData().length);
    //     requestObserver.onNext(chunk);
    //     requestObserver.onCompleted();

    // } catch (RuntimeException e) {
    //     // Cancel RPC
    //     e.printStackTrace();
    //     requestObserver.onError(e);
    //     throw e;
    // } catch (Exception e) {
    //     e.printStackTrace();
    // }
  }

  public void shutDown() {
    this.stopped = true;
    try {
      interrupt();
      join(5000);
    } catch (Exception ie) {
      ie.printStackTrace();
    }
  }
}
