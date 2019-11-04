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

import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.TimeUnit;

import org.apache.spark.SparkConf;
import org.apache.spark.unsafe.UnsafeAlignedOffset;
import org.apache.spark.unsafe.memory.MemoryBlock;
import org.apache.spark.unsafe.memory.OpsPointer;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import com.google.protobuf.ByteString;

final class OpsTransferer<K, V> extends Thread {

  private boolean stopped = false;

  private final OpsPreShuffleWriter<K, V> masterWriter;
  private final int id;
  private final String targetIp;
  private final int targetPort;
  private final ManagedChannel channel;
  private final StreamObserver<Page> requestObserver;

  public OpsTransferer(OpsPreShuffleWriter<K, V> master, int id, String ip, int port) {
    this.masterWriter = master;
    this.id = id;
    this.targetIp = ip;
    this.targetPort = port;

    System.out.println("OpsTransferer start. Target: " + ip + ":" + port);

    channel = ManagedChannelBuilder.forAddress(ip, port).usePlaintext().build();
    OpsShuffleDataGrpc.OpsShuffleDataStub asyncStub = OpsShuffleDataGrpc.newStub(channel);

    this.requestObserver = asyncStub.transfer(new StreamObserver<Ack>() {

      @Override
      public void onNext(Ack ack) {

      }
      
      @Override
      public void onError(Throwable t) {
          System.out.println("gRPC error" + t.getMessage());
          // System.out.println("gRPC channel break down. Re-addPendingShuffle.");
          // shuffleHandler.addPendingShuffles(shuffle);
          try {
              channel.shutdown().awaitTermination(100, TimeUnit.MILLISECONDS);
          } catch (Exception e) {
              e.printStackTrace();
              //TODO: handle exception
          }
      }
      
      @Override
      public void onCompleted() {
          System.out.println("Transfer completed.");
      
          // ShuffleCompletedConf shuffleC = new ShuffleCompletedConf(new ShuffleConf(shuffle.getTask(), shuffle.getDstNode(), shuffle.getNum()), hadoopPath);
          // shuffleHandler.addPendingShuffleHandlerTask(new ShuffleHandlerTask(shuffleC));
          try {
              channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
          } catch (Exception e) {
              e.printStackTrace();
              //TODO: handle exception
          }
      }
    });
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

  private void shuffle(MemoryBlock block) throws IllegalArgumentException {
    // final int uaoSize = UnsafeAlignedOffset.getUaoSize();
    // final int diskWriteBufferSize = 1024 * 1024;
    // final byte[] writeBuffer = new byte[diskWriteBufferSize];

    // for (OpsPointer pointer : page.pointers) {
    //   // DiskBlockObjectWriter writer = partitionWriters[pointer.partitionId];
    //   final Object recordPage = page.getBaseObject();
    //   final long recordOffsetInPage = pointer.pageOffset;
    //   // int dataRemaining = UnsafeAlignedOffset.getSize(recordPage, recordOffsetInPage);
    //   long dataRemaining = pointer.length;
    //   long recordReadPosition = recordOffsetInPage;
    //   while (dataRemaining > 0) {
    //     final int toTransfer = (int)Math.min(diskWriteBufferSize, dataRemaining);
    //     // Platform.copyMemory(
    //     //   recordPage, recordReadPosition, writeBuffer, Platform.BYTE_ARRAY_OFFSET, toTransfer);
    //     // writer.write(writeBuffer, 0, toTransfer);
    //     recordReadPosition += toTransfer;
    //     dataRemaining -= toTransfer;
    //   }
    //   // writer.recordWritten();
    // }

    // long start = System.currentTimeMillis();
        
    // long duration = System.currentTimeMillis() - start;
    // logger.info("[OPS]-" + shuffle.getTask().getJobId() + "-" + start + "-" + duration + "-" + shuffle.getData().length);
    ByteArrayOutputStream bos = null;
    ObjectOutputStream oos = null;
    try {
      bos = new ByteArrayOutputStream();
      oos = new ObjectOutputStream(bos);
      oos.writeObject(block.pointers);
      oos.close();

      int i = 0;
      for (OpsPointer pointer : block.pointers) {
        if(i < 5) {
            break;
        }
        System.out.println("Serialize pointer " + i + " :" + pointer.pageOffset + ", " + pointer.partitionId + ", " + pointer.length);
        i++;
      }

      Page page = Page.newBuilder()
          .setContent(ByteString.copyFrom((byte[])block.getBaseObject()))
          .setPointers(ByteString.copyFrom(bos.toByteArray())).build();
      // logger.debug("Transfer data. Length: " + block.getBaseObject().length);
      this.requestObserver.onNext(page);
      // this.requestObserver.onCompleted();

    } catch (RuntimeException e) {
        // Cancel RPC
        e.printStackTrace();
        this.requestObserver.onError(e);
        throw e;
    } catch (Exception e) {
        e.printStackTrace();
    } finally {
      try {
        oos.close();
        bos.close();
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public void shutDown() {
    this.stopped = true;
    try {
      this.requestObserver.onCompleted();
      interrupt();
      join(5000);
    } catch (Exception ie) {
      ie.printStackTrace();
    }
  }
}
