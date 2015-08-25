package org.apache.spark.unsafe.sort;

import java.util.LinkedList;

public final class ChunkedArray {

  private final int numWordsPerChunk;
  private final int numWordsPerRecord;

  private final LinkedList<LongArray> chunks = new LinkedList<>();
  private LongArray currentChunk;

  public ChunkedArray(int numWordsPerChunk, int numWordsPerRecord) {
    this.numWordsPerChunk = numWordsPerChunk;
    this.numWordsPerRecord = numWordsPerRecord;
    addChunk();
  }

  public int getNumWordsPerChunk() {
    return numWordsPerChunk;
  }

  public int getNumWordsPerRecord() {
    return numWordsPerRecord;
  }

  public LinkedList<LongArray> getChunks() {
    return chunks;
  }

  public void free() {
    chunks.clear();
  }

  public void append(long data) {
    if (!currentChunk.append(data)) {
      addChunk();
    }
  }

  public void append(long data1, long data2) {
    if (!currentChunk.append(data1, data2)) {
      addChunk();
    }
  }

  private LongArray addChunk() {
    currentChunk = new LongArray(numWordsPerChunk);
    chunks.add(currentChunk);
    return currentChunk;
  }

  public void foreach(ForeachFunction func) {
    for (LongArray chunk : chunks) {
      final int size = chunk.size();
      for (int i = 0; i < size; i += numWordsPerRecord) {
        func.apply(chunk, i);
      }
    }
  }

  public static abstract class ForeachFunction {
    abstract void apply(LongArray chunk, int index);
  }
}
