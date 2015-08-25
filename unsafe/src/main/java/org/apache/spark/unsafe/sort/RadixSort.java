package org.apache.spark.unsafe.sort;

import java.util.Arrays;
import java.util.List;


public class RadixSort {

  public static final int NUM_WORDS_PER_CHUNK = 2048;
  public static final int NUM_WORDS_PER_RECORD = 2;

  public static List<ChunkedArray> sort(ChunkedArray input) {
    if (input.getNumWordsPerRecord() != 2) {
      throw new IllegalArgumentException(
        "num words per record should be 2, but got " + input.getNumWordsPerRecord());
    }

    ChunkedArray[] inputs = {input};
    for (int byteNumber = 0; byteNumber < 10; byteNumber++) {
      Shuffle shuffle = new Shuffle(byteNumber);
      for (int i = 0; i < inputs.length; i++) {
        shuffle.insert(inputs[i]);
      }
      inputs = shuffle.getBuckets();
    }

    return Arrays.asList(inputs);
  }

  private static class Shuffle {

    private final int bitOffset;
    private final long mask;

    private final ChunkedArray[] buckets;

    Shuffle(int byteNumber) {
      bitOffset = byteNumber * 8;
      mask = (1L << 8) - 1;

      buckets = new ChunkedArray[256];
      for (int i = 0; i < 256; i++) {
        buckets[i] = new ChunkedArray(NUM_WORDS_PER_CHUNK, NUM_WORDS_PER_RECORD);
      }
    }

    void insert(ChunkedArray input) {
      for (LongArray chunk : input.getChunks()) {
        final int size = chunk.size();
        for (int i = 0; i < size; i += NUM_WORDS_PER_RECORD) {
          long pointer = chunk.get(i);
          long payload = chunk.get(i + 1);
          int b = (int) ((payload >>> bitOffset) & mask);
          buckets[b].append(pointer, payload);
        }
      }

//      input.foreach(new ChunkedArray.ForeachFunction() {
//        @Override
//        void apply(LongArray chunk, int index) {
//          long pointer = chunk.get(index);
//          long payload = chunk.get(index + 1);
//          int b = (int) ((payload >>> bitOffset) & mask);
//          buckets[b].append(pointer, payload);
//        }
//      });
    }

    ChunkedArray[] getBuckets() {
      return buckets;
    }
  }
}
