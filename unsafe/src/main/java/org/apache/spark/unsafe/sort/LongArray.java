package org.apache.spark.unsafe.sort;


public final class LongArray {

  private final long[] arr;
  private final int capacity;
  private int index = 0;

  public LongArray(int numWords) {
    this.capacity = numWords;
    arr = new long[numWords];
  }

  public int size() {
    return index;
  }

  public long get(int i) {
    return arr[i];
  }

  /**
   * Appends a long value to the array. Returns true if there is more capacity.
   */
  public boolean append(long data) {
    arr[index] = data;
    index++;
    return index < capacity;
  }

  /**
   * Appends two longs to the array. Returns true if there is more capacity.
   */
  public boolean append(long data1, long data2) {
    arr[index] = data1;
    arr[index + 1] = data2;
    index += 2;
    return index < capacity;
  }
}
