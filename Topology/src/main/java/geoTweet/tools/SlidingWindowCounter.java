
package geoTweet.tools;

import java.io.Serializable;
import java.util.Map;

/**
 * This class counts objects in a sliding window fashion.
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
*/

public final class SlidingWindowCounter<T> implements Serializable {

  private SlotBasedCounter<T> objCounter;
  private int headSlot;
  private int tailSlot;
  private int windowLengthInSlots;

  public SlidingWindowCounter(int windowLengthInSlots) {
    if (windowLengthInSlots < 2) {
      throw new IllegalArgumentException(
          "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
    }
    this.windowLengthInSlots = windowLengthInSlots;
    this.objCounter = new SlotBasedCounter<T>(this.windowLengthInSlots);

    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void incrementCount(T obj) {
    objCounter.incrementCount(obj, headSlot);
  }

  /**
   * Return the current (total) counts of all tracked objects, then advance the window.
   * Whenever this method is called, we consider the counts of the current sliding window to be available to and
   * successfully processed "upstream" (i.e. by the caller). Knowing this we will start counting any subsequent
   * objects within the next "chunk" of the sliding window.
   *
   * @return The current (total) counts of all tracked objects.
   */
  public Map<T, Long> getCountsThenAdvanceWindow() {
    Map<T, Long> counts = objCounter.getCounts();
    objCounter.wipeZeros();
    objCounter.wipeSlot(tailSlot);
    advanceHead();
    return counts;
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slotAfter(tailSlot);
  }

  private int slotAfter(int slot) {
    return (slot + 1) % windowLengthInSlots;
  }

}
