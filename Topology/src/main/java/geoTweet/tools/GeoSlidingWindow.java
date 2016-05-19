
package geoTweet.tools;

import java.io.Serializable;
import java.util.Map;
import java.util.List;

import geoTweet.tools.Geo;

/**
 * This class counts objects in a sliding window fashion.
 * It is designed 1) to give multiple "producer" threads write access to the counter, i.e. being able to increment
 * counts of objects, and 2) to give a single "consumer" thread (e.g. {@link PeriodicSlidingWindowCounter}) read access
 * to the counter. Whenever the consumer thread performs a read operation, this class will advance the head slot of the
 * sliding window counter. This means that the consumer thread indirectly controls where writes of the producer threads
 * will go to. Also, by itself this class will not advance the head slot.
*/

public final class GeoSlidingWindow<T> implements Serializable {

  private GeoSlotBasedCounter<T> objCounter;
  private int headSlot;
  private int tailSlot;
  private int windowLengthInSlots;

  public GeoSlidingWindow(int windowLengthInSlots) {
    if (windowLengthInSlots < 2) {
      throw new IllegalArgumentException(
          "Window length in slots must be at least two (you requested " + windowLengthInSlots + ")");
    }
    this.windowLengthInSlots = windowLengthInSlots;
    this.objCounter = new GeoSlotBasedCounter<T>(this.windowLengthInSlots);

    this.headSlot = 0;
    this.tailSlot = slotAfter(headSlot);
  }

  public void incrementCount(T obj, String longitude, String latitude) {
    objCounter.incrementCount(obj, headSlot, longitude, latitude);
  }

  public Map<T, List<Geo>> getCountsThenAdvanceWindow() {
    Map<T, List<Geo>> geoSnapShot = objCounter.getAllGeos();
    objCounter.wipeZeros();
    objCounter.wipeSlot(tailSlot);
    advanceHead();
    return geoSnapShot;
  }

  private void advanceHead() {
    headSlot = tailSlot;
    tailSlot = slotAfter(tailSlot);
  }

  private int slotAfter(int slot) {
    return (slot + 1) % windowLengthInSlots;
  }

}
