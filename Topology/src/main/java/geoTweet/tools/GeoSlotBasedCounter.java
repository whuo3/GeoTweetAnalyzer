package geoTweet.tools;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedList;

import geoTweet.tools.Geo;

/**
 * This class provides per-slot counts of the occurrences of objects.
 * <p/>
 * It can be used, for instance, as a building block for implementing sliding window counting of objects.
 *
 * @param <T> The type of those objects we want to count.
 */

public final class GeoSlotBasedCounter<T> implements Serializable {

  private final Map<T, List<List<Geo>>> objToGeos = new HashMap<>();
  private final int numSlots;

  public GeoSlotBasedCounter(int numSlots) {
    if (numSlots <= 0) {
      throw new IllegalArgumentException("Number of slots must be greater than zero (you requested " + numSlots + ")");
    }
    this.numSlots = numSlots;
  }

  public void incrementCount(T obj, int slot, String longitude, String latitude) {
    List<List<Geo>> windows = objToGeos.get(obj);
    if (windows == null) {
      windows = new ArrayList<>();
      for(int i = 0; i < this.numSlots; i++) {
        windows.add(new LinkedList<Geo>());
      }
      objToGeos.put(obj, windows);
    }
    windows.get(slot).add(new Geo(longitude, latitude));
  }

  public long getCount(T obj, int slot) {
    List<List<Geo>> windows = objToGeos.get(obj);
    if (windows == null) {
      return 0;
    }
    else {
      return windows.get(slot).size();
    }
  }

  // public Map<T, Long> getCounts() {
  //   Map<T, Long> result = new HashMap<T, Long>();
  //   for (T obj : objToCounts.keySet()) {
  //     result.put(obj, computeTotalCount(obj));
  //   }
  //   return result;
  // }

  public Map<T, Long> getCounts() {
    Map<T, Long> result = new HashMap<T, Long>();
    for (T obj : objToGeos.keySet()) {
      result.put(obj, computeTotalCount(obj));
    }
    return result;
  }

  public Map<T, List<Geo>> getAllGeos() {
    Map<T, List<Geo>> result = new HashMap<>();
    for (T obj : objToGeos.keySet()) {
      List<List<Geo>> curList = objToGeos.get(obj);
      List<Geo> newList = new ArrayList<Geo>();
      //Get all Geo for a specific Hashtag
      for(int i = 0; i < curList.size(); i++) {
        List<Geo> l = curList.get(i);
        for(Geo geo : l) {
          newList.add(geo);
        }
      }
      result.put(obj, newList);
    }
    return result;
  }

  private long computeTotalCount(T obj) {
    List<List<Geo>> windows = objToGeos.get(obj);
    long total = 0;
    for (List<Geo> l : windows) {
      total += l.size();
    }
    return total;
  }

  /**
   * Reset the slot count of any tracked objects to zero for the given slot.
   *
   * @param slot
   */
  public void wipeSlot(int slot) {
    for (T obj : objToGeos.keySet()) {
      resetSlotCountToZero(obj, slot);
    }
  }

  private void resetSlotCountToZero(T obj, int slot) {
    List<List<Geo>> windows = objToGeos.get(obj);
    windows.get(slot).clear();
  }

  private boolean shouldBeRemovedFromCounter(T obj) {
    return computeTotalCount(obj) == 0;
  }

  /**
   * Remove any object from the counter whose total count is zero (to free up memory).
   */
  public void wipeZeros() {
    Set<T> objToBeRemoved = new HashSet<T>();
    for (T obj : objToGeos.keySet()) {
      if (shouldBeRemovedFromCounter(obj)) {
        objToBeRemoved.add(obj);
      }
    }
    for (T obj : objToBeRemoved) {
      objToGeos.remove(obj);
    }
  }

}
