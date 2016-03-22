package com.deblox.myproject;

import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by keghol on 6/27/14.
 *
 * simple storage backend mock with timestamps in the data
 *
 */
public class StorageBackend {

  private static final Logger logger = LoggerFactory.getLogger(StorageBackend.class);
  Map<String, Object> map;

  public StorageBackend() {
    map = new HashMap<>();
  }

  /**
   * Return the object
   *
   * @param uuid
   * @return
   */
  public Object get(String uuid) {
    try {
      return map.get(uuid);
    } catch (Exception e) {
      e.printStackTrace();
      return null;
    }
  }

  /**
   * Set the new object with timeAtPersist of the "store" event, and timeToPersist the length of time since serviceA
   * received the request minus the "persist" time.
   *
   * requires a jsonobject with a "data" object.
   * {
   *   "data": {
   *
   *   }
   * }
   *
   * @param uuid
   * @param value
   * @return
   */
  public JsonObject set(String uuid, JsonObject value) {

    // time of request at edge
    long edgeTimeOfRequestStart = value.getLong(DxConstants.edgeStartTimeOfRequest);
    long nanoTime = System.nanoTime();

    // time now
    long timeAtPersist = System.currentTimeMillis();

    // time since the edge received this request
    long timeDeltaBeforePersist = timeAtPersist - edgeTimeOfRequestStart;

    // response
    JsonObject t = new JsonObject()
            .put("id", uuid)
            .put(DxConstants.persistWallClockTime, timeAtPersist);

    // put the clock time in if its NOT present, else keep it.
    if (value.getLong(DxConstants.persistWallClockTime, null) == null) {
      value.put(DxConstants.persistWallClockTime, timeAtPersist);
    } else {
      logger.info("Keeping old persist time");
    }

    // pop the edge time off
    value.remove(DxConstants.edgeStartTimeOfRequest);

    // store the "data" from the object
    logger.info("Storing: " + uuid + ":" + value); //.getJsonObject("data"));

    // store the "data" key's contents
    map.put(uuid, value); //.getValue("data"));

    // store the "persistTime" in the response
    t.put(DxConstants.persistTimeCost, System.nanoTime() - nanoTime + "n"); // nanosecond to persist ONLY
    t.put(DxConstants.edgeStartTimeOfRequest, edgeTimeOfRequestStart); // copy the time at edge of the request
    t.put(DxConstants.edgeToPersistTimeDelta, timeDeltaBeforePersist); // the time from the edge to "here"

    // return the response.
    return t;
  }

  public int getSize() {
    return map.size();
  }

  public Map getMap() {
    return map;
  }

}