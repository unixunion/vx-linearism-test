package com.deblox.myproject;

import io.vertx.core.json.JsonObject;

/**
 * Created by keghol on 20/03/16.
 */
public class DxTimestamp {

  public static JsonObject setTimeOfRequest(JsonObject jo) {
    jo.put(DxConstants.edgeStartTimeOfRequest, System.currentTimeMillis());
    return jo;
  }

  public static JsonObject setDeltaTimeSinceRequestStart(JsonObject jo) {
    jo.put("edge.timeDelta", System.currentTimeMillis() - jo.getLong(DxConstants.edgeStartTimeOfRequest));
    return jo;
  }

}
