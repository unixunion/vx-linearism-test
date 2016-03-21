package com.deblox.messaging;

import com.deblox.myproject.DxConstants;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;

/**
 * Created by keghol on 11/02/16.
 */
public class Responses {


  public static void sendOK(String className, Message<?> msg, DeliveryOptions deliveryOptions, JsonObject data) {
    JsonObject response = new JsonObject();
    response.put("status", "ok");
    response.put("class", className);
    response.put("data", data);
    sendMsg(className, msg, deliveryOptions, response);
  }

  public static void sendOK(String className, Message<?> msg) {
    JsonObject response = new JsonObject();
    response.put("status", "ok");
    response.put("class", className);
    sendMsg(className, msg, response);
  }

  public static void sendError(String className, Message<?> msg, String reason) {
    JsonObject response = new JsonObject();
    response.put("status", "error");
    response.put("data", reason);
    sendMsg(className, msg, response);
  }

  static void sendMsg(String className, Message<?> msg, JsonObject document) {
    document.put("class", className);
    msg.reply(document);
  }

  static void sendMsg(String className, Message<?> msg, DeliveryOptions deliveryOptions, JsonObject document) {
    document.put("class", className);
    msg.reply(document, deliveryOptions);
  }

  /**
   * get the address key out of the data key
   * @param event
   * @return
   */
  public static String getAddress(JsonObject event) {
    return event.getString("address");
  }

  /**
   * get the "register" only
   * @param event
   * @return
   */
  public static String getRegisterId(JsonObject event) {
    return event.getString(DxConstants.REGISTER_ID_FIELD_NAME);
  }

  public static String getRequestId(JsonObject event) {
    return event.getString("requestId");
  }

  public static JsonObject createRequest(String action, String register, JsonObject data, String requestId) {
  return new JsonObject()
          .put("action", action)
          .put("register", register)
          .put("requestId", requestId)
          .put("data", data);
  }

}
