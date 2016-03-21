package com.deblox.myproject.unit.test;

import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

/**
 * Created by keghol on 19/03/16.
 */
public class PerformanceTest  extends AbstractVerticle implements AsyncResultHandler<Message<JsonObject>> {

  private static final Logger logger = LoggerFactory.getLogger(PerformanceTest.class);
  private int requestCredits = 100;
  private EventBus eb;

  Map<String, JsonObject> map = new HashMap<>();

  public void start(Future<Void> startFuture) throws Exception {
    logger.info("Startup with Config: " + config().toString());
    eb = vertx.eventBus();
    eb.consumer("perftest-address", event -> {
      logger.debug("Starting Test");
      makeRequest();
    });
    vertx.setTimer(250, res -> startFuture.complete());

    vertx.setPeriodic(1000, res -> {
      if (requestCredits<=0) {
        requestCredits = 2000;
        makeRequest();
      }
    });

  }


  /**
   * Create a request to read/write
   *
   */
  private void doRequest() {

    try {
      JsonObject request = new JsonObject()
              .put("action", Math.random() < 0.5 ? "w" : "r")
              .put("timeOfRequest", System.currentTimeMillis());

      if (request.getString("action") == "w") {
        request.put("data", new JsonObject()
                .put("key", "value")
        );
        logger.debug("requesting write: " + request.toString());
        eb.send("storage-write-address", request, new DeliveryOptions().setSendTimeout(1000), this);
      } else {
        Random random = new Random();
        List<String> keys = new ArrayList<String>(map.keySet());
        String randomKey = keys.get(random.nextInt(keys.size()));
        request.put("id", randomKey);
        logger.debug("waiting for read: " + request.toString());
        eb.send("storage-read-address", request, new DeliveryOptions().setSendTimeout(1000), this);
      }
    } catch (Exception e) {
      logger.warn("Error");
      makeRequest();
    }

  }

  private void makeRequest() {
      while (requestCredits > 0) {
        try {
          doRequest();
          requestCredits--;
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
  }

  @Override
  public void handle(AsyncResult<Message<JsonObject>> event) {
    logger.debug(event.result());

    try {
      logger.debug(event.result().body().toString());
      map.put(event.result().body().getString("id"), event.result().body());
    } catch (Exception e) {
      e.printStackTrace();
    }

    requestCredits++;
    makeRequest();

  }
}
