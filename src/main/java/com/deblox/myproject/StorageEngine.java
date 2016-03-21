package com.deblox.myproject;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created by keghol on 19/03/16.
 *
 *
 * Wait for average time of Writes to propegate to at least 2 nodes then perform reads.
 * Write a journal of "state" for a partibular time to correlate clients with truth
 *
 *
 */
public class StorageEngine extends AbstractVerticle implements Handler<Message<JsonObject>> {

  private static final Logger logger = LoggerFactory.getLogger(StorageEngine.class);
  static final MetricRegistry metrics = new MetricRegistry();
  private final Meter requests = metrics.meter("requests");
  private final Meter aided = metrics.meter("aided");

  public static EventBus eb;
  private StorageBackend storageBackend;
  ConsoleReporter reporter;
  String uniqueAddress;
  DeliveryOptions deliveryOptions;
  Map<String, Message<JsonObject>> waitingQueue;

  public void start(Future<Void> startFuture) throws Exception {
    logger.info("Startup with Config: " + config().toString());
    reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    reporter.start(15, TimeUnit.SECONDS);

    // unique instance identifier
    uniqueAddress = UUID.randomUUID().toString();
    logger.info("nodeId: " + uniqueAddress);

    deliveryOptions = new DeliveryOptions()
            .addHeader("nodeId", uniqueAddress)
            .setSendTimeout(5000);

    storageBackend = new StorageBackend();
    eb = vertx.eventBus();

    waitingQueue = new HashMap<>();

    // start consumers
    eb.consumer("storage-read-address", this); // reads are request / response
    eb.consumer("storage-write-address", this); // writes are request / response and published
    eb.consumer(uniqueAddress, this); // this node can receive missing docs on this address

    vertx.setTimer(250, res -> startFuture.complete());

    vertx.setPeriodic(3000, res2 -> {
      logger.info("WAITQUEUE: " + waitingQueue.size());
      logger.info("OBJECTS: " + storageBackend.getSize());
      storageBackend.getMap().entrySet().forEach(res -> {
        logger.info(res.toString());
      });
    });
  }

  @Override
  public void handle(Message<JsonObject> event) {
    logger.info(event.body().toString());

    // create a holder for the document
    JsonObject document;





    // if this request was targeted at this node, its a response to a update request
    if (event.address().equals(uniqueAddress)) {
      logger.info("Cache update for me from fellow node! " + event.body());

      // make the document the right format for the persistence layer.
      JsonObject newDocument = new JsonObject().put("data", event.body().getJsonObject("value"));
      newDocument.put(DxConstants.edgeToPersistTimeDelta, event.body().getString(DxConstants.edgeToPersistTimeDelta));

      storageBackend.set(event.body().getString("id"), newDocument);
      event.reply(new JsonObject().put("action", "thanks"), deliveryOptions);

      // finalize the event in the queue
      waitingQueue.get(event.body().getString("id"))
              .reply(event.body(), deliveryOptions);

      // remove the event from the waitqueue
      waitingQueue.remove(event.body().getString("id"));

      // nop the future actions of this chain
      event.body().put("action", "nop");
    }

    if (event.body().getString("action") == null) {
      logger.warn("No action specified");
      event.body().put("action", "nop");
    }

    switch (event.body().getString("action")) {

      case "nop":
        event.reply("nop");
        break;

      // peer requesting update for a document key
      case "u":
        logger.info("Request for Update: " + event.body().toString());

        // get document by unwrapping origin request ID
        JsonObject updateDocument = storageBackend.get(event.body().getString("id"));
        logger.info("Backend response: " + updateDocument);

        if (updateDocument != null && event.body().getString("address") != null) {
          logger.info("Providing update for peer: " + updateDocument.toString());
          eb.send(event.body().getString("address"), updateDocument, deliveryOptions, resp -> {
            aided.mark();
            logger.info("Successfully aided a fellow node with: " + updateDocument.toString());
          });
        } else if (updateDocument == null && event.body().getString("address") == null) {
          logger.info("New document publish");
          document = storageBackend.set(event.body().getString("id"), event.body());
        } else {
          // wait! im also missing this document
          logger.info("Missing the requested document, not responding");
        }
        break;

      // read a document by id from the store
      case "r":

        logger.info("Reading Document: " + event.body().getString("id"));
        JsonObject readDocument = storageBackend.get(event.body().getString("id"));

        if (readDocument == null) {
          logger.warn("No such document in the store, asking the cluster!");

          // put the request in the waitQueue
          waitingQueue.put(event.body().getString("id"), event);

          // the request
          JsonObject request = new JsonObject()
                  .put("address", uniqueAddress) // address to receive responses on
                  .put("action", "u")
                  .put("id", event.body().getString("id"));

          // broadcast the request to the entire cluster, will handle the response on on the unique address
          eb.publish("storage-read-address", request, deliveryOptions);

        } else {
          logger.info("Document found: " + readDocument.toString());
          // respond with the document to a "update" request
          requests.mark();
          event.reply(readDocument, deliveryOptions);
        }
        break;

      // write a new document into the store, returning the id, also publish the write
      // to the rest of the cluster
      case "w":
        requests.mark();

        logger.info("Storing Document " + event.body());

        // create a new document
        JsonObject writeDocument = storageBackend.set(UUID.randomUUID().toString(), event.body());

        // persist the document on the cluster
        if (event.address().equals("storage-write-address")) { //&& event.replyAddress() != null
          logger.info("Passing to Cluster: " + writeDocument);
          eb.publish("storage-write-address", writeDocument.put("action", "u"), deliveryOptions);
        } else {
          logger.info("Updating self only");
        }

        event.reply(writeDocument, deliveryOptions);

        break;

    }
  }
}