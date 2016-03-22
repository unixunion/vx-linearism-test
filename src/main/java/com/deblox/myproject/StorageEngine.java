package com.deblox.myproject;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.deblox.messaging.Responses;
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

    // headers for messaging
    deliveryOptions = new DeliveryOptions()
            .addHeader("nodeId", uniqueAddress)
            .setSendTimeout(5000);

    // storage backend
    storageBackend = new StorageBackend();

    // eventbus holder
    eb = vertx.eventBus();

    // Map for pending requests when data not immediately available.
    waitingQueue = new HashMap<>();

    // start consumers
    eb.consumer("storage-read-address", this); // reads are request / response
    eb.consumer("storage-write-address", this); // writes are request / response and published
    eb.consumer(uniqueAddress, this); // this node can receive missing docs on this address

    // completion of deployment event
    vertx.setTimer(250, res -> startFuture.complete());

    // dump periodic stats
    vertx.setPeriodic(3000, res2 -> {
      logger.info("WAITQUEUE: " + waitingQueue.size());
      logger.info("OBJECTS: " + storageBackend.getSize());
      logger.info("Dumping Objects to Log");
      storageBackend.getMap().entrySet().forEach(res -> {
        logger.info("Object: " + res.toString());
      });
    });
  }


  /**
   *
   * {
   *   "action": "ru / ur / w / r"
   *   "register": "register address to read / write to"
   *   "data" : {
   *    "address": " the unique address to response to if UR"
   *    "requestId": "id of a request for update waitqueue request object"
   *    "other data if W"
   *   }
   * }
   *
   * @param event
   */
  @Override
  public void handle(Message<JsonObject> event) {
    logger.info(event.body().toString());

    // create a holder for the document
    JsonObject document;

    switch (event.body().getString("action")) {

      /**
       * code for dealing with "request-for-update" from peers
       */
      case DxConstants.REQUEST_UPDATE:
        // request for update
        logger.info("A peer is requesting a document: " + event.body());

        // get the register address the peer is requesting from the event
        String ruRegisterId = Responses.getRegisterId(event.body());

        // the private reply address of the requestor
        String ruAddress = Responses.getAddress(event.body());

        // if "my" storage contains the register, return it on the "private" peer unique "address"
        if (storageBackend.getMap().containsKey(ruRegisterId)) {

          JsonObject ruResponse = Responses.createRequest(
                  DxConstants.UPDATE_RESPONSE,
                  ruRegisterId,
                  (JsonObject)storageBackend.get(ruRegisterId),
                  Responses.getRequestId(event.body())
          );

          logger.info("Sending the peer: " + ruResponse.getJsonObject("data").toString());

          // send the message
          logger.info("Peer address: " + ruAddress);
          eb.send(ruAddress, ruResponse.getJsonObject("data"));
        }
        break;

      /**
       * Deals with document updates either because this node requested it, or its a general announcement.
       */
      case DxConstants.UPDATE_RESPONSE:

        JsonObject urResponse = new JsonObject();

        if (event.headers().get("nodeId") != uniqueAddress) {

          logger.info("Updating a document: " + event.body());

          // Lets just throw some numbers into the document
          JsonObject urDocument = DxTimestamp.setTimeOfRequest(event.body());
//          urDocument.put("data", urDocument.getJsonObject("data").getJsonObject("data"));

          // store the entire document and its metrics and whatnot.
          logger.info("Persisting Document: " + urDocument);
          storageBackend.set(Responses.getRegisterId(event.body()), urDocument);

          // if its NOT a publish
          if (event.replyAddress() != null) {
            // create a response to the peer
            urResponse = DxTimestamp.setDeltaTimeSinceRequestStart(event.body());

            // send thanks
            Responses.sendOK(this.getClass().getSimpleName(), event, deliveryOptions, urResponse);

          } else {
            logger.info("Discontinue, reply address:" + event.replyAddress());
          }

          // finalize any requests waiting on this data.
          if (waitingQueue.containsKey(Responses.getRequestId(event.body()))) {
            logger.info("Finalizing requestId: " + Responses.getRegisterId(event.body()));
            waitingQueue.get(Responses.getRequestId(event.body())).reply(storageBackend.get(Responses.getRegisterId(event.body())), deliveryOptions);
          } else {
            logger.info("No one is waiting for this");
          }

        } else {
          logger.info("Ignoring a message from myself.");
        }

        break;

      /**
       * writes data into a register, broadcasts write event to the cluster
       */
      case  DxConstants.WRITE_REQUEST:
        // write a register
        logger.info("Write request for " + event.body());

        String wrRegisterId = Responses.getRegisterId(event.body());

        // persist the entire document as is.
        JsonObject wrDocument = storageBackend.set(wrRegisterId, event.body());

        // tell cluster about the new document
        eb.publish("storage-write-address", event.body().put("action", DxConstants.UPDATE_RESPONSE), deliveryOptions);

        event.reply(wrDocument, deliveryOptions);

        break;

      /**
       * Reads some key from the storage system, if its not available, puts the request in a queue,
       * and asks the cluster for a callback with the data.
       */
      case DxConstants.READ_REQUEST:
        // read a register
        logger.info("Read Request: " + event.body());

        String rrRegisterId = Responses.getRegisterId(event.body());
        String uniqueRequestId = UUID.randomUUID().toString();
        event.body().put("requestId", uniqueAddress);

        if (storageBackend.getMap().containsKey(rrRegisterId)) {
          // return the data
          event.reply(storageBackend.get(rrRegisterId), deliveryOptions);
        } else {
          // request from cluster with timeout
          logger.info("Missing this document, requesting from cluster");

          waitingQueue.put(uniqueRequestId, event);

          JsonObject request = new JsonObject()
                  .put("action", DxConstants.REQUEST_UPDATE)
                  .put("register", rrRegisterId)
                  .put("requestId", uniqueRequestId)
                  .put("address", uniqueAddress)
                  .put("data", new JsonObject()
                  );

          eb.publish("storage-read-address", request, deliveryOptions);

        }

        break;

    }
  }
}
