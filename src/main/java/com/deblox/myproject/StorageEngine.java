package com.deblox.myproject;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.deblox.messaging.Responses;
import io.netty.handler.codec.serialization.ObjectDecoder;
import io.vertx.core.*;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;

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
  SharedData sd;


  public void start(Future<Void> startFuture) throws Exception {

    logger.info("Startup with Config: " + config().toString());

    reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    reporter.start(260, TimeUnit.SECONDS);

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
    sd = vertx.sharedData();

    // Map for pending requests when data not immediately available.
    waitingQueue = new HashMap<>();

    // start consumers
    eb.consumer("storage-read-address", this); // reads are request / response
    eb.consumer("storage-write-address", this); // writes are request / response and published
    eb.consumer(uniqueAddress, this); // this node can receive missing docs on this address

    sd.<String, JsonArray>getClusterWideMap("nodes", getMap -> {
      if (getMap.succeeded()) {

        // get the map
        AsyncMap<String, JsonArray> nodesMap = getMap.result();

        // create the nodes object if its missing
        nodesMap.putIfAbsent("nodesList", new JsonArray(), newList -> {
          if (newList.succeeded()) {

            if (newList.result() == null) {
              nodesMap.put("nodesList", new JsonArray().add(uniqueAddress), res -> {
                logger.info("New List");
              });
            } else {
              logger.info("Updating Existing List: " + newList.result().toString());
              nodesMap.put("nodesList", newList.result().add(uniqueAddress), update -> {
                logger.info("Update Complete");
              });
            }
          }
        });

      } else {
        logger.warn("No clusterwide map support");
      }

    });


    // completion of deployment event
    vertx.setTimer(250, res -> startFuture.complete());

    // dump periodic stats
    vertx.setPeriodic(360000, res2 -> {
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

    logger.info("");
    logger.info("address: " + event.address() + " reply: " + event.replyAddress() + " body:" + event.body().toString());

    // create a holder for the document
    JsonObject document;


    switch (event.body().getString("action")) {


      case "ping":
        event.reply(new JsonObject().put("action", "reply"));
        break;

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

          // store the entire document and its metrics and whatnot.
          logger.info("Persisting Document: " + urDocument);
          event.body().put("nodes.persisted.to", event.body().getJsonArray("nodes.persisted.to", new JsonArray()).add(uniqueAddress));
          storageBackend.set(Responses.getRegisterId(event.body()), urDocument);

          // if its NOT a publish
          if (event.replyAddress() != null) {
            // create a response to the peer
            urResponse = DxTimestamp.setDeltaTimeSinceRequestStart(event.body());

            // if this update needs to be persisted across nodes
            if (event.body().containsKey("persistCount") && event.body().getInteger("persistCount")>0) {
              logger.info("UR: Ensuring Persist Count is honoured: " + event.body());
              event.body().put("persistCount", (event.body().getInteger("persistCount"))-1);
              eb.send("storage-write-address", event.body().put("action", DxConstants.UPDATE_RESPONSE), deliveryOptions, persist-> {
                if (persist.succeeded()) {
                  logger.info("UR Persist Response," + persist.result().body());
                  JsonObject wrDocument = storageBackend.set(Responses.getRegisterId(event.body()), event.body());
                  wrDocument.put("cluster.persist.sync", true);
                  event.reply(wrDocument, deliveryOptions);
                } else {
                  logger.error("UR: Persist failure");
                  Responses.sendError(this.getClass().getSimpleName(), event,  "error honouring persistence");
                }
              });
            } else {
              // send thanks
              logger.info("Confirming Persist");
              event.reply(urResponse, deliveryOptions);
//              Responses.sendOK(this.getClass().getSimpleName(), event, deliveryOptions, urResponse);
            }





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
        event.body().put("nodes.persisted.to", event.body().getJsonArray("nodes.persisted.to", new JsonArray()).add(uniqueAddress));
        JsonObject wrDocument = storageBackend.set(wrRegisterId, event.body());


        // tell cluster about the new document
        eb.publish("storage-write-address", event.body().put("action", DxConstants.UPDATE_RESPONSE), deliveryOptions);

        // if persistence is demanaded before this call returns
        if (event.body().containsKey("persistCount") && event.body().getInteger("persistCount")>0) {
          logger.info("WR: Ensuring Persist Count is honoured" + event.body());
          event.body().put("persistCount", (event.body().getInteger("persistCount"))-1);

          getRandomNode(randomAddress -> {
            if (randomAddress != null) {
              logger.info("Asking " + randomAddress + " to Persist");
              JsonObject persistRequest = event.body()
                      .put("action", DxConstants.UPDATE_RESPONSE);

              // send the persistence request to the cluster
              eb.send(randomAddress, persistRequest, deliveryOptions, persist-> {
                if (persist.succeeded()) {
                  logger.info("Persist succeeded: " + persist.result().body().toString());
                  wrDocument.put("cluster.persist.sync", true);
                  storageBackend.set(Responses.getRegisterId(event.body()), wrDocument);
                  event.reply(wrDocument, deliveryOptions);
                } else {
                  logger.error("Persist Failed");
                  Responses.sendError(this.getClass().getSimpleName(), event, "unable to persist across enough nodes");
                }
              });
            }
          });


        } else {
          wrDocument.put("cluster.persist.async", true);
          event.reply(wrDocument, deliveryOptions);
        }

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




  private void getRandomNode(Handler<String> handler) {
    sd.<String, JsonArray>getClusterWideMap("nodes", res -> {
      if (res.succeeded()) {
        AsyncMap<String, JsonArray> nodesMap = res.result();

        nodesMap.get("nodesList", res2 -> {
          if (res2.succeeded()) {
            logger.info("Got Nodes List: " + res2.result().toString());

            // remove my address from the options
            JsonArray jo = res2.result();
            jo.remove(uniqueAddress);

            if (jo.size()>1) {

//            boolean foundNode = false;
              long t = vertx.setPeriodic(100, tr -> {
                // get a address
                String nodeAddress = jo.getString((int) Math.floor(Math.random() * jo.size()));
                eb.send(nodeAddress, new JsonObject().put("action", "ping"), deliveryOptions, ping -> {
                  if (ping.succeeded()) {
                    vertx.cancelTimer(tr);
                    handler.handle(nodeAddress);
                  } else {
                    logger.warn("Node " + nodeAddress + " is dead");
                    jo.remove(nodeAddress);
                    res2.result().remove(nodeAddress);
                  }
                });
              });
            } else {
              logger.warn("Cluster is too small");
            }

            // call the handler with the address
//            handler.handle(nodeAddress);

          } else {
            logger.error("Unable to get node from map");
            handler.handle(null);
          }
        });

      } else {
        logger.warn("No clusterwide map support");
        handler.handle(null);
      }
    });
  }

}
