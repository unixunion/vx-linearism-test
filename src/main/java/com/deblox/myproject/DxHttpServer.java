package com.deblox.myproject;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;


public class DxHttpServer extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(DxHttpServer.class);
  EventBus eb;
  private DeliveryOptions deliveryOptions;

  @Override
  public void start(Future<Void> startFuture) {
    logger.info("Starting Up with Config: " + config().toString());

    deliveryOptions = new DeliveryOptions().setSendTimeout(1000);

    Router router = Router.router(vertx);
    router.exceptionHandler(throwable ->{
      logger.error(throwable.getMessage());
    });

    router.route(HttpMethod.POST, "/api/v1").handler(ctx -> {
      addOne(ctx);
    });

    HttpServerOptions httpServerOptions = new HttpServerOptions()
            .setSsl(false);

    // the server itself
    vertx.createHttpServer(httpServerOptions).requestHandler(router::accept).listen(config().getInteger("port", 8080));

    eb = vertx.eventBus();

    // send back deployment complete
    startFuture.complete();
  }

  private void addOne(RoutingContext routingContext) {
    routingContext.request().bodyHandler(res -> {
      JsonObject jo = res.toJsonObject();

      String address;

      if (jo.getString("action") == "r") {
        address = "storage-read-address";
      } else {
        address = "storage-write-address";
      }

      eb.send(address, DxTimestamp.setTimeOfRequest(jo), deliveryOptions, (AsyncResult<Message<JsonObject>> event) -> {
        if (event.succeeded()) {
          routingContext.response().end(event.result().body().toString());
        } else {
          routingContext.response().end(new JsonObject().put("error", "timeout waiting for document").toString());
        }
      });
    });
  }

}