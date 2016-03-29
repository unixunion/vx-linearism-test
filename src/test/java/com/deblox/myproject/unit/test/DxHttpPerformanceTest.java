package com.deblox.myproject.unit.test;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.http.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.TimeUnit;


public class DxHttpPerformanceTest extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(DxHttpPerformanceTest.class);
  static final MetricRegistry metrics = new MetricRegistry();
  private final Meter successRequests = metrics.meter("successRequests");
  private final Meter failures = metrics.meter("failures");
  private final Histogram requestsHistogram = metrics.histogram("requestsHistogram");
  private final Histogram failureHistogram = metrics.histogram("failureHistogram");
  EventBus eb;
  private int requestCredits = 100;

  HttpClient client;
  ConsoleReporter reporter;

  @Override
  public void start(Future<Void> startFuture) {
    logger.info("Starting Up with Config: " + config().toString());
    eb = vertx.eventBus();

    HttpClientOptions clientOptions = new HttpClientOptions()
            .setPipelining(true)
            .setMaxPoolSize(30);

    client = vertx.createHttpClient(clientOptions);

    reporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
    reporter.start(5, TimeUnit.SECONDS);

    startFuture.complete();
    makeRequest();

    vertx.setPeriodic(3000, res -> {
      logger.info("requestCredits: " + requestCredits);
    });

  }

  private void makeRequest() {
    while (requestCredits > 0) {


      final long[] t = {System.currentTimeMillis()};
      HttpClientRequest req = client.request(HttpMethod.POST,
              config().getInteger("port", 8080),
              config().getString("host", "ec2-52-53-220-31.us-west-1.compute.amazonaws.com"),
              config().getString("url", "/api/v1"), res -> {
                res.bodyHandler(body -> {
                  if (body.toString().equals("ok")) {
                    successRequests.mark();
                    requestsHistogram.update(System.currentTimeMillis() - t[0]);
                  } else {
                    failures.mark();
                    failureHistogram.update(System.currentTimeMillis() - t[0]);
                  }
                  requestCredits++;
                  makeRequest();
                });

                res.exceptionHandler(ex -> {
                  failures.mark();
                  requestCredits++;
                  makeRequest();
                });

//                res.endHandler(end -> {
//                  t[0] = System.currentTimeMillis();
//                });

              }
      );

      req.endHandler(eh -> {
        logger.info("end");
      });

      req.end(new JsonObject()
              .put("somekey", "somevalue")
              .put("foo", new JsonObject()
                      .put("data", "here")
                      .put("key", "value")
              )
              .toString());
      requestCredits--;
    }
  }


}
