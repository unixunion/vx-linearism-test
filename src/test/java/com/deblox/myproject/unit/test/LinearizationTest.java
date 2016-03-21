package com.deblox.myproject.unit.test;

import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/*
 * Example of an asynchronous unit test written in JUnit style using vertx-unit
 *
 * @author <a href="http://tfox.org">Tim Fox</a>
 */
@RunWith(VertxUnitRunner.class)
public class LinearizationTest extends AbstractVerticle {

  Vertx vertx;
  EventBus eb;
  private static final Logger logger = LoggerFactory.getLogger(LinearizationTest.class);

  @Before
  public void before(TestContext context) {
    logger.info("@Before");

    Async async = context.async();

    Vertx.clusteredVertx(new VertxOptions().setClustered(true), vx -> {
      vertx = vx.result();
      eb = vx.result().eventBus();
      vx.result().deployVerticle(PerformanceTest.class.getName(), res -> {
        if (res.succeeded()) {
          async.complete();
        } else {
          context.fail();
        }
    });

    });
  }

  @After
  public void after(TestContext context) {
    logger.info("@After");
    Async async = context.async();

    vertx.close( event -> {
      async.complete();
    });

  }

  @Test
  public void test(TestContext test) {

    logger.info("Testing");

    Async async = test.async();

    eb.send("perftest-address", new JsonObject(), res -> {
      if (res.succeeded()) {
      }
    });


  }


}