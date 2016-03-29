# deBlox Vert.x 3 template v1.0.0
a modified version of [vertx-gradle-template](https://github.com/vert-x/vertx-gradle-template). This template also contains
a few starting points for building simple web stack.

## features
* Vert.x 3.2.1
* HTTP Web Server
* WebSocket Client
* WebSocket Bridge
* MVEL Templating Engine
* Authentication Provider
* Authentication Handler on the EventBus
* Materialize CSS
* Modular Service Boot Class
* Logback logging framework

## boot class
`Boot.java` reads a specified *conf.json* file and starts up the classes as desribed in the config. each verticle has its own configuration's within the *conf.json*. example:

```json
{
  "conf_source": "this is the root config file",
  "vertx_options": {
    "blocked_thread_check_period": 1000,
    "max_event_loop_execute_time": 2000000000,
    "max_worker_execute_time": 60000000000,
    "quorum_size": 1,
    "ha_group": "__DEFAULT__",
    "ha_enabled": false,
    "metrics_enabled": false
  },
  "services": ["com.deblox.myproject.PingVerticle"],
  "com.deblox.myproject.PingVerticle": {
    "config": {
      "foo": "bar",
      "baz": {}
    },
    "worker": false,
    "multiThreaded": false,
    "isolationGroup": null,
    "ha": false,
    "extraClasspath": null,
    "instances": 1,
    "redeploy": true,
    "redeployScanPeriod": 250,
    "redeployGracePeriod": 1000
  }
}
```

## idea
to generate the idea files

```sh
./gradlew idea
```

## testing

```sh
./gradlew test -i
```

## running from Idea
the Boot.java class can be run directly and accepts `-conf` argument for specifiying config json.


## building fatJar
the gradle task *shadowJar* will build a executable jar.

```
./gradlew shadowJar
```

## running
when running as a fatJar, remember to specify the alternate logging implementation.


```
JAVA_OPTS="-Dvertx.logger-delegate-factory-class-name=io.vertx.core.logging.SLF4JLogDelegateFactory -Djgroups.bind_addr=match-interface:en0* -Djava.net.preferIPv4Stack=true"
java $JAVA_OPTS -jar my-module-1.0.0-final-fat.jar -cp /dir/with/logback/xml
```

curl -d '{"action":"w", "register": "foo2", "data": {"hello": "world2"}, "persistCount": 1}' http://localhost:8080/api/v1
curl -d '{"action":"r", "register": "foo2"}' http://localhost:8080/api/v1
