---
layout: page
title: Metrics
---

Colossus uses the (currently nameless) metrics library.

## Introduction

High-throughput Colossus services can serve hundreds of thousands of requests
per second, which can easily translate to millions of recordable events per
second.  The Metrics library provides a way to work with metrics with as little
overhead as possible. It is configurable via a [typesafe config](https://github.com/typesafehub/config).
See [reference config](https://github.com/tumblr/colossus/blob/master/colossus-metrics/src/main/resources/reference.conf)
The Colossus server and clients define several metrics be default.

### Collection Intervals

Collection intervals define how often the raw data from all event collectors
are snapshotted and merged together into a single database of values.

The provided config creates a metric system with both 1 second and 1 minute
collection intervals.  The 1 second interval is used to show
real-time metrics where rates and histograms are all showing values reflective
of the last second of activity, whereas the 1 minute intervals are used for
reporting values to an external database in which case the values reported are
all reflective of the last minute of activity.

### Default Metrics in Colossus

Running the Colossus Client and Colossus Server makes several metrics available
in a default service, the server is prefixed with the service name, the client
is prefixed with the service name AND the client name

#### Server 


| metric name | Description | Tags |
|-------|-------|-------|
|**system/fd_count**|the open file descriptor counts|N/A|
|**system/gc/cycles**|number of garbage collection cycles|type : ConcurrentMarkSweep, ParNew|
|**system/gc/msec**|the milliseconds it takes for garbage collection|type : ConcurrentMarkSweep, ParNew|
|**system/memory**|the total memory availble to the JVM|type: free, max, allocated|
|**requests**|the amount of request to the service|status_code: http code(example: 201, 400)|
|**concurrent_requests**|the amount of request to the service in a time interval|N/A|
|**requests_per_connection**|the amount of different async requests made in a single request|label: max, mean, etc|
|**errors**|the amount of exceptionst that have bubbled up in the service|type: exception name|
|**latency**|the time (in ms) it takes to complete a request|label:max,mean, etc  status_code:200, 404, etc| 
|**worker/event_loops**|how many event loops were selected in a given interval|worker: the worker Id for a given event loop|
|**worker/connections**|the connections registrered for a new or reconnecting client|worker: the worker Id for a given event loop|
|**worker/rejected_connections**|the number of connections that weren't able to be created by the worker|server: servername, worker: worker id|
|**connections**|the amount of connections that are used for work|N/A|
|**refused_connections**|the amount of connections that were rejected due to the connection limiter|N/A|
|**connects**|the amount of connections attempted, this includes both connections and refused_connections|N/A|
|**closed**|the number of connections that are closed|cause: string for the connection closing|
|**highwaters**|the amount threads that have changed connection state (normal or highwater_|N/A|


#### Client

| metric name | Description | Tags |
|-------|-------|-------|
|**requests**|the amount of requests the client made|client_port: the port in use|
|**errors**|the exceptions that bubbled up to the client|type: the exception|
|**dropped_requests**|the number of requests that cannot be sent back to the client|client_port: the port in use|
|**connection_failures**|if a connection failed to an external host|client_port: the port in use|
|**disconnects**|if a connection is lost due to being closed, that isn't a connection failure (example: connection closed)|client_port|
|**latency**|the time (in ms) it takes to complete a request, this includes time in the queue|label:max,mean, etc  client_port: the port in use| 
|**transit_time**|the time (in ms) it takes to complete a request, this does NOT include queue time|label:max,mean, etc  client_port: the port in use| 
|**queue_time**|the time between a request is made to when the call is made|label:max,mean, etc  client_port: the port in use| 



### Metric Filters

### Metric Addresses and Tags

Every metric has a url-like address used to identify it.  

One of the most important aspects of metrics is that values can be tagged.  For
example, a rate can track hits to API endpoints, using a "endpoint" tag to break
down the usage by endpoint.

{% highlight scala %}

val rate = Rate("my-rate")
rate.hit(Map("endpoint" -> "foo"))
rate.hit(Map("endpoint" -> "bar"))
rate.hit(Map("endpoint" -> "bar"))

{% endhighlight %}

This will in turn lead to

| endpoint | value |
|----------|-------|
| foo      | 1     |
| bar      | 2     |


## Getting Started

If you are using colossus, it depends on the metrics library and pulls it in.
Otherwise you must add the following to your build.sbt/Build.scala

{% highlight scala %}

libraryDependencies += "com.tumblr" %% "colossus-metrics" % "{{ site.latest_version }}"

{% endhighlight %}

From there, the only required step is to spin up a `MetricSystem`

{% highlight scala %}

import akka.actor._
import metrics._

implicit val actorSystem = ActorSystem()

//create the metric system from a typesafe config
implicit val metricSystem = MetricSystem(MetricSystemConfig.load("application"))

val rate = Rate("my-rate")

rate.hit()

{% endhighlight %}

### Namespaces

When creating a new collector, you always need an implicit `MetricNamespace` in
scope.  The `MetricSystem` itself acts as the root namespace, but you can use it
to create sub-namespaces:

{% highlight scala %}
val metricSystem = MetricSystem()

implicit val namespace = metricSystem / "foo" / "bar"

//this rate will have the address "/foo/bar/baz"
val rate = Rate("baz")

{% endhighlight %}

### Metric Tick Complete example

The example below illustrates creating a service with a url called badUrl and
doing a metric tick when an endpoint is hit.

{% highlight scala %}
implicit val ac = ActorSystem("badearl")
  val metricSystem = MetricSystem("badurl") //defaults in MetricSystemConfig
  val reporterConfig = MetricReporterConfig(
    metricSenders = OpenTsdbSender("myhost", 4242) :: Nil,
    filters = MetricReporterFilter.All
  )

  metricSystem.collectionIntervals.get(1.minute).foreach{
    _.report(reporterConfig)
  }

  implicit val io = IOSystem("badurl", None, metricSystem)
  implicit val metricNamespace = io.metrics

  val badUrl = Rate("badurl")

  Server.start("hello-world", 9000){
    worker => new Initializer(worker) {
      def onConnect = context => new HttpService(context) {
        def handle = {
          case request @ Get on Root / "url" => {

            //bad url, metric tick
            if(!request.head.parameters.contains("myurl")) {
              badUrl.hit()
            }
            Callback.successful(request.ok("received response"))
          }
        }
      }
    }
  }
  
  //creates a stats url on 9001 that's visible to the client
  serviceui.UIServer.start(9001)
  
{% endhighlight %}

## Available Collectors

All collectors are thread-safe.

### Counter

A counter simply allows you to set, increment, and decrement values:

{% highlight scala %}

val counter = Counter("my-counter")

counter.set(Map("foo" -> "bar"), 2)

counter.increment(Map("foo" -> "bar"))

{% endhighlight %}


### Rate

A rate is like a counter, but resets at the beginning of each collection
interval.  Rates are also tracked for every interval.

{% highlight scala %}

val rate = Rate("my-rate")

rate.hit()

{% endhighlight %}



### Histogram

Histograms can gather statistical data about values added to them.  By default,
histograms are setup with various percentiles defined, the values are sorted
and placed in the appropriate percentiles.


{% highlight scala %}

val hist = Histogram("my-histogram", percentiles = List(0.5, 0.99, 0.999))

hist.add(12)
hist.add(1)
hist.add(98765)

{% endhighlight %}


## Metric Reporting

Metric reporters are used to take the periodically generated snapshots of
metrics and report them to an external system.  A `MetricSender` is the
interface to the remote system and is responsible for properly formatting
metrics and handling all communication.  Colossus currently has native support
for OpenTSDB.

{% highlight scala %}

import akka.actor._
import colossus.metrics._
import scala.concurrent.duration._

implicit val system = ActorSystem()
implicit val metrics = MetricSystem(MetricSystemConfig.load("application"))

val reporterConfig = MetricReporterConfig(
  metricSenders = Seq(OpenTsdbSender("host", 123)),
  filters = MetricReporterFilter.All
)

//A reporter must be attached to a specific collection interval.  
metrics.collectionIntervals.get(1.minute).foreach(_.report(reporterConfig))

//now once per minute the value of this rate and any other metrics will be reported
val rate = Rate("myrate")

{% endhighlight %}


