---
layout: page
title: Metrics
---

Colossus uses the (currently nameless) metrics library.

## Introduction

High-throughput Colossus services can serve hundreds of thousands of requests
per second, which can easily translate to millions of recordable events per
second.  The Metrics library provides a way to work with metrics with as little
overhead as possible.

### Collection Intervals

Collection intervals define how often the raw data from all event collectors
are snapshotted and merged together into a single database of values.

By default, a metric system is created with both 1 second and 1 minute
collection intervals.  The 1 second interval is used to show
real-time metrics where rates and histograms are all showing values reflective
of the last second of activity, whereas the 1 minute intervals are used for
reporting values to an external database in which case the values reported are
all reflective of the last minute of activity.

### Metric Addresses and Tags

Every metric has a url-like address used to identify it.  

One of the most important aspects of metrics is that values can be tagged.  For example, a rate can track hits to API endpoints, using a "endpoint" tag to break down the usage by endpoint.

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

//create the metric system
val metricSystem = MetricSystem("/my-service")

//while you can create multiple collections, in most cases you'll want to just
use the base collection
import metricSystem.base

val rate = Rate("/my-rate")

rate.hit()

{% endhighlight %}


## Available Collectors

All collectors are thread-safe.

### Counter

A counter simply allows you to set, increment, and decrement values:

{% highlight scala %}

val counter = Counter("/my-counter")

counter.set(2, Map("foo" -> "bar"))

counter.increment(Map("foo" -> "bar"))

{% endhighlight %}


### Rate

A rate is like a counter, but resets at the beginning of each collection
interval.  Rates are also tracked for every interval.

{% highlight scala %}

val rate = Rate("/my-rate")

rate.hit()

{% endhighlight %}



### Histogram

Histograms can gather statistical data about values added to them.  By default,
histograms are setup with log-scale buckets, best for recording latency, but
this can be overridden.


{% highlight scala %}

val hist = Histogram("/my-histogram", percentiles = List(0.5, 0.99, 0.999))

hist.add(12)
hist.add(1)
hist.add(98765)

{% endhighlight %}


## Metric Reporting

Currently metric reporting is mostly focused on reporting to OpenTSDB.  To setup reporting you basically need 2 things:

* A MetricSender - this is the object that encodes metrics to be sent
* A set of metric filters - These are used to select and aggregate which metrics to send

In addition to OpenTSDB, metrics may also be logged to file. To use logging, change the MetricSystem to use
a LoggingSender as the metrics reporter:

{% highlight scala %}

import akka.actor._
import metrics._
import scala.concurrent.duration._

implicit val actor_system = ActorSystem()

//create the metric system
val metric_system = MetricSystem("/my-service")

//create the config, providing LoggerSender as the MetricSender
val metric_config = MetricReporterConfig(LoggerSender)

//set this as the reporting for the metric system
metric_system.metricIntervals(1.minute).report(metric_config)

//get a collection
val collection = metric_system.sharedCollection

//proceed as normal

{% endhighlight %}


