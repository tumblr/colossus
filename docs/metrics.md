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

### Aggregation Intervals

Aggregation intervals define how often the raw data from all event collectors
are snapshotted and merged together into a single database of values.

By default, a metric system is created with both 1 second and 1 minute
aggregation intervals.  The idea here is the 1 second interval is used to show
real-time metrics where rates and histograms are all showing values reflective
of the last second of activity, whereas the 1 minute intervals are used for
reporting values to an external database in which case the values reported are
all reflective of the last minute of activity.



## Getting Started

If you are using colossus, it depends on the metrics library and pulls it in.  Otherwise you must add the following to your build.sbt/Build.scala

{% highlight scala %}

libraryDependencies += "com.tumblr" %% "colossus-metrics" % "{{ site.latest_version }}"

{% endhighlight %}

From there, the only required step is to spin up a `MetricSystem`

{% highlight scala %}

import akka.actor._
import metrics._
import scala.concurrent.duration._

implicit val actor_system = ActorSystem()

//create the metric system
val metric_system = MetricSystem("/my-service")

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


