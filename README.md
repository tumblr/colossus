Colossus
=========

[![Build Status](https://api.travis-ci.org/tumblr/colossus.png?branch=master)](https://travis-ci.org/tumblr/colossus)

[![Maven Central](https://maven-badges.herokuapp.com/maven-central/com.tumblr/colossus_2.11/badge.svg)](https://maven-badges.herokuapp.com/maven-central/com.tumblr/colossus_2.11)

[![codecov.io](http://codecov.io/github/tumblr/colossus/coverage.svg?branch=master)](http://codecov.io/github/tumblr/colossus?branch=master)

Colossus is a lightweight I/O framework for building Scala services.
Colossus是一个用来写Scala微服务的轻型I/O框架。

Full documentation can be found here : http://tumblr.github.io/colossus

For general discussion and Q&A, check out the [Google Group](https://groups.google.com/forum/#!forum/colossus-users).

Colossus takes part in the TechEmpower web framework benchmarks under the [JSON serialization](https://www.techempower.com/benchmarks/#test=json) and [Plaintext](https://www.techempower.com/benchmarks/#test=plaintext) tests.

Here's an overview of what you can find in this repo

* `colossus` : The framework
* `colossus-metrics` : high-performance metrics library (does not depend on colossus)
* `colossus-examples` : A few simple examples that can be run
* `colossus-testkit` : Small library containing a few useful tools for testing
* `colossus-tests` : The unit and integration tests for colossus
* `colossus-docs` : Markdown documentation

### Benchmarking

1. Start the main app in examples project:

    ```sbtshell
    sbt colossus-examples/run
    ```
    
2. Run `wrk` with 50 connections, across 2 threads, a pipeline size of 16, for 10 seconds:

    ```bash
    wrk -c 50 -t 2 -d 10 --latency -s pipeline.lua http://localhost:9007/plaintext -- 16
    ```

**Note**: You definitely want to run it several times to make sure you get a good average. First time is always slower. 
Throughput is the best metric here, since it's a single number and in this benchmark correlates very well with latency.
Performance takes a hit if you are constantly compiling, running, and shutting down. Best to compile, kill SBT, restart, 
then run. Very tedious, but the most accurate. Also when running on your mac be very aware of running other apps and it
is usually best to kill CPU heavy apps.

### License

Copyright 2016 Tumblr Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

