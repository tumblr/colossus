Colossus
=========

[![Build Status](https://api.travis-ci.org/tumblr/colossus.png?branch=master)](https://travis-ci.org/tumblr/colossus)


Colossus is a lightweight I/O framework for building scala services.

Full documentation can be found here : http://tumblr.github.io/colossus

For general discussion and Q&A, check out the [Google Group](https://groups.google.com/forum/#!forum/colossus-users).

Here's an overview of what you can find in this repo

* `colossus` : The framework
* `colossus-metrics` : high-performance metrics library (does not depend on colossus)
* `colossus-examples` : A few simple examples that can be run
* `colossus-testkit` : Small library containing a few useful tools for testing
* `colossus-tests` : The unit and integration tests for colossus

### Third-party protocol extensions

We're currently asking anyone working on implementing support for new protocols to build your project as a separate repo and publish your own artifacts.  This will make it much easier for you to maintain your code as well as help us keep the main codebase slim.  Once you have published artifacts that build for the latest Colossus development release, let us know and we'll add it to the list.

For any improvements to the core protocols we support, or for general bug fixes and improvements, please see the [Contributing Guidelines](https://github.com/tumblr/colossus/blob/master/CONTRIBUTING.md).

Our growing list of known third-party protocol development:

* MongoDB codec: https://github.com/fehmicansaglam/colossus-extensions

### License

Copyright 2014 Tumblr Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

