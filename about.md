---
layout: page
title: About
permalink: /about/
---

### Current State of Things

Colossus is currently in it's "pre-1.0" phase, meaning things are working, but
the framework in under very heavy development.  We're working very hard to make
Colossus as clean and usable as possible without making significant performance
sacrifices.


### Release Workflow

Currently we're following an accelerated release workflow.  Currently there is
no backwards compatibility between versions, but we have somewhat formalized
the release schedule to help everyone manage upgrading.  Releases are versioned with the scheme :

{% highlight plaintext %}
[MAJOR].[MINOR].[BUILD]
{% endhighlight %}

Right now the major version is fixed, minor versions introduce breaking
changes, and build versions aim to be backwards compatible (but may have small
breaking changes if there's a pressing need).  Eventually we'll move to a more
solid semantic versioning scheme where only major releases introduce breaking
changes

### Source

The repo is on [Github]({{ site.github_repo_url }}) . You will need to have SBT setup on your computer.  It currently contains several sub-projects


### License

Colossus is released under the [Apache License version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

This documentation is released under the [Creative Commons Attribution 3.0 Unported](http://creativecommons.org/licenses/by/3.0/) License. 


