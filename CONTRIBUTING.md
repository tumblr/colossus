# Contributing

Because Colossus is pre 1.0 and under heavy development, there is lots of
room for improvement and we are not afraid to make large-scale breaking changes
if it makes sense.  All bug fixes and feature improvements are welcome, but if
you are planning on implementing a significant change, especially one that
changes an API or breaks backwards compatibility, you should verify with one of
the core contributors that your work won't conflict with any ongoing or planned
changes.

## Branches

+ `master` contains the latest published version.
+ `develop` contains the next version.
+ `feature/*` for large features needing multiple PRs.

We use the [GitFlow](https://datasift.github.io/gitflow/IntroducingGitFlow.html) model for dealing with branches and releases.

## Pull Requests

1. Make sure an issue exists in the issue tracker for the work you want to contribute.
2. Fork the repo and create a branch from `develop`.
    + If it is a small change that will go into the next release then it can be named anything.
    + If it is a large feature that will require multiple PRs, then prefix the name with `feature/`.
3. Write the change.
4. If necessary, add tests.
5. If necessary, update [documentation](#documentation).
6. If you've significantly changed low-level code, you must run [benchmarks](#benchmarking) and provide numbers.
7. If you haven't already, complete the [Contributor License Agreement (CLA)](#contributor-license-agreement).
8. Submit your PR against the `develop` branch. Include the text "Fixes #issue-number" so when the PR is merged, the issue is automatically closed.
9. Once approved by two core contributors, the PR can be merged into `develop`.


## Documentation

[Paradox](https://github.com/lightbend/paradox) is used to turn the markdown files in `colosuss-docs/src/main/paradox` into HTML. To generate:

```
sbt
> project colossus-docs
> paradox
```

Then open `colossus-docs/target/paradox/site/main/index.html` to see the site.

When a new version is released, the version should be added to the top of `docs.yml` and the rake task ran:

```bash
rake site:publish
```

The rake task checks out each tagged version of colossus, runs paradox and generates the scaladocs. Each version is copied to a folder with the version at the top of `docs.yml` used as the latest version. The resulting site is upload to `gh-pages` branch, which github serves at `tumblr.github.io/colossus`.

## Benchmarking

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

## Contributor License Agreement

In order to accept your pull request, we need you to submit a Contributor License Agreement (CLA). A CLA is a way of protecting intellectual property disputes from harming the project. Complete your CLA [here](http://static.tumblr.com/zyubucd/GaTngbrpr/tumblr_corporate_contributor_license_agreement_v1__10-7-14.pdf).

## License

By contributing to Colossus you agree that your contributions will be licensed under its Apache 2.0 license.
