#!/usr/bin/env bash

if [[ $CI != 'true' ]]; then
    echo "Not running on Travis-ci"
    exit 0
fi
 
if [[ $TRAVIS_BRANCH =~ ^v[0-9]+\.[0-9]+\.[0-9]+(-RC[0-9]+|-M[0-9]+)?$ || ($TRAVIS_PULL_REQUEST == "false" && $TRAVIS_BRANCH == "develop" && $(cat version.sbt) =~ "-SNAPSHOT") ]]; then
    # decrypt secrets & untar 
    openssl aes-256-cbc -K $encrypted_9df8dc1a95a3_key -iv $encrypted_9df8dc1a95a3_iv -in secrets.tar.enc -out secrets.tar -d
    tar xvf secrets.tar
    # publish, close & release
    sbt ++$TRAVIS_SCALA_VERSION publishSigned sonatypeReleaseAll
else 
    echo 'Only publishing on version tags or develop snapshots'
    exit 0
fi