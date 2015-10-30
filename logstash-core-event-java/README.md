# logstash-core-event-java

## dev install

- build code with

```
cd logstash-core-event-java
$ gradle build
```

A bunch of warning are expected, it should end with:

```
BUILD SUCCESSFUL
```

- update logstash `Gemfile` to use this gem with:

```
# gem "logstash-core-event", "3.0.0.dev", :path => "./logstash-core-event"
gem "logstash-core-event-java", "3.0.0.dev", :path => "./logstash-core-event-java"
```

and install:

```
$ bin/bundle
```

- install core plugins for tests

```
$ rake test:install-core
```

## specs

```
$ bin/rspec specs
$ bin/rspec logstash-core/spec
$ bin/rspec logstash-core-event-java/spec
```
