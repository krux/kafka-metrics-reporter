Krux Kafka Metrics Reporter
===============================

Used by https://github.com/krux/krux-kafka

This jar is copied to that projects' libs/ dir by its package.sh and relies on the following config in the kafka server config file:

```
# additional logger info
kafka.metrics.reporters=com.krux.metrics.reporter.KafkaGraphiteMetricsReporter
kafka.graphite.metrics.reporter.enabled=true
kafka.graphite.metrics.host=localhost
kafka.graphite.metrics.port=2003

#full tree prefix
kafka.graphite.metrics.env=prod

#if true, log all reported metrics to stdout. Default is false.
kafka.graphite.metrics.log.debug=true
```
