krux-kafka custom reporting jar
===============================

Used by https://github.com/krux/krux-kafka

This jar is copied to that projects' libs/ dir by its package.sh and relies on the following config in server.properties

```
# additional logger info
kafka.metrics.reporters=com.krux.metrics.reporter.KafkaGraphiteMetricsReporter
kafka.graphite.metrics.reporter.enabled=true
kafka.graphite.metrics.host=localhost
kafka.graphite.metrics.port=2003
kafka.graphite.metrics.env=prod
```
