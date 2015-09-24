Krux Kafka Metrics Reporter
===============================

The Krux Kafka Reporter is a drop-in jar that utilizes the Kafka server custom reporting interface to automatically produce topic consumption lag stats reporting on a per-consumer/per-partition basis.  It pushes those stats directly to a Graphite server configured in the Kafka configuration file. This reporter produces the same offset results as the [Kafka ConsumerOffsetChecker](http://kafka.apache.org/documentation.html#basic_ops_consumer_lag) command-line tool for *all* consumers currently consuming from the broker cluster.

Use
---
Make this jar available to a Kafka broker by placing it on the classpath, and add the following lines to your Kafka configuration...

    # custom metrics logger config
    kafka.metrics.reporters=com.krux.metrics.reporter.KafkaGraphiteMetricsReporter
    kafka.graphite.metrics.reporter.enabled=true
    kafka.graphite.metrics.host=localhost
    kafka.graphite.metrics.port=2003
    # optional - in a cluster, only this machine will send stats
    kafka.broker.stats.sender=my.host.name.com

    #full tree prefix
    kafka.graphite.metrics.env=prod

    #if true, log all reported metrics to stdout. Default is false. (useful for testing)
    kafka.graphite.metrics.log.debug=true


Metrics will fall under a prefix of "env".kafka.consumer.topic_lag prefix, where "env" is specified by the `kafka.graphite.metrics.env` in the config settings above.

To prevent all brokers in your cluster from sending the same, duplicated information to Graphite, you should set the `kafka.broker.stats.sender` property to the fully qualified domain name of one of the machines in the broker cluster. Only that machine will forward consumer lag stats.  This approach allows a single, common configuration across all hosts in your cluster.

Releases
--------
All releases are available as pre-built jars [here](https://github.com/krux/kafka-metrics-reporter/releases). You can also build the source from scratch using Maven.

    mvn clean package
    
will produce a jar with all dependencies at `target/kafka-metrics-reporter-1.1.4.jar`.



 