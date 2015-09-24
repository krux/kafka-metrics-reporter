package com.krux.metrics.reporter;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;

import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

public class KafkaGraphiteMetricsReporter implements KafkaMetricsReporter, KafkaGraphiteMetricsReporterMBean {

    static Logger LOG = Logger.getLogger(KafkaGraphiteMetricsReporter.class);
    static String GRAPHITE_DEFAULT_HOST = "localhost";
    static int GRAPHITE_DEFAULT_PORT = 2003;
    static String GRAPHITE_DEFAULT_PREFIX = "kafka";
    static String GRAPHITE_DEFAULT_SUFFIX = "";

    boolean initialized = false;
    boolean running = false;
    GraphiteReporter reporter = null;
    String graphiteHost = GRAPHITE_DEFAULT_HOST;
    int graphitePort = GRAPHITE_DEFAULT_PORT;
    String graphiteGroupPrefix = GRAPHITE_DEFAULT_PREFIX;
    String graphiteSuffix = GRAPHITE_DEFAULT_SUFFIX;
    String hostMatch = "";
    MetricPredicate predicate = MetricPredicate.ALL;
    boolean logDebugToStdOut = false;

    @Override
    public String getMBeanName() {
        return "kafka:type=com.krux.metrics.reporter.KafkaStatsdMetricsReporter";
    }

    @Override
    public synchronized void startReporter(long pollingPeriodSecs) {
        if (initialized && !running) {
            reporter.start(pollingPeriodSecs, TimeUnit.SECONDS);
            running = true;
            LOG.info(String.format("Started Kafka Graphite metrics reporter with polling period %d seconds",
                    pollingPeriodSecs));
        }
    }

    @Override
    public synchronized void stopReporter() {
        if (initialized && running) {
            // reporter.shutdown();
            running = false;
            LOG.info("Stopped Kafka Graphite metrics reporter");
            try {
                reporter = new GraphiteReporter(Metrics.defaultRegistry(), graphiteHost, graphitePort, graphiteGroupPrefix,
                        graphiteSuffix);
            } catch (IOException e) {
                LOG.error("Unable to initialize GraphiteReporter", e);
            }
        }
    }

    @Override
    public synchronized void init(VerifiableProperties props) {
        if (!initialized) {
            KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
            graphiteHost = props.getString("kafka.graphite.metrics.host", GRAPHITE_DEFAULT_HOST);
            graphitePort = props.getInt("kafka.graphite.metrics.port", GRAPHITE_DEFAULT_PORT);
            graphiteGroupPrefix = props.getString("kafka.graphite.metrics.env", GRAPHITE_DEFAULT_PREFIX);
            hostMatch = props.getString("kafka.broker.stats.sender", "");
            System.setProperty("kafka.broker.stats.sender", hostMatch);
            
            System.setProperty("kafka.graphite.metrics.log.debug",
                    props.getString("kafka.graphite.metrics.log.debug", "false"));
            System.setProperty("zookeeper.connect", props.getString("zookeeper.connect", "localhost:2181"));
            System.setProperty("kafka.http.status.port", props.getString("kafka.http.status.port", "6091"));
            System.setProperty("kafka.http.status.port", props.getString("kafka.http.status.port", "6091"));
            
            try {
                graphiteSuffix = InetAddress.getLocalHost().getHostName().toLowerCase();
                if (graphiteSuffix.contains(".")) {
                    String[] parts = graphiteSuffix.split("\\.");
                    graphiteSuffix = parts[0];
                }
            } catch (UnknownHostException e1) {
                LOG.error(e1);
            }
            String regex = props.getString("kafka.graphite.metrics.exclude.regex", null);

            LOG.info("Initialize GraphiteReporter [" + graphiteHost + "," + graphitePort + "," + graphiteGroupPrefix + "," + hostMatch + "]");

            if (regex != null) {
                predicate = new RegexMetricPredicate(regex);
            }
            try {
                reporter = new GraphiteReporter(Metrics.defaultRegistry(), graphiteHost, graphitePort, graphiteGroupPrefix,
                        graphiteSuffix
                );
            } catch (IOException e) {
                LOG.error("Unable to initialize GraphiteReporter", e);
            }
            if (props.getBoolean("kafka.graphite.metrics.reporter.enabled", false)) {
                initialized = true;
                startReporter(metricsConfig.pollingIntervalSecs());
                LOG.debug("GraphiteReporter started.");
            }
        }
    }
}