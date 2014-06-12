package com.krux.metrics.reporter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import kafka.metrics.KafkaMetricsConfig;
import kafka.metrics.KafkaMetricsReporter;
import kafka.utils.VerifiableProperties;

import org.apache.log4j.Logger;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.MetricPredicate;

public class KafkaStatsDMetricsReporter implements KafkaMetricsReporter, KafkaStatsDMetricsReporterMBean {

    static Logger log = Logger.getLogger(KafkaStatsDMetricsReporter.class);
    static String STATSD_DEFAULT_HOST = "localhost";
    static int STATSD_DEFAULT_PORT = 8125;
    static String STATSD_DEFAULT_PREFIX = "kafka";

    boolean initialized = false;
    boolean running = false;
    StatsdReporter reporter = null;
    String _statsdHost = STATSD_DEFAULT_HOST;
    int _statsdPort = STATSD_DEFAULT_PORT;
    String _statsdGroupPrefix = STATSD_DEFAULT_PREFIX;
    MetricPredicate predicate = MetricPredicate.ALL;

    @Override
    public String getMBeanName() {
        return "kafka:type=com.krux.metrics.reporter.KafkaStatsdMetricsReporter";
    }

    @Override
    public synchronized void startReporter(long pollingPeriodSecs) {
        if (initialized && !running) {
            reporter.start(pollingPeriodSecs, TimeUnit.SECONDS);
            running = true;
            log.info(String.format("Started Kafka Statsd metrics reporter with polling period %d seconds",
                    pollingPeriodSecs));
        }
    }

    @Override
    public synchronized void stopReporter() {
        if (initialized && running) {
            // reporter.shutdown();
            running = false;
            log.info("Stopped Kafka Statsd metrics reporter");
            try {
                
                reporter = new StatsdReporter( Metrics.defaultRegistry(), _statsdHost, _statsdPort, _statsdGroupPrefix );

            } catch (IOException e) {
                log.error("Unable to initialize StatsdReporter", e);
            }
        }
    }

    @Override
    public synchronized void init(VerifiableProperties props) {
        if (!initialized) {
            KafkaMetricsConfig metricsConfig = new KafkaMetricsConfig(props);
            _statsdHost = props.getString("kafka.statsd.metrics.host", STATSD_DEFAULT_HOST);
            _statsdPort = props.getInt("kafka.statsd.metrics.port", STATSD_DEFAULT_PORT);
            _statsdGroupPrefix = props.getString("kafka.statsd.metrics.group", STATSD_DEFAULT_PREFIX);
            String regex = props.getString("kafka.statsd.metrics.exclude.regex", null);

            log.info("Initialize StatsdReporter [" + _statsdHost + "," + _statsdPort + "," + _statsdGroupPrefix + "]");

            if (regex != null) {
                predicate = new RegexMetricPredicate(regex);
            }
            try {
                reporter = new StatsdReporter( Metrics.defaultRegistry(), _statsdHost, _statsdPort, _statsdGroupPrefix );

            } catch (IOException e) {
                log.error("Unable to initialize StatsdReporter", e);
            }
            if (props.getBoolean("kafka.statsd.metrics.reporter.enabled", false)) {
                initialized = true;
                startReporter(metricsConfig.pollingIntervalSecs());
                log.info("StatsdReporter started. Polling interval: " + metricsConfig.pollingIntervalSecs() );
            }
        }
    }
}