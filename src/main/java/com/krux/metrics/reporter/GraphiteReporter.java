package com.krux.metrics.reporter;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.lang.Thread.State;
import java.net.Socket;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

import kafka.api.OffsetRequest;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.consumer.SimpleConsumer;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import scala.Predef;
import scala.Tuple2;
import scala.collection.JavaConverters;

import com.fasterxml.jackson.jr.ob.JSON;
import com.krux.metrics.http.status.HttpServer;
import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Clock;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Gauge;
import com.yammer.metrics.core.Histogram;
import com.yammer.metrics.core.Metered;
import com.yammer.metrics.core.Metric;
import com.yammer.metrics.core.MetricName;
import com.yammer.metrics.core.MetricPredicate;
import com.yammer.metrics.core.MetricProcessor;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.core.Sampling;
import com.yammer.metrics.core.Summarizable;
import com.yammer.metrics.core.Timer;
import com.yammer.metrics.core.VirtualMachineMetrics;
import com.yammer.metrics.reporting.AbstractPollingReporter;
import com.yammer.metrics.stats.Snapshot;

/**
 * A simple reporter which sends out application metrics to a <a
 * href="http://graphite.wikidot.com/faq">Graphite</a> server periodically.
 */
public class GraphiteReporter extends AbstractPollingReporter implements MetricProcessor<Long> {
    private static final Logger LOG = LoggerFactory.getLogger(GraphiteReporter.class);
    protected final String prefix;
    protected final String suffix;
    protected final MetricPredicate predicate;
    protected final Locale locale = Locale.US;
    protected final Clock clock;
    protected final SocketProvider socketProvider;
    protected final VirtualMachineMetrics vm;
    protected Writer writer;
    public boolean printVMMetrics = true;
    
    static private CuratorFramework _client;
    
    static {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 5);
        String zkConnect = System.getProperty("zookeeper.connect");
        //LOG.info("zkConnect: " + zkConnect);
        System.out.println("zkConnect: " + zkConnect);
        _client = CuratorFrameworkFactory.newClient(zkConnect, retryPolicy);
        _client.start();
        
//        Integer port = Integer.parseInt( System.getProperty( "krux.kafka.status.reporter.http.port" ) );
//        Thread t = new Thread( new HttpServer( port ) );
//        t.start();
    }

    /**
     * Enables the graphite reporter to send data for the default metrics
     * registry to graphite server with the specified period.
     * 
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     */
    public static void enable(long period, TimeUnit unit, String host, int port) {
        enable(Metrics.defaultRegistry(), period, unit, host, port);
    }

    /**
     * Enables the graphite reporter to send data for the given metrics registry
     * to graphite server with the specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port) {
        enable(metricsRegistry, period, unit, host, port, null, null);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     */
    public static void enable(long period, TimeUnit unit, String host, int port, String prefix, String suffix) {
        enable(Metrics.defaultRegistry(), period, unit, host, port, prefix, suffix);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port,
            String prefix, String suffix) {
        enable(metricsRegistry, period, unit, host, port, prefix, MetricPredicate.ALL, suffix);
    }

    /**
     * Enables the graphite reporter to send data to graphite server with the
     * specified period.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param period
     *            the period between successive outputs
     * @param unit
     *            the time unit of {@code period}
     * @param host
     *            the host name of graphite server (carbon-cache agent)
     * @param port
     *            the port number on which the graphite server is listening
     * @param prefix
     *            the string which is prepended to all metric names
     * @param predicate
     *            filters metrics to be reported
     */
    public static void enable(MetricsRegistry metricsRegistry, long period, TimeUnit unit, String host, int port,
            String prefix, MetricPredicate predicate, String suffix) {
        try {
            final GraphiteReporter reporter = new GraphiteReporter(metricsRegistry, prefix, suffix, predicate,
                    new DefaultSocketProvider(host, port), Clock.defaultClock());
            reporter.start(period, unit);
        } catch (Exception e) {
            LOG.error("Error creating/starting Graphite reporter:", e);
        }
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(String host, int port, String prefix, String suffix) throws IOException {
        this(Metrics.defaultRegistry(), host, port, prefix, suffix);
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param host
     *            is graphite server
     * @param port
     *            is port on which graphite server is running
     * @param prefix
     *            is prepended to all names reported to graphite
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String host, int port, String prefix, String suffix)
            throws IOException {
        this(metricsRegistry, prefix, suffix, MetricPredicate.ALL, new DefaultSocketProvider(host, port), Clock
                .defaultClock());
        LOG.info("Instantiated " + this.getClass().getCanonicalName());
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, String suffix, MetricPredicate predicate,
            SocketProvider socketProvider, Clock clock) throws IOException {
        this(metricsRegistry, prefix, suffix, predicate, socketProvider, clock, VirtualMachineMetrics.getInstance());
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @param vm
     *            a {@link VirtualMachineMetrics} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, String suffix, MetricPredicate predicate,
            SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm) throws IOException {
        this(metricsRegistry, prefix, suffix, predicate, socketProvider, clock, vm, "graphite-reporter");
    }

    /**
     * Creates a new {@link GraphiteReporter}.
     * 
     * @param metricsRegistry
     *            the metrics registry
     * @param prefix
     *            is prepended to all names reported to graphite
     * @param predicate
     *            filters metrics to be reported
     * @param socketProvider
     *            a {@link SocketProvider} instance
     * @param clock
     *            a {@link Clock} instance
     * @param vm
     *            a {@link VirtualMachineMetrics} instance
     * @throws IOException
     *             if there is an error connecting to the Graphite server
     */
    public GraphiteReporter(MetricsRegistry metricsRegistry, String prefix, String suffix, MetricPredicate predicate,
            SocketProvider socketProvider, Clock clock, VirtualMachineMetrics vm, String name) throws IOException {
        super(metricsRegistry, name);
        this.socketProvider = socketProvider;
        this.vm = vm;

        this.clock = clock;

        if (prefix != null) {
            // Pre-append the "." so that we don't need to make anything
            // conditional later.
            this.prefix = "ymetrics2." + prefix + ".";
        } else {
            this.prefix = "ymetrics2.";
        }

        if (suffix != null) {
            this.suffix = "." + suffix;
        } else {
            this.suffix = "";
        }

        this.predicate = predicate;
    }

    @Override
    public void run() {
        Socket socket = null;
        try {

            socket = this.socketProvider.get();
            writer = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            boolean logDebugToStdOut = Boolean.parseBoolean(System.getProperty("kafka.graphite.metrics.log.debug", "false"));
            if (logDebugToStdOut) {
                System.out.println("Sending stats to " + socket.getInetAddress() + ":" + socket.getPort());
            }

            final long epoch = clock.time() / 1000;

            if (this.printVMMetrics) {
                printVmMetrics(epoch);
            }
            printRegularMetrics(epoch);

            printConsumerLagMetrics(epoch);

            writer.flush();
            // LOG.info( "Sent stats to graphite" );
        } catch (Exception e) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Error writing to Graphite", e);
                e.printStackTrace(System.err);
                e.printStackTrace(System.out);
            } else {
                LOG.warn("Error writing to Graphite: {}", e.getMessage());
                e.printStackTrace(System.err);
                e.printStackTrace(System.out);
            }
            if (writer != null) {
                try {
                    writer.flush();
                } catch (IOException e1) {
                    LOG.error("Error while flushing writer:", e1);
                    e.printStackTrace(System.err);
                    e.printStackTrace(System.out);
                }
            }
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    LOG.error("Error while closing socket:", e);
                }
            }
            writer = null;
        }
    }

    private void printConsumerLagMetrics(final long epoch) {

        try {

            try {
                List<String> children = _client.getChildren().forPath("/consumers");

                for (String child : children) {
                    String fullTopicPath = "/consumers/" + child + "/offsets";
                    List<String> childTopics = _client.getChildren().forPath(fullTopicPath);
                    for (String topic : childTopics) {
                        byte[] data = _client.getData().forPath("/brokers/topics/" + topic);
                        if (data != null) {
                            String dataStr = new String(data, "UTF8");
                            // parse json into map
                            Map<String, Object> partitionMap = JSON.std.mapFrom(dataStr);
                            Map<String, Object> realPartitionMap = (Map<String, Object>) partitionMap.get("partitions");

                            for (String pid : realPartitionMap.keySet()) {
                                // get offset
                                String offsetPath = "/consumers/" + child + "/offsets/" + topic + "/" + pid;
                                byte[] offsetBytes = _client.getData().forPath(offsetPath);
                                long offset = Long.parseLong(new String(offsetBytes, "UTF8"));

                                String ownerPath = "/consumers/" + child + "/owners/" + topic + "/" + pid;
                                String ownerStr;
                                // this call will fail if there are no consumers
                                // currently listening, so trap
                                try {
                                    byte[] ownerBytes = _client.getData().forPath(ownerPath);
                                    ownerStr = new String(ownerBytes, "UTF8");
                                } catch (Exception e) {
                                    ownerStr = "none";
                                }

                                String leadersAndIsrPath = "/brokers/topics/" + topic + "/partitions/" + pid + "/state";
                                byte[] topicPartitionAndIsrBytes = _client.getData().forPath(leadersAndIsrPath);
                                String partionAndIsr;
                                Integer leader;
                                String consumerInfoStr;
                                String consumerHost;
                                Integer consumerPort;
                                long lastOffset = 0;
                                if (topicPartitionAndIsrBytes != null) {
                                    partionAndIsr = new String(topicPartitionAndIsrBytes, "UTF8");
                                    Map<String, Object> leaderIsrMap = JSON.std.mapFrom(partionAndIsr);
                                    leader = (Integer) leaderIsrMap.get("leader");

                                    String consumerPath = "/brokers/ids/" + leader;

                                    byte[] consumerInfoBytes = _client.getData().forPath(consumerPath);

                                    if (consumerInfoBytes != null) {
                                        consumerInfoStr = new String(consumerInfoBytes, "UTF8");
                                        Map<String, Object> consumerMap = JSON.std.mapFrom(consumerInfoStr);
                                        consumerHost = (String) consumerMap.get("host");
                                        consumerPort = (Integer) consumerMap.get("port");

                                        SimpleConsumer sc = new SimpleConsumer(consumerHost, consumerPort, 10000, 100000,
                                                "GraphiteReporterOffsetChecker");

                                        lastOffset = getLastOffset(sc, topic, Integer.parseInt(pid),
                                                OffsetRequest.LatestTime(), "GraphiteReporterOffsetChecker");
                                    }
                                }
                                if (!ownerStr.equals("none")) {
                                    sendInt(epoch, "kafka.consumer.lag." + child + "." + ownerStr.replace(".", "-"), "lag",
                                            (lastOffset - offset));
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    protected void printRegularMetrics(final Long epoch) {
        for (Entry<String, SortedMap<MetricName, Metric>> entry : getMetricsRegistry().groupedMetrics(predicate).entrySet()) {
            for (Entry<MetricName, Metric> subEntry : entry.getValue().entrySet()) {
                final Metric metric = subEntry.getValue();
                if (metric != null) {
                    try {
                        metric.processWith(this, subEntry.getKey(), epoch);
                    } catch (Exception ignored) {
                        LOG.error("Error printing regular metrics:", ignored);
                    }
                }
            }
        }
    }

    protected void sendInt(long timestamp, String name, String valueName, long value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%d", value));
    }

    protected void sendFloat(long timestamp, String name, String valueName, double value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%2.2f", value));
    }

    protected void sendObjToGraphite(long timestamp, String name, String valueName, Object value) {
        sendToGraphite(timestamp, name, valueName + " " + String.format(locale, "%s", value));
    }

    protected void sendToGraphite(long timestamp, String name, String value) {
        try {
            if (!prefix.isEmpty()) {
                writer.write(prefix);
            }
            writer.write(sanitizeString(name));
            writer.write('.');
            String[] parts = value.split(" ");
            writer.write(parts[0]);
            if (!suffix.isEmpty()) {
                writer.write(suffix);
            }
            writer.write(' ');
            writer.write(parts[1]);
            writer.write(' ');
            writer.write(Long.toString(timestamp));
            writer.write('\n');
            writer.flush();

            boolean logDebugToStdOut = Boolean.parseBoolean(System.getProperty("kafka.graphite.metrics.log.debug", "false"));
            if (logDebugToStdOut) {
                StringBuilder sb = new StringBuilder();
                if (!prefix.isEmpty()) {
                    sb.append(prefix);
                }
                sb.append(sanitizeString(name));
                sb.append('.');
                sb.append(value);
                sb.append(' ');
                sb.append(Long.toString(timestamp));
                System.out.println(sb.toString());
            }

        } catch (IOException e) {
            LOG.error("Error sending to Graphite:", e);
            e.printStackTrace(System.err);
            e.printStackTrace(System.out);
        }
    }

    protected String sanitizeName(MetricName name) {
        final StringBuilder sb = new StringBuilder().append(name.getGroup()).append('.').append(name.getType()).append('.');
        if (name.hasScope()) {
            sb.append(name.getScope()).append('.');
        }
        sb.append(name.getName());
        return sb.toString();
    }

    protected String sanitizeString(String s) {
        return s.replace(' ', '-');
    }

    @Override
    public void processGauge(MetricName name, Gauge<?> gauge, Long epoch) throws IOException {
        sendObjToGraphite(epoch, sanitizeName(name), "value", gauge.value());
    }

    @Override
    public void processCounter(MetricName name, Counter counter, Long epoch) throws IOException {
        sendInt(epoch, sanitizeName(name), "count", counter.count());
    }

    @Override
    public void processMeter(MetricName name, Metered meter, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);
        sendInt(epoch, sanitizedName, "count", meter.count());
        sendFloat(epoch, sanitizedName, "meanRate", meter.meanRate());
        sendFloat(epoch, sanitizedName, "1MinuteRate", meter.oneMinuteRate());
        sendFloat(epoch, sanitizedName, "5MinuteRate", meter.fiveMinuteRate());
        sendFloat(epoch, sanitizedName, "15MinuteRate", meter.fifteenMinuteRate());
    }

    @Override
    public void processHistogram(MetricName name, Histogram histogram, Long epoch) throws IOException {
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, histogram);
        sendSampling(epoch, sanitizedName, histogram);
    }

    @Override
    public void processTimer(MetricName name, Timer timer, Long epoch) throws IOException {
        processMeter(name, timer, epoch);
        final String sanitizedName = sanitizeName(name);
        sendSummarizable(epoch, sanitizedName, timer);
        sendSampling(epoch, sanitizedName, timer);
    }

    protected void sendSummarizable(long epoch, String sanitizedName, Summarizable metric) throws IOException {
        sendFloat(epoch, sanitizedName, "min", metric.min());
        sendFloat(epoch, sanitizedName, "max", metric.max());
        sendFloat(epoch, sanitizedName, "mean", metric.mean());
        sendFloat(epoch, sanitizedName, "stddev", metric.stdDev());
    }

    protected void sendSampling(long epoch, String sanitizedName, Sampling metric) throws IOException {
        final Snapshot snapshot = metric.getSnapshot();
        sendFloat(epoch, sanitizedName, "median", snapshot.getMedian());
        sendFloat(epoch, sanitizedName, "75percentile", snapshot.get75thPercentile());
        sendFloat(epoch, sanitizedName, "95percentile", snapshot.get95thPercentile());
        sendFloat(epoch, sanitizedName, "98percentile", snapshot.get98thPercentile());
        sendFloat(epoch, sanitizedName, "99percentile", snapshot.get99thPercentile());
        sendFloat(epoch, sanitizedName, "999percentile", snapshot.get999thPercentile());
    }

    protected void printVmMetrics(long epoch) {
        sendFloat(epoch, "jvm.memory", "heap_usage", vm.heapUsage());
        sendFloat(epoch, "jvm.memory", "non_heap_usage", vm.nonHeapUsage());
        for (Entry<String, Double> pool : vm.memoryPoolUsage().entrySet()) {
            sendFloat(epoch, "jvm.memory.memory_pool_usages", sanitizeString(pool.getKey()), pool.getValue());
        }

        sendInt(epoch, "jvm", "daemon_thread_count", vm.daemonThreadCount());
        sendInt(epoch, "jvm", "thread_count", vm.threadCount());
        sendInt(epoch, "jvm", "uptime", vm.uptime());
        sendFloat(epoch, "jvm", "fd_usage", vm.fileDescriptorUsage());

        for (Entry<State, Double> entry : vm.threadStatePercentages().entrySet()) {
            sendFloat(epoch, "jvm.thread-states", entry.getKey().toString().toLowerCase(), entry.getValue());
        }

        for (Entry<String, VirtualMachineMetrics.GarbageCollectorStats> entry : vm.garbageCollectors().entrySet()) {
            final String name = "jvm.gc." + sanitizeString(entry.getKey());
            sendInt(epoch, name, "time", entry.getValue().getTime(TimeUnit.MILLISECONDS));
            sendInt(epoch, name, "runs", entry.getValue().getRuns());
        }
    }

    public static class DefaultSocketProvider implements SocketProvider {

        private final String host;
        private final int port;

        public DefaultSocketProvider(String host, int port) {
            this.host = host;
            this.port = port;

        }

        @Override
        public Socket get() throws Exception {
            return new Socket(this.host, this.port);
        }

    }

    public static <A, B> scala.collection.immutable.Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala().toMap(Predef.<Tuple2<A, B>> conforms());
    }

    public static long getLastOffset(SimpleConsumer consumer, String topic, int partition, long whichTime, String clientName) {
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfo = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfo.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        kafka.javaapi.OffsetRequest request = new kafka.javaapi.OffsetRequest(requestInfo,
                kafka.api.OffsetRequest.CurrentVersion(), clientName);
        kafka.javaapi.OffsetResponse response = consumer.getOffsetsBefore(request);

        if (response.hasError()) {
            System.out
                    .println("Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return 0;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }
}
