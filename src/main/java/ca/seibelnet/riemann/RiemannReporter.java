package ca.seibelnet.riemann;

import com.aphyr.riemann.Proto;
import com.codahale.metrics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;


/**
 * This is basically a direct copy of {{GraphiteReporter}} adapted to use RiemannClient
 */
public class RiemannReporter extends ScheduledReporter {

    /**
     * Returns a new {@link Builder} for {@link RiemannReporter}.
     *
     * @param registry the registry to report
     * @return a {@link Builder} instance for a {@link RiemannReporter}
     */
    public static Builder forRegistry(MetricRegistry registry) {
        return new Builder(registry);
    }

    /**
     * A builder for {@link RiemannReporter} instances. Defaults to not using a prefix, using the
     * default clock, converting rates to events/second, converting durations to milliseconds, and
     * not filtering metrics.
     */
    public static class Builder {
        private final MetricRegistry registry;
        private Clock clock;
        private TimeUnit rateUnit;
        private TimeUnit durationUnit;
        private MetricFilter filter;
        private Float ttl;

        private Builder(MetricRegistry registry) {
            this.registry = registry;
            this.clock = Clock.defaultClock();
            this.rateUnit = TimeUnit.SECONDS;
            this.durationUnit = TimeUnit.MILLISECONDS;
            this.filter = MetricFilter.ALL;
            this.ttl = 10.0f;
        }

        /**
         * Use the given {@link Clock} instance for the time.
         *
         * @param clock a {@link Clock} instance
         * @return {@code this}
         */
        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        /**
         * Convert rates to the given time unit.
         *
         * @param rateUnit a unit of time
         * @return {@code this}
         */
        public Builder convertRatesTo(TimeUnit rateUnit) {
            this.rateUnit = rateUnit;
            return this;
        }

        /**
         * Convert durations to the given time unit.
         *
         * @param durationUnit a unit of time
         * @return {@code this}
         */
        public Builder convertDurationsTo(TimeUnit durationUnit) {
            this.durationUnit = durationUnit;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param filter a {@link MetricFilter}
         * @return {@code this}
         */
        public Builder filter(MetricFilter filter) {
            this.filter = filter;
            return this;
        }

        /**
         * Only report metrics which match the given filter.
         *
         * @param ttl Default time to live of Riemann event.
         * @return {@code this}
         */
        public Builder withTtl(Float ttl) {
            this.ttl = ttl;
            return this;
        }

        /**
         * Builds a {@link RiemannReporter} with the given properties, sending metrics using the
         * given {@link Riemann} event.
         *
         * @param riemann a {@link Riemann} event
         * @return a {@link RiemannReporter}
         */
        public RiemannReporter build(Riemann riemann) {
            return new RiemannReporter(registry,
                    riemann,
                    clock,
                    rateUnit,
                    durationUnit,
                    ttl,
                    filter);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(RiemannReporter.class);

    private final Riemann riemann;
    private final Clock clock;

    private RiemannReporter(MetricRegistry registry,
                            Riemann riemann,
                            Clock clock,
                            TimeUnit rateUnit,
                            TimeUnit durationUnit,
                            Float ttl,
                            MetricFilter filter) {
        super(registry, "riemann-reporter", filter, rateUnit, durationUnit);
        this.riemann = riemann;
        this.clock = clock;

        riemann.setDefaultTtl(ttl);
    }

    @Override
    public void report(SortedMap<String, Gauge> gauges,
                       SortedMap<String, Counter> counters,
                       SortedMap<String, Histogram> histograms,
                       SortedMap<String, Meter> meters,
                       SortedMap<String, Timer> timers) {
        final long timestamp = clock.getTime() / 1000;

        log.debug("Reporting metrics: for " + timestamp + " at " + System.currentTimeMillis());
        // oh it'd be lovely to use Java 7 here
        try {
            riemann.connect();
            List<Proto.Event> events = new ArrayList<Proto.Event>();

            for (Map.Entry<String, Gauge> entry : gauges.entrySet()) {
                events.add(reportGauge(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Counter> entry : counters.entrySet()) {
                events.add(reportCounter(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
                events.addAll(reportHistogram(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Meter> entry : meters.entrySet()) {
                events.addAll(reportMetered(entry.getKey(), entry.getValue(), timestamp));
            }

            for (Map.Entry<String, Timer> entry : timers.entrySet()) {
                events.addAll(reportTimer(entry.getKey(), entry.getValue(), timestamp));
            }

            riemann.client.sendEvents(events);

        } catch (IOException e) {
            log.warn("Unable to report to Riemann", riemann, e);
        } finally {
            log.debug("Completed at " + System.currentTimeMillis());
        }
    }

    private ArrayList<Proto.Event> reportTimer(String name, Timer timer, long timestamp) throws IOException {
        final Snapshot snapshot = timer.getSnapshot();

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(riemann.event(name + " max").metric(convertDuration(snapshot.getMax())).time(timestamp).build());
        events.add(riemann.event(name + " mean").metric(convertDuration(snapshot.getMean())).time(timestamp).build());
        events.add(riemann.event(name + " min").metric(convertDuration(snapshot.getMin())).time(timestamp).build());
        events.add(riemann.event(name + " stddev").metric(convertDuration(snapshot.getStdDev())).time(timestamp).build());
        events.add(riemann.event(name + " p50").metric(convertDuration(snapshot.getMedian())).time(timestamp).build());
        events.add(riemann.event(name + " p75").metric(convertDuration(snapshot.get75thPercentile())).time(timestamp).build());
        events.add(riemann.event(name + " p95").metric(convertDuration(snapshot.get95thPercentile())).time(timestamp).build());
        events.add(riemann.event(name + " p98").metric(convertDuration(snapshot.get98thPercentile())).time(timestamp).build());
        events.add(riemann.event(name + " p99").metric(convertDuration(snapshot.get99thPercentile())).time(timestamp).build());
        events.add(riemann.event(name + " p999").metric(convertDuration(snapshot.get999thPercentile())).time(timestamp).build());

        reportMetered(name, timer, timestamp);

        return events;
    }

    private ArrayList<Proto.Event> reportMetered(String name, Metered meter, long timestamp) throws IOException {

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(riemann.event(name + " count").metric(meter.getCount()).time(timestamp).build());
        events.add(riemann.event(name + " m1_rate").metric(convertRate(meter.getOneMinuteRate())).time(timestamp).build());
        events.add(riemann.event(name + " m5_rate").metric(convertRate(meter.getFiveMinuteRate())).time(timestamp).build());
        events.add(riemann.event(name + " m15_rate").metric(convertRate(meter.getFifteenMinuteRate())).time(timestamp).build());
        events.add(riemann.event(name + " mean_rate").metric(convertRate(meter.getMeanRate())).time(timestamp).build());

        return events;
    }

    private List<Proto.Event> reportHistogram(String name, Histogram histogram, long timestamp) throws IOException {
        final Snapshot snapshot = histogram.getSnapshot();

        ArrayList<Proto.Event> events = new ArrayList<Proto.Event>();

        events.add(riemann.event(name + " count").metric(histogram.getCount()).time(timestamp).build());
        events.add(riemann.event(name + " max").metric(snapshot.getMax()).time(timestamp).build());
        events.add(riemann.event(name + " mean").metric(snapshot.getMean()).time(timestamp).build());
        events.add(riemann.event(name + " min").metric(snapshot.getMin()).time(timestamp).build());
        events.add(riemann.event(name + " stddev").metric(snapshot.getStdDev()).time(timestamp).build());
        events.add(riemann.event(name + " p50").metric(snapshot.getMedian()).time(timestamp).build());
        events.add(riemann.event(name + " p75").metric(snapshot.get75thPercentile()).time(timestamp).build());
        events.add(riemann.event(name + " p95").metric(snapshot.get95thPercentile()).time(timestamp).build());
        events.add(riemann.event(name + " p98").metric(snapshot.get98thPercentile()).time(timestamp).build());
        events.add(riemann.event(name + " p99").metric(snapshot.get99thPercentile()).time(timestamp).build());
        events.add(riemann.event(name + " p999").metric(snapshot.get999thPercentile()).time(timestamp).build());

        return events;

    }

    private Proto.Event reportCounter(String name, Counter counter, long timestamp) throws IOException {
        return riemann.event(name + " count").metric(counter.getCount()).time(timestamp).build();
    }

    private Proto.Event reportGauge(String name, Gauge gauge, long timestamp) throws IOException, IllegalStateException {
        Object o = gauge.getValue();

        if (o instanceof Float) {
            return riemann.event(name).metric((Float) o).time(timestamp).build();
        } else if (o instanceof Double) {
            return riemann.event(name).metric((Double) o).time(timestamp).build();
        } else if (o instanceof Byte) {
            return riemann.event(name).metric((Byte) o).time(timestamp).build();
        } else if (o instanceof Short) {
            return riemann.event(name).metric((Short) o).time(timestamp).build();
        } else if (o instanceof Integer) {
            return riemann.event(name).metric((Integer) o).time(timestamp).build();
        } else if (o instanceof Long) {
            return riemann.event(name).metric((Long) o).time(timestamp).build();
        } else {
            throw new IllegalStateException("Guage was of an unknown type: " + o.getClass().toString());
        }

    }

}
