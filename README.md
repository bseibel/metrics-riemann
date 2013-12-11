metrics-riemann
===============

A basic Riemann reporter for yammer metrics.

Based mostly off of the metrics-graphite reporter with a few tweaks.

* Metrics are sent in batch to Riemann
* Uses a persistent tcp connection to Riemann (should handle reconnects just fine)


Usage
==============

Example:

      val registry = new MetricRegistry()

      val riemann = new Riemann("riemann.host.com", 5555)
      val reporter = RiemannReporter
        .forRegistry(registry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .withTtl(15.0f)
        .filter(MetricFilter.ALL)
        .build(riemann)

      reporter.start(5, TimeUnit.SECONDS)

