package main

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	errorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "error_total",
		Help:      "The total number of errors occurring",
	})

	featureHitCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "feature_cache_hit_total",
		Help:      "Features cache hits",
	})

	featureMissCounter = promauto.NewCounter(prometheus.CounterOpts{
		Namespace: "insided_server",
		Name:      "feature_miss_hit_total",
		Help:      "Features miss hits",
	})

	versionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "insided",
		Name:      "version",
		Help:      "App version.",
	}, []string{"version"})

	dataVersionGauge = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "insided",
		Name:      "dataset_version",
		Help:      "Dataset version.",
	}, []string{"version"})
)
