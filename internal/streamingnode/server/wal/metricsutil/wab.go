package metricsutil

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

// NewWriteAheadBufferMetrics creates a new WriteAheadBufferMetrics.
func NewWriteAheadBufferMetrics(
	pchannel string,
	capacity int,
) *WriteAheadBufferMetrics {
	constLabel := prometheus.Labels{
		metrics.NodeIDLabelName:     paramtable.GetStringNodeID(),
		metrics.WALChannelLabelName: pchannel,
	}
	metrics.WALWriteAheadBufferCapacityBytes.With(constLabel).Set(float64(capacity))

	return &WriteAheadBufferMetrics{
		constLabel:       constLabel,
		total:            metrics.WALWriteAheadBufferEntryTotal.With(constLabel),
		size:             metrics.WALWriteAheadBufferSizeBytes.With(constLabel),
		earliestTimeTick: metrics.WALWriteAheadBufferEarliestTimeTick.With(constLabel),
		latestTimeTick:   metrics.WALWriteAheadBufferLatestTimeTick.With(constLabel),
	}
}

type WriteAheadBufferMetrics struct {
	constLabel       prometheus.Labels
	total            prometheus.Gauge
	size             prometheus.Gauge
	earliestTimeTick prometheus.Gauge
	latestTimeTick   prometheus.Gauge
}

func (m *WriteAheadBufferMetrics) Observe(
	total int,
	bytes int,
	earliestTimeTick uint64,
	latestTimeTick uint64,
) {
	m.total.Set(float64(total))
	m.size.Set(float64(bytes))
	m.earliestTimeTick.Set(tsoutil.PhysicalTimeSeconds(earliestTimeTick))
	m.latestTimeTick.Set(tsoutil.PhysicalTimeSeconds(latestTimeTick))
}

func (m *WriteAheadBufferMetrics) Close() {
	metrics.WALWriteAheadBufferEntryTotal.Delete(m.constLabel)
	metrics.WALWriteAheadBufferSizeBytes.Delete(m.constLabel)
	metrics.WALWriteAheadBufferEarliestTimeTick.Delete(m.constLabel)
	metrics.WALWriteAheadBufferLatestTimeTick.Delete(m.constLabel)
	metrics.WALWriteAheadBufferCapacityBytes.Delete(m.constLabel)
}
