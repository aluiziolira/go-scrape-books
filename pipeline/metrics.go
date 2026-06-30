package pipeline

import "sync"

// PipelineStats is an immutable snapshot of pipeline progress.
type PipelineStats struct {
	Processed        int64
	ValidationErrors map[string]int
}

type metrics struct {
	mu         sync.Mutex
	processed  int64
	validation map[string]int
}

func newMetrics() metrics {
	return metrics{
		validation: make(map[string]int),
	}
}

func (m *metrics) incrementProcessed() {
	m.mu.Lock()
	m.processed++
	m.mu.Unlock()
}

func (m *metrics) addValidation(kind string) {
	m.mu.Lock()
	m.validation[kind]++
	m.mu.Unlock()
}

func (m *metrics) snapshot() PipelineStats {
	m.mu.Lock()
	defer m.mu.Unlock()

	copyValidation := make(map[string]int, len(m.validation))
	for k, v := range m.validation {
		copyValidation[k] = v
	}

	return PipelineStats{
		Processed:        m.processed,
		ValidationErrors: copyValidation,
	}
}
