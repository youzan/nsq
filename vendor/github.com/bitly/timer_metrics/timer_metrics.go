package timer_metrics

import (
	"fmt"
	"log"
	"math"
	"sort"
	"sync"
	"time"
)

type TimerMetrics struct {
	sync.Mutex
	timings     durations
	prefix      string
	statusEvery int
	position    int
}

// start a new TimerMetrics to print out metrics every n times
func NewTimerMetrics(statusEvery int, prefix string) *TimerMetrics {
	s := &TimerMetrics{
		statusEvery: statusEvery,
		prefix:      prefix,
	}
	if statusEvery > 0 {
		if statusEvery < 100 {
			log.Printf("Warning: use more than 100 status values for accurate percentiles (configured with %d)", statusEvery)
		}
		s.timings = make(durations, 0, statusEvery)
	}
	return s
}

type durations []time.Duration

func (s durations) Len() int           { return len(s) }
func (s durations) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s durations) Less(i, j int) bool { return s[i] < s[j] }

func percentile(perc float64, arr durations) time.Duration {
	length := len(arr)
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Ceil(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}

type Stats struct {
	Prefix string
	Count  int
	Avg    time.Duration
	P95    time.Duration
	P99    time.Duration
}

func (s *Stats) String() string {
	p95Ms := s.P95.Seconds() * 1000
	p99Ms := s.P99.Seconds() * 1000
	avgMs := s.Avg.Seconds() * 1000
	return fmt.Sprintf("%s finished %d - 99th: %.02fms - 95th: %.02fms - avg: %.02fms",
		s.Prefix, s.Count, p99Ms, p95Ms, avgMs)
}

func (m *TimerMetrics) getStats() *Stats {
	var total time.Duration
	for _, v := range m.timings {
		total += v
	}

	// make a copy of timings so we still rotate through in order
	timings := make(durations, len(m.timings))
	copy(timings, m.timings)
	sort.Sort(timings)
	var avg time.Duration
	if len(timings) > 0 {
		avg = total / time.Duration(len(m.timings))
	}
	return &Stats{
		Prefix: m.prefix,
		Count:  len(m.timings),
		Avg:    avg,
		P95:    percentile(95.0, timings),
		P99:    percentile(99.0, timings),
	}
}

// get the current Stats
func (m *TimerMetrics) Stats() *Stats {
	m.Lock()
	s := m.getStats()
	m.Unlock()
	return s
}

// record a delta from time.Now()
func (m *TimerMetrics) Status(startTime time.Time) {
	if m.statusEvery == 0 {
		return
	}
	m.StatusDuration(time.Now().Sub(startTime))
}

// Record a duration, printing out stats every statusEvery interval
func (m *TimerMetrics) StatusDuration(duration time.Duration) {
	if m.statusEvery == 0 {
		return
	}

	m.Lock()
	m.position++
	var looped bool
	if m.position > m.statusEvery {
		// loop back around
		looped = true
		m.position = 1
	}
	if m.position > len(m.timings) {
		m.timings = append(m.timings, duration)
	} else {
		m.timings[m.position-1] = duration
	}

	if !looped {
		m.Unlock()
		return
	}

	stats := m.getStats()
	m.Unlock()

	log.Printf("%s", stats)
}
