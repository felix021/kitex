package limiters

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/cloudwego/kitex/pkg/limitsuite"
)

var _ limitsuite.Limiter = (*PercentageLimiter)(nil)

const (
	defaultName           = "PercentageLimiter"
	defaultInterval       = time.Second
	defaultMinSampleCount = 200

	percentageMultiplier = 10000
)

// CandidateChecker determines (in a middleware) whether a request should be checked by the limiter
type CandidateChecker func(ctx context.Context, request, response interface{}) bool

// PercentageLimiter blocks candidate requests exceeding given percentage limit
type PercentageLimiter struct {
	name string

	interval         time.Duration
	percentage       int64
	minSampleCount   int32
	isLimitCandidate CandidateChecker

	stopSignal chan byte

	totalCount             int64
	allowedCandidatesCount int64
}

// PercentageLimiterOption
type PercentageLimiterOption func(l *PercentageLimiter)

func WithName(name string) PercentageLimiterOption {
	return func(l *PercentageLimiter) {
		l.name = name
	}
}

func WithInterval(interval time.Duration) PercentageLimiterOption {
	return func(l *PercentageLimiter) {
		l.interval = interval
	}
}

func WithMinSampleCount(count int) PercentageLimiterOption {
	return func(l *PercentageLimiter) {
		l.minSampleCount = int32(count)
	}
}

func NewPercentageLimiter(percentage float64, candidateChecker CandidateChecker,
	opts ...PercentageLimiterOption) *PercentageLimiter {
	limiter := &PercentageLimiter{
		percentage:       int64(percentage * percentageMultiplier), // int64 multiplication is faster
		isLimitCandidate: candidateChecker,
		name:             defaultName,
		interval:         defaultInterval,
		minSampleCount:   defaultMinSampleCount,
		stopSignal:       make(chan byte, 1),
	}
	for _, option := range opts {
		option(limiter)
	}
	if limiter.interval > 0 {
		go limiter.resetPeriodically()
	}
	return limiter
}

func (l *PercentageLimiter) String() string {
	return fmt.Sprintf("PercentageLimiter(interval=%v, percentage=%v/%v, minSampleCount=%v, name=%v)",
		l.interval, l.percentage, percentageMultiplier, l.minSampleCount, l.name)
}

func (l *PercentageLimiter) resetPeriodically() {
	ticker := time.NewTicker(l.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			l.Reset()
		case <-l.stopSignal:
			return
		}
	}
}

func (l *PercentageLimiter) Allow(ctx context.Context, request, response interface{}) bool {
	total := atomic.AddInt64(&l.totalCount, 1)
	if !l.isLimitCandidate(ctx, request, response) {
		return true
	}
	allowedCandidates := atomic.AddInt64(&l.allowedCandidatesCount, 1)
	if total <= int64(l.minSampleCount) {
		return true
	}
	if !l.exceedPercentage(allowedCandidates, total) {
		return true
	}
	// rejected
	atomic.AddInt64(&l.allowedCandidatesCount, -1)
	return false
}

func (l *PercentageLimiter) exceedPercentage(candidateCount int64, totalCount int64) bool {
	return candidateCount*percentageMultiplier > totalCount*l.percentage
}

func (l *PercentageLimiter) Reset() {
	atomic.StoreInt64(&l.totalCount, 0)
	atomic.StoreInt64(&l.allowedCandidatesCount, 0)
}

func (l *PercentageLimiter) Close() {
	select {
	case l.stopSignal <- 0:
	default:
	}
}
