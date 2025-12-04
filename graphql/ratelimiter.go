package graphqlclient

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type contextKey string

const (
	// DefaultThreshold is the default safety margin.
	// If remaining capacity is below this, we wait.
	// Shopify limit is usually 40 points/sec bucket size 1000 (standard) or 80/2000 (plus).
	// We want to stop when we have used e.g. 1300 out of 2000.
	DefaultUsageThresholdRatio = 0.65 // Use up to 65% of the limit.

	CostContextKey contextKey = "shopify_graphql_cost"
)

func WithEstimatedCost(ctx context.Context, cost int) context.Context {
	return context.WithValue(ctx, CostContextKey, cost)
}

type RateLimiter struct {
	mu sync.Mutex

	limit     int
	baseUsed  int // Usage reported by the server (from headers)
	pending   int // Estimated cost of in-flight requests
	remaining int

	ratio float64
}

func NewRateLimiter(ratio float64) *RateLimiter {
	if ratio <= 0 {
		ratio = DefaultUsageThresholdRatio
	}
	return &RateLimiter{
		ratio: ratio,
		// Initial safe values until we get first header
		limit:     1000, // Assume Standard for safety. Let's start conservative.
		remaining: 1000,
	}
}

func (r *RateLimiter) Wait(ctx context.Context, estimatedCost int) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	// If we haven't received any headers yet, just proceed.
	if r.limit == 0 {
		r.pending += estimatedCost
		return nil
	}

	// Calculate max allowed usage
	maxAllowed := int(float64(r.limit) * r.ratio)

	// Current estimated usage = baseUsed + pending
	currentUsage := r.baseUsed + r.pending

	// Check if current usage + estimated cost exceeds max allowed
	if currentUsage+estimatedCost > maxAllowed {
		needed := (currentUsage + estimatedCost) - maxAllowed

		// Calculate restore rate.
		// Standard: 50 points/sec (limit 1000). Plus: 100 points/sec (limit 2000).
		// Restore rate is roughly limit / 20.
		restoreRate := r.limit / 20
		if restoreRate == 0 {
			restoreRate = 50 // Fallback
		}

		sleepDuration := time.Duration(needed*1000/restoreRate) * time.Millisecond
		sleepDuration += 100 * time.Millisecond

		log.Debugf("Rate limit reached (Used: %d, Pending: %d, Cost: %d, Max: %d). Sleeping for %v", r.baseUsed, r.pending, estimatedCost, maxAllowed, sleepDuration)

		r.mu.Unlock()

		select {
		case <-ctx.Done():
			r.mu.Lock()
			return ctx.Err()
		case <-time.After(sleepDuration):
			r.mu.Lock()
			// Optimistically reduce baseUsed, assuming leak.
			// We don't touch pending because those requests are still in flight (or we are just starting one).
			r.baseUsed -= needed
			if r.baseUsed < 0 {
				r.baseUsed = 0
			}
		}
	}

	// Reserve the cost
	r.pending += estimatedCost
	return nil
}

func (r *RateLimiter) Done(estimatedCost int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.pending -= estimatedCost
	if r.pending < 0 {
		r.pending = 0
	}
}

func (r *RateLimiter) Update(header string) {
	if header == "" {
		return
	}

	parts := strings.Split(header, "/")
	if len(parts) != 2 {
		return
	}

	used, err1 := strconv.Atoi(parts[0])
	limit, err2 := strconv.Atoi(parts[1])

	if err1 != nil || err2 != nil {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	r.baseUsed = used
	r.limit = limit
	r.remaining = limit - used
}
