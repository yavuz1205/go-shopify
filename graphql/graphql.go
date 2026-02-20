package graphqlclient

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"

	"github.com/r0busta/graphql"
)

const (
	shopifyBaseDomain        = "myshopify.com"
	shopifyAccessTokenHeader = "X-Shopify-Access-Token"

	defaultAPIProtocol = "https"
	defaultAPIEndpoint = "graphql.json"
	defaultAPIBasePath = "admin/api"

	graphQLErrorCodeThrottled = "THROTTLED"
)

// Option is used to configure options.
type Option func(t *transport)

// WithVersion optionally sets the API version if the passed string is valid.
func WithVersion(apiVersion string) Option {
	return func(t *transport) {
		if apiVersion != "" {
			t.apiBasePath = fmt.Sprintf("%s/%s", defaultAPIBasePath, apiVersion)
		}
	}
}

// WithToken optionally sets access token.
func WithToken(token string) Option {
	return func(t *transport) {
		t.accessToken = token
	}
}

// WithPrivateAppAuth optionally sets private app credentials (API key and access token).
func WithPrivateAppAuth(apiKey string, accessToken string) Option {
	return func(t *transport) {
		t.apiKey = apiKey
		t.accessToken = accessToken
	}
}

func WithRateLimiter(limiter *RateLimiter) Option {
	return func(t *transport) {
		t.rateLimiter = limiter
	}
}

type transport struct {
	accessToken string
	apiKey      string
	apiBasePath string
	rateLimiter *RateLimiter
}

func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if t.rateLimiter != nil {
		estimatedCost := 50
		if v := req.Context().Value(CostContextKey); v != nil {
			if c, ok := v.(int); ok {
				estimatedCost = c
			}
		}

		if err := t.rateLimiter.Wait(req.Context(), estimatedCost); err != nil {
			return nil, err
		}
		defer t.rateLimiter.Done(estimatedCost)
	}

	isAccessTokenSet := t.accessToken != ""
	areBasicAuthCredentialsSet := t.apiKey != "" && isAccessTokenSet

	if areBasicAuthCredentialsSet {
		req.SetBasicAuth(t.apiKey, t.accessToken)
	} else if isAccessTokenSet {
		req.Header.Set(shopifyAccessTokenHeader, t.accessToken)
	}

	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if t.rateLimiter != nil {
		t.rateLimiter.Update(resp.Header.Get("X-Shopify-Shop-Api-Call-Limit"))
	}

	if err := annotateThrottleRetryAfter(resp); err != nil {
		return nil, err
	}

	return resp, nil
}

// NewClient creates a new client (in fact, just a simple wrapper for a graphql.Client).
func NewClient(shopName string, opts ...Option) *graphql.Client {
	transport := &transport{
		apiBasePath: defaultAPIBasePath,
	}

	for _, opt := range opts {
		opt(transport)
	}

	httpClient := &http.Client{
		Transport: transport,
	}

	url := buildAPIEndpoint(shopName, transport.apiBasePath)

	return graphql.NewClient(url, httpClient)
}

func buildAPIEndpoint(shopName string, apiPathPrefix string) string {
	return fmt.Sprintf("%s://%s.%s/%s/%s", defaultAPIProtocol, shopName, shopifyBaseDomain, apiPathPrefix, defaultAPIEndpoint)
}

type graphQLError struct {
	Message    string `json:"message"`
	Extensions struct {
		Code string `json:"code"`
	} `json:"extensions"`
}

type graphQLCost struct {
	RequestedQueryCost float64 `json:"requestedQueryCost"`
	ThrottleStatus     struct {
		CurrentlyAvailable float64 `json:"currentlyAvailable"`
		RestoreRate        float64 `json:"restoreRate"`
	} `json:"throttleStatus"`
}

func (c graphQLCost) RetryAfterSeconds() float64 {
	if c.ThrottleStatus.RestoreRate <= 0 {
		return 0
	}

	diff := c.ThrottleStatus.CurrentlyAvailable - c.RequestedQueryCost
	if diff < 0 {
		return -diff / c.ThrottleStatus.RestoreRate
	}

	return 0
}

type graphQLResponseEnvelope struct {
	Data       json.RawMessage `json:"data,omitempty"`
	Errors     []graphQLError  `json:"errors,omitempty"`
	Extensions struct {
		Cost graphQLCost `json:"cost"`
	} `json:"extensions,omitempty"`
}

func annotateThrottleRetryAfter(resp *http.Response) error {
	if resp == nil || resp.Body == nil {
		return nil
	}

	// Skip non-JSON payloads. Most GraphQL responses include this header.
	if contentType := resp.Header.Get("Content-Type"); contentType != "" && !strings.Contains(contentType, "json") {
		return nil
	}

	originalBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if err := resp.Body.Close(); err != nil {
		return err
	}

	// Ensure downstream client can still decode the response body.
	restoreBody := func(payload []byte) {
		resp.Body = io.NopCloser(bytes.NewReader(payload))
		resp.ContentLength = int64(len(payload))
		resp.Header.Set("Content-Length", fmt.Sprintf("%d", len(payload)))
	}
	restoreBody(originalBody)

	if len(originalBody) == 0 {
		return nil
	}

	var envelope graphQLResponseEnvelope
	if err := json.Unmarshal(originalBody, &envelope); err != nil {
		return nil
	}
	if len(envelope.Errors) == 0 {
		return nil
	}

	retryAfterSeconds := envelope.Extensions.Cost.RetryAfterSeconds()
	if retryAfterSeconds <= 0 {
		return nil
	}

	retryAfterSeconds = math.Round(retryAfterSeconds*1000) / 1000

	updated := false
	for i := range envelope.Errors {
		if envelope.Errors[i].Extensions.Code != graphQLErrorCodeThrottled {
			continue
		}
		if strings.Contains(envelope.Errors[i].Message, "retry_after=") {
			continue
		}

		envelope.Errors[i].Message = fmt.Sprintf("%s (retry_after=%.3fs)", strings.TrimSpace(envelope.Errors[i].Message), retryAfterSeconds)
		updated = true
	}

	if !updated {
		return nil
	}

	patchedBody, err := json.Marshal(envelope)
	if err != nil {
		return nil
	}
	restoreBody(patchedBody)
	return nil
}
