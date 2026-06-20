// Package jellygrail contains Jellygrail-specific integration hooks.
package jellygrail

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
)

const (
	defaultScanTriggerPort = "16685"
	defaultScanTriggerPath = "/app/trigger_remote_scan"
	triggerDebounce        = 10 * time.Second
)

var (
	triggerMu   sync.Mutex
	lastTrigger = map[string]time.Time{}
)

// ScanTriggerURL returns the URL used to ask Jellygrail to scan.
func ScanTriggerURL() string {
	if rawURL := strings.TrimSpace(os.Getenv("JELLYGRAIL_SCAN_TRIGGER_URL")); rawURL != "" {
		return rawURL
	}
	port := strings.TrimSpace(os.Getenv("WEBSERVICE_INTERNAL_PORT"))
	if port == "" {
		port = defaultScanTriggerPort
	}
	return "http://127.0.0.1:" + port + defaultScanTriggerPath
}

// TriggerScan asks the local Jellygrail service to trigger jgScanJob.
func TriggerScan(ctx context.Context, provider, reason string) error {
	rawURL := ScanTriggerURL()
	u, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	q := u.Query()
	if provider != "" {
		q.Set("provider", provider)
	}
	if reason != "" {
		q.Set("reason", reason)
	}
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, "GET", u.String(), nil)
	if err != nil {
		return err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fs.Debugf(nil, "Jellygrail scan trigger failed: url=%s: %v", u.Redacted(), err)
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err = fmt.Errorf("jellygrail scan trigger returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		fs.Debugf(nil, "Jellygrail scan trigger failed: %v", err)
		return err
	}
	fs.Infof(nil, "Jellygrail scan triggered: provider=%s reason=%s", provider, reason)
	return nil
}

// TriggerScanAsyncDebounced triggers a scan in the background, suppressing bursts.
func TriggerScanAsyncDebounced(ctx context.Context, provider, reason string) {
	key := strings.TrimSpace(provider)
	if key == "" {
		key = "rclone"
	}
	now := time.Now()
	triggerMu.Lock()
	if !lastTrigger[key].IsZero() && now.Sub(lastTrigger[key]) < triggerDebounce {
		triggerMu.Unlock()
		fs.Debugf(nil, "Jellygrail scan trigger debounced: provider=%s reason=%s", provider, reason)
		return
	}
	lastTrigger[key] = now
	triggerMu.Unlock()

	go func() {
		_ = TriggerScan(ctx, provider, reason)
	}()
}
