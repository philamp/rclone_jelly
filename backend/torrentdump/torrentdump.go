// Package torrentdump stores torrent hashes shared between Jelly remotes.
package torrentdump

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
)

const (
	// DumpInterval is the default interval between local dump writes.
	DumpInterval = 2 * time.Hour
	// RemoteDumpGlob is where remote WebDAV dumps are expected locally.
	RemoteDumpGlob = "/mounts/remote_webdav/dumps/dump_*.gob"

	defaultJellygrailScanTriggerPort = "16685"
	defaultJellygrailScanTriggerPath = "/app/trigger_remote_scan"
)

var btihRe = regexp.MustCompile(`(?i)\b[0-9a-f]{40}\b|[a-z2-7]{32}`)

var scanTargetLogOnce sync.Once

// Dump is the on-disk gob format.
type Dump struct {
	Version   int
	Provider  string
	CreatedAt time.Time
	Hashes    []string
}

// Path returns the local dump path for a backend provider name.
func Path(provider string) string {
	return filepath.Join(config.GetCacheDir(), "dumps", "dump_"+strings.ToLower(provider)+".gob")
}

// RemoteScanTargetProvider returns the configured provider for remote dump imports.
func RemoteScanTargetProvider() string {
	target := strings.ToLower(strings.TrimSpace(os.Getenv("REMOTE_SCAN_TARGET_PROVIDER")))
	scanTargetLogOnce.Do(func() {
		if target == "" {
			fs.Infof(nil, "Torrent dump remote scan target provider: <empty> (imports disabled)")
			return
		}
		fs.Infof(nil, "Torrent dump remote scan target provider: %s", target)
	})
	return target
}

// JellygrailScanTriggerURL returns the URL used to ask Jellygrail to scan.
func JellygrailScanTriggerURL() string {
	if rawURL := strings.TrimSpace(os.Getenv("JELLYGRAIL_SCAN_TRIGGER_URL")); rawURL != "" {
		return rawURL
	}
	port := strings.TrimSpace(os.Getenv("WEBSERVICE_INTERNAL_PORT"))
	if port == "" {
		port = defaultJellygrailScanTriggerPort
	}
	return "http://127.0.0.1:" + port + defaultJellygrailScanTriggerPath
}

// TriggerJellygrailScan asks the local Jellygrail service to trigger jgScanJob.
func TriggerJellygrailScan(ctx context.Context, provider, reason string) error {
	rawURL := JellygrailScanTriggerURL()
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
		fs.Debugf(nil, "Torrent dump Jellygrail scan trigger failed: url=%s: %v", u.Redacted(), err)
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		err = fmt.Errorf("jellygrail scan trigger returned %s: %s", resp.Status, strings.TrimSpace(string(body)))
		fs.Debugf(nil, "Torrent dump Jellygrail scan trigger failed: %v", err)
		return err
	}
	fs.Infof(nil, "Torrent dump Jellygrail scan triggered: provider=%s reason=%s", provider, reason)
	return nil
}

// Magnet returns a magnet URI for a hash or passes through a magnet URI.
func Magnet(hash string) string {
	hash = strings.TrimSpace(hash)
	if strings.HasPrefix(strings.ToLower(hash), "magnet:") {
		return hash
	}
	return "magnet:?xt=urn:btih:" + NormalizeHash(hash)
}

// NormalizeHash returns a normalized BTIH value if one can be found.
func NormalizeHash(value string) string {
	hash := ExtractHash(value)
	if len(hash) == 40 {
		return strings.ToLower(hash)
	}
	return strings.ToUpper(hash)
}

// ExtractHash finds a BTIH hash in a hash string or magnet URI.
func ExtractHash(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	if strings.HasPrefix(strings.ToLower(value), "magnet:") {
		if u, err := url.Parse(value); err == nil {
			for _, xt := range u.Query()["xt"] {
				const prefix = "urn:btih:"
				if strings.HasPrefix(strings.ToLower(xt), prefix) {
					return cleanHash(xt[len(prefix):])
				}
			}
		}
	}
	return cleanHash(value)
}

func cleanHash(value string) string {
	if match := btihRe.FindString(value); match != "" {
		return match
	}
	return ""
}

// Write writes hashes to a local gob dump.
func Write(path, provider string, hashes map[string]struct{}) error {
	if len(hashes) == 0 {
		return nil
	}
	values := make([]string, 0, len(hashes))
	for hash := range hashes {
		hash = NormalizeHash(hash)
		if hash != "" {
			values = append(values, hash)
		}
	}
	sort.Strings(values)
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	dump := Dump{
		Version:   1,
		Provider:  provider,
		CreatedAt: time.Now(),
		Hashes:    values,
	}
	encodeErr := gob.NewEncoder(out).Encode(&dump)
	closeErr := out.Close()
	if encodeErr != nil {
		_ = os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	err = os.Rename(tmpPath, path)
	if err != nil {
		return err
	}
	return os.Chmod(path, 0644)
}

// Read reads a gob dump from path.
func Read(path string) (*Dump, error) {
	in, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer in.Close()
	var dump Dump
	err = gob.NewDecoder(in).Decode(&dump)
	if err != nil {
		return nil, err
	}
	if dump.Version == 0 {
		return nil, errors.New("invalid dump version")
	}
	return &dump, nil
}

// RemoteDumpPaths returns remote dump files mounted locally.
func RemoteDumpPaths() []string {
	paths, _ := filepath.Glob(RemoteDumpGlob)
	sort.Strings(paths)
	return paths
}
