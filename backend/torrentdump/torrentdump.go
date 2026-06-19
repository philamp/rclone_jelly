// Package torrentdump stores torrent hashes shared between Jelly remotes.
package torrentdump

import (
	"encoding/gob"
	"errors"
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
