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
	DumpInterval = 15 * time.Minute
	// RemoteDumpGlob is where remote WebDAV dumps are expected locally.
	RemoteDumpGlob = "/mounts/remote_webdav/dumps/dump_*.gob"
	// RemoteNZBGlob is where remote WebDAV NZB dumps are expected locally.
	RemoteNZBGlob = "/mounts/remote_webdav/dumps/*.nzb"
)

var btihRe = regexp.MustCompile(`(?i)\b[0-9a-f]{40}\b|[a-z2-7]{32}`)

var (
	scanTargetLogOnce sync.Once
	remoteDumpLogOnce sync.Once
)

// Dump is the on-disk gob format.
type Dump struct {
	Version   int
	Provider  string
	CreatedAt time.Time
	Hashes    []string
}

// Path returns the local dump path for a backend provider name.
func Path(provider string) string {
	return filepath.Join(DumpDir(), "dump_"+strings.ToLower(provider)+".gob")
}

// DumpDir returns the local directory used for dump artifacts.
func DumpDir() string {
	return filepath.Join(config.GetCacheDir(), "dumps")
}

// NZBFilename returns the dump filename for an NZB transfer name.
func NZBFilename(name string) string {
	name = strings.TrimSpace(name)
	if name == "" {
		name = "transfer"
	}
	name = strings.NewReplacer("/", "_", "\\", "_").Replace(name)
	if strings.HasSuffix(strings.ToLower(name), ".nzb") {
		return name
	}
	return name + ".nzb"
}

// LocalNZBPath returns the local path for an NZB dump filename or transfer name.
func LocalNZBPath(name string) string {
	return filepath.Join(DumpDir(), NZBFilename(name))
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

// RemoteDumpImportEnabled returns whether remote WebDAV dumps should be read.
func RemoteDumpImportEnabled() bool {
	location := strings.TrimSpace(os.Getenv("REMOTE_WEB_DAV_LOCATION"))
	enabled := strings.HasPrefix(strings.ToLower(location), "http") && location != "http://hostname-or-ip:8389"
	remoteDumpLogOnce.Do(func() {
		if enabled {
			fs.Infof(nil, "Torrent dump remote WebDAV import enabled: %s", location)
			return
		}
		fs.Infof(nil, "Torrent dump remote WebDAV import disabled: REMOTE_WEB_DAV_LOCATION=%q", location)
	})
	return enabled
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
	if !RemoteDumpImportEnabled() {
		return nil
	}
	paths, _ := filepath.Glob(RemoteDumpGlob)
	sort.Strings(paths)
	return paths
}

// RemoteNZBPaths returns remote NZB dump files mounted locally.
func RemoteNZBPaths() []string {
	if !RemoteDumpImportEnabled() {
		return nil
	}
	paths, _ := filepath.Glob(RemoteNZBGlob)
	sort.Strings(paths)
	return paths
}
