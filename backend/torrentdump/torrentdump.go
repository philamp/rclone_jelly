// Package torrentdump stores torrent hashes shared between Jelly remotes.
package torrentdump

import (
	"encoding/gob"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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
	// RemoteTorrentGlob is where remote WebDAV torrent dumps are expected locally.
	RemoteTorrentGlob = "/mounts/remote_webdav/dumps/*.torrent"
	incrementWidth    = 10
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
	Hashes    []HashEntry
}

// HashEntry is an incremented hash value in a dump.
type HashEntry struct {
	Increment uint64
	Hash      string
}

// ImportState tracks the last imported remote dump increments.
type ImportState struct {
	Version             int
	HashIncrements      map[string]uint64
	SourceFileIncrement uint64
}

// Path returns the local dump path for a backend provider name.
func Path(provider string) string {
	return filepath.Join(DumpDir(), "dump_"+strings.ToLower(provider)+".gob")
}

// DumpDir returns the local directory used for dump artifacts.
func DumpDir() string {
	return filepath.Join(config.GetCacheDir(), "dumps")
}

// ImportStatePath returns the local import state path for a backend provider name.
func ImportStatePath(provider string) string {
	return filepath.Join(DumpDir(), "import_state_"+strings.ToLower(provider)+".gob")
}

// SourceFilename returns the dump filename for a source file transfer name.
func SourceFilename(name, ext string) string {
	_, name = SplitIncrementPrefix(name)
	name = strings.TrimSpace(name)
	if name == "" {
		name = "transfer"
	}
	ext = strings.TrimSpace(strings.ToLower(ext))
	if ext == "" {
		ext = ".nzb"
	}
	if !strings.HasPrefix(ext, ".") {
		ext = "." + ext
	}
	name = strings.NewReplacer("/", "_", "\\", "_").Replace(name)
	lowerName := strings.ToLower(name)
	if strings.HasSuffix(lowerName, ".nzb") || strings.HasSuffix(lowerName, ".torrent") {
		return name
	}
	return name + ext
}

// SourceDumpFilename returns the increment-prefixed dump filename for a source file transfer name.
func SourceDumpFilename(dir, name, ext string) string {
	filename := SourceFilename(name, ext)
	if existing := existingSourceDumpFilename(dir, filename); existing != "" {
		return existing
	}
	return fmtIncrement(nextSourceIncrement(dir)) + "_" + filename
}

// NZBFilename returns the dump filename for an NZB transfer name.
func NZBFilename(name string) string {
	return SourceFilename(name, ".nzb")
}

// LocalSourcePath returns the local path for a source file dump filename or transfer name.
func LocalSourcePath(name, ext string) string {
	dir := DumpDir()
	return filepath.Join(dir, SourceDumpFilename(dir, name, ext))
}

// LocalNZBPath returns the local path for an NZB dump filename or transfer name.
func LocalNZBPath(name string) string {
	return LocalSourcePath(name, ".nzb")
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

// SplitIncrementPrefix strips a leading decimal increment prefix.
func SplitIncrementPrefix(value string) (uint64, string) {
	value = strings.TrimSpace(value)
	sep := strings.IndexByte(value, '_')
	if sep != incrementWidth {
		return 0, value
	}
	for _, c := range value[:sep] {
		if c < '0' || c > '9' {
			return 0, value
		}
	}
	increment, err := strconv.ParseUint(value[:sep], 10, 64)
	if err != nil || increment == 0 {
		return 0, value
	}
	return increment, value[sep+1:]
}

func fmtIncrement(increment uint64) string {
	return fmt.Sprintf("%0*d", incrementWidth, increment)
}

// Write writes hashes to a local gob dump.
func Write(path, provider string, hashes map[string]struct{}) error {
	if len(hashes) == 0 {
		return nil
	}
	existing := existingHashIncrements(path)
	next := uint64(1)
	for _, increment := range existing {
		if increment >= next {
			next = increment + 1
		}
	}
	entries := make([]HashEntry, 0, len(hashes))
	for hash := range hashes {
		hash = NormalizeHash(hash)
		if hash != "" {
			increment := existing[hash]
			if increment == 0 {
				increment = next
				next++
			}
			entries = append(entries, HashEntry{
				Increment: increment,
				Hash:      hash,
			})
		}
	}
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Increment < entries[j].Increment
	})
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
		Hashes:    entries,
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

func existingHashIncrements(path string) map[string]uint64 {
	out := make(map[string]uint64)
	dump, err := Read(path)
	if err != nil {
		return out
	}
	for _, entry := range dump.Hashes {
		if entry.Hash != "" && entry.Increment > 0 {
			out[entry.Hash] = entry.Increment
		}
	}
	return out
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

// ReadImportState reads the persistent import state for a provider.
func ReadImportState(provider string) (*ImportState, error) {
	path := ImportStatePath(provider)
	in, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return &ImportState{Version: 1, HashIncrements: make(map[string]uint64)}, nil
	}
	if err != nil {
		return nil, err
	}
	defer in.Close()
	var state ImportState
	err = gob.NewDecoder(in).Decode(&state)
	if err != nil {
		return nil, err
	}
	if state.HashIncrements == nil {
		state.HashIncrements = make(map[string]uint64)
	}
	if state.Version == 0 {
		state.Version = 1
	}
	return &state, nil
}

// WriteImportState writes the persistent import state for a provider.
func WriteImportState(provider string, state *ImportState) error {
	if state == nil {
		return nil
	}
	if state.HashIncrements == nil {
		state.HashIncrements = make(map[string]uint64)
	}
	state.Version = 1
	path := ImportStatePath(provider)
	err := os.MkdirAll(filepath.Dir(path), 0700)
	if err != nil {
		return err
	}
	tmpPath := path + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	encodeErr := gob.NewEncoder(out).Encode(state)
	closeErr := out.Close()
	if encodeErr != nil {
		_ = os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, path)
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

// RemoteSourcePaths returns remote NZB and torrent dump files mounted locally.
func RemoteSourcePaths() []string {
	if !RemoteDumpImportEnabled() {
		return nil
	}
	var paths []string
	for _, pattern := range []string{RemoteNZBGlob, RemoteTorrentGlob} {
		matches, _ := filepath.Glob(pattern)
		paths = append(paths, matches...)
	}
	sort.Strings(paths)
	return paths
}

// SourcePathIncrement returns the increment prefix of a source dump path.
func SourcePathIncrement(path string) uint64 {
	increment, _ := SplitIncrementPrefix(filepath.Base(path))
	return increment
}

// SourceComparableFilename returns the source filename without its increment prefix.
func SourceComparableFilename(path string) string {
	_, filename := SplitIncrementPrefix(filepath.Base(path))
	ext := filepath.Ext(filename)
	return SourceFilename(filename, ext)
}

func existingSourceDumpFilename(dir, filename string) string {
	for _, pattern := range []string{"*.nzb", "*.torrent"} {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, match := range matches {
			if SourceComparableFilename(match) == filename {
				return filepath.Base(match)
			}
		}
	}
	return ""
}

func nextSourceIncrement(dir string) uint64 {
	next := uint64(1)
	for _, pattern := range []string{"*.nzb", "*.torrent"} {
		matches, _ := filepath.Glob(filepath.Join(dir, pattern))
		for _, match := range matches {
			increment := SourcePathIncrement(match)
			if increment >= next {
				next = increment + 1
			}
		}
	}
	return next
}
