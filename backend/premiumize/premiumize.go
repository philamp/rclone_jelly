// Package premiumize provides a transfer-oriented Premiumize backend.
package premiumize

import (
	"context"
	"crypto/sha1"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/premiumize/api"
	"github.com/rclone/rclone/backend/torrentdump"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fserrors"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/rest"
)

const (
	minSleep         = 200 * time.Millisecond
	maxSleep         = 30 * time.Second
	decayConstant    = 2
	rootURL          = "https://www.premiumize.me/api"
	cacheDuration    = 10 * time.Second
	checkDuration    = 48 * time.Hour
	cleanupDelay     = 30 * time.Second
	rateLimitBackoff = 5 * time.Second
	sourceTorrent    = "torrent"
	statusFinished   = "finished"
	statusSeeding    = "seeding"
)

var errReadOnly = errors.New("premiumize transfer remotes are read only")

// Register with Fs.
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "premiumize",
		Description: "Premiumize transfer view",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "api_key",
			Help:      "Premiumize API key.",
			Sensitive: true,
		}, {
			Name:     config.ConfigEncoding,
			Help:     config.ConfigEncodingHelp,
			Advanced: true,
			Default: (encoder.Display |
				encoder.EncodeBackSlash |
				encoder.EncodeDoubleQuote |
				encoder.EncodeInvalidUtf8),
		}},
	})
}

// Options defines the configuration for this backend.
type Options struct {
	APIKey string               `config:"api_key"`
	Enc    encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a Premiumize transfer remote.
type Fs struct {
	name      string
	root      string
	opt       Options
	features  *fs.Features
	srv       *rest.Client
	dlSrv     *rest.Client
	pacer     *fs.Pacer
	storePath string
	dumpPath  string

	mu              sync.Mutex
	cacheTime       time.Time
	dirs            map[string]*entry
	files           map[string]*entry
	stored          map[string]storedTransfer
	knownHashes     map[string]struct{}
	completedHashes map[string]struct{}

	transferChecks  map[string]transferCheck
	transferSources map[string]transferSourceInfo
	folderCache     map[string]cachedFolder
	activeOpens     map[string]int
	pendingCleanup  map[string]pendingCleanup
	startupDone     chan struct{}
}

// Object describes a Premiumize file.
type Object struct {
	fs             *Fs
	remote         string
	hasMetaData    bool
	size           int64
	modTime        time.Time
	id             string
	mimeType       string
	url            string
	transferID     string
	transferRoot   string
	transferFileID string
	transferDirID  string
	transferSrc    string
	contentPath    string
}

type openReadCloser struct {
	io.ReadCloser
	object *Object
	closed bool
}

type transferCheck struct {
	checkedAt     time.Time
	src           string
	cacheHit      bool
	skipCheck     bool
	skipPermanent bool
}

type transferSourceInfo struct {
	src        string
	sourceType string
}

type cachedFolder struct {
	loadedAt time.Time
	content  []api.Item
	file     *api.Item
}

type pendingCleanup struct {
	transferID string
	fileID     string
	folderID   string
}

func (c pendingCleanup) empty() bool {
	return c.transferID == "" || (c.fileID == "" && c.folderID == "")
}

type entry struct {
	remote         string
	name           string
	isDir          bool
	id             string
	size           int64
	modTime        time.Time
	mimeType       string
	url            string
	transferID     string
	transferRoot   string
	transferFileID string
	transferDirID  string
	transferSrc    string
	contentPath    string
	folderID       string
}

type storedTransfer struct {
	ID       string
	Name     string
	Src      string
	FileID   string
	FolderID string
	Content  []storedDirectFile
	StoredAt time.Time
}

type storedDirectFile struct {
	Path string
	Size int64
	Link string
}

type persistentStore struct {
	Version   int
	Transfers map[string]storedTransfer
}

func parsePath(path string) string {
	return strings.Trim(path, "/")
}

var retryErrorCodes = []int{
	429,
	500,
	502,
	503,
	504,
	509,
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func redactedURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "<invalid url>"
	}
	u.RawQuery = ""
	u.Fragment = ""
	return u.String()
}

func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		body = nil
	}
	e := api.Response{
		Status:  resp.Status,
		Message: string(body),
	}
	if body != nil {
		_ = json.Unmarshal(body, &e)
	}
	if e.Message == "" {
		e.Message = resp.Status
	}
	return &e
}

// Name of the remote.
func (f *Fs) Name() string { return f.name }

// Root of the remote.
func (f *Fs) Root() string { return f.root }

// String converts this Fs to a string.
func (f *Fs) String() string { return fmt.Sprintf("premiumize root '%s'", f.root) }

// Features returns the optional features of this Fs.
func (f *Fs) Features() *fs.Features { return f.features }

// NewFs constructs an Fs from the path.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.APIKey == "" {
		return nil, errors.New("premiumize api_key is required")
	}

	root = parsePath(root)
	client := fshttp.NewClient(ctx)
	srv := rest.NewClient(client).SetRoot(rootURL)
	srv.SetHeader("Authorization", "Bearer "+opt.APIKey)
	srv.SetErrorHandler(errorHandler)

	f := &Fs{
		name:            name,
		root:            root,
		opt:             *opt,
		srv:             srv,
		dlSrv:           rest.NewClient(client),
		pacer:           fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		storePath:       premiumizeStorePath(name, root),
		dumpPath:        torrentdump.Path("premiumize"),
		dirs:            make(map[string]*entry),
		files:           make(map[string]*entry),
		stored:          make(map[string]storedTransfer),
		knownHashes:     make(map[string]struct{}),
		completedHashes: make(map[string]struct{}),
		transferChecks:  make(map[string]transferCheck),
		transferSources: make(map[string]transferSourceInfo),
		folderCache:     make(map[string]cachedFolder),
		activeOpens:     make(map[string]int),
		pendingCleanup:  make(map[string]pendingCleanup),
		startupDone:     make(chan struct{}),
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
	}).Fill(ctx, f)
	f.initStaticDirs()
	err = f.loadStore()
	if err != nil {
		fs.Errorf(f, "Failed to load Premiumize persistent transfer cache: %v", err)
	}
	for _, stored := range f.stored {
		f.recordKnownHashLocked(stored.Src)
		f.recordCompletedHashLocked(stored.Src)
	}
	go f.startRuntimeTasks(ctx)

	if root != "" && root != sourceTorrent {
		err = f.refresh(ctx)
		if err != nil {
			return nil, err
		}
		if _, ok := f.dirs[root]; !ok {
			if _, ok := f.files[root]; ok {
				return f, fs.ErrorIsFile
			}
			return nil, fs.ErrorDirNotFound
		}
	}
	return f, nil
}

func premiumizeStorePath(name, root string) string {
	sum := sha1.Sum([]byte(name + "\x00" + root))
	filename := hex.EncodeToString(sum[:]) + ".gob"
	return filepath.Join(config.GetCacheDir(), "premiumize", filename)
}

func (f *Fs) initStaticDirs() {
	now := time.Now()
	f.dirs[""] = &entry{remote: "", name: "", isDir: true, id: "root", modTime: now}
	f.dirs[sourceTorrent] = &entry{remote: sourceTorrent, name: sourceTorrent, isDir: true, id: sourceTorrent, modTime: now}
}

func (f *Fs) loadStore() error {
	in, err := os.Open(f.storePath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer fs.CheckClose(in, &err)

	var store persistentStore
	err = gob.NewDecoder(in).Decode(&store)
	if err != nil {
		return err
	}
	if store.Transfers == nil {
		store.Transfers = make(map[string]storedTransfer)
	}
	f.stored = store.Transfers
	fs.Debugf(f, "Loaded Premiumize persistent transfer cache: transfers=%d", len(f.stored))
	return nil
}

func (f *Fs) saveStore() error {
	err := os.MkdirAll(filepath.Dir(f.storePath), 0700)
	if err != nil {
		return err
	}
	tmpPath := f.storePath + ".tmp"
	out, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	store := persistentStore{
		Version:   1,
		Transfers: f.stored,
	}
	encodeErr := gob.NewEncoder(out).Encode(&store)
	closeErr := out.Close()
	if encodeErr != nil {
		_ = os.Remove(tmpPath)
		return encodeErr
	}
	if closeErr != nil {
		_ = os.Remove(tmpPath)
		return closeErr
	}
	return os.Rename(tmpPath, f.storePath)
}

func (f *Fs) recordKnownHashLocked(value string) {
	hash := torrentdump.NormalizeHash(value)
	if hash != "" {
		f.knownHashes[hash] = struct{}{}
	}
}

func (f *Fs) recordCompletedHashLocked(value string) {
	hash := torrentdump.NormalizeHash(value)
	if hash != "" {
		f.knownHashes[hash] = struct{}{}
		f.completedHashes[hash] = struct{}{}
	}
}

func (f *Fs) localKnownHashes() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]struct{}, len(f.knownHashes))
	for hash := range f.knownHashes {
		out[hash] = struct{}{}
	}
	return out
}

func (f *Fs) localCompletedHashes() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make(map[string]struct{}, len(f.completedHashes))
	for hash := range f.completedHashes {
		out[hash] = struct{}{}
	}
	return out
}

func (f *Fs) writeHashDump() {
	hashes := f.localCompletedHashes()
	err := torrentdump.Write(f.dumpPath, "premiumize", hashes)
	if err != nil {
		fs.Debugf(f, "Premiumize dump write failed: %v", err)
		return
	}
	fs.Debugf(f, "Premiumize dump written: path=%s hashes=%d", f.dumpPath, len(hashes))
}

func premiumizeIsScanTarget() bool {
	target := torrentdump.RemoteScanTargetProvider()
	return target == "premiumize" || target == "pm"
}

func (f *Fs) importRemoteDumps(ctx context.Context) {
	if !premiumizeIsScanTarget() {
		return
	}
	local := f.localKnownHashes()
	for _, dumpPath := range torrentdump.RemoteDumpPaths() {
		dump, err := torrentdump.Read(dumpPath)
		if err != nil {
			fs.Debugf(f, "Premiumize remote dump read failed: path=%s: %v", dumpPath, err)
			continue
		}
		for _, hash := range dump.Hashes {
			hash = torrentdump.NormalizeHash(hash)
			if hash == "" {
				continue
			}
			if _, ok := local[hash]; ok {
				continue
			}
			err = f.createTransfer(ctx, torrentdump.Magnet(hash))
			if err != nil {
				fs.Debugf(f, "Premiumize remote dump import failed: hash=%s provider=%s: %v", hash, dump.Provider, err)
				continue
			}
			local[hash] = struct{}{}
			f.mu.Lock()
			f.knownHashes[hash] = struct{}{}
			f.mu.Unlock()
			fs.Debugf(f, "Premiumize remote dump hash imported: hash=%s provider=%s", hash, dump.Provider)
		}
	}
}

func (f *Fs) startRuntimeTasks(ctx context.Context) {
	err := f.refresh(ctx)
	if err != nil {
		fs.Debugf(f, "Premiumize startup refresh failed before dump/import tasks: %v", err)
	}
	close(f.startupDone)
	f.writeHashDump()
	f.importRemoteDumps(ctx)

	ticker := time.NewTicker(torrentdump.DumpInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			f.writeHashDump()
			f.importRemoteDumps(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (f *Fs) callJSON(ctx context.Context, opts *rest.Opts, in, out any) error {
	var resp *http.Response
	var err error
	err = f.pacer.Call(func() (bool, error) {
		resp, err = f.srv.CallJSON(ctx, opts, in, out)
		retry, retryErr := shouldRetry(ctx, resp, err)
		if retry || retryErr != nil {
			return retry, retryErr
		}
		response := apiResponse(out)
		if response == nil || response.AsErr() == nil {
			return false, nil
		}
		switch response.Code {
		case "rate_limit_reached", "slow_down":
			fs.Debugf(f, "Premiumize API asks to slow down: %s", response.Error())
			return true, pacer.RetryAfterError(response, rateLimitBackoff)
		}
		return false, response
	})
	if err != nil {
		return err
	}

	response := apiResponse(out)
	if response == nil {
		return nil
	}
	return response.AsErr()
}

func apiResponse(out any) *api.Response {
	switch result := out.(type) {
	case *api.Response:
		return result
	case *api.TransferListResponse:
		return &result.Response
	case *api.FolderListResponse:
		return &result.Response
	case *api.Item:
		return &result.Response
	case *api.CacheCheckResponse:
		return &result.Response
	case *api.TransferSourceResponse:
		return &result.Response
	case *api.DirectDLResponse:
		return &result.Response
	case *api.TransferCreateResponse:
		return &result.Response
	default:
		return nil
	}
}

func (f *Fs) listTransfers(ctx context.Context) ([]api.Transfer, error) {
	opts := rest.Opts{
		Method: "GET",
		Path:   "/transfer/list",
	}
	var result api.TransferListResponse
	fs.Debugf(f, "Premiumize API call: GET /transfer/list")
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: GET /transfer/list: %v", err)
		return nil, err
	}
	fs.Debugf(f, "Premiumize API response: GET /transfer/list items=%d", len(result.Transfers))
	return result.Transfers, nil
}

func (f *Fs) listFolder(ctx context.Context, folderID string) (*api.FolderListResponse, error) {
	params := url.Values{}
	params.Set("id", folderID)
	params.Set("includebreadcrumbs", "false")
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/folder/list",
		Parameters: params,
	}
	var result api.FolderListResponse
	fs.Debugf(f, "Premiumize API call: GET /folder/list id=%s", folderID)
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: GET /folder/list id=%s: %v", folderID, err)
		return nil, err
	}
	fs.Debugf(f, "Premiumize API response: GET /folder/list id=%s items=%d", folderID, len(result.Content))
	return &result, nil
}

func (f *Fs) itemDetails(ctx context.Context, fileID string) (*api.Item, error) {
	params := url.Values{}
	params.Set("id", fileID)
	opts := rest.Opts{
		Method:     "GET",
		Path:       "/item/details",
		Parameters: params,
	}
	var result api.Item
	fs.Debugf(f, "Premiumize API call: GET /item/details id=%s", fileID)
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: GET /item/details id=%s: %v", fileID, err)
		return nil, err
	}
	fs.Debugf(f, "Premiumize API response: GET /item/details id=%s", fileID)
	return &result, nil
}

func (f *Fs) cacheCheck(ctx context.Context, src string) (bool, error) {
	params := url.Values{}
	params.Add("items[]", src)
	opts := rest.Opts{
		Method:          "POST",
		Path:            "/cache/check",
		MultipartParams: params,
	}
	var result api.CacheCheckResponse
	fs.Debugf(f, "Premiumize API call: POST /cache/check")
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: POST /cache/check: %v", err)
		return false, err
	}
	hit := len(result.ResponseHits) > 0 && result.ResponseHits[0]
	fs.Debugf(f, "Premiumize API response: POST /cache/check hit=%t", hit)
	return hit, nil
}

func (f *Fs) transferSource(ctx context.Context, transfer api.Transfer) (transferSourceInfo, error) {
	params := url.Values{}
	params.Set("id", transfer.ID)
	opts := rest.Opts{
		Method:          "POST",
		Path:            "/transfer/source",
		MultipartParams: params,
	}
	var result api.TransferSourceResponse
	fs.Debugf(f, "Premiumize API call: POST /transfer/source id=%s", transfer.ID)
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: POST /transfer/source id=%s: %v", transfer.ID, err)
		if transfer.Src != "" {
			fs.Debugf(f, "Premiumize transfer/source failed for transfer %s, falling back to transfer/list src", transfer.ID)
			return transferSourceInfo{src: transfer.Src}, nil
		}
		return transferSourceInfo{}, err
	}
	if result.URL == "" && transfer.Src != "" {
		fs.Debugf(f, "Premiumize transfer/source returned empty source for transfer %s, falling back to transfer/list src", transfer.ID)
		return transferSourceInfo{src: transfer.Src, sourceType: result.Type}, nil
	}
	fs.Debugf(f, "Premiumize API response: POST /transfer/source id=%s type=%s has_url=%t", transfer.ID, result.Type, result.URL != "")
	return transferSourceInfo{src: result.URL, sourceType: result.Type}, nil
}

func (f *Fs) directDL(ctx context.Context, src string) ([]api.DirectDLContent, error) {
	params := url.Values{}
	params.Set("src", src)
	opts := rest.Opts{
		Method:          "POST",
		Path:            "/transfer/directdl",
		MultipartParams: params,
	}
	var result api.DirectDLResponse
	fs.Debugf(f, "Premiumize API call: POST /transfer/directdl")
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: POST /transfer/directdl: %v", err)
		return nil, err
	}
	fs.Debugf(f, "Premiumize API response: POST /transfer/directdl items=%d", len(result.Content))
	return result.Content, nil
}

func (f *Fs) createTransfer(ctx context.Context, src string) error {
	params := url.Values{}
	params.Set("src", src)
	opts := rest.Opts{
		Method:          "POST",
		Path:            "/transfer/create",
		MultipartParams: params,
	}
	var result api.TransferCreateResponse
	fs.Debugf(f, "Premiumize API call: POST /transfer/create")
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: POST /transfer/create: %v", err)
		return err
	}
	fs.Debugf(f, "Premiumize API response: POST /transfer/create id=%s", result.ID)
	return nil
}

func (f *Fs) queueCleanupLocked(transferID, fileID, folderID string) {
	if transferID == "" {
		return
	}
	f.pendingCleanup[transferID] = pendingCleanup{
		transferID: transferID,
		fileID:     fileID,
		folderID:   folderID,
	}
	fs.Debugf(f, "Premiumize cloud cleanup deferred: transfer_id=%s active_opens=%d delay=%v", transferID, f.activeOpens[transferID], cleanupDelay)
	go f.runPendingCleanupAfter(transferID, cleanupDelay)
}

func (f *Fs) runPendingCleanupAfter(transferID string, delay time.Duration) {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	<-timer.C

	var cleanup pendingCleanup
	f.mu.Lock()
	if f.activeOpens[transferID] != 0 {
		f.mu.Unlock()
		return
	}
	cleanup = f.pendingCleanup[transferID]
	delete(f.pendingCleanup, transferID)
	f.mu.Unlock()

	if !cleanup.empty() {
		fs.Debugf(f, "Premiumize deferred cloud cleanup starting: transfer_id=%s", cleanup.transferID)
		err := f.completeCloudCleanup(context.Background(), cleanup)
		if err != nil {
			fs.Debugf(f, "Premiumize deferred cloud cleanup failed: transfer_id=%s: %v", cleanup.transferID, err)
		}
	}
}

func (f *Fs) startOpen(transferID string) {
	if transferID == "" {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.activeOpens[transferID]++
}

func (f *Fs) finishOpen(transferID string) {
	if transferID == "" {
		return
	}
	var cleanup pendingCleanup
	f.mu.Lock()
	if f.activeOpens[transferID] > 1 {
		f.activeOpens[transferID]--
		f.mu.Unlock()
		return
	}
	delete(f.activeOpens, transferID)
	cleanup = f.pendingCleanup[transferID]
	delete(f.pendingCleanup, transferID)
	f.mu.Unlock()

	if !cleanup.empty() {
		fs.Debugf(f, "Premiumize deferred cloud cleanup starting: transfer_id=%s", cleanup.transferID)
		err := f.completeCloudCleanup(context.Background(), cleanup)
		if err != nil {
			fs.Debugf(f, "Premiumize deferred cloud cleanup failed: transfer_id=%s: %v", cleanup.transferID, err)
		}
	}
}

func (f *Fs) queueStoredCleanupsLocked() {
	for transferID, stored := range f.stored {
		cleanup := pendingCleanup{
			transferID: transferID,
			fileID:     stored.FileID,
			folderID:   stored.FolderID,
		}
		if cleanup.empty() {
			continue
		}
		if _, ok := f.pendingCleanup[transferID]; ok {
			continue
		}
		f.queueCleanupLocked(cleanup.transferID, cleanup.fileID, cleanup.folderID)
	}
}

func isUsenetSource(source transferSourceInfo) bool {
	sourceType := strings.ToLower(strings.TrimSpace(source.sourceType))
	src := strings.ToLower(strings.TrimSpace(source.src))
	return sourceType == "file" && (src == "/api/job/src" || strings.HasPrefix(src, "/api/job/src?"))
}

func (f *Fs) cachedTransferCheck(ctx context.Context, transfer api.Transfer) (src string, cacheHit bool, err error) {
	if cached, ok := f.transferChecks[transfer.ID]; ok && (cached.skipPermanent || time.Since(cached.checkedAt) < checkDuration) {
		fs.Debugf(f, "Premiumize transfer check cache hit: transfer_id=%s age=%v hit=%t skip=%t permanent=%t", transfer.ID, time.Since(cached.checkedAt), cached.cacheHit, cached.skipCheck, cached.skipPermanent)
		return cached.src, cached.cacheHit, nil
	}

	source, ok := f.transferSources[transfer.ID]
	if !ok {
		source, err = f.transferSource(ctx, transfer)
		if err != nil {
			return "", false, err
		}
		f.transferSources[transfer.ID] = source
	}
	src = source.src
	if src == "" {
		return "", false, nil
	}
	if isUsenetSource(source) {
		f.transferChecks[transfer.ID] = transferCheck{
			checkedAt:     time.Now(),
			src:           src,
			skipCheck:     true,
			skipPermanent: true,
		}
		fs.Debugf(f, "Premiumize cache/check skipped for usenet source: transfer_id=%s source_type=%s", transfer.ID, source.sourceType)
		return src, false, nil
	}
	f.recordCompletedHashLocked(src)
	cacheHit, err = f.cacheCheck(ctx, src)
	if err != nil {
		return src, false, err
	}

	f.transferChecks[transfer.ID] = transferCheck{
		checkedAt: time.Now(),
		src:       src,
		cacheHit:  cacheHit,
	}
	return src, cacheHit, nil
}

func (f *Fs) rememberTransferHash(ctx context.Context, transfer api.Transfer) {
	if _, ok := f.transferSources[transfer.ID]; ok {
		source := f.transferSources[transfer.ID]
		if !isUsenetSource(source) {
			f.recordKnownHashLocked(source.src)
		}
		return
	}
	source, err := f.transferSource(ctx, transfer)
	if err != nil {
		fs.Debugf(f, "Premiumize transfer/source failed while remembering hash: transfer_id=%s: %v", transfer.ID, err)
		return
	}
	f.transferSources[transfer.ID] = source
	if isUsenetSource(source) {
		f.transferChecks[transfer.ID] = transferCheck{
			checkedAt:     time.Now(),
			src:           source.src,
			skipCheck:     true,
			skipPermanent: true,
		}
		return
	}
	f.recordKnownHashLocked(source.src)
}

func (f *Fs) refresh(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if time.Since(f.cacheTime) < cacheDuration && f.dirs != nil && f.files != nil {
		return nil
	}

	transfers, err := f.listTransfers(ctx)
	if err != nil {
		if len(f.stored) == 0 {
			return fmt.Errorf("couldn't list transfers: %w", err)
		}
		fs.Debugf(f, "Premiumize transfer/list failed, using persistent transfer cache only: %v", err)
		transfers = nil
	}

	dirs := make(map[string]*entry)
	files := make(map[string]*entry)
	dirs[""] = &entry{remote: "", name: "", isDir: true, id: "root", modTime: time.Now()}

	var addDir func(remote string, modTime time.Time, id string)
	addDir = func(remote string, modTime time.Time, id string) {
		remote = strings.Trim(remote, "/")
		if existing, ok := dirs[remote]; ok {
			if existing.id == "" && id != "" {
				existing.id = id
			}
			return
		}
		name := path.Base(remote)
		if remote == "" {
			name = ""
		}
		dirs[remote] = &entry{remote: remote, name: name, isDir: true, id: id, modTime: modTime}
		if parent := path.Dir(remote); remote != "" && parent != "." {
			addDir(parent, modTime, "")
		}
	}

	now := time.Now()
	addDir(sourceTorrent, now, sourceTorrent)
	liveTransfers := make(map[string]struct{}, len(transfers))

	for _, transfer := range transfers {
		f.rememberTransferHash(ctx, transfer)
		if !transferReady(transfer) {
			continue
		}
		liveTransfers[transfer.ID] = struct{}{}
		transferName := cleanSegment(f.opt.Enc.ToStandardName(transfer.Name))
		if transferName == "" {
			transferName = transfer.ID
		}
		baseDir := uniqueDir(dirs, path.Join(sourceTorrent, transferName), transfer.ID, now)
		transferFileID := transfer.FileID.String()
		transferDirID := transfer.FolderID.String()
		dirs[baseDir].transferID = transfer.ID
		dirs[baseDir].transferRoot = baseDir
		dirs[baseDir].transferFileID = transferFileID
		dirs[baseDir].transferDirID = transferDirID
		dirs[baseDir].folderID = transferDirID

		src, cacheHit, err := f.cachedTransferCheck(ctx, transfer)
		if err != nil {
			fs.Debugf(f, "Premiumize transfer check failed for transfer %s, falling back to folder data: %v", transfer.ID, err)
		}
		if src != "" && cacheHit {
			content, err := f.directDL(ctx, src)
			if err != nil {
				fs.Debugf(f, "Premiumize directdl failed for transfer %s, falling back to folder data: %v", transfer.ID, err)
			} else {
				err = f.storeDirectDLTransfer(transfer, src, transferFileID, transferDirID, content)
				if err == nil {
					f.queueCleanupLocked(transfer.ID, transferFileID, transferDirID)
				} else {
					fs.Errorf(f, "Premiumize persistent transfer cache failed for transfer %s, keeping cloud item: %v", transfer.ID, err)
				}
				addDirectDLContent(files, dirs, addDir, f.opt.Enc, baseDir, transfer.Name, content, transfer.ID, baseDir, transferFileID, transferDirID, src)
				continue
			}
		}
	}
	f.addStoredTransfers(files, dirs, addDir, liveTransfers)
	f.queueStoredCleanupsLocked()

	f.dirs = dirs
	f.files = files
	f.cacheTime = time.Now()
	return nil
}

func (f *Fs) storeDirectDLTransfer(transfer api.Transfer, src, transferFileID, transferDirID string, content []api.DirectDLContent) error {
	stored := storedTransfer{
		ID:       transfer.ID,
		Name:     transfer.Name,
		Src:      src,
		FileID:   transferFileID,
		FolderID: transferDirID,
		StoredAt: time.Now(),
		Content:  make([]storedDirectFile, 0, len(content)),
	}
	for i := range content {
		stored.Content = append(stored.Content, storedDirectFile{
			Path: content[i].Path,
			Size: content[i].Size,
			Link: content[i].Link,
		})
	}
	f.stored[transfer.ID] = stored
	f.recordCompletedHashLocked(src)
	err := f.saveStore()
	if err != nil {
		delete(f.stored, transfer.ID)
		return err
	}
	fs.Debugf(f, "Stored Premiumize transfer in persistent cache: transfer_id=%s files=%d", transfer.ID, len(stored.Content))
	return nil
}

func (f *Fs) replaceStoredTransferContent(transferID string, content []api.DirectDLContent) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.stored[transferID]
	if !ok {
		return fs.ErrorObjectNotFound
	}
	stored.Content = stored.Content[:0]
	for i := range content {
		stored.Content = append(stored.Content, storedDirectFile{
			Path: content[i].Path,
			Size: content[i].Size,
			Link: content[i].Link,
		})
	}
	stored.StoredAt = time.Now()
	f.stored[transferID] = stored
	return f.saveStore()
}

func (f *Fs) storedFile(transferID, contentPath string) (storedTransfer, storedDirectFile, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.stored[transferID]
	if !ok {
		return storedTransfer{}, storedDirectFile{}, false
	}
	for i := range stored.Content {
		if stored.Content[i].Path == contentPath {
			return stored, stored.Content[i], true
		}
	}
	return stored, storedDirectFile{}, false
}

func (f *Fs) updateCachedFileURL(transferID, contentPath, downloadURL string, size int64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, info := range f.files {
		if info.transferID == transferID && info.contentPath == contentPath {
			info.url = downloadURL
			if size > 0 {
				info.size = size
			}
		}
	}
}

func (f *Fs) addStoredTransfers(files map[string]*entry, dirs map[string]*entry, addDir func(string, time.Time, string), liveTransfers map[string]struct{}) {
	for _, stored := range f.stored {
		if _, ok := liveTransfers[stored.ID]; ok {
			continue
		}
		transferName := cleanSegment(f.opt.Enc.ToStandardName(stored.Name))
		if transferName == "" {
			transferName = stored.ID
		}
		modTime := stored.StoredAt
		if modTime.IsZero() {
			modTime = time.Now()
		}
		baseDir := uniqueDir(dirs, path.Join(sourceTorrent, transferName), stored.ID, modTime)
		content := make([]api.DirectDLContent, 0, len(stored.Content))
		for i := range stored.Content {
			content = append(content, api.DirectDLContent{
				Path: stored.Content[i].Path,
				Size: stored.Content[i].Size,
				Link: stored.Content[i].Link,
			})
		}
		addDirectDLContent(files, dirs, addDir, f.opt.Enc, baseDir, stored.Name, content, stored.ID, baseDir, stored.FileID, stored.FolderID, stored.Src)
		fs.Debugf(f, "Restored Premiumize transfer from persistent cache: transfer_id=%s files=%d", stored.ID, len(stored.Content))
	}
}

func addDirectDLContent(files map[string]*entry, dirs map[string]*entry, addDir func(string, time.Time, string), enc encoder.MultiEncoder, baseDir, transferName string, content []api.DirectDLContent, transferID, transferRoot, transferFileID, transferDirID, transferSrc string) {
	now := time.Now()
	for i := range content {
		item := &content[i]
		remote := encodePath(enc, cleanDirectDLPath(item.Path, transferName))
		if remote == "" {
			remote = fmt.Sprintf("%d", i)
		}
		addDirectDLFile(files, dirs, addDir, path.Join(baseDir, remote), item, transferID, transferRoot, transferFileID, transferDirID, transferSrc, now)
	}
}

func cleanDirectDLPath(value, transferName string) string {
	value = strings.Trim(value, "/")
	transferName = strings.Trim(transferName, "/")
	if transferName == "" {
		return value
	}
	if strings.EqualFold(path.Clean(value), path.Clean(transferName)) {
		return path.Base(value)
	}
	withSlash := transferName + "/"
	if strings.HasPrefix(strings.ToLower(value), strings.ToLower(withSlash)) {
		return value[len(withSlash):]
	}
	return value
}

func addDirectDLFile(files map[string]*entry, dirs map[string]*entry, addDir func(string, time.Time, string), remote string, item *api.DirectDLContent, transferID, transferRoot, transferFileID, transferDirID, transferSrc string, modTime time.Time) {
	remote = uniqueFile(files, remote, transferID, path.Base(remote))
	parent := path.Dir(remote)
	if parent != "." {
		addDir(parent, modTime, "")
	}
	files[remote] = &entry{
		remote:         remote,
		name:           path.Base(remote),
		id:             fmt.Sprintf("%s:%s", transferID, remote),
		size:           item.Size,
		modTime:        modTime,
		url:            item.Link,
		transferID:     transferID,
		transferRoot:   transferRoot,
		transferFileID: transferFileID,
		transferDirID:  transferDirID,
		transferSrc:    transferSrc,
		contentPath:    item.Path,
	}
	_ = dirs
}

func transferReady(t api.Transfer) bool {
	return t.Status == statusFinished || t.Status == statusSeeding
}

func itemTime(item *api.Item) time.Time {
	if item.CreatedAt > 0 {
		return time.Unix(item.CreatedAt, 0)
	}
	return time.Now()
}

func encodePath(enc encoder.MultiEncoder, value string) string {
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = cleanSegment(enc.ToStandardName(parts[i]))
	}
	return path.Join(parts...)
}

func cleanSegment(value string) string {
	value = strings.TrimSpace(value)
	value = strings.Trim(value, "/")
	if value == "." || value == ".." {
		return ""
	}
	return value
}

func uniqueDir(dirs map[string]*entry, remote, id string, modTime time.Time) string {
	remote = strings.Trim(remote, "/")
	out := remote
	if _, ok := dirs[out]; ok {
		out = addSuffix(remote, id)
	}
	dirs[out] = &entry{remote: out, name: path.Base(out), isDir: true, id: id, modTime: modTime}
	return out
}

func uniqueFile(files map[string]*entry, remote, transferID, fileID string) string {
	remote = strings.Trim(remote, "/")
	if _, ok := files[remote]; !ok {
		return remote
	}
	return addSuffix(remote, fmt.Sprintf("%s-%s", transferID, fileID))
}

func addSuffix(remote, suffix string) string {
	ext := path.Ext(remote)
	base := strings.TrimSuffix(remote, ext)
	return fmt.Sprintf("%s (%s)%s", base, suffix, ext)
}

func parentDir(remote string) string {
	parent := path.Dir(remote)
	if parent == "." {
		return ""
	}
	return parent
}

func (f *Fs) actualPath(remote string) string {
	remote = strings.Trim(remote, "/")
	if f.root == "" {
		return remote
	}
	return path.Join(f.root, remote)
}

func (f *Fs) fromRoot(remote string) string {
	remote = strings.Trim(remote, "/")
	if f.root == "" {
		return remote
	}
	if remote == f.root {
		return ""
	}
	return strings.TrimPrefix(remote, f.root+"/")
}

func (f *Fs) shouldRefreshForDir(actualDir string) bool {
	f.mu.Lock()
	cacheTime := f.cacheTime
	f.mu.Unlock()
	if cacheTime.IsZero() {
		return actualDir != ""
	}
	if actualDir == "" {
		return false
	}
	if cacheDuration <= 0 || time.Since(cacheTime) < cacheDuration {
		return false
	}
	return actualDir == sourceTorrent
}

func (f *Fs) cachedDirExists(actualDir string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.dirs[actualDir]
	return ok
}

func (f *Fs) listFromCache(actualDir string) (entries fs.DirEntries, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.dirs[actualDir]; !ok {
		return nil, fs.ErrorDirNotFound
	}
	for remote, info := range f.dirs {
		if remote != "" && parentDir(remote) == actualDir {
			entries = append(entries, fs.NewDir(f.fromRoot(remote), info.modTime).SetID(info.id))
		}
	}
	for remote, info := range f.files {
		if parentDir(remote) == actualDir {
			o, err := f.newObjectWithInfo(f.fromRoot(remote), info)
			if err != nil {
				return nil, err
			}
			entries = append(entries, o)
		}
	}
	sort.Sort(entries)
	return entries, nil
}

func (f *Fs) objectFromCache(remote string) (fs.Object, bool, error) {
	f.mu.Lock()
	info, ok := f.files[f.actualPath(remote)]
	if ok {
		infoCopy := *info
		info = &infoCopy
	}
	f.mu.Unlock()
	if !ok {
		return nil, false, nil
	}
	obj, err := f.newObjectWithInfo(remote, info)
	return obj, true, err
}

func (f *Fs) ensureDirLoaded(ctx context.Context, actualDir string) error {
	f.mu.Lock()
	info, ok := f.dirs[actualDir]
	if !ok {
		f.mu.Unlock()
		return fs.ErrorDirNotFound
	}
	infoCopy := *info
	cacheKey := infoCopy.folderID
	if cacheKey == "" && infoCopy.transferFileID != "" {
		cacheKey = "file:" + infoCopy.transferFileID
	}
	if cacheKey == "" {
		f.mu.Unlock()
		return nil
	}
	cached, hasCached := f.folderCache[cacheKey]
	hasChildren := false
	for remote := range f.dirs {
		if remote != "" && parentDir(remote) == actualDir {
			hasChildren = true
			break
		}
	}
	if !hasChildren {
		for remote := range f.files {
			if parentDir(remote) == actualDir {
				hasChildren = true
				break
			}
		}
	}
	if hasCached && hasChildren {
		f.mu.Unlock()
		return nil
	}
	f.mu.Unlock()

	var singleFile *api.Item
	var folderContent []api.Item
	var err error
	if hasCached {
		if cached.file != nil {
			fileCopy := *cached.file
			singleFile = &fileCopy
		}
		folderContent = append(folderContent, cached.content...)
	} else if infoCopy.folderID != "" {
		var folder *api.FolderListResponse
		folder, err = f.listFolder(ctx, infoCopy.folderID)
		if err != nil {
			return err
		}
		folderContent = append(folderContent, folder.Content...)
	} else if infoCopy.transferFileID != "" {
		singleFile, err = f.itemDetails(ctx, infoCopy.transferFileID)
		if err != nil {
			return err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if !hasCached {
		cached = cachedFolder{
			loadedAt: time.Now(),
			content:  append([]api.Item(nil), folderContent...),
		}
		if singleFile != nil {
			fileCopy := *singleFile
			cached.file = &fileCopy
		}
		f.folderCache[cacheKey] = cached
	}

	var addDir func(remote string, modTime time.Time, id string) *entry
	addDir = func(remote string, modTime time.Time, id string) *entry {
		remote = strings.Trim(remote, "/")
		if existing, ok := f.dirs[remote]; ok {
			if existing.id == "" && id != "" {
				existing.id = id
			}
			return existing
		}
		name := path.Base(remote)
		if remote == "" {
			name = ""
		}
		f.dirs[remote] = &entry{remote: remote, name: name, isDir: true, id: id, modTime: modTime}
		if parent := path.Dir(remote); remote != "" && parent != "." {
			addDir(parent, modTime, "")
		}
		return f.dirs[remote]
	}
	addFile := func(remote string, item *api.Item) {
		remote = uniqueFile(f.files, remote, infoCopy.transferID, item.ID)
		parent := path.Dir(remote)
		if parent != "." {
			addDir(parent, itemTime(item), "")
		}
		f.files[remote] = &entry{
			remote:         remote,
			name:           path.Base(remote),
			id:             item.ID,
			size:           item.Size,
			modTime:        itemTime(item),
			mimeType:       item.MimeType,
			url:            item.Link,
			transferID:     infoCopy.transferID,
			transferRoot:   infoCopy.transferRoot,
			transferFileID: infoCopy.transferFileID,
			transferDirID:  infoCopy.transferDirID,
		}
	}

	if singleFile != nil {
		fileRemote := encodePath(f.opt.Enc, singleFile.Name)
		if fileRemote == "" {
			fileRemote = singleFile.ID
		}
		addFile(path.Join(actualDir, fileRemote), singleFile)
	}
	if len(folderContent) > 0 {
		for i := range folderContent {
			item := folderContent[i]
			item.Name = cleanSegment(f.opt.Enc.ToStandardName(item.Name))
			if item.Name == "" {
				item.Name = item.ID
			}
			childRemote := path.Join(actualDir, item.Name)
			switch item.Type {
			case api.ItemTypeFolder:
				dir := addDir(childRemote, itemTime(&item), item.ID)
				dir.transferID = infoCopy.transferID
				dir.transferRoot = infoCopy.transferRoot
				dir.transferFileID = infoCopy.transferFileID
				dir.transferDirID = infoCopy.transferDirID
				dir.folderID = item.ID
			case api.ItemTypeFile:
				addFile(childRemote, &item)
			default:
				fs.Debugf(f, "Ignoring Premiumize item %q - unknown type %q", item.Name, item.Type)
			}
		}
	}
	return nil
}

// List the objects and directories in dir into entries.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dir = strings.Trim(dir, "/")
	actualDir := f.actualPath(dir)
	if !f.shouldRefreshForDir(actualDir) && f.cachedDirExists(actualDir) {
		err = f.ensureDirLoaded(ctx, actualDir)
		if err != nil {
			return nil, err
		}
		return f.listFromCache(actualDir)
	}
	err = f.refresh(ctx)
	if err != nil {
		if f.cachedDirExists(actualDir) {
			loadErr := f.ensureDirLoaded(ctx, actualDir)
			if loadErr != nil {
				return nil, loadErr
			}
			return f.listFromCache(actualDir)
		}
		return nil, err
	}
	err = f.ensureDirLoaded(ctx, actualDir)
	if err != nil {
		return nil, err
	}
	return f.listFromCache(actualDir)
}

func (f *Fs) newObjectWithInfo(remote string, info *entry) (fs.Object, error) {
	o := &Object{fs: f, remote: remote}
	return o, o.setMetaData(info)
}

// NewObject finds the Object at remote.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	remote = strings.Trim(remote, "/")
	obj, found, err := f.objectFromCache(remote)
	if found || err != nil {
		return obj, err
	}
	err = f.ensureDirLoaded(ctx, parentDir(f.actualPath(remote)))
	if err != nil && err != fs.ErrorDirNotFound {
		return nil, err
	}
	obj, found, err = f.objectFromCache(remote)
	if found || err != nil {
		return obj, err
	}
	err = f.refresh(ctx)
	if err != nil {
		return nil, err
	}
	err = f.ensureDirLoaded(ctx, parentDir(f.actualPath(remote)))
	if err != nil && err != fs.ErrorDirNotFound {
		return nil, err
	}
	obj, found, err = f.objectFromCache(remote)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, fs.ErrorObjectNotFound
	}
	return obj, nil
}

// Mkdir is unsupported on read-only Premiumize transfer remotes.
func (f *Fs) Mkdir(ctx context.Context, dir string) error { return errReadOnly }

// Rmdir is unsupported on read-only Premiumize transfer remotes.
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return errReadOnly }

// Put is unsupported on read-only Premiumize transfer remotes.
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	return nil, errReadOnly
}

// Precision returns the precision of this Fs.
func (f *Fs) Precision() time.Duration { return fs.ModTimeNotSupported }

// DirCacheFlush resets the in-memory listing cache.
func (f *Fs) DirCacheFlush() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cacheTime = time.Time{}
}

// Hashes returns the supported hash sets.
func (f *Fs) Hashes() hash.Set { return hash.Set(hash.None) }

// Fs returns the parent Fs.
func (o *Object) Fs() fs.Info { return o.fs }

// String returns a string version.
func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// Remote returns the remote path.
func (o *Object) Remote() string { return o.remote }

// Hash returns an unsupported hash.
func (o *Object) Hash(ctx context.Context, t hash.Type) (string, error) {
	return "", hash.ErrUnsupported
}

// Size returns the size of an object in bytes.
func (o *Object) Size() int64 { return o.size }

func (o *Object) setMetaData(info *entry) error {
	if info == nil || info.isDir {
		return fs.ErrorNotAFile
	}
	o.hasMetaData = true
	o.size = info.size
	o.modTime = info.modTime
	o.id = info.id
	o.mimeType = info.mimeType
	o.url = info.url
	o.transferID = info.transferID
	o.transferRoot = info.transferRoot
	o.transferFileID = info.transferFileID
	o.transferDirID = info.transferDirID
	o.transferSrc = info.transferSrc
	o.contentPath = info.contentPath
	return nil
}

func (o *Object) readMetaData(ctx context.Context) error {
	if o.hasMetaData {
		return nil
	}
	obj, err := o.fs.NewObject(ctx, o.remote)
	if err != nil {
		return err
	}
	return o.setMetaData(&entry{
		remote:         obj.Remote(),
		size:           obj.Size(),
		modTime:        obj.ModTime(ctx),
		id:             obj.(*Object).id,
		mimeType:       obj.(*Object).mimeType,
		url:            obj.(*Object).url,
		transferID:     obj.(*Object).transferID,
		transferRoot:   obj.(*Object).transferRoot,
		transferFileID: obj.(*Object).transferFileID,
		transferDirID:  obj.(*Object).transferDirID,
		transferSrc:    obj.(*Object).transferSrc,
		contentPath:    obj.(*Object).contentPath,
	})
}

// ModTime returns the modification time of the object.
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// SetModTime is unsupported on read-only Premiumize transfer remotes.
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error { return errReadOnly }

// Storable returns whether this object is storable.
func (o *Object) Storable() bool { return true }

func (o *Object) openDownloadURL(ctx context.Context, downloadURL string, options []fs.OpenOption) (io.ReadCloser, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: downloadURL,
		Options: options,
	}
	var resp *http.Response
	var err error
	fs.Debugf(o, "Premiumize CDN open: %s", redactedURL(downloadURL))
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.dlSrv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		fs.Debugf(o, "Premiumize CDN open failed: %s: %v", redactedURL(downloadURL), err)
		return nil, err
	}
	if resp != nil {
		fs.Debugf(o, "Premiumize CDN open response: %s status=%d", redactedURL(downloadURL), resp.StatusCode)
	}
	return resp.Body, nil
}

func (rc *openReadCloser) Close() error {
	if rc.closed {
		return nil
	}
	rc.closed = true
	err := rc.ReadCloser.Close()
	rc.object.fs.finishOpen(rc.object.transferID)
	return err
}

func (o *Object) wrapOpen(in io.ReadCloser) io.ReadCloser {
	if o.transferID == "" {
		return in
	}
	o.fs.startOpen(o.transferID)
	return &openReadCloser{ReadCloser: in, object: o}
}

func (o *Object) refreshStoredDownloadURL(ctx context.Context) error {
	if o.transferID == "" || o.transferSrc == "" || o.contentPath == "" {
		return errors.New("object is not backed by a persistent Premiumize transfer")
	}
	_, _, ok := o.fs.storedFile(o.transferID, o.contentPath)
	if !ok {
		return errors.New("persistent Premiumize transfer entry not found")
	}

	cacheHit, err := o.fs.cacheCheck(ctx, o.transferSrc)
	if err != nil {
		return fmt.Errorf("cache/check after CDN failure failed: %w", err)
	}
	if !cacheHit {
		fs.Debugf(o, "Premiumize cache miss after stored CDN failure, recreating transfer: transfer_id=%s", o.transferID)
		err = o.fs.createTransfer(ctx, o.transferSrc)
		if err != nil {
			return fmt.Errorf("recreate transfer after cache miss failed: %w", err)
		}
		err = o.fs.deleteStoredTransfer(o.transferID)
		if err != nil {
			return fmt.Errorf("remove stale persistent transfer after recreation failed: %w", err)
		}
		o.fs.pruneTransferFromCache(o.transferID, o.transferRoot)
		return errors.New("stored Premiumize link expired; recreated transfer and removed stale local entry")
	}

	content, err := o.fs.directDL(ctx, o.transferSrc)
	if err != nil {
		return fmt.Errorf("directdl after cache hit failed: %w", err)
	}
	err = o.fs.replaceStoredTransferContent(o.transferID, content)
	if err != nil {
		return fmt.Errorf("update persistent transfer after directdl failed: %w", err)
	}
	for i := range content {
		if content[i].Path == o.contentPath {
			o.url = content[i].Link
			o.size = content[i].Size
			o.fs.updateCachedFileURL(o.transferID, o.contentPath, o.url, o.size)
			fs.Debugf(o, "Premiumize stored CDN URL refreshed: transfer_id=%s path=%s", o.transferID, o.contentPath)
			return nil
		}
	}
	return fmt.Errorf("directdl refreshed transfer but did not return path %q", o.contentPath)
}

// Open an object for read.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	err := o.readMetaData(ctx)
	if err != nil {
		return nil, err
	}
	if o.url == "" {
		err = o.refreshStoredDownloadURL(ctx)
		if err != nil {
			return nil, fmt.Errorf("can't download - missing download URL: %w", err)
		}
	}
	fs.FixRangeOption(options, o.size)
	in, err := o.openDownloadURL(ctx, o.url, options)
	if err == nil {
		return o.wrapOpen(in), nil
	}
	if o.transferSrc == "" || o.contentPath == "" {
		return nil, err
	}
	fs.Debugf(o, "Premiumize stored CDN URL failed, trying to refresh it: %v", err)
	refreshErr := o.refreshStoredDownloadURL(ctx)
	if refreshErr != nil {
		return nil, fmt.Errorf("%w; refresh failed: %v", err, refreshErr)
	}
	fs.FixRangeOption(options, o.size)
	in, err = o.openDownloadURL(ctx, o.url, options)
	if err != nil {
		return nil, err
	}
	return o.wrapOpen(in), nil
}

// Update is unsupported on read-only Premiumize transfer remotes.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errReadOnly
}

func (f *Fs) deleteTransferRecord(ctx context.Context, transferID string) error {
	params := url.Values{}
	params.Set("id", transferID)
	opts := rest.Opts{
		Method:          "POST",
		Path:            "/transfer/delete",
		MultipartParams: params,
	}
	var result api.Response
	fs.Debugf(f, "Premiumize API call: POST /transfer/delete id=%s", transferID)
	err := f.callJSON(ctx, &opts, nil, &result)
	if err != nil {
		fs.Debugf(f, "Premiumize API error: POST /transfer/delete id=%s: %v", transferID, err)
		return err
	}
	fs.Debugf(f, "Premiumize API response: POST /transfer/delete id=%s", transferID)
	return nil
}

func (f *Fs) deleteCloudItem(ctx context.Context, fileID, folderID string) error {
	if folderID != "" {
		params := url.Values{}
		params.Set("id", folderID)
		opts := rest.Opts{
			Method:          "POST",
			Path:            "/folder/delete",
			MultipartParams: params,
		}
		var result api.Response
		fs.Debugf(f, "Premiumize API call: POST /folder/delete id=%s", folderID)
		err := f.callJSON(ctx, &opts, nil, &result)
		if err != nil {
			fs.Debugf(f, "Premiumize API error: POST /folder/delete id=%s: %v", folderID, err)
			return err
		}
		fs.Debugf(f, "Premiumize API response: POST /folder/delete id=%s", folderID)
		return nil
	}
	if fileID != "" {
		params := url.Values{}
		params.Set("id", fileID)
		opts := rest.Opts{
			Method:          "POST",
			Path:            "/item/delete",
			MultipartParams: params,
		}
		var result api.Response
		fs.Debugf(f, "Premiumize API call: POST /item/delete id=%s", fileID)
		err := f.callJSON(ctx, &opts, nil, &result)
		if err != nil {
			fs.Debugf(f, "Premiumize API error: POST /item/delete id=%s: %v", fileID, err)
			return err
		}
		fs.Debugf(f, "Premiumize API response: POST /item/delete id=%s", fileID)
	}
	return nil
}

func (f *Fs) completeCloudCleanup(ctx context.Context, cleanup pendingCleanup) error {
	if cleanup.empty() {
		return nil
	}
	err := f.deleteCloudItem(ctx, cleanup.fileID, cleanup.folderID)
	if err != nil {
		return err
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	stored, ok := f.stored[cleanup.transferID]
	if !ok {
		return nil
	}
	stored.FileID = ""
	stored.FolderID = ""
	f.stored[cleanup.transferID] = stored
	err = f.saveStore()
	if err != nil {
		return err
	}
	fs.Debugf(f, "Premiumize cloud cleanup marked done in persistent cache: transfer_id=%s", cleanup.transferID)
	return nil
}

func (f *Fs) pruneTransferFromCache(transferID, transferRoot string) {
	transferRoot = strings.Trim(transferRoot, "/")
	f.mu.Lock()
	defer f.mu.Unlock()
	for remote, info := range f.files {
		if info.transferID == transferID {
			delete(f.files, remote)
		}
	}
	if transferRoot != "" {
		for remote := range f.dirs {
			if remote == transferRoot || strings.HasPrefix(remote, transferRoot+"/") {
				delete(f.dirs, remote)
			}
		}
	}
}

func (f *Fs) hasStoredTransfer(transferID string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	_, ok := f.stored[transferID]
	return ok
}

func (f *Fs) deleteStoredTransfer(transferID string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.stored[transferID]; !ok {
		return nil
	}
	delete(f.stored, transferID)
	err := f.saveStore()
	if err != nil {
		return err
	}
	fs.Debugf(f, "Deleted Premiumize transfer from persistent cache: transfer_id=%s", transferID)
	return nil
}

// Remove deletes the whole Premiumize transfer folder that contains this object.
func (o *Object) Remove(ctx context.Context) error {
	err := o.readMetaData(ctx)
	if err != nil {
		return fmt.Errorf("Remove: failed to read metadata: %w", err)
	}
	if o.transferID == "" {
		return errors.New("can't delete - missing transfer ID")
	}
	isStored := o.fs.hasStoredTransfer(o.transferID)
	fs.Debugf(o, "Deleting containing Premiumize transfer: transfer_id=%s file_id=%s folder_id=%s", o.transferID, o.transferFileID, o.transferDirID)
	err = o.fs.deleteCloudItem(ctx, o.transferFileID, o.transferDirID)
	if err != nil {
		if !isStored {
			return err
		}
		fs.Debugf(o, "Ignoring Premiumize cloud delete error for stored transfer %s: %v", o.transferID, err)
	}
	err = o.fs.deleteTransferRecord(ctx, o.transferID)
	if err != nil {
		if !isStored {
			return err
		}
		fs.Debugf(o, "Ignoring Premiumize transfer delete error for stored transfer %s: %v", o.transferID, err)
	}
	err = o.fs.deleteStoredTransfer(o.transferID)
	if err != nil {
		return err
	}
	o.fs.pruneTransferFromCache(o.transferID, o.transferRoot)
	return nil
}

// MimeType of an Object if known.
func (o *Object) MimeType(ctx context.Context) string { return o.mimeType }

// ID returns the ID of the Object if known.
func (o *Object) ID() string { return o.id }

var (
	_ fs.Fs              = (*Fs)(nil)
	_ fs.DirCacheFlusher = (*Fs)(nil)
	_ fs.Object          = (*Object)(nil)
	_ fs.MimeTyper       = (*Object)(nil)
	_ fs.IDer            = (*Object)(nil)
)
