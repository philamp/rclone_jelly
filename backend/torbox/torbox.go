// Package torbox provides an interface to TorBox torrents and Usenet downloads.
package torbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/rclone/rclone/backend/torbox/api"
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
	minSleep      = 200 * time.Millisecond
	maxSleep      = 5 * time.Second
	decayConstant = 2
	rootURL       = "https://api.torbox.app/v1/api"
	cacheDuration = 2 * time.Second
)

var errReadOnly = errors.New("torbox remotes are read only")

// Register with Fs.
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "torbox",
		Description: "TorBox",
		NewFs:       NewFs,
		Options: []fs.Option{{
			Name:      "api_key",
			Help:      "TorBox API key.",
			Sensitive: true,
		}, {
			Name:     "folder_mode",
			Help:     `Choose whether torrents are grouped into virtual folders. Use "folders" for shows/movies/default torrent folders, or "files" for all files in the root. Default: "folders".`,
			Advanced: true,
			Default:  "folders",
		}, {
			Name:     "regex_shows",
			Help:     `Regex used to classify torrents as shows.`,
			Advanced: true,
			Default:  `(?i)(S[0-9]{2}|SEASON|COMPLETE|[^457a-z\W\s]-[0-9]+)`,
		}, {
			Name:     "regex_movies",
			Help:     `Regex used to classify torrents as movies.`,
			Advanced: true,
			Default:  `(?i)(19|20)([0-9]{2} ?\.?)`,
		}, {
			Name:     "bypass_cache",
			Help:     "Ask TorBox for fresh torrent list data instead of cached data.",
			Advanced: true,
			Default:  false,
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
	APIKey      string               `config:"api_key"`
	FolderMode  string               `config:"folder_mode"`
	RegexShows  string               `config:"regex_shows"`
	RegexMovie  string               `config:"regex_movies"`
	BypassCache bool                 `config:"bypass_cache"`
	Enc         encoder.MultiEncoder `config:"encoding"`
}

// Fs represents a TorBox remote.
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	srv      *rest.Client
	dlSrv    *rest.Client
	pacer    *fs.Pacer

	mu        sync.Mutex
	cacheTime time.Time
	dirs      map[string]*entry
	files     map[string]*entry
	dlURLs    map[string]string
}

// Object describes a TorBox file.
type Object struct {
	fs          *Fs
	remote      string
	hasMetaData bool
	size        int64
	modTime     time.Time
	id          string
	mimeType    string
	source      sourceType
	transferID  int
	fileID      int
}

type entry struct {
	remote     string
	name       string
	isDir      bool
	id         string
	size       int64
	modTime    time.Time
	mimeType   string
	source     sourceType
	transferID int
	fileID     int
}

type sourceType string

const (
	sourceTorrent sourceType = "torrent"
	sourceUsenet  sourceType = "usenet"
)

// Name of the remote.
func (f *Fs) Name() string { return f.name }

// Root of the remote.
func (f *Fs) Root() string { return f.root }

// String converts this Fs to a string.
func (f *Fs) String() string { return fmt.Sprintf("torbox root '%s'", f.root) }

// Features returns the optional features of this Fs.
func (f *Fs) Features() *fs.Features { return f.features }

func parsePath(path string) string {
	return strings.Trim(path, "/")
}

var retryErrorCodes = []int{
	429,
	500,
	502,
	503,
	504,
}

func shouldRetry(ctx context.Context, resp *http.Response, err error) (bool, error) {
	if fserrors.ContextError(ctx, &err) {
		return false, err
	}
	return fserrors.ShouldRetry(err) || fserrors.ShouldRetryHTTP(resp, retryErrorCodes), err
}

func errorHandler(resp *http.Response) error {
	body, err := rest.ReadBody(resp)
	if err != nil {
		body = nil
	}
	e := api.Response{
		Success: false,
		ErrorID: resp.Status,
		Detail:  string(body),
	}
	if body != nil {
		_ = json.Unmarshal(body, &e)
	}
	if e.Detail == "" {
		e.Detail = resp.Status
	}
	return &e
}

// NewFs constructs an Fs from the path.
func NewFs(ctx context.Context, name, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}
	if opt.APIKey == "" {
		return nil, errors.New("torbox api_key is required")
	}

	root = parsePath(root)
	client := fshttp.NewClient(ctx)
	srv := rest.NewClient(client).SetRoot(rootURL)
	srv.SetHeader("Authorization", "Bearer "+opt.APIKey)
	srv.SetErrorHandler(errorHandler)

	f := &Fs{
		name:   name,
		root:   root,
		opt:    *opt,
		srv:    srv,
		dlSrv:  rest.NewClient(client),
		pacer:  fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(minSleep), pacer.MaxSleep(maxSleep), pacer.DecayConstant(decayConstant))),
		dirs:   make(map[string]*entry),
		files:  make(map[string]*entry),
		dlURLs: make(map[string]string),
	}
	f.features = (&fs.Features{
		CaseInsensitive:         true,
		CanHaveEmptyDirectories: true,
		ReadMimeType:            true,
	}).Fill(ctx, f)

	if root != "" {
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

type transferList struct {
	source sourceType
	items  []api.Transfer
}

func (f *Fs) listTransfers(ctx context.Context, source sourceType) ([]api.Transfer, error) {
	const limit = 1000
	var transfers []api.Transfer
	offset := 0
	endpoint := "/" + string(source) + "s/mylist"
	if source == sourceUsenet {
		endpoint = "/usenet/mylist"
	}
	for {
		params := url.Values{}
		params.Set("limit", strconv.Itoa(limit))
		params.Set("offset", strconv.Itoa(offset))
		if f.opt.BypassCache {
			params.Set("bypass_cache", "true")
		}
		opts := rest.Opts{
			Method:     "GET",
			Path:       endpoint,
			Parameters: params,
		}
		var resp *http.Response
		var result api.TransferListResponse
		var err error
		err = f.pacer.Call(func() (bool, error) {
			resp, err = f.srv.CallJSON(ctx, &opts, nil, &result)
			return shouldRetry(ctx, resp, err)
		})
		if err != nil {
			return nil, err
		}
		if !result.Success {
			return nil, &result.Response
		}
		transfers = append(transfers, result.Data...)
		if len(result.Data) < limit {
			break
		}
		offset += len(result.Data)
	}
	return transfers, nil
}

func (f *Fs) refresh(ctx context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if time.Since(f.cacheTime) < cacheDuration && f.dirs != nil && f.files != nil {
		return nil
	}
	torrents, err := f.listTransfers(ctx, sourceTorrent)
	if err != nil {
		return fmt.Errorf("couldn't list torrents: %w", err)
	}
	usenet, err := f.listTransfers(ctx, sourceUsenet)
	if err != nil {
		return fmt.Errorf("couldn't list usenet downloads: %w", err)
	}
	lists := []transferList{
		{source: sourceTorrent, items: torrents},
		{source: sourceUsenet, items: usenet},
	}

	dirs := make(map[string]*entry)
	files := make(map[string]*entry)
	dirs[""] = &entry{remote: "", name: "", isDir: true, id: "root", modTime: time.Now()}

	var addDir func(remote string, modTime time.Time)
	addDir = func(remote string, modTime time.Time) {
		remote = strings.Trim(remote, "/")
		if _, ok := dirs[remote]; ok {
			return
		}
		name := path.Base(remote)
		if remote == "" {
			name = ""
		}
		dirs[remote] = &entry{remote: remote, name: name, isDir: true, id: remote, modTime: modTime}
		if parent := path.Dir(remote); remote != "" && parent != "." {
			addDir(parent, modTime)
		}
	}

	for _, list := range lists {
		for _, transfer := range list.items {
			if !transferReady(transfer) {
				continue
			}
			modTime := parseTime(transfer.CachedAt, transfer.UpdatedAt, transfer.CreatedAt)
			category := f.category(transfer.Name)
			transferName := cleanSegment(f.opt.Enc.ToStandardName(transfer.Name))
			if transferName == "" {
				transferName = strconv.Itoa(transfer.ID)
			}

			baseDir := ""
			if f.opt.FolderMode == "folders" {
				addDir(category, modTime)
				baseDir = uniqueDir(dirs, path.Join(category, transferName), list.source, transfer.ID, modTime)
			}

			for _, file := range transfer.Files {
				fileRemote := cleanFilePath(file, transfer.Name, transfer.Hash)
				if fileRemote == "" {
					fileRemote = file.Name
				}
				fileRemote = encodePath(f.opt.Enc, fileRemote)
				if f.opt.FolderMode == "folders" {
					fileRemote = path.Join(baseDir, fileRemote)
				}
				fileRemote = uniqueFile(files, fileRemote, list.source, transfer.ID, file.ID)
				parent := path.Dir(fileRemote)
				if parent != "." {
					addDir(parent, modTime)
				}
				files[fileRemote] = &entry{
					remote:     fileRemote,
					name:       path.Base(fileRemote),
					id:         fmt.Sprintf("%s:%d:%d", list.source, transfer.ID, file.ID),
					size:       file.Size,
					modTime:    modTime,
					mimeType:   file.MimeType,
					source:     list.source,
					transferID: transfer.ID,
					fileID:     file.ID,
				}
			}
		}
	}

	f.dirs = dirs
	f.files = files
	f.cacheTime = time.Now()
	return nil
}

func transferReady(t api.Transfer) bool {
	if t.DownloadFinished {
		return true
	}
	switch strings.ToLower(t.DownloadState) {
	case "completed":
		return true
	default:
		return false
	}
}

func parseTime(values ...string) time.Time {
	for _, value := range values {
		if value == "" {
			continue
		}
		if t, err := time.Parse(time.RFC3339Nano, value); err == nil {
			return t
		}
	}
	return time.Now()
}

func (f *Fs) category(name string) string {
	showRe, showErr := regexp.Compile(f.opt.RegexShows)
	movieRe, movieErr := regexp.Compile(f.opt.RegexMovie)
	isShow := showErr == nil && showRe.MatchString(name)
	isMovie := movieErr == nil && movieRe.MatchString(name)
	if isShow {
		return "shows"
	}
	if isMovie {
		return "movies"
	}
	return "default"
}

func cleanFilePath(file api.File, torrentName, torrentHash string) string {
	value := file.AbsolutePath
	if value == "" {
		value = file.Name
	}
	value = strings.Trim(value, "/")
	torrentName = strings.Trim(torrentName, "/")
	torrentHash = strings.Trim(torrentHash, "/")
	value = stripPathAfterSegment(value, torrentName)
	value = stripPathPrefix(value, "completed", torrentHash)
	if strings.EqualFold(path.Clean(value), path.Clean(torrentName)) {
		value = file.Name
	}
	value = stripPathPrefix(value, torrentName)
	if value == "" {
		value = file.ShortName
	}
	return value
}

func stripPathAfterSegment(value, segment string) string {
	segment = strings.Trim(segment, "/")
	if segment == "" {
		return value
	}
	parts := strings.Split(value, "/")
	for i, part := range parts {
		if strings.EqualFold(part, segment) {
			if i+1 >= len(parts) {
				return ""
			}
			return path.Join(parts[i+1:]...)
		}
	}
	return value
}

func stripPathPrefix(value string, prefixes ...string) string {
	for _, prefix := range prefixes {
		prefix = strings.Trim(prefix, "/")
		if prefix == "" {
			continue
		}
		if strings.EqualFold(value, prefix) {
			return ""
		}
		withSlash := prefix + "/"
		if strings.HasPrefix(strings.ToLower(value), strings.ToLower(withSlash)) {
			value = value[len(withSlash):]
		}
	}
	return value
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

func uniqueDir(dirs map[string]*entry, remote string, source sourceType, id int, modTime time.Time) string {
	remote = strings.Trim(remote, "/")
	out := remote
	if _, ok := dirs[out]; ok {
		out = addSuffix(remote, fmt.Sprintf("%s-%d", source, id))
	}
	dirs[out] = &entry{remote: out, name: path.Base(out), isDir: true, id: fmt.Sprintf("%s:%d", source, id), modTime: modTime}
	return out
}

func uniqueFile(files map[string]*entry, remote string, source sourceType, transferID, fileID int) string {
	remote = strings.Trim(remote, "/")
	if _, ok := files[remote]; !ok {
		return remote
	}
	return addSuffix(remote, fmt.Sprintf("%s-%d-%d", source, transferID, fileID))
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
		return true
	}
	if cacheDuration <= 0 || time.Since(cacheTime) < cacheDuration {
		return false
	}
	if actualDir == "" {
		return true
	}
	if f.opt.FolderMode == "folders" {
		switch actualDir {
		case "shows", "movies", "default":
			return true
		}
	}
	return false
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

// List the objects and directories in dir into entries.
func (f *Fs) List(ctx context.Context, dir string) (entries fs.DirEntries, err error) {
	dir = strings.Trim(dir, "/")
	actualDir := f.actualPath(dir)
	if !f.shouldRefreshForDir(actualDir) && f.cachedDirExists(actualDir) {
		return f.listFromCache(actualDir)
	}
	err = f.refresh(ctx)
	if err != nil {
		if f.cachedDirExists(actualDir) {
			return f.listFromCache(actualDir)
		}
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
	err = f.refresh(ctx)
	if err != nil {
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

// Mkdir is unsupported on read-only TorBox remotes.
func (f *Fs) Mkdir(ctx context.Context, dir string) error { return errReadOnly }

// Rmdir is unsupported on read-only TorBox remotes.
func (f *Fs) Rmdir(ctx context.Context, dir string) error { return errReadOnly }

// Put is unsupported on read-only TorBox remotes.
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
	o.source = info.source
	o.transferID = info.transferID
	o.fileID = info.fileID
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
		remote:     obj.Remote(),
		size:       obj.Size(),
		modTime:    obj.ModTime(ctx),
		source:     obj.(*Object).source,
		transferID: obj.(*Object).transferID,
		fileID:     obj.(*Object).fileID,
		mimeType:   obj.(*Object).mimeType,
		id:         obj.(*Object).id,
	})
}

// ModTime returns the modification time of the object.
func (o *Object) ModTime(ctx context.Context) time.Time { return o.modTime }

// SetModTime is unsupported on read-only TorBox remotes.
func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error { return errReadOnly }

// Storable returns whether this object is storable.
func (o *Object) Storable() bool { return true }

func (o *Object) downloadKey() string {
	return fmt.Sprintf("%s:%d:%d", o.source, o.transferID, o.fileID)
}

func (f *Fs) cachedDownloadURL(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.dlURLs[key]
}

func (f *Fs) setDownloadURL(key, downloadURL string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dlURLs[key] = downloadURL
}

func (f *Fs) clearDownloadURL(key string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.dlURLs, key)
}

func (o *Object) downloadRequest() (string, url.Values, error) {
	params := url.Values{}
	params.Set("token", o.fs.opt.APIKey)
	switch o.source {
	case sourceTorrent:
		params.Set("torrent_id", strconv.Itoa(o.transferID))
	case sourceUsenet:
		params.Set("usenet_id", strconv.Itoa(o.transferID))
	default:
		return "", nil, fmt.Errorf("can't download - unknown source %q", o.source)
	}
	params.Set("file_id", strconv.Itoa(o.fileID))
	params.Set("redirect", "true")
	params.Set("append_name", "true")
	requestPath := "/torrents/requestdl"
	if o.source == sourceUsenet {
		requestPath = "/usenet/requestdl"
	}
	return requestPath, params, nil
}

func (o *Object) requestDownloadURL(ctx context.Context) (string, error) {
	requestPath, params, err := o.downloadRequest()
	if err != nil {
		return "", err
	}
	opts := rest.Opts{
		Method:       "GET",
		Path:         requestPath,
		Parameters:   params,
		NoRedirect:   true,
		IgnoreStatus: true,
	}
	var resp *http.Response
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.srv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return "", err
	}
	if resp == nil {
		return "", errors.New("requestdl returned no response")
	}
	if resp.StatusCode >= 300 && resp.StatusCode <= 399 {
		defer fs.CheckClose(resp.Body, &err)
		location := resp.Header.Get("Location")
		if location == "" {
			return "", fmt.Errorf("requestdl returned %s without Location header", resp.Status)
		}
		locationURL, err := url.Parse(location)
		if err != nil {
			return "", err
		}
		return resp.Request.URL.ResolveReference(locationURL).String(), nil
	}
	body, readErr := rest.ReadBody(resp)
	if readErr != nil {
		return "", readErr
	}
	var result struct {
		api.Response
		Data string `json:"data"`
	}
	if err = json.Unmarshal(body, &result); err == nil && result.Data != "" {
		return result.Data, nil
	}
	return "", fmt.Errorf("requestdl returned %s without download URL", resp.Status)
}

func (o *Object) openDownloadURL(ctx context.Context, downloadURL string, options []fs.OpenOption) (io.ReadCloser, error) {
	opts := rest.Opts{
		Method:  "GET",
		RootURL: downloadURL,
		Options: options,
	}
	var resp *http.Response
	var err error
	err = o.fs.pacer.Call(func() (bool, error) {
		resp, err = o.fs.dlSrv.Call(ctx, &opts)
		return shouldRetry(ctx, resp, err)
	})
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// Open an object for read.
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (io.ReadCloser, error) {
	err := o.readMetaData(ctx)
	if err != nil {
		return nil, err
	}
	if o.transferID == 0 {
		return nil, errors.New("can't download - missing transfer ID")
	}
	fs.FixRangeOption(options, o.size)
	key := o.downloadKey()
	downloadURL := o.fs.cachedDownloadURL(key)
	if downloadURL != "" {
		in, err := o.openDownloadURL(ctx, downloadURL, options)
		if err == nil {
			return in, nil
		}
		o.fs.clearDownloadURL(key)
		fs.Debugf(o, "Cached TorBox download URL failed, requesting a new one: %v", err)
	}
	downloadURL, err = o.requestDownloadURL(ctx)
	if err != nil {
		return nil, err
	}
	o.fs.setDownloadURL(key, downloadURL)
	in, err := o.openDownloadURL(ctx, downloadURL, options)
	if err != nil {
		o.fs.clearDownloadURL(key)
		return nil, err
	}
	return in, nil
}

// Update is unsupported on read-only TorBox remotes.
func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	return errReadOnly
}

// Remove is unsupported on read-only TorBox remotes.
func (o *Object) Remove(ctx context.Context) error { return errReadOnly }

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
