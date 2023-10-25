package vfs

import (
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/log"
	"github.com/rclone/rclone/vfs/vfscache"



	// delta from read.go
	"context"
	"errors"
	"time"

	"github.com/rclone/rclone/fs/accounting"
	"github.com/rclone/rclone/fs/chunkedreader"
	"github.com/rclone/rclone/fs/hash"

	// from vfscache/item.go
	// "github.com/rclone/rclone/lib/ranges" DEPRECATED
)

// RWFileHandle is a handle that can be open for read and write.
//
// It will be open to a temporary file which, when closed, will be
// transferred to the remote.
type RWFileHandle struct {
	baseHandle
	
	// read only variables
	file  *File
	d     *Dir
	flags int            // open flags
	item  *vfscache.Item // cached file item

	// read write variables protected by mutex
	mu          sync.Mutex
	offset      int64 // file pointer offset
	closed      bool  // set if handle has been closed
	opened      bool
	writeCalled bool // if any Write() methods have been called


	done        func(ctx context.Context, err error)
	// mu          sync.Mutex
	cond        *sync.Cond // cond lock for out of sequence reads
	// closed      bool       // set if handle has been closed
	r           *accounting.Account
	readCalled  bool  // set if read has been called
	size        int64 // size of the object (0 for unknown length)
	// offset      int64 // offset of read of o
	roffset     int64 // offset of Read() calls
	noSeek      bool
	sizeUnknown bool // set if size of source is not known
	// file        *File
	hash        *hash.MultiHasher
	// opened      bool
	remote      string
	// jellygrail custom
	currentDirectReadMode bool
	openedSource bool
	openedCache bool
}

// Check interfaces
var (
	_ io.Reader   = (*RWFileHandle)(nil)
	_ io.ReaderAt = (*RWFileHandle)(nil)
	_ io.Seeker   = (*RWFileHandle)(nil)
	_ io.Closer   = (*RWFileHandle)(nil)
)


func newRWFileHandle(d *Dir, f *File, flags int) (fh *RWFileHandle, err error) {
	var mhash *hash.MultiHasher
	o := f.getObject()
	defer log.Trace(f.Path(), "")("err=%v", &err)
	// get an item to represent this from the cache
	item := d.vfs.cache.Item(f.Path())

	if !f.VFS().Opt.NoChecksum {
		hashes := hash.NewHashSet(o.Fs().Hashes().GetOne()) // just pick one hash
		mhash, err = hash.NewMultiHasherTypes(hashes)
		if err != nil {
			fs.Errorf(o.Fs(), "newReadFileHandle hash error: %v", err)
		}
	}

	exists := f.exists() || (item.Exists() && !item.WrittenBack())

	// if O_CREATE and O_EXCL are set and if path already exists, then return EEXIST
	if flags&(os.O_CREATE|os.O_EXCL) == os.O_CREATE|os.O_EXCL && exists {
		return nil, EEXIST
	}

	fh = &RWFileHandle{
		file:  f,
		d:     d,
		flags: flags,
		item:  item,

		// from read.go
		remote:      o.Remote(),
		noSeek:      f.VFS().Opt.NoSeek,
		// file:        f,
		hash:        mhash,
		size:        nonNegative(o.Size()),
		sizeUnknown: o.Size() < 0,
	}

	// truncate immediately if O_TRUNC is set or O_CREATE is set and file doesn't exist
	if !fh.readOnly() && (fh.flags&os.O_TRUNC != 0 || (fh.flags&os.O_CREATE != 0 && !exists)) {
		err = fh.Truncate(0)
		if err != nil {
			return nil, fmt.Errorf("cache open with O_TRUNC: failed to truncate: %w", err)
		}
		// we definitely need to write back the item even if we don't write to it
		item.Dirty()
	}

	if !fh.readOnly() {
		fh.file.addWriter(fh)
	}

	// from read.go :
	fh.cond = sync.NewCond(&fh.mu) 
	return fh, nil
}

// readOnly returns whether flags say fh is read only
func (fh *RWFileHandle) readOnly() bool {
	return (fh.flags & accessModeMask) == os.O_RDONLY
}

// writeOnly returns whether flags say fh is write only
func (fh *RWFileHandle) writeOnly() bool {
	return (fh.flags & accessModeMask) == os.O_WRONLY
}

// openPending opens the file if there is a pending open
//
// call with the lock held
func (fh *RWFileHandle) openPending() (err error) {
	if fh.openedCache {
		return nil
	}
	defer log.Trace(fh.logPrefix(), "")("err=%v", &err)

	fh.file.muRW.Lock()
	defer fh.file.muRW.Unlock()

	o := fh.file.getObject()
	err = fh.item.Open(o)
	if err != nil {
		return fmt.Errorf("open RW handle failed to open cache file: %w", err)
	}

	size := fh._size() // update size in file and read size
	if fh.flags&os.O_APPEND != 0 {
		fh.offset = size
		fs.Debugf(fh.logPrefix(), "open at offset %d", fh.offset)
	} else {
		fh.offset = 0
	}
	fh.opened = true
	fh.openedCache = true // jellygrail custom
	fh.d.addObject(fh.file) // make sure the directory has this object in it now
	return nil
}

// from read.go, append with "Source"
// openPending opens the file if there is a pending open
// call with the lock held
func (fh *RWFileHandle) openPendingSource() (err error) {
	if fh.openedSource {
		return nil
	}
	o := fh.file.getObject()
	r, err := chunkedreader.New(context.TODO(), o, int64(fh.file.VFS().Opt.ChunkSize), int64(fh.file.VFS().Opt.ChunkSizeLimit)).Open()
	if err != nil {
		return err
	}
	tr := accounting.GlobalStats().NewTransfer(o)
	fh.done = tr.Done
	fh.r = tr.Account(context.TODO(), r).WithBuffer() // account the transfer
	fh.opened = true
	fh.openedSource = true // jellygrail custom

	return nil
}

// String converts it to printable
// we keep this version, no read.go import here
func (fh *RWFileHandle) String() string {
	if fh == nil {
		return "<nil *RWFileHandle>"
	}
	if fh.file == nil {
		return "<nil *RWFileHandle.file>"
	}
	return fh.file.String() + " (rw)"
}

// Node returns the Node associated with this - satisfies Noder interface
// we keep this version, no read.go import here
func (fh *RWFileHandle) Node() Node {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.file
}

// updateSize updates the size of the file if necessary
//
// Must be called with fh.mu held
func (fh *RWFileHandle) updateSize() {
	// If read only or not opened then ignore
	if fh.readOnly() || !fh.opened {
		return
	}
	size := fh._size()
	fh.file.setSize(size)
}

// close the file handle returning EBADF if it has been
// closed already.
//
// Must be called with fh.mu held
//
// Note that we leave the file around in the cache on error conditions
// to give the user a chance to recover it.
func (fh *RWFileHandle) close() (err error) {
	defer log.Trace(fh.logPrefix(), "")("err=%v", &err)
	fh.file.muRW.Lock()
	defer fh.file.muRW.Unlock()

	if fh.closed {
		return ECLOSED
	}

	fh.closed = true
	fh.updateSize()
	if fh.opened {
		err = fh.item.Close(fh.file.setObject)
		fh.opened = false
		fh.openedCache = false
	} else {
		// apply any pending mod times if any
		_ = fh.file.applyPendingModTime()
	}

	if !fh.readOnly() {
		fh.file.delWriter(fh)
	}

	return err
}

func (fh *RWFileHandle) closeSource() error {
	if fh.closed {
		return ECLOSED
	}
	fh.closed = true
	fh.openedSource = false
	

	if fh.opened {
		var err error
		// in dyn mode, deal with fh.openedCache as well
		if fh.openedCache {
			err = fh.item.Close(fh.file.setObject)
			fh.opened = false
			fh.openedCache = false
			if !fh.readOnly() {
				fh.file.delWriter(fh)
			}
		}
		// TODO-jellygrail: err overwrritten below, tofix
			

		defer func() {
			fh.done(context.TODO(), err)
		}()
		// Close first so that we have hashes
		err = fh.r.Close()
		if err != nil {
			return err
		}
		// Now check the hash
		err = fh.checkHash()
		if err != nil {
			return err
		}

		
		
	}
	return nil
}

// Close closes the file
func (fh *RWFileHandle) Close() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()

	if(!fh.currentDirectReadMode){
		return fh.close()
	}else{
		return fh.closeSource()
	}

}

// Flush is called each time the file or directory is closed.
// Because there can be multiple file descriptors referring to a
// single opened file, Flush can be called multiple times.
func (fh *RWFileHandle) Flush() error {

	if(!fh.currentDirectReadMode){

		fh.mu.Lock()
		fs.Debugf(fh.logPrefix(), "RWFileHandle.Flush")
		fh.updateSize()
		fh.mu.Unlock()
		return nil
		
	}else{
		if fh.openedCache {
			fh.updateSize()
		}
		
		fh.mu.Lock()
		defer fh.mu.Unlock()
		if !fh.opened {
			return nil
		}
		// fs.Debugf(fh.remote, "ReadFileHandle.Flush")
	
		if err := fh.checkHash(); err != nil {
			fs.Errorf(fh.remote, "ReadFileHandle.Flush error: %v", err)
			return err
		}
	
		// fs.Debugf(fh.remote, "ReadFileHandle.Flush OK")
		return nil

	}
		
}

// Release is called when we are finished with the file handle
//
// It isn't called directly from userspace so the error is ignored by
// the kernel
func (fh *RWFileHandle) Release() error {
	
	fh.mu.Lock()
	defer fh.mu.Unlock()


	if(!fh.currentDirectReadMode){
		
		fs.Debugf(fh.logPrefix(), "RWFileHandle.Release")
		if fh.closed {
			// Don't return an error if called twice
			return nil
		}
		err := fh.close()
		if err != nil {
			fs.Errorf(fh.logPrefix(), "RWFileHandle.Release error: %v", err)
		}
		return err

	}else{
		
		if !fh.opened {
			return nil
		}
		if fh.closed {
			fs.Debugf(fh.remote, "ReadFileHandle.Release nothing to do")
			return nil
		}
		fs.Debugf(fh.remote, "ReadFileHandle.Release closing")
		err := fh.close()
		if err != nil {
			fs.Errorf(fh.remote, "ReadFileHandle.Release error: %v", err)
		} else {
			// fs.Debugf(fh.remote, "ReadFileHandle.Release OK")
		}
		return err

	}
		
}

// _size returns the size of the underlying file and also sets it in
// the owning file
//
// call with the lock held
func (fh *RWFileHandle) _size() int64 {
	size, err := fh.item.GetSize()
	if err != nil {
		o := fh.file.getObject()
		if o != nil {
			size = o.Size()
		} else {
			fs.Errorf(fh.logPrefix(), "Couldn't read size of file")
			size = 0
		}
	}
	fh.file.setSize(size)
	return size
}

// Size returns the size of the underlying file
func (fh *RWFileHandle) Size() int64 {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh._size()
}

// Stat returns info about the file
func (fh *RWFileHandle) Stat() (os.FileInfo, error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	return fh.file, nil
}

// _readAt bytes from the file at off
//
// if release is set then it releases the mutex just before doing the IO
//
// call with lock held
func (fh *RWFileHandle) _readAt(b []byte, off int64, release bool, DirectReadModeROCache bool) (n int, err error) {
	
	defer log.Trace(fh.logPrefix(), "size=%d, off=%d", len(b), off)("n=%d, err=%v", &n, &err)
	if fh.closed {
		return n, ECLOSED
	}
	if fh.writeOnly() {
		return n, EBADF
	}
	if off >= fh._size() {
		return n, io.EOF
	}
	if err = fh.openPending(); err != nil {
		return n, err
	}
	if release {
		// Do the writing with fh.mu unlocked
		fh.mu.Unlock()
	}
	// if !DirectReadModeROCache {
		// n, err = fh.item.ReadAt(b, off)
		// fs.Debugf("### read_write.go _readAt CALLED ### (RW-CACHE) (atoffset=%s)", "")
	// } else {
		// n, err = fh.item.fd.ReadAt(b, off)
		// n, err = fh.item.ReadAt(b, off, DirectReadModeROCache)
		// fs.Debugf("### read_write.go _readAt CALLED ### (RO-CACHE) (atoffset=%s)", "")
	// }

	n, err = fh.item.ReadAt(b, off, DirectReadModeROCache)
	
	
	if release {
		fh.mu.Lock()
	}
	return n, err
}

func (fh *RWFileHandle) checkHash() error {
	if fh.hash == nil || !fh.readCalled || fh.offset < fh.size {
		return nil
	}

	o := fh.file.getObject()
	for hashType, dstSum := range fh.hash.Sums() {
		srcSum, err := o.Hash(context.TODO(), hashType)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				// if it was file not found then at
				// this point we don't care any more
				continue
			}
			return err
		}
		if !hash.Equals(dstSum, srcSum) {
			return fmt.Errorf("corrupted on transfer: %v hash differ %q vs %q", hashType, dstSum, srcSum)
		}
	}

	return nil
}

func waitSequentialSource(what string, remote string, cond *sync.Cond, maxWait time.Duration, poff *int64, off int64) {
	var (
		timeout = time.NewTimer(maxWait)
		done    = make(chan struct{})
		abort   = false
	)
	go func() {
		select {
		case <-timeout.C:
			// take the lock to make sure that cond.Wait() is called before
			// cond.Broadcast. NB cond.L == mu
			cond.L.Lock()
			// set abort flag and give all the waiting goroutines a kick on timeout
			abort = true
			fs.Debugf(remote, "aborting in-sequence %s wait, off=%d", what, off)
			cond.Broadcast()
			cond.L.Unlock()
		case <-done:
		}
	}()
	for *poff != off && !abort {
		fs.Debugf(remote, "waiting for in-sequence %s to %d for %v", what, off, maxWait)
		cond.Wait()
	}
	// tidy up end timer
	close(done)
	timeout.Stop()
	if *poff != off {
		fs.Debugf(remote, "failed to wait for in-sequence %s to %d", what, off)
	}
}

func (fh *RWFileHandle) seek(offset int64, reopen bool) (err error) {
	if fh.noSeek {
		return ESPIPE
	}
	fh.hash = nil
	if !reopen {
		ar := fh.r.GetAsyncReader()
		// try to fulfill the seek with buffer discard
		if ar != nil && ar.SkipBytes(int(offset-fh.offset)) {
			fh.offset = offset
			return nil
		}
	}
	fh.r.StopBuffering() // stop the background reading first
	oldReader := fh.r.GetReader()
	r, ok := oldReader.(*chunkedreader.ChunkedReader)
	if !ok {
		fs.Logf(fh.remote, "ReadFileHandle.Read expected reader to be a ChunkedReader, got %T", oldReader)
		reopen = true
	}
	if !reopen {
		fs.Debugf(fh.remote, "ReadFileHandle.seek from %d to %d (fs.RangeSeeker)", fh.offset, offset)
		_, err = r.RangeSeek(context.TODO(), offset, io.SeekStart, -1)
		if err != nil {
			fs.Debugf(fh.remote, "ReadFileHandle.Read fs.RangeSeeker failed: %v", err)
			return err
		}
	} else {
		fs.Debugf(fh.remote, "ReadFileHandle.seek from %d to %d", fh.offset, offset)
		// close old one
		err = oldReader.Close()
		if err != nil {
			fs.Debugf(fh.remote, "ReadFileHandle.Read seek close old failed: %v", err)
		}
		// re-open with a seek
		o := fh.file.getObject()
		r = chunkedreader.New(context.TODO(), o, int64(fh.file.VFS().Opt.ChunkSize), int64(fh.file.VFS().Opt.ChunkSizeLimit))
		_, err := r.Seek(offset, 0)
		if err != nil {
			fs.Debugf(fh.remote, "ReadFileHandle.Read seek failed: %v", err)
			return err
		}
		r, err = r.Open()
		if err != nil {
			fs.Debugf(fh.remote, "ReadFileHandle.Read seek failed: %v", err)
			return err
		}
	}
	fh.r.UpdateReader(context.TODO(), r)
	fh.offset = offset
	return nil
}

// added from read.go and renamed with +Source
func (fh *RWFileHandle) readAtSource(p []byte, off int64) (n int, err error) {
	fs.Debugf("### read_write.go readAtSource CALLED / DYN-MODE DIRECT confirmed ### %s", "")
	// defer log.Trace(fh.remote, "p[%d], off=%d", len(p), off)("n=%d, err=%v", &n, &err)
	err = fh.openPendingSource() // FIXME pending open could be more efficient in the presence of seek (and retries)
	if err != nil {
		return 0, err
	}
	// fs.Debugf(fh.remote, "ReadFileHandle.Read size %d offset %d", reqSize, off)
	if fh.closed {
		fs.Errorf(fh.remote, "ReadFileHandle.Read error: %v", EBADF)
		return 0, ECLOSED
	}
	maxBuf := 1024 * 1024
	if len(p) < maxBuf {
		maxBuf = len(p)
	}
	if gap := off - fh.offset; gap > 0 && gap < int64(8*maxBuf) {
		waitSequentialSource("read", fh.remote, fh.cond, fh.file.VFS().Opt.ReadWait, &fh.offset, off)
	}
	doSeek := off != fh.offset
	if doSeek && fh.noSeek {
		return 0, ESPIPE
	}
	var newOffset int64
	retries := 0
	reqSize := len(p)
	doReopen := false
	lowLevelRetries := fs.GetConfig(context.TODO()).LowLevelRetries
	for {
		if doSeek {
			// Are we attempting to seek beyond the end of the
			// file - if so just return EOF leaving the underlying
			// file in an unchanged state.
			if off >= fh.size {
				fs.Debugf(fh.remote, "ReadFileHandle.Read attempt to read beyond end of file: %d > %d", off, fh.size)
				return 0, io.EOF
			}
			// Otherwise do the seek
			err = fh.seek(off, doReopen)
		} else {
			err = nil
		}
		if err == nil {
			if reqSize > 0 {
				fh.readCalled = true
			}
			n, err = io.ReadFull(fh.r, p)
			newOffset = fh.offset + int64(n)
			// if err == nil && rand.Intn(10) == 0 {
			// 	err = errors.New("random error")
			// }
			if err == nil {
				break
			} else if (err == io.ErrUnexpectedEOF || err == io.EOF) && (newOffset == fh.size || fh.sizeUnknown) {
				if fh.sizeUnknown {
					// size is now known since we have read to the end
					fh.sizeUnknown = false
					fh.size = newOffset
				}
				// Have read to end of file - reset error
				err = nil
				break
			}
		}
		if retries >= lowLevelRetries {
			break
		}
		retries++
		fs.Errorf(fh.remote, "ReadFileHandle.Read error: low level retry %d/%d: %v", retries, lowLevelRetries, err)
		doSeek = true
		doReopen = true
	}
	if err != nil {
		fs.Errorf(fh.remote, "ReadFileHandle.Read error: %v", err)
	} else {
		fh.offset = newOffset
		// fs.Debugf(fh.remote, "ReadFileHandle.Read OK")

		if fh.hash != nil {
			_, err = fh.hash.Write(p[:n])
			if err != nil {
				fs.Errorf(fh.remote, "ReadFileHandle.Read HashError: %v", err)
				return 0, err
			}
		}

		// If we have no error and we didn't fill the buffer, must be EOF
		if n != len(p) {
			err = io.EOF
		}
	}
	fh.cond.Broadcast() // wake everyone up waiting for an in-sequence read
	return n, err
}


// ReadAt bytes from the file at off
// merged with ReadAt from read.go
func (fh *RWFileHandle) ReadAt(b []byte, off int64) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	fs.Debugf("### read_write.go ReadAt CALLED / BEFORE-SWITCH ### ", "")
	if(!fh.item.AllowDirectReadUpdate()){
		fs.Debugf("### read_write.go ReadAt CALLED / FULL-MODE RW-CACHE ### ", "")
		fh.currentDirectReadMode = false
		return fh._readAt(b, off, true, false)
	}else{
		
		fh.currentDirectReadMode = true

		
		// jellygrail custom ----- switch between fd.read and readAtSource
		offset := off
		size := int64(len(b))
		itemSize := fh._size()
		if offset+size > itemSize {
			size = itemSize - offset
		}
		// r := ranges.Range{Pos: offset, Size: size} DEPRECATED
		
		// present := fh.item.info.Rs.Present(r) DEPRECATED

		present := fh.item.GetInfoRsPresent(offset, size)
		
		if present {
			// switch to a custom _readAt without cache write 
			fs.Debugf("### read_write.go ReadAt CALLED / DYN-MODE RO-CACHE ### %s", "")
			// fh.item.info.ATime = time.Now()
			// Do the reading with Item.mu unlocked and cache protected by preAccess -> not needed as we never delete the "partial" "cache" in this forked version
			// return fh.item.fd.ReadAt(b, off) for going directly (deprecated)
			// 4th arg true sets the _readAt to RO
			return fh._readAt(b, off, true, true)
		}
		fs.Debugf("### read_write.go ReadAt CALLED / DYN-MODE DIRECT ### %s", "")
		// ---- jellygrail custom
		
		return fh.readAtSource(b, off)
	}
}

// Read bytes from the file
func (fh *RWFileHandle) Read(b []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	fs.Debugf("### read_write.go Read CALLED NOT SUPPOSED TO BE ! ### %s", "")
	if(!fh.item.AllowDirectReadUpdate()){
		fh.currentDirectReadMode = false
		n, err = fh._readAt(b, fh.offset, false, false)
		fh.offset += int64(n)
		return n, err

	}else{
		fh.currentDirectReadMode = true
		if fh.roffset >= fh.size && !fh.sizeUnknown {
			return 0, io.EOF
		}
		n, err = fh.readAtSource(b, fh.roffset)
		fh.roffset += int64(n)
		return n, err

	}

}

// Seek to new file position
// merged with Seek from read.go
func (fh *RWFileHandle) Seek(offset int64, whence int) (ret int64, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	fs.Debugf("### read_write.go Seek CALLED NOT SUPPOSED TO BE ! ### %s", "")

	if(!fh.currentDirectReadMode){
	
		if fh.closed {
			return 0, ECLOSED
		}
		if !fh.opened && offset == 0 && whence != 2 {
			return 0, nil
		}
		if err = fh.openPending(); err != nil {
			return ret, err
		}
		switch whence {
		case io.SeekStart:
			fh.offset = 0
		case io.SeekEnd:
			fh.offset = fh._size()
		}
		fh.offset += offset
		// we don't check the offset - the next Read will
		return fh.offset, nil

	}else{
		// from read.go
		if fh.noSeek {
			return 0, ESPIPE
		}
		size := fh.size
		switch whence {
		case io.SeekStart:
			fh.roffset = 0
		case io.SeekEnd:
			fh.roffset = size
		}
		fh.roffset += offset
		// we don't check the offset - the next Read will
		return fh.roffset, nil
	}
	
}

// _writeAt bytes to the file at off
//
// if release is set then it releases the mutex just before doing the IO
//
// call with lock held
func (fh *RWFileHandle) _writeAt(b []byte, off int64, release bool) (n int, err error) {
	defer log.Trace(fh.logPrefix(), "size=%d, off=%d", len(b), off)("n=%d, err=%v", &n, &err)
	if fh.closed {
		return n, ECLOSED
	}
	if fh.readOnly() {
		return n, EBADF
	}
	if err = fh.openPending(); err != nil {
		return n, err
	}
	if fh.flags&os.O_APPEND != 0 {
		// From open(2): Before each write(2), the file offset is
		// positioned at the end of the file, as if with lseek(2).
		size := fh._size()
		fh.offset = size
		off = fh.offset
	}
	fh.writeCalled = true
	if release {
		// Do the writing with fh.mu unlocked
		fh.mu.Unlock()
	}
	n, err = fh.item.WriteAt(b, off)
	if release {
		fh.mu.Lock()
	}
	if err != nil {
		return n, err
	}

	_ = fh._size()
	return n, err
}

// WriteAt bytes to the file at off
func (fh *RWFileHandle) WriteAt(b []byte, off int64) (n int, err error) {
	fh.mu.Lock()
	n, err = fh._writeAt(b, off, true)
	if fh.flags&os.O_APPEND != 0 {
		fh.offset += int64(n)
	}
	fh.mu.Unlock()
	return n, err
}

// Write bytes to the file
func (fh *RWFileHandle) Write(b []byte) (n int, err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	n, err = fh._writeAt(b, fh.offset, false)
	fh.offset += int64(n)
	return n, err
}

// WriteString a string to the file
func (fh *RWFileHandle) WriteString(s string) (n int, err error) {
	return fh.Write([]byte(s))
}

// Truncate file to given size
//
// Call with mutex held
func (fh *RWFileHandle) _truncate(size int64) (err error) {
	if size == fh._size() {
		return nil
	}
	fh.file.setSize(size)
	return fh.item.Truncate(size)
}

// Truncate file to given size
func (fh *RWFileHandle) Truncate(size int64) (err error) {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return ECLOSED
	}
	if err = fh.openPending(); err != nil {
		return err
	}
	return fh._truncate(size)
}

// Sync commits the current contents of the file to stable storage. Typically,
// this means flushing the file system's in-memory copy of recently written
// data to disk.
func (fh *RWFileHandle) Sync() error {
	fh.mu.Lock()
	defer fh.mu.Unlock()
	if fh.closed {
		return ECLOSED
	}
	if !fh.openedCache {
		return nil
	}
	if fh.readOnly() {
		return nil
	}
	return fh.item.Sync()
}

func (fh *RWFileHandle) logPrefix() string {
	return fmt.Sprintf("%s(%p)", fh.file.Path(), fh)
}

// Chdir changes the current working directory to the file, which must
// be a directory.
func (fh *RWFileHandle) Chdir() error {
	return ENOSYS
}

// Chmod changes the mode of the file to mode.
func (fh *RWFileHandle) Chmod(mode os.FileMode) error {
	return ENOSYS
}

// Chown changes the numeric uid and gid of the named file.
func (fh *RWFileHandle) Chown(uid, gid int) error {
	return ENOSYS
}

// Fd returns the integer Unix file descriptor referencing the open file.
func (fh *RWFileHandle) Fd() uintptr {
	return 0xdeadbeef // FIXME
}

// Name returns the name of the file from the underlying Object.
func (fh *RWFileHandle) Name() string {
	return fh.file.String()
}

// Readdir reads the contents of the directory associated with file.
func (fh *RWFileHandle) Readdir(n int) ([]os.FileInfo, error) {
	return nil, ENOSYS
}

// Readdirnames reads the contents of the directory associated with file.
func (fh *RWFileHandle) Readdirnames(n int) (names []string, err error) {
	return nil, ENOSYS
}
