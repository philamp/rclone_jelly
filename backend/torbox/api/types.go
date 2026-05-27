// Package api contains definitions for using the TorBox API.
package api

import "fmt"

// Response is returned by TorBox API endpoints.
type Response struct {
	Success bool   `json:"success"`
	ErrorID string `json:"error"`
	Detail  string `json:"detail"`
}

// Error satisfies the error interface.
func (e *Response) Error() string {
	if e.ErrorID != "" {
		return fmt.Sprintf("%s: %s", e.ErrorID, e.Detail)
	}
	return e.Detail
}

// TransferListResponse is the response to transfer list endpoints.
type TransferListResponse struct {
	Response
	Data []Transfer `json:"data"`
}

// Transfer is a TorBox torrent or Usenet item.
type Transfer struct {
	ID               int    `json:"id"`
	Hash             string `json:"hash"`
	Name             string `json:"name"`
	Size             int64  `json:"size"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	DownloadState    string `json:"download_state"`
	ExpiresAt        string `json:"expires_at"`
	DownloadPresent  bool   `json:"download_present"`
	DownloadFinished bool   `json:"download_finished"`
	Cached           bool   `json:"cached"`
	CachedAt         string `json:"cached_at"`
	Files            []File `json:"files"`
}

// File is a file inside a TorBox torrent item.
type File struct {
	ID                int    `json:"id"`
	MD5               string `json:"md5"`
	Hash              string `json:"hash"`
	Name              string `json:"name"`
	Size              int64  `json:"size"`
	Zipped            bool   `json:"zipped"`
	MimeType          string `json:"mimetype"`
	ShortName         string `json:"short_name"`
	AbsolutePath      string `json:"absolute_path"`
	OpenSubtitlesHash string `json:"opensubtitles_hash"`
}
