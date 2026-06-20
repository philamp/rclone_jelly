// Package api contains definitions for using the Premiumize API.
package api

import "fmt"

// Response is embedded in Premiumize API responses.
type Response struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
	Code    string `json:"code,omitempty"`
}

// Error satisfies the error interface.
func (e *Response) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("%s: %s", e.Code, e.Message)
	}
	if e.Message != "" {
		return fmt.Sprintf("%s: %s", e.Status, e.Message)
	}
	return e.Status
}

// AsErr checks the status and returns an error if needed.
func (e *Response) AsErr() error {
	if e.Status == "success" {
		return nil
	}
	return e
}

// TransferListResponse is returned by /transfer/list.
type TransferListResponse struct {
	Response
	Transfers []Transfer `json:"transfers"`
}

// AccountInfoResponse is returned by /account/info.
type AccountInfoResponse struct {
	Response
	PremiumUntil int64 `json:"premium_until"`
}

// Transfer is a Premiumize transfer item.
type Transfer struct {
	ID       string         `json:"id"`
	Name     string         `json:"name"`
	Status   string         `json:"status"`
	Progress float64        `json:"progress"`
	Message  string         `json:"message"`
	FolderID NullableString `json:"folder_id"`
	FileID   NullableString `json:"file_id"`
	Src      string         `json:"src"`
}

// NullableString accepts string, null, or numeric IDs.
type NullableString string

// String returns the underlying value.
func (s NullableString) String() string {
	return string(s)
}

// UnmarshalJSON decodes a nullable or numeric string.
func (s *NullableString) UnmarshalJSON(b []byte) error {
	switch string(b) {
	case "null", `""`:
		*s = ""
		return nil
	}
	if len(b) >= 2 && b[0] == '"' && b[len(b)-1] == '"' {
		*s = NullableString(b[1 : len(b)-1])
		return nil
	}
	*s = NullableString(b)
	return nil
}

const (
	// ItemTypeFolder is a folder item.
	ItemTypeFolder = "folder"
	// ItemTypeFile is a file item.
	ItemTypeFile = "file"
)

// Item is a file or folder in Premiumize cloud storage.
type Item struct {
	Response
	ID        string `json:"id"`
	Name      string `json:"name"`
	Type      string `json:"type"`
	CreatedAt int64  `json:"created_at"`
	Size      int64  `json:"size,omitempty"`
	MimeType  string `json:"mime_type,omitempty"`
	Link      string `json:"link,omitempty"`
	FolderID  string `json:"folder_id,omitempty"`
}

// FolderListResponse is returned by /folder/list.
type FolderListResponse struct {
	Response
	Name     string `json:"name,omitempty"`
	ParentID string `json:"parent_id,omitempty"`
	FolderID string `json:"folder_id,omitempty"`
	Content  []Item `json:"content"`
}

// CacheCheckResponse is returned by /cache/check.
type CacheCheckResponse struct {
	Response
	ResponseHits []bool `json:"response"`
}

// TransferSourceResponse is returned by /transfer/source.
type TransferSourceResponse struct {
	Response
	Type string `json:"type"`
	URL  string `json:"url"`
}

// DirectDLResponse is returned by /transfer/directdl.
type DirectDLResponse struct {
	Response
	Content []DirectDLContent `json:"content"`
}

// DirectDLContent is a downloadable file returned by /transfer/directdl.
type DirectDLContent struct {
	Path string `json:"path"`
	Size int64  `json:"size"`
	Link string `json:"link"`
}

// TransferCreateResponse is returned by /transfer/create.
type TransferCreateResponse struct {
	Response
	ID string `json:"id,omitempty"`
}
