package server

import (
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/wzshiming/s3d/pkg/storage"
)

// matchETag reports whether the comma-separated list of entity-tags in
// headerValue contains a tag matching etag (the object's ETag without
// quotes), per RFC 7232 section 2.3.2. A headerValue of "*" matches any
// existing object. Weak entity-tags ("W/" prefix) only match when weak
// comparison is requested. Quotes around entity-tags are optional to stay
// lenient with clients that send unquoted ETags.
func matchETag(headerValue, etag string, weak bool) bool {
	for _, candidate := range strings.Split(headerValue, ",") {
		candidate = strings.TrimSpace(candidate)
		if candidate == "" {
			continue
		}
		if candidate == "*" {
			return true
		}
		if strings.HasPrefix(candidate, "W/") {
			if !weak {
				continue
			}
			candidate = strings.TrimPrefix(candidate, "W/")
		}
		if strings.Trim(candidate, `"`) == etag {
			return true
		}
	}
	return false
}

// checkPreconditions evaluates the If-Match, If-Unmodified-Since,
// If-None-Match and If-Modified-Since request headers against the object
// following RFC 7232 section 6, which is the order applied by S3. It writes
// the 412 or 304 response and returns true when the request must not be
// served.
func (s *S3Handler) checkPreconditions(w http.ResponseWriter, r *http.Request, info *storage.ObjectInfo) bool {
	// The Last-Modified header has second precision.
	modTime := info.ModTime.Truncate(time.Second)

	if im := r.Header.Get("If-Match"); im != "" {
		if !matchETag(im, info.ETag, false) {
			s.preconditionFailedResponse(w, r)
			return true
		}
	} else if ius := r.Header.Get("If-Unmodified-Since"); ius != "" {
		if t, err := http.ParseTime(ius); err == nil && modTime.After(t) {
			s.preconditionFailedResponse(w, r)
			return true
		}
	}

	if inm := r.Header.Get("If-None-Match"); inm != "" {
		if matchETag(inm, info.ETag, true) {
			if r.Method == http.MethodGet || r.Method == http.MethodHead {
				s.notModifiedResponse(w, r, info)
			} else {
				s.preconditionFailedResponse(w, r)
			}
			return true
		}
	} else if ims := r.Header.Get("If-Modified-Since"); ims != "" {
		if t, err := http.ParseTime(ims); err == nil && !modTime.After(t) &&
			(r.Method == http.MethodGet || r.Method == http.MethodHead) {
			s.notModifiedResponse(w, r, info)
			return true
		}
	}

	// The preconditions have been evaluated; remove the headers so that
	// http.ServeContent does not re-evaluate them with stricter ETag
	// quoting rules.
	r.Header.Del("If-Match")
	r.Header.Del("If-None-Match")
	r.Header.Del("If-Modified-Since")
	r.Header.Del("If-Unmodified-Since")

	return false
}

// preconditionFailedResponse writes a 412 Precondition Failed error response.
func (s *S3Handler) preconditionFailedResponse(w http.ResponseWriter, r *http.Request) {
	s.response(w, r, "PreconditionFailed", "At least one of the pre-conditions you specified did not hold", http.StatusPreconditionFailed)
}

// notModifiedResponse writes a 304 Not Modified response. The response has
// no body and no checksum headers so that clients do not attempt to validate
// a checksum against the empty body.
func (s *S3Handler) notModifiedResponse(w http.ResponseWriter, r *http.Request, info *storage.ObjectInfo) {
	s.setHeaders(w, r)
	w.Header().Set("ETag", fmt.Sprintf("%q", info.ETag))
	w.Header().Set("Last-Modified", info.ModTime.UTC().Format(http.TimeFormat))
	w.WriteHeader(http.StatusNotModified)
}

// checkRange validates the Range header against the object size following
// RFC 7233 as applied by S3. Unsatisfiable ranges are answered with a 416
// InvalidRange error, in which case it returns true. Malformed Range headers
// are dropped from the request so that the object is served whole, matching
// S3 behavior.
func (s *S3Handler) checkRange(w http.ResponseWriter, r *http.Request, info *storage.ObjectInfo) bool {
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		return false
	}

	// When If-Range is present and does not match the object, the Range
	// header is ignored and the whole object is served.
	if ir := r.Header.Get("If-Range"); ir != "" && !ifRangeMatches(ir, info) {
		return false
	}

	valid, satisfiable := parseRangeHeader(rangeHeader, info.Size)
	if !valid {
		// S3 ignores Range headers it cannot parse.
		r.Header.Del("Range")
		return false
	}
	if !satisfiable {
		w.Header().Set("Content-Range", fmt.Sprintf("bytes */%d", info.Size))
		s.response(w, r, "InvalidRange", "The requested range is not satisfiable", http.StatusRequestedRangeNotSatisfiable)
		return true
	}
	return false
}

// ifRangeMatches mirrors the If-Range evaluation done by http.ServeContent:
// the header value matches when it is the object's strong ETag or a time
// equal to the object's modification time.
func ifRangeMatches(ir string, info *storage.ObjectInfo) bool {
	if strings.HasPrefix(ir, "W/") {
		// Weak entity-tags never match If-Range.
		return false
	}
	if strings.HasPrefix(ir, `"`) {
		return strings.Trim(ir, `"`) == info.ETag
	}
	t, err := http.ParseTime(ir)
	if err != nil {
		return false
	}
	return t.Unix() == info.ModTime.Unix()
}

// parseRangeHeader parses a Range header value per RFC 7233. It reports
// whether the header is a syntactically valid bytes range specification and
// whether at least one of the requested ranges is satisfiable for an object
// of the given size.
func parseRangeHeader(rangeHeader string, size int64) (valid, satisfiable bool) {
	const prefix = "bytes="
	if !strings.HasPrefix(rangeHeader, prefix) {
		return false, false
	}
	seen := false
	for _, spec := range strings.Split(strings.TrimPrefix(rangeHeader, prefix), ",") {
		spec = strings.TrimSpace(spec)
		if spec == "" {
			continue
		}
		start, end, ok := strings.Cut(spec, "-")
		if !ok {
			return false, false
		}
		start, end = strings.TrimSpace(start), strings.TrimSpace(end)
		seen = true
		if start == "" {
			// A suffix-byte-range-spec ("-N") requests the last N
			// bytes and is only satisfiable when N is not zero and
			// the object is not empty.
			n, err := strconv.ParseInt(end, 10, 64)
			if err != nil || n < 0 {
				return false, false
			}
			if n > 0 && size > 0 {
				satisfiable = true
			}
			continue
		}
		first, err := strconv.ParseInt(start, 10, 64)
		if err != nil || first < 0 {
			return false, false
		}
		if end != "" {
			last, err := strconv.ParseInt(end, 10, 64)
			if err != nil || last < first {
				return false, false
			}
		}
		// An int-range is satisfiable when it starts before the end of
		// the object.
		if first < size {
			satisfiable = true
		}
	}
	if !seen {
		return false, false
	}
	return true, satisfiable
}
