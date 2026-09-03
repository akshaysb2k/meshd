package proxy

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"net/textproto"
	"strings"
)

// hopByHop are the headers that apply to a single transport connection and must
// not be forwarded. Failing to strip these is the classic proxy bug: pass
// Connection: close upstream and you tear down a pooled connection on every
// request.
var hopByHop = []string{
	"Connection",
	"Proxy-Connection",
	"Keep-Alive",
	"Proxy-Authenticate",
	"Proxy-Authorization",
	"Te",
	"Trailer",
	"Transfer-Encoding",
	"Upgrade",
}

// bufferedBody holds a request body that may need replaying across attempts.
type bufferedBody struct {
	buf []byte
	// replayable is false when the body exceeded the buffer limit, in which
	// case the request cannot be retried or hedged.
	replayable bool
	// rest carries the unread remainder for oversized bodies.
	rest io.ReadCloser
}

// bufferBody reads up to limit bytes so retries can replay the request. A body
// larger than the limit streams through untouched and disables retries; the
// alternative -- buffering unbounded uploads in proxy memory -- is how you turn
// a retry feature into an OOM.
func bufferBody(r *http.Request, limit int64) (*bufferedBody, error) {
	if r.Body == nil || r.Body == http.NoBody {
		return &bufferedBody{replayable: true}, nil
	}
	if r.ContentLength == 0 {
		return &bufferedBody{replayable: true}, nil
	}
	var buf bytes.Buffer
	n, err := io.Copy(&buf, io.LimitReader(r.Body, limit))
	if err != nil {
		return nil, err
	}
	if n < limit {
		return &bufferedBody{buf: buf.Bytes(), replayable: true}, nil
	}
	return &bufferedBody{buf: buf.Bytes(), replayable: false, rest: r.Body}, nil
}

// reader returns a fresh reader over the buffered body.
func (b *bufferedBody) reader() io.ReadCloser {
	if b == nil || (len(b.buf) == 0 && b.rest == nil) {
		return http.NoBody
	}
	head := bytes.NewReader(b.buf)
	if b.rest == nil {
		return io.NopCloser(head)
	}
	return struct {
		io.Reader
		io.Closer
	}{io.MultiReader(head, b.rest), b.rest}
}

func (b *bufferedBody) length() int64 {
	if b == nil {
		return 0
	}
	if !b.replayable {
		return -1
	}
	return int64(len(b.buf))
}

// outboundRequest builds the upstream request for one attempt.
func outboundRequest(r *http.Request, target string, path string, body *bufferedBody, reqID string) (*http.Request, error) {
	u := *r.URL
	u.Path = path
	out, err := http.NewRequestWithContext(r.Context(), r.Method, target+u.RequestURI(), body.reader())
	if err != nil {
		return nil, err
	}
	out.ContentLength = body.length()
	out.Header = cloneHeader(r.Header)
	stripHopByHop(out.Header, r.Header)

	out.Host = r.Host
	if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		prior := out.Header.Get("X-Forwarded-For")
		if prior != "" {
			ip = prior + ", " + ip
		}
		out.Header.Set("X-Forwarded-For", ip)
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	out.Header.Set("X-Forwarded-Proto", scheme)
	if out.Header.Get("X-Forwarded-Host") == "" {
		out.Header.Set("X-Forwarded-Host", r.Host)
	}
	out.Header.Set("X-Request-Id", reqID)
	return out, nil
}

func cloneHeader(h http.Header) http.Header {
	out := make(http.Header, len(h))
	for k, vs := range h {
		out[k] = append([]string(nil), vs...)
	}
	return out
}

// stripHopByHop removes connection-scoped headers, including any field the
// inbound Connection header names.
func stripHopByHop(dst, src http.Header) {
	for _, v := range src.Values("Connection") {
		for _, f := range strings.Split(v, ",") {
			if f = textproto.TrimString(f); f != "" {
				dst.Del(f)
			}
		}
	}
	for _, h := range hopByHop {
		dst.Del(h)
	}
}

// copyResponseHeaders moves upstream headers onto the client response.
func copyResponseHeaders(dst http.Header, src http.Header) {
	for k, vs := range src {
		for _, v := range vs {
			dst.Add(k, v)
		}
	}
	for _, h := range hopByHop {
		dst.Del(h)
	}
}

// newRequestID returns a 16-hex-character correlation id.
func newRequestID() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "0000000000000000"
	}
	return hex.EncodeToString(b[:])
}

// flushWriter flushes after each write so server-sent events and chunked
// streams are not held in the proxy's buffer.
type flushWriter struct {
	w  io.Writer
	rc *http.ResponseController
}

func (f flushWriter) Write(p []byte) (int, error) {
	n, err := f.w.Write(p)
	if f.rc != nil {
		_ = f.rc.Flush()
	}
	return n, err
}
