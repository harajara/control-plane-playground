package httpapi

import (
	"net/http"
)

// Normalize ServeMux's own 404/405 errors as well as application errors.
type problemWriter struct {
	http.ResponseWriter
	request    *http.Request
	suppressed bool
	wrote      bool
}

func (p *problemWriter) WriteHeader(status int) {
	if p.wrote {
		return
	}
	p.wrote = true
	if status >= 400 && p.Header().Get("Content-Type") != "application/problem+json" {
		p.suppressed = true
		code := "request_failed"
		if status == 404 {
			code = "route_not_found"
		}
		if status == 405 {
			code = "method_not_allowed"
		}
		p.Header().Del("Content-Length")
		fail(p.ResponseWriter, p.request, status, code, http.StatusText(status))
		return
	}
	p.ResponseWriter.WriteHeader(status)
}
func (p *problemWriter) Write(b []byte) (int, error) {
	if !p.wrote {
		p.WriteHeader(200)
	}
	if p.suppressed {
		return len(b), nil
	}
	return p.ResponseWriter.Write(b)
}
func (p *problemWriter) Flush() {
	if !p.wrote {
		p.WriteHeader(200)
	}
	if f, ok := p.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
func (p *problemWriter) Unwrap() http.ResponseWriter { return p.ResponseWriter }
