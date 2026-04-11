package progress

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
)

type LogHandler struct {
	renderer *Renderer
	level    slog.Level
	mu       sync.Mutex
	pending  []string
}

func (h *LogHandler) Handle(ctx context.Context, r slog.Record) error {
	var b strings.Builder
	b.WriteString(r.Time.Format("2006-01-02 15:04:05"))
	b.WriteByte(' ')
	b.WriteString(r.Level.String())
	b.WriteByte(' ')
	b.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		fmt.Fprintf(&b, " %s=%v", a.Key, a.Value)
		return true
	})
	h.mu.Lock()
	h.pending = append(h.pending, b.String())
	h.mu.Unlock()
	return nil
}

func (h *LogHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *LogHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *LogHandler) WithGroup(name string) slog.Handler {
	return h
}
