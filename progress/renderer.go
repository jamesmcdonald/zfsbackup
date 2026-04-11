package progress

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"golang.org/x/term"
)

type Renderer struct {
	mu         sync.Mutex
	bars       []*Bar
	delay      time.Duration
	sigwinch   chan os.Signal
	output     *os.File
	width      int
	logHandler slog.Handler
}

func NewRenderer(level slog.Level) *Renderer {
	r := Renderer{
		bars:     make([]*Bar, 0),
		delay:    time.Millisecond * 100,
		sigwinch: make(chan os.Signal),
		output:   os.Stderr,
	}
	h := LogHandler{renderer: &r, level: level}
	r.logHandler = &h
	return &r
}

func (r *Renderer) LogHandler() slog.Handler {
	return r.logHandler
}

func (r *Renderer) AddBar(b *Bar) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.bars = append(r.bars, b)
}

func termWidth(t *os.File) int {
	fd := int(t.Fd())
	width, _, err := term.GetSize(fd)
	if err != nil {
		return 80
	}
	return width
}

func (r *Renderer) draw(rewind int, completed []*Bar) {
	// Move back up rewind lines
	if rewind > 0 {
		fmt.Fprintf(r.output, "\033[%dA", rewind)
	}

	// Handle logs
	r.logHandler.(*LogHandler).mu.Lock()
	logs := r.logHandler.(*LogHandler).pending
	r.logHandler.(*LogHandler).pending = []string{}
	r.logHandler.(*LogHandler).mu.Unlock()

	for _, log := range logs {
		fmt.Fprintf(r.output, "%s\033[0K\n", log)
	}

	for _, bar := range completed {
		bar.draw(r.output, r.width)
	}
	for _, bar := range r.bars {
		bar.draw(r.output, r.width)
	}
}

func (r *Renderer) Run(ctx context.Context) {
	signal.Notify(r.sigwinch, syscall.SIGWINCH)
	r.width = termWidth(r.output)
	ticker := time.NewTicker(r.delay)
	rewind := 0
	for {
		select {
		case <-ticker.C:
		case <-r.sigwinch:
			r.width = termWidth(r.output)
		case <-ctx.Done():
			r.draw(rewind, nil)
			return
		}
		var completed []*Bar
		var active []*Bar
		r.mu.Lock()
		for _, bar := range r.bars {
			if bar.done.Load() {
				completed = append(completed, bar)
			} else {
				active = append(active, bar)
			}
		}
		r.bars = active
		r.mu.Unlock()
		r.draw(rewind, completed)
		rewind = len(r.bars)
	}
}
