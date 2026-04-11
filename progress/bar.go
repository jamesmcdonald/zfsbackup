package progress

import (
	"fmt"
	"os"
	"strings"
	"sync/atomic"
)

type Bar struct {
	label    string
	target   int64
	pos      atomic.Int64
	done     atomic.Bool
	abbrLeft bool
}

func NewBar(label string, target int64) *Bar {
	return &Bar{
		label:    label,
		target:   target,
		abbrLeft: false,
	}
}

func (b *Bar) Add(n int64) {
	b.pos.Add(n)
}

func (b *Bar) Done() {
	b.done.Store(true)
}

func (b *Bar) displayLabel(width int) string {
	if len(b.label) > width {
		if b.abbrLeft {
			return "..." + b.label[len(b.label)-(width-3):]
		} else {
			return b.label[:width-3] + "..."
		}
	}
	return b.label
}

func (b *Bar) draw(output *os.File, width int) {
	pct := float64(b.pos.Load()) / float64(b.target)
	filled := int(pct * float64(width-30))
	fillChar := "="
	if filled > width-30 {
		filled = width - 30
		fillChar = ">"
	}
	dl := b.displayLabel(20)
	// Kill to EOL after printing
	pctText := "Done"
	if !b.done.Load() {
		pctText = fmt.Sprintf("%3.0f%%", pct*100)
	}
	fmt.Fprintf(output, "%20s [%-*s] %s\033[0K\n", dl, width-30, strings.Repeat(fillChar, filled), pctText)
}
