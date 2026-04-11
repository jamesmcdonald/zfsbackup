package progress

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamesmcdonald/zfsbackup/zfs"
)

const maxSamples = 60

type sample struct {
	t time.Time
	n int64
}

type Bar struct {
	label    string
	target   int64
	pos      atomic.Int64
	done     atomic.Bool
	abbrLeft bool

	mu          sync.Mutex
	samples     [maxSamples]sample
	sampleIdx   int
	sampleCount int
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

func (b *Bar) recordSample(n int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.samples[b.sampleIdx] = sample{t: time.Now(), n: n}
	b.sampleIdx = (b.sampleIdx + 1) % maxSamples
	if b.sampleCount < maxSamples {
		b.sampleCount++
	}
}

func (b *Bar) rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.sampleCount < 2 {
		return 0
	}
	oldest := (b.sampleIdx - b.sampleCount + maxSamples) % maxSamples
	newest := (b.sampleIdx - 1 + maxSamples) % maxSamples
	dt := b.samples[newest].t.Sub(b.samples[oldest].t).Seconds()
	if dt <= 0 {
		return 0
	}
	return float64(b.samples[newest].n-b.samples[oldest].n) / dt
}

func (b *Bar) draw(output *os.File, width int) {
	pos := b.pos.Load()
	b.recordSample(pos)

	pct := float64(pos) / float64(b.target)

	sizeText := zfs.HumanBytesFraction(pos, b.target)
	rateText := zfs.HumanBytesRate(b.rate())

	// 20 (label) + 2 ( [) + 2 (] ) + 4 (pct) + 2 (  ) + sizeText + 2 (  ) + 12 (rateText fixed)
	overhead := 20 + 2 + 2 + 4 + 2 + len(sizeText) + 2 + 12
	barWidth := max(width-overhead, 5)

	// bar contents
	// label [1 space] [10 size (xxxx.y mib)] [1 space] [11 speed (xx.yy kib/s) or (xxxx kib/s)] [1 space] [0 or more bar + space] [4 pct]

	filled := int(pct * float64(barWidth))
	fillChar := "="
	if filled > barWidth {
		filled = barWidth
		fillChar = ">"
	}

	dl := b.displayLabel(20)
	pctText := "Done"
	if !b.done.Load() {
		pctText = fmt.Sprintf("%3.0f%%", pct*100)
	}
	// Kill to EOL after printing
	fmt.Fprintf(output, "%20s [%-*s] %s  %s  %s\033[0K\n", dl, barWidth, strings.Repeat(fillChar, filled), pctText, sizeText, rateText)
}
