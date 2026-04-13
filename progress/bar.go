package progress

import (
	"fmt"
	"math"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jamesmcdonald/zfsbackup/zfs"
)

// ewmaTau is the time constant (seconds) for the exponential moving average of
// the transfer rate. Larger values produce a smoother but slower-responding display.
const ewmaTau = 10.0

type Bar struct {
	label    string
	target   int64
	pos      atomic.Int64
	abbrLeft bool

	mu           sync.Mutex
	done         bool
	startTime    time.Time
	doneTime     time.Time
	lastN        int64
	lastTime     time.Time
	smoothedRate float64
}

func NewBar(label string, target int64) *Bar {
	return &Bar{
		label:     label,
		target:    target,
		startTime: time.Now(),
	}
}

func (b *Bar) Add(n int64) {
	b.pos.Add(n)
}

func (b *Bar) Done() {
	b.mu.Lock()
	b.done = true
	b.doneTime = time.Now()
	b.mu.Unlock()
}

func (b *Bar) isDone() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.done
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
	now := time.Now()
	if !b.lastTime.IsZero() {
		dt := now.Sub(b.lastTime).Seconds()
		if dt > 0 {
			instantRate := float64(n-b.lastN) / dt
			if b.smoothedRate == 0 {
				b.smoothedRate = instantRate
			} else {
				alpha := 1 - math.Exp(-dt/ewmaTau)
				b.smoothedRate = alpha*instantRate + (1-alpha)*b.smoothedRate
			}
		}
	}
	b.lastN = n
	b.lastTime = now
}

func (b *Bar) rate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.smoothedRate
}

// avgRate returns total bytes divided by total elapsed seconds, for use in the
// done state where the smoothed instantaneous rate is less meaningful.
func (b *Bar) avgRate() float64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	elapsed := b.doneTime.Sub(b.startTime).Seconds()
	if elapsed <= 0 {
		return 0
	}
	return float64(b.target) / elapsed
}

func (b *Bar) elapsed() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	secs := int(b.doneTime.Sub(b.startTime).Seconds())
	return fmt.Sprintf("%03d:%02d:%02d", secs/3600, (secs%3600)/60, secs%60)
}

func (b *Bar) eta() string {
	pos := b.pos.Load()
	r := b.rate()
	if r <= 0 || pos >= b.target {
		return "---:--:--"
	}
	secs := int(float64(b.target-pos) / r)
	return fmt.Sprintf("%03d:%02d:%02d", secs/3600, (secs%3600)/60, secs%60)
}

func (b *Bar) draw(output *os.File, width, labelWidth int) {
	pos := b.pos.Load()
	b.recordSample(pos)

	pct := float64(pos) / float64(b.target)

	sizeText := zfs.HumanBytesFraction(pos, b.target)

	var rateText, etaText, pctText string
	if b.isDone() {
		etaText = b.elapsed()
		pctText = "Done"
		rateText = zfs.HumanBytesRate(b.avgRate())
	} else {
		etaText = b.eta()
		pctText = fmt.Sprintf("%3.0f%%", pct*100)
		rateText = zfs.HumanBytesRate(b.rate())
	}

	// labelWidth + 2 ([) + 2 (]) + 9 (eta) + 2 (  ) + 4 (pct) + 2 (  ) + sizeText + 2 (  ) + 12 (rateText fixed)
	overhead := labelWidth + 2 + 2 + 9 + 2 + 4 + 2 + len(sizeText) + 2 + 12
	barWidth := max(width-overhead, 5)

	filled := int(pct * float64(barWidth))
	fillChar := "="
	if filled > barWidth {
		filled = barWidth
		fillChar = ">"
	}

	dl := b.displayLabel(labelWidth)
	// Kill to EOL after printing
	fmt.Fprintf(output, "%*s [%-*s] %s  %s  %s  %s\033[0K\n", labelWidth, dl, barWidth, strings.Repeat(fillChar, filled), etaText, pctText, sizeText, rateText)
}
