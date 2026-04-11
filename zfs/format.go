package zfs

import "fmt"

const (
	kB = 1 << (10 * (iota + 1))
	mB
	gB
	tB
	pB
	eB
)

func unitFor(size int64) (int64, string) {
	switch {
	case size >= eB:
		return eB, "EiB"
	case size >= pB:
		return pB, "PiB"
	case size >= tB:
		return tB, "TiB"
	case size >= gB:
		return gB, "GiB"
	case size >= mB:
		return mB, "MiB"
	case size >= kB:
		return kB, "kiB"
	default:
		return 1, "B"
	}
}

func HumanBytes(size int64) string {
	div, suffix := unitFor(size)
	if div == 1 {
		return fmt.Sprintf("%d B", size)
	}
	return fmt.Sprintf("%d %s", size/div, suffix)
}

// HumanBytesFraction formats pos and total as "x/y UUU" using the unit of total.
func HumanBytesFraction(pos, total int64) string {
	div, suffix := unitFor(total)
	if div == 1 {
		return fmt.Sprintf("%d/%d B", pos, total)
	}
	return fmt.Sprintf("%d/%d %s", pos/div, total/div, suffix)
}

// HumanBytesRate formats a bytes/sec rate as a fixed-width "nnn.nn UUU/s" string.
// The unit suffix is left-padded to 3 chars so the total width is always 12.
func HumanBytesRate(bytesPerSec float64) string {
	div, suffix := unitFor(int64(bytesPerSec))
	return fmt.Sprintf("%6.2f %-3s/s", bytesPerSec/float64(div), suffix)
}
