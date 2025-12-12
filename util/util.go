package util

import "fmt"

const (
	kB = 1 << (10 * (iota + 1))
	mB
	gB
	tB
	pB
	eB
)

func HumanBytes(size int64) string {
	switch {
	case size >= eB:
		return format(size, eB, "EiB")
	case size >= pB:
		return format(size, pB, "PiB")
	case size >= tB:
		return format(size, tB, "TiB")
	case size >= gB:
		return format(size, gB, "GiB")
	case size >= mB:
		return format(size, mB, "MiB")
	case size >= kB:
		return format(size, kB, "kiB")
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func format(size int64, unit int64, suffix string) string {
	return fmt.Sprintf("%.2f %s", float64(size)/float64(unit), suffix)
}
