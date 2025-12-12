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

func PrettyBytes(size int64) string {
	switch {
	case size >= eB:
		return format(size, eB, "EB")
	case size >= pB:
		return format(size, pB, "PB")
	case size >= tB:
		return format(size, tB, "TB")
	case size >= gB:
		return format(size, gB, "GB")
	case size >= mB:
		return format(size, mB, "MB")
	case size >= kB:
		return format(size, kB, "kB")
	default:
		return fmt.Sprintf("%d B", size)
	}
}

func format(size int64, unit int64, suffix string) string {
	return fmt.Sprintf("%.2f %s", float64(size)/float64(unit), suffix)
}
