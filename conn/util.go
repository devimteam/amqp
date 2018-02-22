package conn

// Returns 2^x
// If x is zero, returns 0
func expOf2(x uint) uint {
	if x == 0 {
		return 0
	}
	return 2 << uint(x-1)
}
