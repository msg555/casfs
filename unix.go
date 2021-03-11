package main

func S_ISDIR(mode uint32) bool {
	return ((mode & 0170000) == 0040000)
}
