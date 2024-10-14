package main

import "os"

func main() {
	os.Rename("before.txt", "after.txt")
}
