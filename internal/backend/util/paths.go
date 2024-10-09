package util

import "os"

type Modes struct {
	Dir  os.FileMode
	File os.FileMode
}

var DefaultModes = Modes{Dir: 0700, File: 0600}

func DeriveModesFromFileInfo(fi os.FileInfo, err error) Modes {
	m := DefaultModes
	if err != nil {
		return m
	}

	if fi.Mode()&0040 != 0 { // Group has read access
		m.Dir |= 0070
		m.File |= 0060
	}

	return m
}
