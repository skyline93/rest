package repository

import "github.com/skyline93/rest/internal/rest"

type uploadTask struct {
	packer *packer
	tpe    rest.BlobType
}

type packerUploader struct {
	uploadQueue chan uploadTask
}
