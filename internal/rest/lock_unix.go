package rest

import (
	"os/user"
	"strconv"

	"github.com/pkg/errors"
)

// uidGidInt returns uid, gid of the user as a number.
func uidGidInt(u *user.User) (uid, gid uint32, err error) {
	ui, err := strconv.ParseUint(u.Uid, 10, 32)
	if err != nil {
		return 0, 0, errors.Errorf("invalid UID %q", u.Uid)
	}
	gi, err := strconv.ParseUint(u.Gid, 10, 32)
	if err != nil {
		return 0, 0, errors.Errorf("invalid GID %q", u.Gid)
	}
	return uint32(ui), uint32(gi), nil
}
