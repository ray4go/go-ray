package ray

import (
	"github.com/ray4go/go-ray/ray/internal/consts"
	"errors"
	"fmt"
)

// Errors may be returned by goray APIs.
var (
	ErrTimeout   = errors.New("timeout to get object")
	ErrCancelled = errors.New("task cancelled")
)

func newError(code int64) error {
	switch code {
	case consts.ErrorCode_Failed:
		return errors.New("failed")
	case consts.ErrorCode_Timeout:
		return ErrTimeout
	case consts.ErrorCode_Cancelled:
		return ErrCancelled
	default:
		panic(fmt.Sprintf("unknown error code: %v", code))
	}
}
