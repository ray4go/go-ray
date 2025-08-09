package ray

import (
	"github.com/ray4go/go-ray/ray/internal"
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
	case internal.ErrorCode_Failed:
		return errors.New("failed")
	case internal.ErrorCode_Timeout:
		return ErrTimeout
	case internal.ErrorCode_Cancelled:
		return ErrCancelled
	default:
		panic(fmt.Sprintf("unknown error code: %v", code))
	}
}
