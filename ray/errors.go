package ray

import (
	"errors"
	"fmt"

	"github.com/ray4go/go-ray/ray/internal/consts"
)

// Errors may be returned by goray APIs.
var (
	ErrTimeout           = errors.New("timeout to get object")
	ErrCancelled         = errors.New("task cancelled")
	ErrObjectRefNotFound = errors.New("objectRef not found")
)

func newError(code int64) error {
	switch code {
	case consts.ErrorCode_Failed:
		return errors.New("failed")
	case consts.ErrorCode_Timeout:
		return ErrTimeout
	case consts.ErrorCode_Cancelled:
		return ErrCancelled
	case consts.ErrorCode_ObjectRefNotFound:
		return ErrObjectRefNotFound
	default:
		return fmt.Errorf("unknown error (code: %v)", code)
	}
}
