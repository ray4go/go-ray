package ray

import (
	"errors"
	"fmt"
)

var (
	ErrTimeout   = errors.New("timeout to get object")
	ErrCancelled = errors.New("task cancelled")
)

func NewError(code int64) error {
	switch code {
	case ErrorCode_Failed:
		return errors.New("failed")
	case ErrorCode_Timeout:
		return ErrTimeout
	case ErrorCode_Cancelled:
		return ErrCancelled
	default:
		panic(fmt.Sprintf("unknown error code: %v", code))
	}
}
