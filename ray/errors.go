package ray

import (
	"errors"
	"fmt"
	"github.com/bytedance/gg/gson"
	"io"

	"github.com/ray4go/go-ray/ray/internal/consts"
)

// Errors may be returned by goray APIs.
var (
	ErrTimeout           = errors.New("timeout to get object")
	ErrCancelled         = errors.New("task cancelled")
	ErrObjectRefNotFound = errors.New("objectRef not found")
)

type panicError struct {
	Message   string
	Traceback string
}

func (e panicError) Error() string {
	return e.Message
}

func (e panicError) Format(s fmt.State, verb rune) {
	switch verb {
	case 'v':
		if s.Flag('+') {
			fmt.Fprintf(s, "panic: %s\n\n%s", e.Error(), e.Traceback)
			return
		}
		fallthrough
	case 's':
		io.WriteString(s, e.Error())
	case 'q':
		fmt.Fprintf(s, "%q", e.Error())
	}
}

func newError(code int64, data []byte) error {
	switch code {
	case consts.ErrorCode_Failed:
		return errors.New(string(data))
	case consts.ErrorCode_Timeout:
		return ErrTimeout
	case consts.ErrorCode_Cancelled:
		return ErrCancelled
	case consts.ErrorCode_ObjectRefNotFound:
		return ErrObjectRefNotFound
	case consts.ErrorCode_Panic:
		res, err := gson.Unmarshal[panicError](data)
		if err != nil {
			return errors.New(string(data))
		}
		return res
	default:
		return fmt.Errorf("unknown error, code: %v, Message: %s", code, string(data))
	}
}
