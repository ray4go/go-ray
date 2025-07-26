package internal

const CmdBitsLen = 10
const CmdBitsMask = (1 << CmdBitsLen) - 1

const (
	ErrorCode_Success   = 0
	ErrorCode_Failed    = iota
	ErrorCode_Timeout   = iota
	ErrorCode_Cancelled = iota
)

const (
	Go2PyCmd_Init          = 0
	Go2PyCmd_ExeRemoteTask = iota
	Go2PyCmd_GetObject     = iota
	Go2PyCmd_PutObject     = iota
	Go2PyCmd_WaitObject    = iota
	Go2PyCmd_CancelObject  = iota

	Go2PyCmd_NewActor        = iota
	Go2PyCmd_ActorMethodCall = iota
	Go2PyCmd_KillActor       = iota
	Go2PyCmd_GetActor        = iota

	Go2PyCmd_ExePyCode = iota
)

const (
	Py2GoCmd_StartDriver     = 0
	Py2GoCmd_RunTask         = iota
	Py2GoCmd_NewActor        = iota
	Py2GoCmd_ActorMethodCall = iota
)
