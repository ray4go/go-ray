package internal

const CmdBitsLen = 10
const CmdBitsMask = (1 << CmdBitsLen) - 1

const (
	ErrorCode_Success = iota
	ErrorCode_Failed
	ErrorCode_Timeout
	ErrorCode_Cancelled
)

const (
	Go2PyCmd_Init = iota
	Go2PyCmd_ExeRemoteTask
	Go2PyCmd_GetObject
	Go2PyCmd_PutObject
	Go2PyCmd_WaitObject
	Go2PyCmd_CancelObject

	Go2PyCmd_NewActor
	Go2PyCmd_NewPythonActor
	Go2PyCmd_ActorMethodCall
	Go2PyCmd_KillActor
	Go2PyCmd_GetActor

	Go2PyCmd_ExePythonRemoteTask
	Go2PyCmd_ExePythonLocalTask

	Go2PyCmd_ExePyCode
)

const (
	Py2GoCmd_StartDriver = iota
	Py2GoCmd_GetTaskActorList
	Py2GoCmd_GetActorMethods

	Py2GoCmd_RunTask
	Py2GoCmd_NewActor
	Py2GoCmd_ActorMethodCall
	Py2GoCmd_CloseActor
)
