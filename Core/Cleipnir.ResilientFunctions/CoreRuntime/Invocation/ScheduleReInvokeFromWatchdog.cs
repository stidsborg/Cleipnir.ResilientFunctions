using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal delegate Task ScheduleReInvokeFromWatchdog(FunctionInstanceId functionId, RestartedFunction restartedFunction, Action onCompletion);