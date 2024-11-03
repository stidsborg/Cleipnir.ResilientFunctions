using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal delegate Task ScheduleRestartFromWatchdog(StoredInstance flow, RestartedFunction restartedFunction, Action onCompletion);