using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal delegate Task ScheduleRestartFromWatchdog(StoredId storedId, RestartedFunction restartedFunction, Action onCompletion);