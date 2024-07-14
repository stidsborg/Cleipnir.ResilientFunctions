using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal delegate Task ScheduleRestartFromWatchdog(FlowInstance flow, RestartedFunction restartedFunction, Action onCompletion);