using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

/// <summary>
/// Resumes execution of an already-claimed flow from the snapshot returned by the store's restart call. Implemented
/// by <see cref="Invoker{TParam,TReturn}"/> (which owns the user function and drives the execution loop) and held by
/// the non-generic <see cref="FlowsManager"/> so it can restart flows without knowing their parameter/return types.
/// </summary>
internal interface IFlowRestarter
{
    Task ScheduleRestart(StoredId storedId, RestartedFunction restartedFunction, Action onCompletion);
}
