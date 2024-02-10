using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TState, TReturn>(
    FunctionId FunctionId, 
    Status Status,
    int Epoch,
    long LeaseExpiration,
    TParam Param, 
    TState State, 
    TReturn? Result,
    DateTime? PostponedUntil,
    PreviouslyThrownException? PreviouslyThrownException
) where TParam : notnull where TState : WorkflowState, new();