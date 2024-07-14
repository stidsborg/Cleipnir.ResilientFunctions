using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TReturn>(
    FlowId FlowId, 
    Status Status,
    int Epoch,
    long LeaseExpiration,
    TParam? Param, 
    TReturn? Result,
    string? DefaultState,
    DateTime? PostponedUntil,
    PreviouslyThrownException? PreviouslyThrownException
);