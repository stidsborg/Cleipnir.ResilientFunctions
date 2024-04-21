using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TReturn>(
    FunctionId FunctionId, 
    Status Status,
    int Epoch,
    long LeaseExpiration,
    TParam Param, 
    TReturn? Result,
    string? DefaultState,
    DateTime? PostponedUntil,
    PreviouslyThrownException? PreviouslyThrownException
) where TParam : notnull;