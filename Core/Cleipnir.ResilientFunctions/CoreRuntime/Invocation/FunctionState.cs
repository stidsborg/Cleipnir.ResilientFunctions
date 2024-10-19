using System;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TReturn>(
    FlowId FlowId, 
    Status Status,
    int Epoch,
    long Expires,
    TParam? Param, 
    TReturn? Result,
    PreviouslyThrownException? PreviouslyThrownException
);