using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TReturn>(
    Status Status,
    long Expires,
    ReplicaId? Owner,
    TParam? Param, 
    TReturn? Result,
    FatalWorkflowException? FatalWorkflowException
);