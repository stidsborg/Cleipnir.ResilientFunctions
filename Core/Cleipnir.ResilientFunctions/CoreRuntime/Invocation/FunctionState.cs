using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record FunctionState<TParam, TReturn>(
    Status Status,
    int Epoch,
    long Expires,
    TParam? Param, 
    TReturn? Result,
    FatalWorkflowException? FatalWorkflowException
);