using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public record FunctionCompletion<TResult>(TResult Result, FlowId Sender) : FunctionCompletion(Sender);
public record FunctionCompletion(FlowId Sender);