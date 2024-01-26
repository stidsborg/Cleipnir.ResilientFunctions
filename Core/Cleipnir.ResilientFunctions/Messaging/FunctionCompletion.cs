using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public record FunctionCompletion<TResult>(TResult Result, FunctionId Sender) : FunctionCompletion(Sender);
public record FunctionCompletion(FunctionId Sender);