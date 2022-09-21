using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record Context(FunctionId FunctionId, InvocationMode InvocationMode);