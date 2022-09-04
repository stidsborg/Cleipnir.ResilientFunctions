using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Invocation;

public record Context(FunctionId FunctionId, InvocationMode InvocationMode);