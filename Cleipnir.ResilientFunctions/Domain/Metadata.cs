namespace Cleipnir.ResilientFunctions.Domain;

public record Metadata<TParam>(FunctionId FunctionId, TParam Param)
    where TParam : notnull;