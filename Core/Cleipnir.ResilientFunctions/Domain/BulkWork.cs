namespace Cleipnir.ResilientFunctions.Domain;

public record BulkWork<TParam>(FunctionInstanceId InstanceId, TParam Param);