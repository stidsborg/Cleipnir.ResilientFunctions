namespace Cleipnir.ResilientFunctions.Domain;

public record BulkWork<TParam>(FlowInstance Instance, TParam Param);