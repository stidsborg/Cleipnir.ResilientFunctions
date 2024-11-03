namespace Cleipnir.ResilientFunctions.Domain;

public record BulkWork<TParam>(string Instance, TParam Param);