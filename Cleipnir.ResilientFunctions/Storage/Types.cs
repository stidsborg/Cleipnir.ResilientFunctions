using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Storage
{
    public record StoredFunction(
        FunctionId FunctionId,
        StoredParameter Parameter,
        StoredScrapbook? Scrapbook,
        Status Status,
        StoredResult? Result,
        StoredFailure? Failure,
        long? PostponedUntil,
        int Epoch,
        int SignOfLife
    );
    
    public record StoredFunctionStatus(
        FunctionInstanceId InstanceId, 
        int Epoch,
        int SignOfLife,
        Status Status,
        long? PostponedUntil
    );

    public record StoredParameter(string ParamJson, string ParamType);
    public record StoredResult(string ResultJson, string ResultType);
    public record StoredFailure(string FailedJson, string FailedType);
    public record StoredScrapbook(string? ScrapbookJson, string ScrapbookType);

    internal static class StorageTypeExtensions
    {
        public static object Deserialize(this StoredParameter parameter, ISerializer serializer)
            => serializer.DeserializeParameter(parameter.ParamJson, parameter.ParamType);
        
        public static RScrapbook Deserialize(this StoredScrapbook scrapbook, ISerializer serializer)
            => serializer.DeserializeScrapbook(scrapbook.ScrapbookJson!, scrapbook.ScrapbookType);

        public static Exception Deserialize(this StoredFailure failure, ISerializer serializer)
            => serializer.DeserializeFault(failure.FailedJson, failure.FailedType);

        public static object Deserialize(this StoredResult result, ISerializer serializer)
            => serializer.DeserializeResult(result.ResultJson, result.ResultType);
    }
}