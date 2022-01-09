using System;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

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

    public record StoredParameter(string ParamJson, string ParamType)
    {
        public object Deserialize()
            => Json.Deserialize(ParamJson, Type.GetType(ParamType, throwOnError: true)!)!;
    }

    public record StoredResult(string ResultJson, string ResultType)
    {
        public object Deserialize()
            => ResultJson.DeserializeFromJsonTo(Type.GetType(ResultType, throwOnError: true)!);
    }

    public record StoredFailure(string FailedJson, string FailedType)
    {
        public Exception Deserialize()
            => (Exception) FailedJson.DeserializeFromJsonTo(Type.GetType(FailedType, throwOnError: true)!);
    }

    public record StoredScrapbook(string? ScrapbookJson, string ScrapbookType)
    {
        public RScrapbook Deserialize() 
            => ScrapbookJson == null
                ? (RScrapbook) Activator.CreateInstance(Type.GetType(ScrapbookType, throwOnError: true)!)!
                : (RScrapbook) ScrapbookJson.DeserializeFromJsonTo(Type.GetType(ScrapbookType, throwOnError: true)!);
    }
}