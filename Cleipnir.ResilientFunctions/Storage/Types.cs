using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage
{
    public record StoredFunction(
        FunctionId FunctionId,
        Parameter Parameter1,
        Parameter? Parameter2,
        Scrapbook? Scrapbook,
        long SignOfLife,
        Result? Result
    );
    
    public record Parameter(string ParamJson, string ParamType);
    public record Scrapbook(string? ScrapbookJson, string ScrapbookType, int VersionStamp);

    public record Result(string ResultJson, string ResultType)
    {
        public object Deserialize() => JsonSerializer.Deserialize(ResultJson, Type.GetType(ResultType)!)!;
    }
}