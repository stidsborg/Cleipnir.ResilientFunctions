using System;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage
{
    public record StoredFunction(FunctionId FunctionId, string ParamJson, string ParamType, long SignOfLife);
    
    public record FunctionResult(string ResultJson, string ResultType)
    {
        public object Deserialize() => JsonSerializer.Deserialize(ResultJson, Type.GetType(ResultType)!)!;
    }
}