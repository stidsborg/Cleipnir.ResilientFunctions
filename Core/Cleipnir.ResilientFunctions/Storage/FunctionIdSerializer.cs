using System.Runtime.Serialization;
using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Storage;

public static class FunctionIdSerializer
{
    public static string? SerializeToJsonArray(this FunctionId? functionId)
        => functionId != null
            ? JsonSerializer.Serialize(new[] { functionId.TypeId.Value, functionId.InstanceId.Value })
            : null;
    
    public static FunctionId? DeserializeToFunctionId(this string? jsonArray)
    {
        if (jsonArray == null)
            return null;
        
        var arr = JsonSerializer.Deserialize<string[]>(jsonArray);
        if (arr == null)
            throw new SerializationException($"Unable to deserialize json '{jsonArray}' to function id");
        
        return new FunctionId(arr[0], arr[1]);
    } 
}