using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Storage.Utils;

public static class JsonHelper
{
    public static string? ToJson(object? instance) 
        => instance == null ? null : JsonSerializer.Serialize(instance);
    
    public static T? FromJson<T>(string? json) 
        => json == null ? default : JsonSerializer.Deserialize<T>(json);
}