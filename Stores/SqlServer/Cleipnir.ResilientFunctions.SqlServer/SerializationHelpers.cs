using System.Text.Json;

namespace Cleipnir.ResilientFunctions.SqlServer;

internal static class SerializationHelpers
{
    public static string? ToJson(this object? instance) 
        => instance == null ? null : JsonSerializer.Serialize(instance);
}