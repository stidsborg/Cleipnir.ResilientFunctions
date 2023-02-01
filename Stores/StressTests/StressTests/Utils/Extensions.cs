using System.Text.Json;

namespace Cleipnir.ResilientFunctions.StressTests.StressTests.Utils;

public static class Extensions
{
    public static string ToJson<T>(this T t)
        => JsonSerializer.Serialize(t);
}