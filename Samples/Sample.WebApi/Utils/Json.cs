using System.Text.Json;

namespace Sample.WebApi.Utils;

public static class Json
{
    public static string ToJson<T>(this T t) => JsonSerializer.Serialize(t, typeof(T));
}