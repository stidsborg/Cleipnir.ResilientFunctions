using System.Runtime.Serialization;
using Newtonsoft.Json;

namespace Sample.WebApi.Utils;

public static class Json
{
    private static JsonSerializerSettings SerializerSettings { get; } 
        = new() {TypeNameHandling = TypeNameHandling.Auto };

    public static string ToJson<T>(this T t) => JsonConvert.SerializeObject(t, typeof(T), SerializerSettings);

    public static object DeserializeFromJsonTo(this string json, Type type) => Deserialize(json, type);
    public static T DeserializeFromJsonTo<T>(this string json) => (T) Deserialize(json, typeof(T));

    private static object Deserialize(string json, Type type)
    {
        var deserialized = JsonConvert.DeserializeObject(json, type, SerializerSettings);
        if (deserialized == null)
            throw new SerializationException("Json deserialized to null");

        return deserialized;
    }
}