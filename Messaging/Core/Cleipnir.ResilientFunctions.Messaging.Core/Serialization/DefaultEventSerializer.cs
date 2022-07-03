using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

public class DefaultEventSerializer : IEventSerializer
{
    public static readonly DefaultEventSerializer Instance = new();
    private DefaultEventSerializer() {}

    public string SerializeEvent(object @event)
        => JsonSerializer.Serialize(@event);

    public object DeserializeEvent(string json, string type)
        => JsonSerializer.Deserialize(json, Type.GetType(type, throwOnError: true)!)!;
}