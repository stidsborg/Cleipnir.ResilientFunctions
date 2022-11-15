namespace Cleipnir.ResilientFunctions.Messaging.Serialization;

public interface IEventSerializer
{
    string SerializeEvent(object @event);
    object DeserializeEvent(string json, string type);
}