namespace Cleipnir.ResilientFunctions.Messaging.Core.Serialization;

public interface IEventSerializer
{
    string SerializeEvent(object @event);
    object DeserializeEvent(string json, string type);
}