using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Messaging;

public static class StoredEventsBulkJsonSerializer
{
    public static string SerializeToJson(IEnumerable<StoredEvent> storedEvents)
    {
        var storedEventsArray = storedEvents.ToArray();
        var stringBuilder = new StringBuilder();
        
        stringBuilder.Append('[');
        for (var i = 0; i < storedEventsArray.Length; i++)
        {
            var storedEvent = storedEventsArray[i];
            var idempotencyKey = JsonSerializer.Serialize(storedEvent.IdempotencyKey);
            stringBuilder.Append($@"{{ ""Event"": {storedEvent.EventJson}, ""Type"": ""{storedEvent.EventType}"", ""IdempotencyKey"": {idempotencyKey} }}");
            if (i < storedEventsArray.Length - 1)
                stringBuilder.Append(',');
        }
        stringBuilder.Append(']');

        return stringBuilder.ToString();
    }

    public static IEnumerable<StoredEvent> DeserializeToStoredEvents(string json)
    {
        var events = JsonSerializer.Deserialize<IEnumerable<StoredJsonEvent>>(json)!;

        return events.Select(e =>
            new StoredEvent(
                EventJson: e.Event.GetRawText(),
                EventType: e.Type,
                IdempotencyKey: e.IdempotencyKey
            )
        );
    }
    
    private record StoredJsonEvent(JsonElement Event, string Type, string? IdempotencyKey);
}