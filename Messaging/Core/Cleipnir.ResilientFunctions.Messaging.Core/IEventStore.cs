using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public interface IEventStore
{
    Task Initialize();

    Task AppendEvent(FunctionId functionId, StoredEvent storedEvent);
    Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null);
    Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents);
    Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip);
}