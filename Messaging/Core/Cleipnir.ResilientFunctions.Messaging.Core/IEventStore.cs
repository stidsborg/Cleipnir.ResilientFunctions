using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public interface IEventStore : IDisposable
{
    Task Initialize();
    Task<IDisposable> SubscribeToChanges(FunctionId functionId, Action handler);
    
    Task AppendEvent(FunctionId functionId, StoredEvent storedEvent);
    Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null);
    Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents);
    Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip);
}