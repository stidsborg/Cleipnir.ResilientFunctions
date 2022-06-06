using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public interface IEventStore : IDisposable
{
    Task Initialize();
    Task<IDisposable> SubscribeToChanges(Action<FunctionId> subscriber);
    
    Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null);
    Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip);
}