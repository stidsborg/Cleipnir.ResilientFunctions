using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IEventStore
{
    Task Initialize();

    Task<SuspensionStatus> AppendEvent(FunctionId functionId, StoredEvent storedEvent);
    Task<SuspensionStatus> AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null);
    Task<SuspensionStatus> AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents);
    
    Task Truncate(FunctionId functionId);

    Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId);
    Task<EventsSubscription> SubscribeToEvents(FunctionId functionId);
}