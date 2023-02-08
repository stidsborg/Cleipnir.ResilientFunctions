using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging;

public interface IEventStore
{
    Task Initialize();

    Task AppendEvent(FunctionId functionId, StoredEvent storedEvent);
    Task AppendEvent(FunctionId functionId, string eventJson, string eventType, string? idempotencyKey = null);
    Task AppendEvents(FunctionId functionId, IEnumerable<StoredEvent> storedEvents);
    
    Task Truncate(FunctionId functionId);
    Task<bool> Replace(FunctionId functionId, IEnumerable<StoredEvent> storedEvents, int? expectedCount); 
    
    Task<IEnumerable<StoredEvent>> GetEvents(FunctionId functionId, int skip);
}