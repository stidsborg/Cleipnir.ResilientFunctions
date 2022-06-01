using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Messaging.Core;

public interface IEventStore
{
    Task<IDisposable> SubscribeToChanges(Action<FunctionId> subscriber);
    
    Task AppendEvent(FunctionId functionId, object @event);
    Task<IEnumerable<object>> GetEvents(FunctionId functionId, int skip);
}