using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription
{
    ISubscriptionGroup Group { get; }
    IReactiveChain<object> Source { get; }
    ITimeoutProvider TimeoutProvider { get; }
 
    TimeSpan DefaultMessageSyncDelay { get; }
    
    Task SyncStore(TimeSpan maxSinceLastSynced);
    InterruptCount PushMessages();
}