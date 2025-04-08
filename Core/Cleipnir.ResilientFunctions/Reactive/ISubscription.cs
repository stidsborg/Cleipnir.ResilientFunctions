using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Reactive.Operators;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription
{
    bool IsWorkflowRunning { get; }
    IReactiveChain<object> Source { get; }
 
    TimeSpan DefaultMessageSyncDelay { get; }
    TimeSpan DefaultMessageMaxWait { get; }

    Task Initialize();
    Task SyncStore(TimeSpan maxSinceLastSynced);
    void PushMessages();
    
    Task<RegisterTimeoutResult?> RegisterTimeout();
    Task CancelTimeout();
}