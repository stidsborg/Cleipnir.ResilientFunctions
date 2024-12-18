﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription
{
    bool IsWorkflowRunning { get; }
    IReactiveChain<object> Source { get; }
    IRegisteredTimeouts RegisteredTimeouts { get; }
 
    TimeSpan DefaultMessageSyncDelay { get; }
    TimeSpan DefaultMessageMaxWait { get; }

    Task Initialize();
    Task SyncStore(TimeSpan maxSinceLastSynced);
    void PushMessages();
    
    Task RegisterTimeout();
    Task CancelTimeout();
}