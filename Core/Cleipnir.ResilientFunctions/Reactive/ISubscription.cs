using System;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription : IDisposable
{
    IStream<object> Source { get; }
    ITimeoutProvider TimeoutProvider { get; }
    int SubscriptionGroupId { get; }
    void DeliverExistingAndFuture();
    int DeliverExisting();
}