using System;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription : IDisposable
{
    public int SubscriptionGroupId { get; }
    void DeliverExistingAndFuture();
    int DeliverExisting();
}