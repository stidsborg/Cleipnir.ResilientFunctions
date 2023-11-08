using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime;

namespace Cleipnir.ResilientFunctions.Reactive;

public interface ISubscription : IDisposable
{
    public int EmittedFromSource { get; }
    IReactiveChain<object> Source { get; }
    ITimeoutProvider TimeoutProvider { get; }
    int SubscriptionGroupId { get; }
    void DeliverFuture();
    void DeliverExisting();
    Task StopDelivering();
}