namespace Cleipnir.ResilientFunctions.Reactive;

public interface IStream<T>
{
    ISubscription Subscribe(Subscription<T> subscription, int? subscriptionGroupId = null);
}