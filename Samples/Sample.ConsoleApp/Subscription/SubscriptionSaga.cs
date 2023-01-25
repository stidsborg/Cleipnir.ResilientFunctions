using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils.Monitor;

namespace ConsoleApp.Subscription;

public class SubscriptionSaga
{
    private readonly RAction<Inner.SubscriptionChange> _rAction;
    
    public SubscriptionSaga(RFunctions rFunctions)
    {
        _rAction = rFunctions.RegisterMethod<Inner>().RegisterAction<Inner.SubscriptionChange>(
            nameof(SubscriptionSaga),
            inner => inner.UpdateSubscription
        );
    }

    public Task UpdateSubscription(string subscriptionId, bool status)
        => _rAction.Invoke(subscriptionId, new Inner.SubscriptionChange(subscriptionId, status));

    private class Inner
    {
        private IMonitor Monitor { get; }

        public Inner(IMonitor monitor)
        {
            Monitor = monitor;
        }

        public async Task<Result> UpdateSubscription(SubscriptionChange subscriptionChange, Context context)
        {
            var (subscriptionId, startSubscription) = subscriptionChange;
            await using var @lock = await Monitor.Acquire(group: nameof(UpdateSubscription), key: subscriptionId);
            if (@lock == null)
                return Postpone.For(10_000);

            if (startSubscription)
                await StartSubscription(subscriptionId);
            else
                await StopSubscription(subscriptionId);

            await StoreSubscriptionStatus(startSubscription);
        
            return Succeed.WithoutValue;
        }

        private Task StartSubscription(string subscriptionId) => Task.CompletedTask;
        private Task StopSubscription(string subscriptionId) => Task.CompletedTask;
        private Task StoreSubscriptionStatus(bool subscriptionStatus) => Task.CompletedTask;

        public record SubscriptionChange(string SubscriptionId, bool StartSubscription);   
    }
}