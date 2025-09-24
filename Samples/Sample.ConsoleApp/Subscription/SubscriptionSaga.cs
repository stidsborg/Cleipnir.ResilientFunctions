using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace ConsoleApp.Subscription;

public class SubscriptionSaga
{
    public async Task<Result<Unit>> UpdateSubscription(SubscriptionChange subscriptionChange, Workflow workflow)
    {
        await using var monitor = await workflow.Synchronization.AcquireLock(nameof(UpdateSubscription), subscriptionChange.SubscriptionId);
        var (subscriptionId, startSubscription) = subscriptionChange;

        if (startSubscription)
            await StartSubscription(subscriptionId);
        else
            await StopSubscription(subscriptionId);

        await StoreSubscriptionStatusLocally(startSubscription);

        return Succeed.WithUnit;
    }

    private async Task StartSubscription(string subscriptionId)
    {
        Console.WriteLine("Starting subscription");
        await Task.Delay(1_000);
        Console.WriteLine("Start completed");
    }

    private async Task StopSubscription(string subscriptionId)
    {
        Console.WriteLine("Stopping subscription");
        await Task.Delay(1_000);
        Console.WriteLine("Stop completed");
    }

    private Task StoreSubscriptionStatusLocally(bool subscriptionStatus)
    {
        Console.WriteLine($"Storing '{subscriptionStatus}' for subscription");
        return Task.CompletedTask;
    }

    public record SubscriptionChange(string SubscriptionId, bool StartSubscription);
}