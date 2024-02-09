using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.Subscription;

public class SubscriptionSaga
{
    public async Task<Result> UpdateSubscription(SubscriptionChange subscriptionChange, Scrapbook scrapbook, Workflow workflow)
    {
        var monitor = workflow.Utilities.Monitor;
        var (subscriptionId, startSubscription) = subscriptionChange;
        await using var @lock = await monitor.Acquire(
            group: nameof(UpdateSubscription),
            name: subscriptionChange.SubscriptionId,
            lockId: scrapbook.LockId
        );
        if (@lock == null)
            return Postpone.For(10_000);

        if (startSubscription)
            await StartSubscription(subscriptionId);
        else
            await StopSubscription(subscriptionId);

        await StoreSubscriptionStatusLocally(startSubscription);

        return Succeed.WithoutValue;
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

    public class Scrapbook : RScrapbook
    {
        public string LockId { get; set; } = Guid.NewGuid().ToString();
    }
}