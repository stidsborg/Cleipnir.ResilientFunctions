using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class Saga
{
    public static async Task StartFraudDetection(Transaction transaction, Context context)
    {
        var eventSource = await context.EventSource;
        await MessageBroker.Send(new ApproveTransaction(transaction));

        await eventSource.TimeoutProvider.RegisterTimeout(
            timeoutId: "Timeout",
            expiresAt: transaction.Created.AddSeconds(2)
        );
        
        var next = await eventSource
            .OfTypes<FraudDetectorResult, TimeoutEvent>()
            .Take(3)
            .ToList();

        var approved = next
            .Select(either =>
                either.Match(first: fraudDetectorResult => fraudDetectorResult.Approved, second: timeout => false)
            ).All(_ => _);

        await MessageBroker.Send(approved
            ? new TransactionApproved(transaction)
            : new TransactionDeclined(transaction)
        );
    }
}