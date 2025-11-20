using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class Saga
{
    public static async Task StartFraudDetection(Transaction transaction, Workflow workflow)
    {
        var messages = workflow.Messages;
        await MessageBroker.Send(new ApproveTransaction(transaction));
        
        var results = await messages
            .TakeUntilTimeout("Timeout", TimeSpan.FromSeconds(2))
            .OfType<FraudDetectorResult>()
            .Take(3)
            .Completion();

        var approved = results.Count >= 2 && results.All(result => result.Approved);

        await workflow.Effect.Capture(
            () => MessageBroker.Send(approved
                ? new TransactionApproved(transaction)
                : new TransactionDeclined(transaction))
        );
    }
}