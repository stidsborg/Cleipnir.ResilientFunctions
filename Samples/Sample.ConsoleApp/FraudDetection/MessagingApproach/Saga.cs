using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class Saga
{
    public static async Task StartFraudDetection(Transaction transaction, Context context)
    {
        var messages = context.Messages;
        await MessageBroker.Send(new ApproveTransaction(transaction));
        
        var results = await messages
            .OfType<FraudDetectorResult>()
            .Take(3)
            .TakeUntilTimeout("Timeout", TimeSpan.FromSeconds(2))
            .Completion();

        var approved = results.Count >= 2 && results.All(result => result.Approved);

        await context.Activities.Do(
            "PublishTransactionApproval",
            () => MessageBroker.Send(approved
                ? new TransactionApproved(transaction)
                : new TransactionDeclined(transaction))
        );
    }
}