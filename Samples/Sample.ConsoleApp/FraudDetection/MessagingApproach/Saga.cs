using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class Saga
{
    public static async Task StartFraudDetection(Transaction transaction, Workflow workflow)
    {
        await MessageBroker.Send(new ApproveTransaction(transaction));

        var maxWait = DateTime.UtcNow.AddSeconds(2);
        var results = new List<FraudDetectorResult>();
        for (var i = 0; i < 3; i++)
        {
            var result = await workflow.Message<FraudDetectorResult>(waitUntil: maxWait);
            if (result is null)
                break;
            
            results.Add(result);
        }

        var approved = results.Count >= 2 && results.All(result => result.Approved);

        await workflow.Effect.Capture(
            () => MessageBroker.Send(approved
                ? new TransactionApproved(transaction)
                : new TransactionDeclined(transaction))
        );
    }
}