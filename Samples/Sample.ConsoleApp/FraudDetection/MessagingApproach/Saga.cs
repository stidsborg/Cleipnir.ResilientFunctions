using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;

namespace ConsoleApp.FraudDetection.MessagingApproach;

public static class Saga
{
    public static async Task StartFraudDetection(Transaction transaction, RScrapbook scrapbook, Context context)
    {
        var eventSource = await context.EventSource;
        await MessageBroker.Send(new ApproveTransaction(transaction));
        
        //await received all approvals or timeout occured
        await Task.WhenAny(
            eventSource.Take(3).Last(),
            Task.Delay(TimeSpan.FromSeconds(2))
        );
        
        var approvals = eventSource
                .Existing
                .OfType<FraudDetectorResult>()
                .Select(r => r.Approved)
                .ToList();

        var approved = approvals.Count >= 2 && approvals.All(approved => approved == true);

        await scrapbook.DoAtMostOnce(
            "PublishTransactionApproval",
            () => MessageBroker.Send(approved
                ? new TransactionApproved(transaction)
                : new TransactionDeclined(transaction))
        );
    }
}