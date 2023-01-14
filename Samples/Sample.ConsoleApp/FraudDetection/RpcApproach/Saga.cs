using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.FraudDetection.RpcApproach;

public static class Saga
{
    private static FraudDetector1 FraudDetector1 { get; } = new();
    private static FraudDetector2 FraudDetector2 { get; } = new();
    private static FraudDetector3 FraudDetector3 { get; } = new();
    
    public static async Task<bool> StartFraudDetection(Transaction transaction, RScrapbook scrapbook, Context context)
    {
        var fraudDetector1 = FraudDetector1.Approve(transaction);
        var fraudDetector2 = FraudDetector2.Approve(transaction);
        var fraudDetector3 = FraudDetector3.Approve(transaction);

        var approvals = await TaskHelper.CompletesWithin(
            withinTimeSpan: TimeSpan.FromSeconds(2),
            fraudDetector1, fraudDetector2, fraudDetector3
        );

        return approvals.Count >= 2 && approvals.All(approved => approved);
    }
}