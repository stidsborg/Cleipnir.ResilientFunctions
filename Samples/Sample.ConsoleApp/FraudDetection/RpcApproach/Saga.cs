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
    
    public static async Task<bool> StartFraudDetection(Transaction transaction, Context context)
    {
        var fraudDetector1 = FraudDetector1.Approve(transaction);
        var fraudDetector2 = FraudDetector2.Approve(transaction);
        var fraudDetector3 = FraudDetector3.Approve(transaction);
        var timeout = Task.Delay(TimeSpan.FromSeconds(2));

        var resultsOrTimeouts = await Task.WhenAll(
            Task.WhenAny(fraudDetector1, timeout),
            Task.WhenAny(fraudDetector2, timeout),
            Task.WhenAny(fraudDetector3, timeout)
        );

        var results = resultsOrTimeouts.Where(t => t is Task<bool>).Cast<Task<bool>>();

        var approvals = results.Select(t => t.Result).ToList();
        return approvals.Count >= 2 && approvals.All(approved => approved);
    }
}