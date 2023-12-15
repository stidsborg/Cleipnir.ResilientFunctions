using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using ConsoleApp.Utils;

namespace ConsoleApp.FraudDetection.RpcApproach;

public static class Saga
{
    private static FraudDetector1 FraudDetector1 { get; } = new();
    private static FraudDetector2 FraudDetector2 { get; } = new();
    private static FraudDetector3 FraudDetector3 { get; } = new();
    
    public static async Task<bool> StartFraudDetection(Transaction transaction, Context context)
    {
        var fraudDetector1 = FraudDetector1.Approve(transaction, timeout: TimeSpan.FromSeconds(5));
        var fraudDetector2 = FraudDetector2.Approve(transaction, timeout: TimeSpan.FromSeconds(5));
        var fraudDetector3 = FraudDetector3.Approve(transaction, timeout: TimeSpan.FromSeconds(5));

        var tasks = new[] { fraudDetector1, fraudDetector2, fraudDetector3 };
        await Safe.Try(() => Task.WhenAll(tasks));

        return tasks.Count(t => t is { IsCompletedSuccessfully: true, Result: true }) >= 2;
    }
}