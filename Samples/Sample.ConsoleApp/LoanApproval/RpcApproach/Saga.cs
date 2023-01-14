using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;

namespace ConsoleApp.LoanApproval.RpcApproach;

public static class Saga
{
    private static CreditChecker1 CreditChecker1 { get; } = new();
    private static CreditChecker2 CreditChecker2 { get; } = new();
    private static CreditChecker3 CreditChecker3 { get; } = new();
    
    public static async Task<bool> ApproveLoan(LoanApplication loanApplication, RScrapbook scrapbook, Context context)
    {
        var fraudDetector1 = CreditChecker1.Approve(loanApplication);
        var fraudDetector2 = CreditChecker2.Approve(loanApplication);
        var fraudDetector3 = CreditChecker3.Approve(loanApplication);

        var approvals = await TaskHelper.CompletesWithin(
            withinTimeSpan: TimeSpan.FromSeconds(2),
            fraudDetector1, fraudDetector2, fraudDetector3
        );

        return approvals.Count >= 2 && approvals.All(approved => approved);
    }
}