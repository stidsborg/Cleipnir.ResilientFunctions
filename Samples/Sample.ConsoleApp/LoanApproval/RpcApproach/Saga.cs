﻿using System;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Org.BouncyCastle.Crypto.Parameters;

namespace ConsoleApp.LoanApproval.RpcApproach;

public static class Saga
{
    private static CreditChecker1 CreditChecker1 { get; } = new();
    private static CreditChecker2 CreditChecker2 { get; } = new();
    private static CreditChecker3 CreditChecker3 { get; } = new();
    
    public static async Task<bool> ApproveLoan(LoanApplication loanApplication)
    {
        var fraudDetector1 = CreditChecker1.Approve(loanApplication);
        var fraudDetector2 = CreditChecker2.Approve(loanApplication);
        var fraudDetector3 = CreditChecker3.Approve(loanApplication);

        await Task.WhenAny(
            Task.WhenAll(fraudDetector1, fraudDetector2, fraudDetector3),
            Task.Delay(TimeSpan.FromMinutes(10))
        );

        var approvals = new[] { fraudDetector1, fraudDetector2, fraudDetector3 }
            .Where(task => task.IsCompletedSuccessfully)
            .Select(task => task.Result)
            .ToList();
        
        return approvals.Count >= 2 && approvals.All(approved => approved);
    }
}