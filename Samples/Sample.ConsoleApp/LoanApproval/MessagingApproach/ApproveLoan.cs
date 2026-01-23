using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

namespace ConsoleApp.LoanApproval.MessagingApproach;

public static class ApproveLoan
{
    public static async Task Execute(LoanApplication loanApplication, Workflow workflow)
    {
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        var timeout = await workflow.UtcNow() + TimeSpan.FromMinutes(15);

        var outcomes = new List<CreditCheckOutcome>();
        for (var i = 0; i < 3; i++)
        {
            var outcome = await workflow.Message<CreditCheckOutcome>(waitUntil: timeout);
            if (outcome == null)
                break;
            
            outcomes.Add(outcome);
        }

        if (outcomes.Count < 2)
            await MessageBroker.Send(new LoanApplicationRejected(loanApplication));
        else
            await MessageBroker.Send(
                outcomes.All(o => o.Approved)
                    ? new LoanApplicationApproved(loanApplication)
                    : new LoanApplicationRejected(loanApplication)
            );
    }
}