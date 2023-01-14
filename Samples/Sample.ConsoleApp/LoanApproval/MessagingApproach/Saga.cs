using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain.Events;
using Cleipnir.ResilientFunctions.Reactive;

namespace ConsoleApp.LoanApproval.MessagingApproach;

public static class Saga
{
    public static async Task ApproveLoan(LoanApplication loanApplication, Context context)
    {
        var eventSource = await context.EventSource;
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        await eventSource.TimeoutProvider.RegisterTimeout(
            timeoutId: "Timeout",
            expiresIn: loanApplication.Created.AddMinutes(15)
        );

        var outcomes = await eventSource
            .Take(3)
            .TakeUntil(next => next is Timeout)
            .OfType<CreditCheckOutcome>()
            .ToList();
        
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