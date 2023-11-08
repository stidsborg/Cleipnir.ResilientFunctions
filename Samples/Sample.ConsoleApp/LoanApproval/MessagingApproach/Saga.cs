using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.LoanApproval.MessagingApproach;

public static class Saga
{
    public static async Task ApproveLoan(LoanApplication loanApplication, Context context)
    {
        var eventSource = await context.EventSource;
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        await eventSource.RegisterTimeoutEvent(
            timeoutId: "Timeout",
            expiresAt: loanApplication.Created.AddMinutes(15)
        );

        var outcomesAndTimeout = await eventSource
            .Chunk(3)
            .SuspendUntilFirst();

        var outcomes = outcomesAndTimeout
            .TakeWhile(e => e is CreditCheckOutcome)
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