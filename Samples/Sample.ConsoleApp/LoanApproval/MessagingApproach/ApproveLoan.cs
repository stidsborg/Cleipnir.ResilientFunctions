using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Reactive.Extensions;

namespace ConsoleApp.LoanApproval.MessagingApproach;

public static class ApproveLoan
{
    public static async Task Execute(LoanApplication loanApplication, Workflow workflow)
    {
        var messages = workflow.Messages;
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        var outcomes = await messages
            .OfType<CreditCheckOutcome>()
            .Take(3)
            .TakeUntilTimeout("TimeoutId", expiresAt: loanApplication.Created.AddMinutes(15))
            .SuspendUntilCompletion();

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