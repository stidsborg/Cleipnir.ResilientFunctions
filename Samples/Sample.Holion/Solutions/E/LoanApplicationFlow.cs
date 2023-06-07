using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Reactive;
using Sample.Holion.E;
using Timeout = Cleipnir.ResilientFunctions.Domain.Events.Timeout;

namespace Sample.Holion.Solutions.E;

public class LoanApplicationFlow : Flow<LoanApplication, RScrapbook>
{
    public override async Task Run(LoanApplication loanApplication)
    {
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        await EventSource.TimeoutProvider.RegisterTimeout(
            timeoutId: "Timeout",
            expiresIn: loanApplication.Created.AddMinutes(15)
        );

        var outcomes = await EventSource
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