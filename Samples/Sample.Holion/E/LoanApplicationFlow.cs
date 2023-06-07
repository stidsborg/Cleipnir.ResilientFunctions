using Cleipnir.ResilientFunctions.Domain;

namespace Sample.Holion.E;

public class LoanApplicationFlow : Flow<LoanApplication, RScrapbook>
{
    public override async Task Run(LoanApplication loanApplication)
    {
        await MessageBroker.Send(new PerformCreditCheck(loanApplication.Id, loanApplication.CustomerId, loanApplication.Amount));

        await EventSource.TimeoutProvider.RegisterTimeout(
            timeoutId: "Timeout",
            expiresIn: loanApplication.Created.AddMinutes(15)
        );

        throw new NotImplementedException();
    }
}