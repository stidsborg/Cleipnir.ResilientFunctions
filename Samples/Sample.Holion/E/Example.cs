namespace Sample.Holion.E;

public class Example
{
    public static async Task Perform(Flows flows)
    {
        var loanApplicationFlows = flows.LoanApplicationFlows;
        
        CreditChecker1.Start();
        CreditChecker2.Start();
        CreditChecker3.Start();
        
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is CreditCheckOutcome creditCheckOutcome)
            {
                var writer = loanApplicationFlows.EventSourceWriter(creditCheckOutcome.LoanApplicationId);
                await writer.AppendEvent(creditCheckOutcome);
            }
        });
        
        var loanApplication = new LoanApplication(
            Id: "someId",
            CustomerId: Guid.NewGuid(),
            Amount: 5200.00M,
            Created: DateTime.UtcNow
        );

        await loanApplicationFlows.Run(loanApplication.Id, loanApplication);
    }
}