using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.LoanApproval.MessagingApproach;

namespace ConsoleApp.LoanApproval;

public static class Example
{
    public static async Task PerformRpcApproach()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var approveLoanFunc = functions
            .RegisterFunc<LoanApplication, bool>(
                "LoanApproval",
                RpcApproach.PerformLoan.Execute
            ).Invoke;

        var transaction = new LoanApplication(
            Id: "someId",
            CustomerId: Guid.NewGuid(),
            Amount: 5200.00M,
            Created: DateTime.UtcNow
        );
        
        var transactionApproved = await approveLoanFunc(
            transaction.Id,
            transaction
        );
        
        Console.WriteLine($"Transaction was{(transactionApproved ? "" : "not" )} approved");
    }
    
    public static async Task PerformMessagingApproach()
    {
        CreditChecker1.Start();
        CreditChecker2.Start();
        CreditChecker3.Start();
        
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<LoanApplication>(
                flowType: "LoanApproval",
                ApproveLoan.Execute
            );
        var rFunc = registration.Invoke;

        var messageWriters = registration.MessageWriters;
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is CreditCheckOutcome creditCheckOutcome)
            {
                var writer = messageWriters.For(creditCheckOutcome.LoanApplicationId.ToStoredId(registration.StoredType));
                await writer.AppendMessage(creditCheckOutcome);
            }
        });
        
        var loanApplication = new LoanApplication(
            Id: "someId",
            CustomerId: Guid.NewGuid(),
            Amount: 5200.00M,
            Created: DateTime.UtcNow
        );
        
        await rFunc(loanApplication.Id, loanApplication);
    }
}