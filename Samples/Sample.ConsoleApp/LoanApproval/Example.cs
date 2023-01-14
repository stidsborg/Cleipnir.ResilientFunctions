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
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var rFunc = functions
            .RegisterFunc<LoanApplication, RScrapbook, bool>(
                "LoanApproval",
                RpcApproach.Saga.ApproveLoan
            ).Invoke;

        var transaction = new LoanApplication(
            Id: "someId",
            CustomerId: Guid.NewGuid(),
            Amount: 5200.00M,
            Created: DateTime.UtcNow
        );
        
        var transactionApproved = await rFunc(
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
        
        var functions = new RFunctions(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterAction<LoanApplication>(
                functionTypeId: "LoanApproval",
                Saga.ApproveLoan
            );
        var rFunc = registration.Invoke;

        var eventSourceWriters = registration.EventSourceWriters;
        MessageBroker.Subscribe(async @event =>
        {
            if (@event is CreditCheckOutcome creditCheckOutcome)
            {
                var writer = eventSourceWriters.For(creditCheckOutcome.LoanApplicationId);
                await writer.AppendEvent(creditCheckOutcome);
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