using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using ConsoleApp.FraudDetection.MessagingApproach;
using Saga = ConsoleApp.FraudDetection.RpcApproach.Saga;

namespace ConsoleApp.FraudDetection;

public static class Example
{
    public static async Task PerformRpcApproach()
    {
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var rFunc = functions
            .RegisterFunc<Transaction, bool>(
                "FraudDetection",
                Saga.StartFraudDetection
            ).Invoke;

        var transaction = new Transaction(
            Id: "someId",
            Sender: Guid.NewGuid(),
            Receiver: Guid.NewGuid(),
            Amount: 1200.10M,
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
        FraudDetector1.Start();
        FraudDetector2.Start();
        FraudDetector3.Start();
        
        var store = new InMemoryFunctionStore();
        
        var functions = new FunctionsRegistry(
            store,
            new Settings(unhandledExceptionHandler: Console.WriteLine)
        );

        var registration = functions
            .RegisterFunc<Transaction, bool>(
                "FraudDetection",
                Saga.StartFraudDetection
            );
        var rFunc = registration.Invoke;

        var messageWriters = registration.MessageWriters;
        MessageBroker.Subscribe(async events =>
        {
            switch (events)
            {
                case TransactionApproved transactionApproved:
                {
                    var writer = messageWriters.For(transactionApproved.Transaction.Id.ToFlowInstance());
                    await writer.AppendMessage(transactionApproved);
                    break;
                }
                case TransactionDeclined transactionDeclined:
                {
                    var writer = messageWriters.For(transactionDeclined.Transaction.Id.ToFlowInstance());
                    await writer.AppendMessage(transactionDeclined);
                    break;
                }
            }
        });
        
        var transaction = new Transaction(
            Id: "someId",
            Sender: Guid.NewGuid(),
            Receiver: Guid.NewGuid(),
            Amount: 1200.10M,
            Created: DateTime.UtcNow
        );
        
        var transactionApproved = await rFunc(
            transaction.Id,
            transaction
        );

        Console.WriteLine($"Transaction was{(transactionApproved ? "" : "not" )} approved");
    }
}