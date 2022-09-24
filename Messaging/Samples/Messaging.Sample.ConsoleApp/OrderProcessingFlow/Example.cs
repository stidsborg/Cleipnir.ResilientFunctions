using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Clients;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Domain;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow.Saga;
using Cleipnir.ResilientFunctions.PostgreSQL;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.OrderProcessingFlow;

public static class Example
{
    public static async Task Perform()
    {
        var connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions;";

        await DatabaseHelper.RecreateDatabase(connectionString);
            
        var functionStore = new PostgreSqlFunctionStore(connectionString);
        await functionStore.Initialize();
        var rFunctions = new RFunctions(functionStore);
        var eventStore = new PostgreSqlEventStore(connectionString);
        await eventStore.Initialize();
        var eventSources = new EventSources(eventStore, rFunctions: null);

        //clients
        var messageQueue = new MessageQueueClient();
        var saga = new OrderProcessingSaga(
            rFunctions,
            eventSources,
            new BankClientStub(),
            new EmailClientStub(),
            messageQueue,
            new ProductsClientStub()
        );
        messageQueue.Saga = saga;

        await saga.ProcessOrder(
            new Order(
                "MK-12345",
                CustomerEmail: "coolness@cleipnir.net",
                ProductIds: new[] {"THR-123", "BLDR-549"})
        );

        Console.WriteLine("ORDER PROCESSING COMPLETED");
    }
}