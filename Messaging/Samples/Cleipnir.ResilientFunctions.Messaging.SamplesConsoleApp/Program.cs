using System.Text.Json;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Clients;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Domain;
using Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp.Saga;
using Cleipnir.ResilientFunctions.PostgreSQL;

namespace Cleipnir.ResilientFunctions.Messaging.SamplesConsoleApp
{
    internal static class Program
    {
        private static async Task Main()
        {
            var connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=rfunctions;";

            await DatabaseHelper.RecreateDatabase(connectionString);
            
            var functionStore = new PostgreSqlFunctionStore(connectionString);
            await functionStore.Initialize();
            var rFunctions = new RFunctions(functionStore);
            
            var eventStore = new PostgreSqlEventStore(connectionString);
            var eventSources = new EventSources(eventStore);
            await eventSources.Initialize();
            
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

            _ = saga.ProcessOrder(
                new Order(
                    "MK-12345",
                    CustomerEmail: "coolness@cleipnir.net",
                    ProductIds: new[] {"THR-123", "BLDR-549"})
            );

            await Task.Delay(100);
            Console.WriteLine("PRESS ENTER TO EXIT");
            Console.ReadLine();
        }
    }
}