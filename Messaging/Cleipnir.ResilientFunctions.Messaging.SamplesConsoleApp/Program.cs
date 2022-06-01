using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
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
            
            var saga = new Saga(eventSources);
            var registration = rFunctions.RegisterAction<string, Saga.Scrapbook>(
                "OrderProcessing", saga.ProcessOrder
            );

            var orderId = "1234ABC";
            _ =  registration.Invoke(orderId, orderId);

            await Task.Delay(1_000);
            await eventStore.AppendEvent(
                new FunctionId("OrderProcessing", orderId),
                "hello world"
            );

            await Task.Delay(100);
            Console.WriteLine("PRESS ENTER TO EXIT");
            Console.ReadLine();
        }
    }
}