using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Sample.Holion.Ordering;
using Sample.Holion.Utils;
using Serilog;

namespace Sample.Holion;

internal class Program
{
    public static async Task Main(string[] args)
    {
        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=holion;";
        //await DatabaseHelper.RecreateDatabase(connectionString);
        var store = new PostgreSqlFunctionStore(connectionString);
        await store.Initialize();
        var flows = new Flows(
            store,
            new Settings(
                unhandledExceptionHandler: e => Log.Logger.Error(e, "Framework thrown exception"),
                dependencyResolver: DependencyResolver.Instance,
                crashedCheckFrequency: TimeSpan.FromSeconds(2)
            )
        );

        var order = new Order("MK-54321", CustomerId: Guid.NewGuid(), ProductIds: new[] { Guid.NewGuid()}, 100M);
        await flows.OrderFlows.Run(order.OrderId, order);

        Console.ReadLine();

    }
}