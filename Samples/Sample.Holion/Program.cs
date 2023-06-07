using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Sample.Holion.Utils;
using Serilog;

namespace Sample.Holion;

internal class Program
{
    private const string ConnectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=holion;";
    
    public static async Task RecreateDatabase()
        => await DatabaseHelper.RecreateDatabase(ConnectionString);
    
    public static async Task Main(string[] args)
    {
        await RecreateDatabase();
        
        var store = new PostgreSqlFunctionStore(ConnectionString);
        await store.Initialize();
        var flows = new Flows(
            store,
            new Settings(
                unhandledExceptionHandler: e => Log.Logger.Error(e, "Framework thrown exception"),
                dependencyResolver: DependencyResolver.Instance,
                crashedCheckFrequency: TimeSpan.FromSeconds(2)
            )
        );

        await D.SupportTicket.Example.Perform(flows);
        
        Console.ReadLine();
    }
}