using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.PostgreSQL;

namespace ConsoleApp.Ticking;

public static class TickExample
{
    public static async Task Do()
    {
        const string connectionString = "Server=localhost;Port=5432;Userid=postgres;Password=Pa55word!;Database=flows;";
        await DatabaseHelper.CreateDatabaseIfNotExists(connectionString);
        var store = new PostgreSqlFunctionStore(connectionString, "tickering_flow");
        await store.Initialize();
        
        var registry = new FunctionsRegistry(store, new Settings(unhandledExceptionHandler: Console.WriteLine, leaseLength: TimeSpan.FromSeconds(1)));
        
        var registration = registry.RegisterAction(
            flowType: "Tick",
            inner: async (string param, Workflow workflow) =>
            {
                var state = await workflow.States.CreateOrGetDefault<State>();
                
                while (true)
                {
                    await Task.Delay(1_000);
                    Console.WriteLine($"[{param}]: #{state.I} ticked...");
                    state.I++;
                    await state.Save();
                }
            }
        );

        await registration.Invoke(flowInstance: "TICKER1", param: "TICKER1");
    }
    
    private class State : FlowState
    {
        public int I { get; set; }
    }
}