using System;
using System.Net.Http;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.SqlServer;
using Serilog;

namespace ConsoleApp.Simple;

public static class ElevatorPitchExample
{
    private static readonly HttpClient HttpClient = new HttpClient();
    
    public static async Task ElevatorPitch(string connectionString)
    {
        var store = new SqlServerFunctionStore(connectionString); //simple to use SqlServer as function storage layer - other stores also exist!
        await store.Initialize(); //create table in database - btw the invocation is idempotent!

        var functionsRegistry = new FunctionsRegistry( //this is where you register different resilient function types
            store,
            new Settings(
                unhandledExceptionHandler: //framework exceptions are simply to log and handle otherwise - just register a handler
                e => Log.Error(e, "Resilient Function Framework exception occured"),
                leaseLength: TimeSpan.FromMinutes(1), // you are in control deciding the sweet spot 
                postponedCheckFrequency: TimeSpan.FromMinutes(1) // between quick reaction and pressure on the function store
            )
        );

        var registration = functionsRegistry.RegisterFunc( //making a function resilient is simply a matter of registering it
            functionTypeId: "HttpGetSaga", //a specific resilient function is identified by type and instance id - instance id is provided on invocation
            inner: async Task<string>(string url) => await HttpClient.GetStringAsync(url) //this is the function you are making resilient!
        ); //btw no need to define a cluster - just register it on multiple nodes to get redundancy!
           //also any crashed invocation of the function type will automatically be picked after this point

        var rFunc = registration.Invoke; //you can also re-invoke (useful for manual handling) an existing function or schedule one for invocation
        const string url = "https://google.com";
        var responseBody = await rFunc(functionInstanceId: "google", param: url); //invoking the function - btw you can F11-debug from here into your registered function
        Log.Information("Resilient Function getting {Url} completed successfully with body: {Body}", url, responseBody);
        
        await functionsRegistry.ShutdownGracefully(); //waits for currently invoking functions to complete before shutdown - otw just do not await!
    }
}

