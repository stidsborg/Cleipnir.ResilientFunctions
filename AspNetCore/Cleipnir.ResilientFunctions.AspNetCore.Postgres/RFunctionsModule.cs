using System.Reflection;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Cleipnir.ResilientFunctions.PostgreSQL.Utils;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore.Postgres;

public static class RFunctionsModule
{
    public static IServiceCollection UseResilientFunctions(
        this IServiceCollection services, 
        string connectionString,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null,
        bool initializeDatabase = true
    )
    {
        var functionStore = new PostgreSqlFunctionStore(connectionString);
        var eventStore = new PostgreSqlEventStore(connectionString);
        var arbitrator = new Arbitrator(connectionString);
        var monitor = new  Cleipnir.ResilientFunctions.PostgreSQL.Utils.Monitor(connectionString);
        if (initializeDatabase)
        {
            arbitrator.Initialize().GetAwaiter().GetResult();
            monitor.Initialize().GetAwaiter().GetResult();
        }
        
        return Cleipnir.ResilientFunctions.AspNetCore.Core.RFunctionsModule.UseResilientFunctions(
            services,
            functionStore,
            eventStore,
            options,
            gracefulShutdown,
            rootAssembly ?? Assembly.GetCallingAssembly(),
            initializeDatabase
        );
    }
}