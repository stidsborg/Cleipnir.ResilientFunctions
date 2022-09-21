using System.Reflection;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.Messaging.PostgreSQL;
using Cleipnir.ResilientFunctions.PostgreSQL;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore.Postgres;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services, 
        string connectionString,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null,
        bool initializeStores = true
    )
    {
        var functionStore = new PostgreSqlFunctionStore(connectionString);
        var eventStore = new PostgreSqlEventStore(connectionString);
        return Cleipnir.ResilientFunctions.AspNetCore.Core.RFunctionsModule.AddRFunctionsService(
            services,
            functionStore,
            eventStore,
            options,
            gracefulShutdown,
            rootAssembly,
            initializeStores
        );
    }
}