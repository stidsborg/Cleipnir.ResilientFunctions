using System.Reflection;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.PostgreSQL;
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
        return Cleipnir.ResilientFunctions.AspNetCore.Core.RFunctionsModule.UseResilientFunctions(
            services,
            functionStore,
            options,
            gracefulShutdown,
            rootAssembly ?? Assembly.GetCallingAssembly(),
            initializeDatabase
        );
    }
}