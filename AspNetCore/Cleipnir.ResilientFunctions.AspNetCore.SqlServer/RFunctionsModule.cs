using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.AspNetCore.Core;
using Cleipnir.ResilientFunctions.SqlServer;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore.SqlServer;

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
        var functionStore = new SqlServerFunctionStore(connectionString);
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