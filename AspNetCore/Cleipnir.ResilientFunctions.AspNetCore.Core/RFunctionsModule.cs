using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Cleipnir.ResilientFunctions.AspNetCore.Core;

public static class RFunctionsModule
{
    public static IServiceCollection UseResilientFunctionsWithInMemoryStore(
        this IServiceCollection services,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    )
    {
        var inMemoryFunctionStore = new InMemoryFunctionStore();
        return UseResilientFunctions(
            services,
            inMemoryFunctionStore,
            options,
            gracefulShutdown,
            rootAssembly,
            initializeDatabase: false
        );   
    }

    public static IServiceCollection UseResilientFunctions(
        IServiceCollection services,
        IFunctionStore functionStore,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null,
        bool initializeDatabase = true
    )
    {
        if (initializeDatabase)
            functionStore.Initialize().GetAwaiter().GetResult();

        if (options != null)
            services.AddSingleton(options);
        services.AddSingleton<ServiceProviderDependencyResolver>();
        services.TryAddSingleton<IHttpContextAccessor, HttpContextAccessor>();
        services.AddSingleton(functionStore);

        services.AddSingleton(sp =>
        {
            var dependencyResolver = sp.GetRequiredService<ServiceProviderDependencyResolver>();
            var resolvedSettings = 
            (
                sp.GetService<Options>() 
                ?? new Options()
            ).MapToRFunctionsSettings(dependencyResolver);

            return new RFunctions(functionStore, resolvedSettings);
        });

        rootAssembly ??= Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, rootAssembly, gracefulShutdown));
        
        return services;
    }
}