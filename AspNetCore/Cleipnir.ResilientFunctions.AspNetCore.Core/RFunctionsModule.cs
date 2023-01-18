using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Messaging;
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
            inMemoryFunctionStore,
            options,
            gracefulShutdown,
            rootAssembly,
            initializeDatabase: false,
            arbitrator: new InMemoryArbitrator(),
            monitor: new InMemoryMonitor()
        );   
    }

    public static IServiceCollection UseResilientFunctions(
        IServiceCollection services,
        IFunctionStore functionStore,
        IEventStore eventStore,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null,
        bool initializeDatabase = true,
        IArbitrator? arbitrator = null,
        IMonitor? monitor = null
    )
    {
        if (initializeDatabase)
        {
            functionStore.Initialize().GetAwaiter().GetResult();
            eventStore.Initialize().GetAwaiter().GetResult();
        }

        if (options != null)
            services.AddSingleton(options);
        if (arbitrator != null)
            services.AddSingleton(arbitrator);
        if (monitor != null)
            services.AddSingleton(monitor);
        services.AddSingleton<ServiceProviderDependencyResolver>();
        services.TryAddSingleton<IHttpContextAccessor, HttpContextAccessor>();
        services.AddSingleton(functionStore);
        services.AddSingleton(eventStore);

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