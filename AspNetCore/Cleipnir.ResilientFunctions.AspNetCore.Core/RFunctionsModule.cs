﻿using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Storage;
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
        Assembly? rootAssembly = null,
        bool initializeStores = true
    ) => UseResilientFunctions(
            services,
            new InMemoryFunctionStore(),
            new InMemoryEventStore(),
            options,
            gracefulShutdown,
            rootAssembly,
            initializeStores
        );

    public static IServiceCollection UseResilientFunctions(
        IServiceCollection services,
        IFunctionStore functionStore,
        IEventStore eventStore,
        Func<IServiceProvider, Options>? options = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null,
        bool initializeStores = true
    )
    {
        if (initializeStores)
        {
            functionStore.Initialize().Wait();
            eventStore.Initialize().Wait();
        }

        if (options != null)
            services.AddSingleton(options);
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
        
        services.AddSingleton(sp =>
            {
                var o = sp.GetService<Options>();
                return new EventSources(
                    sp.GetRequiredService<IEventStore>(),
                    sp.GetRequiredService<RFunctions>(),
                    o?.DefaultEventsCheckFrequency,
                    o?.EventSerializer
                );
            } 
            
        );
        
        rootAssembly ??= Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, rootAssembly, gracefulShutdown));
        
        return services;
    }
}