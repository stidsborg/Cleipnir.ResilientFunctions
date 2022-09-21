using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore.Core;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services, 
        Func<IServiceProvider, IFunctionStore> store,
        Func<IServiceProvider, Settings>? settings = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    )
    {
        services.AddSingleton(store);
        if (settings != null)
            services.AddSingleton(settings);
        services.AddSingleton<ServiceProviderDependencyResolver>();
        services.AddSingleton(s =>
        {
            var functionStore = s.GetRequiredService<IFunctionStore>();
            functionStore.Initialize().Wait();
            var dependencyResolver = s.GetRequiredService<ServiceProviderDependencyResolver>();
            var resolvedSettings = (s.GetService<Settings>() ?? new Settings()).MapToRFunctionsSettings(dependencyResolver);

            return new RFunctions(functionStore, resolvedSettings);
        });
        
        rootAssembly ??= Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, rootAssembly, gracefulShutdown));
        
        return services;
    }

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        Func<IServiceProvider, Settings>? settings = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    ) => AddRFunctionsService(
        services,
        _ => store,
        settings,
        gracefulShutdown,
        rootAssembly: rootAssembly ?? Assembly.GetCallingAssembly()
    );

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        Settings? settings = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    ) => AddRFunctionsService(
        services,
        _ => store,
        settings == null ? null : _ => settings,
        gracefulShutdown,
        rootAssembly: rootAssembly ?? Assembly.GetCallingAssembly()
    );
    
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        Func<IServiceProvider, IFunctionStore> store,
        Settings? settings = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    ) => AddRFunctionsService(
        services,
        store,
        settings == null ? null : _ => settings,
        gracefulShutdown,
        rootAssembly: rootAssembly ?? Assembly.GetCallingAssembly()
    );
}