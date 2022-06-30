using System;
using System.Reflection;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        System.Func<IServiceProvider, IFunctionStore> store,
        System.Func<IServiceProvider, Settings>? settings = null,
        bool gracefulShutdown = false,
        Assembly? rootAssembly = null
    )
    {
        services.AddSingleton(store);
        if (settings != null)
            services.AddSingleton(settings);
        
        services.AddSingleton(s =>
        {
            var functionStore = s.GetRequiredService<IFunctionStore>();
            functionStore.Initialize().Wait();
            return new FunctionContainer(functionStore, s.GetService<Settings>());
        });
        
        rootAssembly ??= Assembly.GetCallingAssembly();
        services.AddHostedService(s => new FunctionsService(s, rootAssembly, gracefulShutdown));
        
        return services;
    }

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        System.Func<IServiceProvider, Settings>? settings = null,
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
}