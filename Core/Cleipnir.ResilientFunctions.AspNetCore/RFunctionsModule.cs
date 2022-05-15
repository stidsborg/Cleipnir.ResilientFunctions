using System.Reflection;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services, 
        Func<IServiceProvider, IFunctionStore> store,
        Func<IServiceProvider, Settings>? settings = null,
        bool gracefulShutdown = false
    )
    {
        services.AddSingleton(store);
        if (settings != null)
            services.AddSingleton(settings);
        
        services.AddSingleton(s => new RFunctions(
            s.GetRequiredService<IFunctionStore>(),
            s.GetService<Settings>()
            )
        );
        
        var callingAssembly = Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, callingAssembly, gracefulShutdown));
        
        return services;
    }

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        Func<IServiceProvider, Settings>? settings = null,
        bool gracefulShutdown = false
    ) => AddRFunctionsService(services, _ => store, settings, gracefulShutdown);

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        Settings? settings = null,
        bool gracefulShutdown = false
    ) => AddRFunctionsService(
        services,
        _ => store,
        settings == null ? null : _ => settings,
        gracefulShutdown
    );
}