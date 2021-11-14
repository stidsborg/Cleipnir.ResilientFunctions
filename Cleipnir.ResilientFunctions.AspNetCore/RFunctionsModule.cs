using System.Reflection;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services, 
        Func<IServiceProvider, IFunctionStore> store,
        Func<IServiceProvider, Action<RFunctionException>> unhandledExceptionHandler,
        TimeSpan? unhandledFunctionsCheckFrequency = null
    )
    {
        services.AddSingleton(store);
        services.AddSingleton(unhandledExceptionHandler);
        services.AddSingleton(s => RFunctions.Create(
            s.GetRequiredService<IFunctionStore>(),
            s.GetRequiredService<Action<RFunctionException>>(),
            unhandledFunctionsCheckFrequency
            )
        );
        
        var callingAssembly = Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, callingAssembly));
        
        return services;
    }
    
    public static IServiceCollection AddRFunctionsService<TFunctionStore>(
        this IServiceCollection services,
        Func<IServiceProvider, Action<RFunctionException>> unhandledExceptionHandler,
        TimeSpan? unhandledFunctionsCheckFrequency = null
    ) where TFunctionStore : class, IFunctionStore
    {
        services.AddSingleton<IFunctionStore, TFunctionStore>();
        services.AddSingleton(unhandledExceptionHandler);
        services.AddSingleton(s => 
            RFunctions.Create(
                s.GetRequiredService<IFunctionStore>(),
                s.GetRequiredService<Action<RFunctionException>>(),
                unhandledFunctionsCheckFrequency
            )
        );

        services.AddHostedService<RFunctionsService>();
        return services;
    }
}