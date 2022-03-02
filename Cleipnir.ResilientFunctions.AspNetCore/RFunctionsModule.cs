using System.Reflection;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.Storage;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public static class RFunctionsModule
{
    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services, 
        Func<IServiceProvider, IFunctionStore> store,
        Func<IServiceProvider, Action<RFunctionException>> unhandledExceptionHandler,
        bool gracefulShutdown = false,
        TimeSpan? crashedFunctionCheckFrequency = null,
        TimeSpan? postponedFunctionCheckFrequency = null
    )
    {
        services.AddSingleton(store);
        services.AddSingleton(unhandledExceptionHandler);
        services.AddSingleton(s => RFunctions.Create(
            s.GetRequiredService<IFunctionStore>(),
            unhandledExceptionHandler: s.GetRequiredService<Action<RFunctionException>>(),
            crashedFunctionCheckFrequency,
            postponedFunctionCheckFrequency
            )
        );
        
        var callingAssembly = Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, callingAssembly, gracefulShutdown));
        
        return services;
    }
    
    public static IServiceCollection AddRFunctionsService<TFunctionStore>(
        this IServiceCollection services,
        Func<IServiceProvider, Action<RFunctionException>> unhandledExceptionHandler,
        bool gracefulShutdown = false,
        TimeSpan? crashedFunctionCheckFrequency = null,
        TimeSpan? postponedFunctionCheckFrequency = null
    ) where TFunctionStore : class, IFunctionStore
    {
        services.AddSingleton<IFunctionStore, TFunctionStore>();
        services.AddSingleton(unhandledExceptionHandler);
        services.AddSingleton(s => 
            RFunctions.Create(
                s.GetRequiredService<IFunctionStore>(),
                unhandledExceptionHandler: s.GetRequiredService<Action<RFunctionException>>(),
                crashedFunctionCheckFrequency,
                postponedFunctionCheckFrequency
            )
        );

        var callingAssembly = Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, callingAssembly, gracefulShutdown));        
        return services;
    }

    public static IServiceCollection AddRFunctionsService(
        this IServiceCollection services,
        IFunctionStore store,
        Func<IServiceProvider, Action<RFunctionException>> unhandledExceptionHandler,
        bool gracefulShutdown = false,
        TimeSpan? crashedFunctionCheckFrequency = null,
        TimeSpan? postponedFunctionCheckFrequency = null
    ) 
    {
        services.AddSingleton(store);
        services.AddSingleton(unhandledExceptionHandler);
        services.AddSingleton(s => 
            RFunctions.Create(
                s.GetRequiredService<IFunctionStore>(),
                unhandledExceptionHandler: s.GetRequiredService<Action<RFunctionException>>(),
                crashedFunctionCheckFrequency,
                postponedFunctionCheckFrequency
            )
        );

        var callingAssembly = Assembly.GetCallingAssembly();
        services.AddHostedService(s => new RFunctionsService(s, callingAssembly, gracefulShutdown));
        return services;
    }
}