using System;
using Cleipnir.ResilientFunctions.Messaging.Core;
using Cleipnir.ResilientFunctions.Messaging.Core.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public static class EventSourcesModule
{
    public static IServiceCollection AddEventSources(
        this IServiceCollection services, 
        IEventStore store,
        TimeSpan? defaultPullFrequency = null,
        IEventSerializer? eventSerializer = null
    )
    {
        services.AddSingleton(store);
        services.AddSingleton(sp =>
        {
            var eventStore = sp.GetRequiredService<IEventStore>();
            eventStore.Initialize().Wait();
            return new EventSources(eventStore, defaultPullFrequency, eventSerializer);
        });
        
        return services;
    }
}