using System;
using Cleipnir.ResilientFunctions.Invocation;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public class ServiceProviderEntityFactory : IEntityFactory
{
    private readonly IHttpContextAccessor _httpContext;
    private readonly IServiceProvider _serviceProvider;

    public ServiceProviderEntityFactory(IHttpContextAccessor httpContext, IServiceProvider serviceProvider)
    {
        _httpContext = httpContext;
        _serviceProvider = serviceProvider;
    }

    public ScopedEntity<T> Create<T>() where T : notnull
    {
        if (_httpContext.HttpContext != null)
            return new ScopedEntity<T>(
                _httpContext.HttpContext.RequestServices.GetRequiredService<T>(),
                disposeScope: () => {}
            );
                
        var scope = _serviceProvider.CreateScope();
        var t = scope.ServiceProvider.GetRequiredService<T>();
        return new ScopedEntity<T>(t, scope.Dispose);
    }
}