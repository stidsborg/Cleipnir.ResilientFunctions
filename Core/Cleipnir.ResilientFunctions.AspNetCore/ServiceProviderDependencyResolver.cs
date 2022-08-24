using System;
using Cleipnir.ResilientFunctions.Invocation;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public class ServiceProviderDependencyResolver : IDependencyResolver
{
    private readonly IHttpContextAccessor _httpContext;
    private readonly IServiceProvider _serviceProvider;

    public ServiceProviderDependencyResolver(IHttpContextAccessor httpContext, IServiceProvider serviceProvider)
    {
        _httpContext = httpContext;
        _serviceProvider = serviceProvider;
    }

    public IScopedDependencyResolver CreateScope()
        => _httpContext.HttpContext != null
            ? new HttpContextScopeResolver(_httpContext.HttpContext)
            : new ServiceScopeResolver(_serviceProvider.CreateScope());

    private class ServiceScopeResolver : IScopedDependencyResolver
    {
        private readonly IServiceScope _serviceScope;
        public ServiceScopeResolver(IServiceScope serviceScope) => _serviceScope = serviceScope;

        public T Resolve<T>() where T : notnull => _serviceScope.ServiceProvider.GetRequiredService<T>();
        public void Dispose() => _serviceScope.Dispose();
    }

    private class HttpContextScopeResolver : IScopedDependencyResolver
    {
        private readonly HttpContext _httpContext;
        public HttpContextScopeResolver(HttpContext httpContext) => _httpContext = httpContext;

        public T Resolve<T>() where T : notnull => _httpContext.RequestServices.GetRequiredService<T>();
        public void Dispose() {}
    }
}