using System.Reflection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Cleipnir.ResilientFunctions.AspNetCore;

public class RFunctionsService : IHostedService
{
    private readonly IServiceProvider _services;
    private readonly Assembly _callingAssembly;

    public RFunctionsService(IServiceProvider services, Assembly callingAssembly)
    {
        _services = services;
        _callingAssembly = callingAssembly;
    }

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var iRegisterRFuncOnInstantiationTypes = _callingAssembly
            .GetReferencedAssemblies()
            .Select(Assembly.Load)
            .Concat(new[] {_callingAssembly})
            .SelectMany(a => a.GetTypes())
            .Where(t => t.GetInterfaces().Contains(typeof(IRegisterRFuncOnInstantiation)));

        foreach (var iRegisterRFuncOnInstantiationType in iRegisterRFuncOnInstantiationTypes)
            _ = _services.GetService(iRegisterRFuncOnInstantiationType);

        var rFunctions = _services.GetRequiredService<RFunctions>();
        var iRegisterRFuncs = _services.GetServices<IRegisterRFunc>();

        foreach (var iRegisterRFunc in iRegisterRFuncs)
            iRegisterRFunc.RegisterRFunc(rFunctions);

        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        _services.GetRequiredService<RFunctions>().Dispose();
        return Task.CompletedTask;
    } 
}