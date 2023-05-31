using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Sample.Holion.A.Ordering;
using Sample.Holion.A.Ordering.Clients;

namespace Sample.Holion.Utils;

public class DependencyResolver : IDependencyResolver
{
    public static DependencyResolver Instance { get; } = new();
    private class ScopedDependencyResolver : IScopedDependencyResolver
    {
        public T Resolve<T>() where T : notnull
        {
            if (typeof(T) == typeof(OrderFlow))
                return (T) (object) new OrderFlow(new PaymentProviderClientStub(), new EmailClientStub(), new LogisticsClientStub());

            throw new ArgumentException($"Unable to resolve type: {typeof(T)}");
        }
        
        public void Dispose() { }
    }
    
    public IScopedDependencyResolver CreateScope()
    {
        return new ScopedDependencyResolver();
    }
}