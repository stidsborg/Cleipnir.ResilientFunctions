using System;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class FuncDependencyResolver : IDependencyResolver
{
    private readonly Func<Type, object> _funcResolver;

    public FuncDependencyResolver(Func<Type, object> funcResolver) => _funcResolver = funcResolver;

    public IScopedDependencyResolver CreateScope() => new ScopedDependencyResolver(_funcResolver);

    private class ScopedDependencyResolver : IScopedDependencyResolver
    {
        private readonly Func<Type, object> _resolver;
        public ScopedDependencyResolver(Func<Type, object> resolver) => _resolver = resolver;

        public T Resolve<T>() where T : notnull => (T) _resolver(typeof(T));

        public void Dispose() { }
    }
}