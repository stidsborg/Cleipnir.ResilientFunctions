using System;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public interface IDependencyResolver
{
    IScopedDependencyResolver CreateScope();
}

public interface IScopedDependencyResolver : IDisposable
{
    T Resolve<T>() where T : notnull;
}