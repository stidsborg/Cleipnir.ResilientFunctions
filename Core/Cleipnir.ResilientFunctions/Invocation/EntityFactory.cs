using System;

namespace Cleipnir.ResilientFunctions.Invocation;

public interface IEntityFactory
{
    ScopedEntity<T> Create<T>() where T : notnull;
}

public class ScopedEntity<T> : IDisposable
{
    private Action DisposeScope { get; }
    public T Entity { get; }

    public ScopedEntity(T entity, Action disposeScope)
    {
        Entity = entity;
        DisposeScope = disposeScope;
    }
    
    public void Dispose() => DisposeScope();
}
