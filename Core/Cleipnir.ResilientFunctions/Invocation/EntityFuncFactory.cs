using System;

namespace Cleipnir.ResilientFunctions.Invocation;

public class EntityFuncFactory : IEntityFactory
{
    private readonly Func<Type, object> _createEntity;

    public EntityFuncFactory(Func<Type, object> createEntity) => _createEntity = createEntity;

    public ScopedEntity<T> Create<T>() where T : notnull 
        => new((T)_createEntity(typeof(T)), () => { });
}