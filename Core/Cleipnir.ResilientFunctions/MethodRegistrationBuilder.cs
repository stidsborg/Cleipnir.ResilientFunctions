using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.InnerDecorators;
using Cleipnir.ResilientFunctions.Invocation;

namespace Cleipnir.ResilientFunctions;

public class MethodRegistrationBuilder<TEntity> where TEntity : notnull
{
    private readonly RFunctions _rFunctions;

    public MethodRegistrationBuilder(RFunctions rFunctions) => _rFunctions = rFunctions;

    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TReturn>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        entityFactory,
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<TReturn>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        entityFactory,
        settings
    );
    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result<TReturn>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        entityFactory,
        settings        
    );

    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull => _rFunctions.RegisterMethodFunc(functionTypeId, inner, version, entityFactory, settings);

    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings
        );
    
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings
        );

    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null
    ) where TParam : notnull 
        => _rFunctions.RegisterMethodAction(functionTypeId, inner, version, entityFactory, settings);

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => _rFunctions.RegisterMethodFunc(
            functionTypeId,
            inner,
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, TScrapbook>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );
    
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            entityFactory,
            settings,
            concreteScrapbookType
        );

    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> inner,
        int version = 0,
        Func<EntityAndScope<TEntity>>? entityFactory = null,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => _rFunctions.RegisterMethodAction(functionTypeId, inner, version, entityFactory, settings, concreteScrapbookType);
}