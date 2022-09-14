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

    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, TReturn>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();
    
    // ** ASYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();

    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings        
    ).ConvertToRFuncWithoutScrapbook();
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings        
    ).ConvertToRFuncWithoutScrapbook();

    // ** ASYNC W. RESULT ** //    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();
    
    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task<Result<TReturn>>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull => RegisterFunc(
        functionTypeId,
        InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
        version,
        settings
    ).ConvertToRFuncWithoutScrapbook();

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, Context>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    // ** ASYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );

    // ** SYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );

    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Result>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );

    // ** ASYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result>>> inner,
        int version = 0,
        Settings? settings = null
    ) where TParam : notnull
        => _rFunctions.RegisterMethodAction(
            functionTypeId,
            inner,
            version,
            settings
        );
    
    // ** !! FUNC WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterMethodFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterMethodFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, TReturn>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Result<TReturn>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
   
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => _rFunctions.RegisterMethodFunc(
            functionTypeId,
            inner,
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, TScrapbook>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, TScrapbook, Context>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Result>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );
   
    // ** ASYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            version,
            settings,
            concreteScrapbookType
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result>>> inner,
        int version = 0,
        Settings? settings = null,
        Type? concreteScrapbookType = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => _rFunctions.RegisterMethodAction(functionTypeId, inner, version, settings, concreteScrapbookType);
}