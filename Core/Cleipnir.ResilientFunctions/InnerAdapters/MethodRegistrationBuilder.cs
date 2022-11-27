using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;

namespace Cleipnir.ResilientFunctions.InnerAdapters;

public class MethodRegistrationBuilder<TEntity> where TEntity : notnull
{
    private readonly RFunctions _rFunctions;

    public MethodRegistrationBuilder(RFunctions rFunctions) => _rFunctions = rFunctions;

    // ** !! FUNC !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );

    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull =>
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );

    // ** ASYNC W. RESULT ** //    
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result<TReturn>>>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );
    
    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TReturn> RegisterFunc<TParam, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task<Result<TReturn>>>> inner,
        Settings? settings = null
    ) where TParam : notnull => 
        new RFunc<TParam, TReturn>(
            RegisterFunc(
                functionTypeId,
                InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
                settings
            )
        );

    // ** !! ACTION !! ** //
    // ** SYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, Context>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Task>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** SYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Result>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Context, Result>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. RESULT ** //
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, Task<Result>>> inner,
        Settings? settings = null
    ) where TParam : notnull 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RAction<TParam> RegisterAction<TParam>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, RScrapbook, Context, Task<Result<Unit>>>> inner,
        Settings? settings = null
    ) where TParam : notnull
        => _rFunctions.RegisterMethodAction(functionTypeId, inner, settings);
    
    // ** !! FUNC WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, TReturn>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Result<TReturn>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
   
    // ** ASYNC W. RESULT ** //
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result<TReturn>>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterFunc(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RFunc<TParam, TScrapbook, TReturn> RegisterFunc<TParam, TScrapbook, TReturn>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<TReturn>>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => _rFunctions.RegisterMethodFunc(functionTypeId, inner, settings);
    
    // ** !! ACTION WITH SCRAPBOOK !! ** //
    // ** SYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, TScrapbook>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Action<TParam, TScrapbook, Context>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** ASYNC W. CONTEXT * //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Result>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
    
    // ** SYNC W. RESULT AND CONTEXT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Result>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new()
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );
   
    // ** ASYNC W. RESULT ** //
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Task<Result>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => RegisterAction(
            functionTypeId,
            InnerMethodToAsyncResultAdapters.ToInnerWithTaskResultReturn(inner),
            settings
        );

    // ** ASYNC W. RESULT AND CONTEXT ** //   
    public RAction<TParam, TScrapbook> RegisterAction<TParam, TScrapbook>(
        FunctionTypeId functionTypeId,
        Func<TEntity, Func<TParam, TScrapbook, Context, Task<Result<Unit>>>> inner,
        Settings? settings = null
    ) where TParam : notnull where TScrapbook : RScrapbook, new() 
        => _rFunctions.RegisterMethodAction(functionTypeId, inner, settings);
}