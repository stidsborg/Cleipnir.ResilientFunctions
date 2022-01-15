using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;
using static Cleipnir.ResilientFunctions.Tests.Utils.FlagPosition;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates.RFunctionTests;

public abstract class BarricadedTests
{
    public abstract Task ABarricadedFunctionInvocationThrowsBarricadedException();
    protected async Task ABarricadedFunctionInvocationThrowsBarricadedException(IFunctionStore store)
    {
        var functionTypeId = nameof(ABarricadedFunctionInvocationThrowsBarricadedException).ToFunctionTypeId();
        const string functionInstanceId = "someInstanceId";
        
        await store.Barricade(new FunctionId(functionTypeId, functionInstanceId)).ShouldBeTrueAsync();
        
        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionHandler.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
        );

        var rFunc = rFunctions.Register(
            functionTypeId,
            (string s) => s.ToSucceededRResult().ToTask(),
            idFunc: _ => functionInstanceId
        );

        await Should.ThrowAsync<FunctionBarricadedException>(() => rFunc("hello world"));
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }

    public abstract Task AnExecutingFunctionCannotBeBarricaded();
    protected async Task AnExecutingFunctionCannotBeBarricaded(IFunctionStore store)
    {
        var functionTypeId = nameof(AnExecutingFunctionCannotBeBarricaded).ToFunctionTypeId();
        const string functionInstanceId = "someInstanceId";

        var rFuncIsExecutingFlag = new SyncedFlag();

        var unhandledExceptionHandler = new UnhandledExceptionCatcher();
        using var rFunctions = RFunctions.Create(
            store,
            unhandledExceptionHandler.Catch,
            crashedCheckFrequency: TimeSpan.FromMilliseconds(2),
            postponedCheckFrequency: TimeSpan.FromMilliseconds(2)
        );

        var rFunc = rFunctions.Register(
            functionTypeId,
            (string _) =>
            {
                rFuncIsExecutingFlag.Raise();
                return NeverCompletingTask.OfType<RResult<string>>();
            },
            idFunc: _ => functionInstanceId
        );

        _ = rFunc("hello world");
        await BusyWait.UntilAsync(() => rFuncIsExecutingFlag.Position == Raised);
        
        await store.Barricade(new FunctionId(functionTypeId, functionInstanceId)).ShouldBeFalseAsync();
        unhandledExceptionHandler.ThrownExceptions.ShouldBeEmpty();
    }
}