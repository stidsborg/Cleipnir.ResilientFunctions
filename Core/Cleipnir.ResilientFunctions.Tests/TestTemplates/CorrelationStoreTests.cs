using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class CorrelationStoreTests
{
    public abstract Task SunshineScenario();
    public async Task SunshineScenario(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId = TestFunctionId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId");
        await correlationStore
            .GetCorrelations(correlationId: "SomeCorrelationId")
            .SelectAsync(c => c.Single())
            .ShouldBeAsync(functionId);

        await correlationStore
            .GetCorrelations(functionId)
            .SelectAsync(c => c.Single())
            .ShouldBeAsync("SomeCorrelationId");
    }
    
    public abstract Task TwoDifferentFunctionsCanUseTheSameCorrelationId();
    public async Task TwoDifferentFunctionsCanUseTheSameCorrelationId(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId1 = TestFunctionId.Create();
        var functionId2 = TestFunctionId.Create();

        await correlationStore.SetCorrelation(functionId1, correlationId: "TwoDifferentFunctionsCanUseTheSameCorrelationId");
        await correlationStore.SetCorrelation(functionId2, correlationId: "TwoDifferentFunctionsCanUseTheSameCorrelationId");

        var functions = await correlationStore.GetCorrelations(correlationId: "TwoDifferentFunctionsCanUseTheSameCorrelationId");
        functions.Count.ShouldBe(2);
        functions.Any(f => f == functionId1).ShouldBeTrue();
        functions.Any(f => f == functionId2).ShouldBeTrue();
    }
    
    public abstract Task FunctionCorrelationsCanBeDeleted();
    public async Task FunctionCorrelationsCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId = TestFunctionId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId2");

        await correlationStore.RemoveCorrelations(functionId);
        
        (await correlationStore.GetCorrelations(functionId)).ShouldBeEmpty();
    }
    
    public abstract Task SingleFunctionCorrelationCanBeDeleted();
    public async Task SingleFunctionCorrelationCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId = TestFunctionId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId2");

        await correlationStore.RemoveCorrelation(functionId, "SomeCorrelationId1");

        await correlationStore
            .GetCorrelations(functionId)
            .SelectAsync(c => c.Single())
            .ShouldBeAsync("SomeCorrelationId2");
    }
    
    public abstract Task SingleFunctionCanHaveMultipleCorrelations();
    public async Task SingleFunctionCanHaveMultipleCorrelations(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId = TestFunctionId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId2");

        var correlations = await correlationStore.GetCorrelations(functionId);
        correlations.Count.ShouldBe(2);
        correlations.Any(c => c == "SomeCorrelationId1").ShouldBeTrue();
        correlations.Any(c => c == "SomeCorrelationId2").ShouldBeTrue();
    }
}

