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
        var functionId = TestFlowId.Create();

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
        var functionId1 = TestFlowId.Create();
        var functionId2 = TestFlowId.Create();

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
        var functionId = TestFlowId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId2");

        await correlationStore.RemoveCorrelations(functionId);
        
        (await correlationStore.GetCorrelations(functionId)).ShouldBeEmpty();
    }
    
    public abstract Task SingleFunctionCorrelationCanBeDeleted();
    public async Task SingleFunctionCorrelationCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId = TestFlowId.Create();

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
        var functionId = TestFlowId.Create();

        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId, "SomeCorrelationId2");

        var correlations = await correlationStore.GetCorrelations(functionId);
        correlations.Count.ShouldBe(2);
        correlations.Any(c => c == "SomeCorrelationId1").ShouldBeTrue();
        correlations.Any(c => c == "SomeCorrelationId2").ShouldBeTrue();
    }
    
    public abstract Task FunctionInstancesCanBeFetchedForFunctionTypeAndCorrelation();
    public async Task FunctionInstancesCanBeFetchedForFunctionTypeAndCorrelation(Task<IFunctionStore> storeTask)
    {
        var correlationStore = await storeTask.SelectAsync(s => s.CorrelationStore);
        var functionId1 = TestFlowId.Create();
        var functionId2 = TestFlowId.Create().WithTypeId(functionId1.Type);
        var functionId3 = TestFlowId.Create();

        await correlationStore.SetCorrelation(functionId1, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId2, "SomeCorrelationId1");
        await correlationStore.SetCorrelation(functionId3, "SomeCorrelationId1");

        var instances = await correlationStore.GetCorrelations(functionId1.Type, "SomeCorrelationId1");
        instances.Count.ShouldBe(2);
        instances.Any(i => i == functionId1.Instance).ShouldBeTrue();
        instances.Any(i => i == functionId2.Instance).ShouldBeTrue();
    }
}

