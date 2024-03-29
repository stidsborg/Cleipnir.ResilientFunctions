﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.CoreRuntime.ParameterSerialization;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Tests.Utils;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class StoreCrudTests
{
    private FunctionId FunctionId { get; } = new FunctionId("funcType1", "funcInstance1");
    private TestParameters TestParam { get; } = new TestParameters("Peter", 32);
    private StoredParameter Param => new(TestParam.ToJson(), typeof(TestParameters).SimpleQualifiedName());
    private record TestParameters(string Name, int Age);

    private class TestWorkflowState : WorkflowState
    {
        public string? Note { get; set; }
    }
        
    public abstract Task FunctionCanBeCreatedWithASingleParameterSuccessfully();
    protected async Task FunctionCanBeCreatedWithASingleParameterSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.Initialize();
        await store.Initialize();
        
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Result.ResultJson.ShouldBeNull();
        stored.Result.ResultJson.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.LeaseExpiration.ShouldBe(leaseExpiration);
    }

    public abstract Task FunctionCanBeCreatedWithTwoParametersSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Result.ResultJson.ShouldBeNull();
        stored.Result.ResultType.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.LeaseExpiration.ShouldBe(leaseExpiration);
    }
        
    public abstract Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully();
    protected async Task FunctionCanBeCreatedWithTwoParametersAndStateSuccessfully(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var stored = await store.GetFunction(FunctionId);
        stored!.FunctionId.ShouldBe(FunctionId);
        stored.Parameter.ParamJson.ShouldBe(Param.ParamJson);
        stored.Parameter.ParamType.ShouldBe(Param.ParamType);
        stored.Result.ResultJson.ShouldBeNull();
        stored.Result.ResultType.ShouldBeNull();
        stored.Status.ShouldBe(Status.Executing);
        stored.PostponedUntil.ShouldBeNull();
        stored.Epoch.ShouldBe(0);
        stored.LeaseExpiration.ShouldBe(leaseExpiration);
    }

    public abstract Task FetchingNonExistingFunctionReturnsNull();
    protected async Task FetchingNonExistingFunctionReturnsNull(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.GetFunction(FunctionId).ShouldBeNullAsync();
    }  
   
    public abstract Task LeaseIsUpdatedWhenCurrentEpochMatches();
    protected async Task LeaseIsUpdatedWhenCurrentEpochMatches(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RenewLease(FunctionId, expectedEpoch: 0, leaseExpiration: 1).ShouldBeTrueAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction.LeaseExpiration.ShouldBe(1);
    }
    
    public abstract Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent();
    protected async Task LeaseIsNotUpdatedWhenCurrentEpochIsDifferent(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        var leaseExpiration = DateTime.UtcNow.Ticks;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.RenewLease(FunctionId, expectedEpoch: 1, leaseExpiration: 1).ShouldBeFalseAsync();

        var storedFunction = await store.GetFunction(FunctionId);
        storedFunction!.Epoch.ShouldBe(0);
        storedFunction.LeaseExpiration.ShouldBe(leaseExpiration);
    }

    public abstract Task ExistingFunctionCanBeDeleted();
    public async Task ExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        await store.DeleteFunction(FunctionId).ShouldBeTrueAsync();

        await store.GetFunction(FunctionId).ShouldBeNullAsync();
    }
    
    public abstract Task NonExistingFunctionCanBeDeleted();
    public async Task NonExistingFunctionCanBeDeleted(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.DeleteFunction(FunctionId).ShouldBeFalseAsync();
    }
    
    public abstract Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected();
    public async Task ExistingFunctionIsNotDeletedWhenEpochIsNotAsExpected(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        await store.RestartExecution(
            FunctionId,
            expectedEpoch: 0,
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();
        await store.DeleteFunction(FunctionId, expectedEpoch: 0).ShouldBeFalseAsync();

        await store.GetFunction(FunctionId).ShouldNotBeNullAsync();
    }

    public abstract Task ParameterAndStateCanBeUpdatedOnExistingFunction();
    public async Task ParameterAndStateCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var updatedStoredParameter = new StoredParameter(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName()
        );

        await store.SetParameters(
            FunctionId,
            updatedStoredParameter,
            storedResult: StoredResult.Null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(FunctionId);
        sf.ShouldNotBeNull();
        var param = (string) sf.Parameter.DefaultDeserialize();
        param.ShouldBe("hello world");
    }
    
    public abstract Task ParameterCanBeUpdatedOnExistingFunction();
    public async Task ParameterCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();

        var updatedStoredParameter = new StoredParameter(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName()
        );

        await store.SetParameters(
            FunctionId,
            updatedStoredParameter,
            storedResult: StoredResult.Null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(FunctionId);
        sf.ShouldNotBeNull();
        var param = (string) sf.Parameter.DefaultDeserialize();
        param.ShouldBe("hello world");
    }
    
    public abstract Task StateCanBeUpdatedOnExistingFunction();
    public async Task StateCanBeUpdatedOnExistingFunction(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        
        await store.SetParameters(
            FunctionId,
            storedParameter: Param,
            storedResult: StoredResult.Null,
            expectedEpoch: 0
        ).ShouldBeTrueAsync();
        
        var sf = await store.GetFunction(FunctionId);
        sf.ShouldNotBeNull();
        (sf.Parameter.DefaultDeserialize() is TestParameters).ShouldBeTrue();
    }
    
    public abstract Task ParameterAndStateAreNotUpdatedWhenEpochDoesNotMatch();
    public async Task ParameterAndStateAreNotUpdatedWhenEpochDoesNotMatch(Task<IFunctionStore> storeTask)
    {
        var store = await storeTask;
        await store.CreateFunction(
            FunctionId,
            Param,
            leaseExpiration: DateTime.UtcNow.Ticks,
            postponeUntil: null,
            timestamp: DateTime.UtcNow.Ticks
        ).ShouldBeTrueAsync();
        await store.RestartExecution(
            FunctionId,
            expectedEpoch: 0,
            leaseExpiration: DateTime.UtcNow.Ticks
        ).ShouldNotBeNullAsync();

        var updatedStoredParameter = new StoredParameter(
            "hello world".ToJson(),
            typeof(string).SimpleQualifiedName()
        );

        await store.SetParameters(
            FunctionId,
            updatedStoredParameter,
            storedResult: StoredResult.Null,
            expectedEpoch: 0
        ).ShouldBeFalseAsync();
        
        var sf = await store.GetFunction(FunctionId);
        sf.ShouldNotBeNull();
        (sf.Parameter.DefaultDeserialize() is TestParameters).ShouldBeTrue();
    }
}