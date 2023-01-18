using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.SignOfLifeUpdaterTests;

public class SignOfLifeTestFunctionStore : IFunctionStore
{
    public delegate bool SignOfLifeCallback(FunctionId functionId, int expectedEpoch, int newSignOfLife);

    private readonly SignOfLifeCallback _signOfLifeCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public SignOfLifeTestFunctionStore(SignOfLifeCallback signOfLifeCallback) => _signOfLifeCallback = signOfLifeCallback;

    public IEventStore EventStore => _inner.EventStore;
    public ITimeoutStore TimeoutStore => _inner.TimeoutStore;
    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency)
        => _inner.CreateFunction(functionId, param, storedScrapbook, crashedCheckFrequency);

    public Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch)
        => _inner.IncrementEpoch(functionId, expectedEpoch);

    public Task<bool> RestartExecution(FunctionId functionId, Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, int expectedEpoch, long crashedCheckFrequency)
        => _inner.RestartExecution(functionId, paramAndScrapbook, expectedEpoch, crashedCheckFrequency);

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        var success = _signOfLifeCallback(functionId, expectedEpoch, newSignOfLife);
        return success.ToTask();
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
        => _inner.GetExecutingFunctions(functionTypeId);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
        => _inner.GetPostponedFunctions(functionTypeId, expiresBefore);

    public Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId) 
        => _inner.GetEligibleSuspendedFunctions(functionTypeId);

    public Task<Epoch?> IsFunctionSuspendedAndEligibleForReInvocation(FunctionId functionId)
        => _inner.IsFunctionSuspendedAndEligibleForReInvocation(functionId);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status, 
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil, int expectedEpoch
    ) => _inner.SetFunctionState(functionId, status, storedParameter, storedScrapbook, storedResult, storedException, postponeUntil, expectedEpoch);

    public Task<bool> SaveScrapbookForExecutingFunction(FunctionId functionId, string scrapbookJson, int expectedEpoch)
        => _inner.SaveScrapbookForExecutingFunction(functionId, scrapbookJson, expectedEpoch);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedScrapbook, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
        => _inner.SucceedFunction(functionId, result, scrapbookJson, expectedEpoch);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
        => _inner.PostponeFunction(functionId, postponeUntil, scrapbookJson, expectedEpoch);

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch)
        => _inner.FailFunction(functionId, storedException, scrapbookJson, expectedEpoch);

    public Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch)
        => _inner.SuspendFunction(functionId, suspendUntilEventSourceCountAtLeast, scrapbookJson, expectedEpoch);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
        => _inner.GetFunctionStatus(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
        => _inner.DeleteFunction(functionId, expectedEpoch);
}