using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.SignOfLifeUpdaterTests;

public class SignOfLifeTestFunctionStore : IFunctionStore
{
    public delegate bool SignOfLifeCallback(FunctionId functionId, int expectedEpoch, int newSignOfLife);

    private readonly SignOfLifeCallback _signOfLifeCallback;
    private readonly IFunctionStore _inner = new InMemoryFunctionStore();

    public SignOfLifeTestFunctionStore(SignOfLifeCallback signOfLifeCallback) => _signOfLifeCallback = signOfLifeCallback;

    public Task Initialize() => _inner.Initialize();

    public Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook storedScrapbook, long crashedCheckFrequency, int version)
        => _inner.CreateFunction(functionId, param, storedScrapbook, crashedCheckFrequency, version);

    public Task<bool> IncrementEpoch(FunctionId functionId, int expectedEpoch)
        => _inner.IncrementEpoch(functionId, expectedEpoch);

    public Task<bool> RestartExecution(FunctionId functionId, Tuple<StoredParameter, StoredScrapbook>? paramAndScrapbook, int expectedEpoch, long crashedCheckFrequency, int version)
        => _inner.RestartExecution(functionId, paramAndScrapbook, expectedEpoch, crashedCheckFrequency, version);

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        var success = _signOfLifeCallback(functionId, expectedEpoch, newSignOfLife);
        return success.ToTask();
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
        => _inner.GetExecutingFunctions(functionTypeId, versionUpperBound);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
        => _inner.GetPostponedFunctions(functionTypeId, expiresBefore, versionUpperBound);

    public Task<bool> SetFunctionState(
        FunctionId functionId, Status status,
        StoredParameter storedParameter, StoredScrapbook storedScrapbook, StoredResult storedResult,
        string? errorJson, long? postponeUntil, int expectedEpoch)
        => _inner.SetFunctionState(functionId, status, storedParameter, storedScrapbook, storedResult, errorJson, postponeUntil, expectedEpoch);
    
    public Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
        => _inner.SetScrapbook(functionId, scrapbookJson, expectedEpoch);

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter? storedParameter, StoredScrapbook? storedScrapbook, int expectedEpoch)
        => _inner.SetParameters(functionId, storedParameter, storedScrapbook, expectedEpoch);

    public Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string scrapbookJson, int expectedEpoch)
        => _inner.SucceedFunction(functionId, result, scrapbookJson, expectedEpoch);

    public Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string scrapbookJson, int expectedEpoch)
        => _inner.PostponeFunction(functionId, postponeUntil, scrapbookJson, expectedEpoch);

    public Task<bool> FailFunction(FunctionId functionId, string errorJson, string scrapbookJson, int expectedEpoch)
        => _inner.FailFunction(functionId, errorJson, scrapbookJson, expectedEpoch);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
        => _inner.DeleteFunction(functionId, expectedEpoch, expectedStatus);
}