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

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version)
        => _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency, version);

    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version, string scrapbookJson)
        => _inner.TryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency, version, scrapbookJson);

    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
    {
        var success = _signOfLifeCallback(functionId, expectedEpoch, newSignOfLife);
        return success.ToTask();
    }

    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
        => _inner.GetExecutingFunctions(functionTypeId, versionUpperBound);

    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
        => _inner.GetPostponedFunctions(functionTypeId, expiresBefore, versionUpperBound);

    public Task<bool> SetFunctionState(FunctionId functionId, Status status, string scrapbookJson, StoredResult? result, string? errorJson, long? postponedUntil, int expectedEpoch)
        => _inner.SetFunctionState(functionId, status, scrapbookJson, result, errorJson, postponedUntil, expectedEpoch);

    public Task<bool> SetScrapbook(FunctionId functionId, string scrapbookJson, int expectedEpoch)
        => _inner.SetScrapbook(functionId, scrapbookJson, expectedEpoch);

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => _inner.GetFunction(functionId);

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null, Status? expectedStatus = null)
        => _inner.DeleteFunction(functionId, expectedEpoch, expectedStatus);
}