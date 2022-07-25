using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests.SignOfLifeUpdaterTests;

public delegate Task<bool> CreateFunction(
    FunctionId functionId, 
    StoredParameter param,
    string? scrapbookType,
    long crashedCheckFrequency,
    int version
);

public delegate Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency, int version);

public delegate Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);

public delegate Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int version);
public delegate Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int version);

public delegate Task<bool> SetFunctionState(
    FunctionId functionId,
    Status status,
    string? scrapbookJson,
    StoredResult? result,
    string? errorJson,
    long? postponedUntil,
    int expectedEpoch
);

public delegate Task<StoredFunction?> GetFunction(FunctionId functionId);

public class FunctionStoreMock : IFunctionStore
{
    public Task Initialize() => Task.CompletedTask;
    
    public CreateFunction? SetupCreateFunction { private get; init; }

    public Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        string? scrapbookType,
        long crashedCheckFrequency,
        int version
    ) => SetupCreateFunction == null
        ? true.ToTask()
        : SetupCreateFunction.Invoke(
            functionId, 
            param, 
            scrapbookType,
            crashedCheckFrequency,
            version
        );

    public TryToBecomeLeader? SetupTryToBecomeLeader { private get; init; }
    public Task<bool> TryToBecomeLeader(
        FunctionId functionId, 
        Status newStatus, 
        int expectedEpoch, 
        int newEpoch, 
        long crashedCheckFrequency,
        int version
    ) => SetupTryToBecomeLeader == null
            ? true.ToTask()
            : SetupTryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch, crashedCheckFrequency, version);

    public UpdateSignOfLife? SetupUpdateSignOfLife { private get; init; }
    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
        => SetupUpdateSignOfLife == null
            ? true.ToTask()
            : SetupUpdateSignOfLife(functionId, expectedEpoch, newSignOfLife);
    
    public GetExecutingFunctions? SetupGetExecutingFunctions { private get; init; }
    public Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId, int versionUpperBound)
        => SetupGetExecutingFunctions == null
            ? Enumerable.Empty<StoredExecutingFunction>().ToTask()
            : SetupGetExecutingFunctions(functionTypeId, versionUpperBound);

    public GetPostponedFunctions? SetupGetPostponedFunctions { private get; init; }
    public Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore, int versionUpperBound)
        => SetupGetPostponedFunctions == null
            ? Enumerable.Empty<StoredPostponedFunction>().ToTask()
            : SetupGetPostponedFunctions(functionTypeId, expiresBefore, versionUpperBound);
    
    public SetFunctionState? SetupSetFunctionState { private get; init; }
    public Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        string? errorJson,
        long? postponedUntil,
        int expectedEpoch
    ) => SetupSetFunctionState == null
        ? true.ToTask()
        : SetupSetFunctionState(functionId, status, scrapbookJson, result, errorJson, postponedUntil, expectedEpoch);

    public GetFunction? SetupGetFunction { private get; init; }

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => SetupGetFunction == null
            ? default(StoredFunction).ToTask()
            : SetupGetFunction(functionId);
}