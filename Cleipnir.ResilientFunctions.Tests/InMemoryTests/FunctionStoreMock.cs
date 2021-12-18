using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.Tests.InMemoryTests;

public delegate Task<bool> CreateFunction(
    FunctionId functionId, 
    StoredParameter param,
    string? scrapbookType, 
    Status initialStatus, 
    int initialEpoch, 
    int initialSignOfLife
);

public delegate Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch);

public delegate Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);

public delegate Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
    FunctionTypeId functionTypeId,
    Status status,
    long? expiresBefore = null
);

public delegate Task<bool> SetFunctionState(
    FunctionId functionId,
    Status status,
    string? scrapbookJson,
    StoredResult? result,
    StoredFailure? failed,
    long? postponedUntil,
    int expectedEpoch
);

public delegate Task<StoredFunction?> GetFunction(FunctionId functionId);

public class FunctionStoreMock : IFunctionStore
{
    public CreateFunction? SetupCreateFunction { private get; init; }
    public Task<bool> CreateFunction(
        FunctionId functionId,
        StoredParameter param,
        string? scrapbookType,
        Status initialStatus,
        int initialEpoch,
        int initialSignOfLife
    ) => SetupCreateFunction == null
        ? true.ToTask()
        : SetupCreateFunction.Invoke(functionId, param, scrapbookType, initialStatus, initialEpoch, initialSignOfLife);

    public TryToBecomeLeader? SetupTryToBecomeLeader { private get; init; }
    public Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch)
        => SetupTryToBecomeLeader == null
            ? true.ToTask()
            : SetupTryToBecomeLeader(functionId, newStatus, expectedEpoch, newEpoch);

    public UpdateSignOfLife? SetupUpdateSignOfLife { private get; init; }
    public Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife)
        => SetupUpdateSignOfLife == null
            ? true.ToTask()
            : SetupUpdateSignOfLife(functionId, expectedEpoch, newSignOfLife);

    public GetFunctionsWithStatus? SetupGetFunctionsWithStatus { private get; init; }
    public Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
        FunctionTypeId functionTypeId,
        Status status,
        long? expiresBefore = null
    ) => SetupGetFunctionsWithStatus == null
        ? Enumerable.Empty<StoredFunctionStatus>().ToTask()
        : SetupGetFunctionsWithStatus(functionTypeId, status, expiresBefore);

    public SetFunctionState? SetupSetFunctionState { private get; init; }
    public Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        StoredFailure? failed,
        long? postponedUntil,
        int expectedEpoch
    ) => SetupSetFunctionState == null
        ? true.ToTask()
        : SetupSetFunctionState(functionId, status, scrapbookJson, result, failed, postponedUntil, expectedEpoch);

    public GetFunction? SetupGetFunction { private get; init; }

    public Task<StoredFunction?> GetFunction(FunctionId functionId)
        => SetupGetFunction == null
            ? default(StoredFunction).ToTask()
            : SetupGetFunction(functionId);
}