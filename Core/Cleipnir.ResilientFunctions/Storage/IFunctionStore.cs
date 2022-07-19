using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Domain.Exceptions;
using Cleipnir.ResilientFunctions.ParameterSerialization;

namespace Cleipnir.ResilientFunctions.Storage;

public interface IFunctionStore
{
    public Task Initialize();
    
    // ** CREATION ** // 
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        string? scrapbookType,
        long crashedCheckFrequency
    );

    // ** LEADERSHIP ** //
    Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch, long crashedCheckFrequency);
    Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);

    Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId);
    Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore);

    // ** CHANGES ** //
    Task<bool> SetFunctionState(
        FunctionId functionId,
        Status status,
        string? scrapbookJson,
        StoredResult? result,
        string? errorJson,
        long? postponedUntil,
        int expectedEpoch
    );

    async Task UpdateScrapbook<TScrapbook>(
        FunctionId functionId, 
        Action<TScrapbook> updater,
        IEnumerable<Status> expectedStatuses,
        int? expectedEpoch = null,
        ISerializer? serializer = null) where TScrapbook : RScrapbook, new()
    {
        var sf = await GetFunction(functionId);
        if (sf == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' not found");
        if (sf.Scrapbook == null)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' does not have scrapbook");
        if (!expectedStatuses.Contains(sf.Status))
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected status: '{sf.Status}'");
        if (expectedEpoch != null && expectedEpoch != sf.Epoch)
            throw new UnexpectedFunctionState(functionId, $"Function '{functionId}' did not have expected epoch '{expectedEpoch}' was '{sf.Epoch}'");
        
        serializer ??= DefaultSerializer.Instance;

        var scrapbook = (TScrapbook) serializer.DeserializeScrapbook(sf.Scrapbook.ScrapbookJson, sf.Scrapbook.ScrapbookType);

        updater(scrapbook);

        var scrapbookJson = serializer.SerializeScrapbook(scrapbook);

        var success = await SetFunctionState(
            functionId,
            sf.Status,
            scrapbookJson,
            sf.Result,
            sf.ErrorJson,
            sf.PostponedUntil,
            sf.Epoch
        );

        if (!success)
            throw new ConcurrentModificationException(functionId);
    }

    // ** GETTER ** //
    Task<StoredFunction?> GetFunction(FunctionId functionId);
}