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
    // ** CREATION ** // 
    Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param,
        string? scrapbookType,
        Status initialStatus,
        int initialEpoch,
        int initialSignOfLife
    );

    // ** LEADERSHIP ** //
    Task<bool> TryToBecomeLeader(FunctionId functionId, Status newStatus, int expectedEpoch, int newEpoch);
    Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife);
        
    Task<IEnumerable<StoredFunctionStatus>> GetFunctionsWithStatus(
        FunctionTypeId functionTypeId, 
        Status status,
        long? expiresBefore = null
    ); //todo consider change this to async enumerable?
        
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
        ISerializer? serializer = null) where TScrapbook : RScrapbook
    {
        var sf = await GetFunction(functionId);
        if (sf == null)
            throw new FunctionInvocationException(functionId, $"Function '{functionId}' not found");
        if (sf.Scrapbook == null)
            throw new FunctionInvocationException(functionId, $"Function '{functionId}' does not have scrapbook");
        if (!expectedStatuses.Contains(sf.Status))
            throw new FunctionInvocationException(functionId, $"Function '{functionId}' did not have expected status: '{sf.Status}'");
        
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
            throw new FunctionInvocationException(
                functionId,
                $"Unable to persist function '{functionId}' scrapbook due to concurrent modification"
            );
    }

    // ** GETTER ** //
    Task<StoredFunction?> GetFunction(FunctionId functionId);
}