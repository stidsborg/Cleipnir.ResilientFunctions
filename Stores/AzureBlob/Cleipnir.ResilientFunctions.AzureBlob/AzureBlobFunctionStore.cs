using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;
using Cleipnir.ResilientFunctions.Storage.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobFunctionStore : IFunctionStore
{
    private readonly AzureBlobMessageStore _messageStore;
    public IMessageStore MessageStore => _messageStore;
    private AzureBlobActivityStore _activityStore;
    public IActivityStore ActivityStore => _activityStore;
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }
    
    public string ContainerName { get; }
    private readonly BlobServiceClient _blobServiceClient;
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobFunctionStore(string connectionString, string prefix = "")
    {
        ContainerName = $"{prefix}rfunctions";
        
        _blobServiceClient = new BlobServiceClient(connectionString);
        _blobContainerClient = _blobServiceClient.GetBlobContainerClient(ContainerName);
        _messageStore = new AzureBlobMessageStore(_blobContainerClient);
        _activityStore = new AzureBlobActivityStore(_blobContainerClient);
        TimeoutStore = new AzureBlobTimeoutStore(_blobContainerClient);
        Utilities = new Utilities(new AzureBlobUnderlyingRegister(_blobContainerClient));
    }
    
    public async Task Initialize()
    {
        await _blobContainerClient.CreateIfNotExistsAsync();
    }

    public async Task<bool> CreateFunction(
        FunctionId functionId, 
        StoredParameter param, 
        StoredScrapbook scrapbook, 
        long leaseExpiration,
        long? postponeUntil,
        long timestamp)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", param.ParamType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", param.ParamJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbook.ScrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbook.ScrapbookJson },
            }
        );

        try
        {
            await blobClient.UploadAsync( 
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        postponeUntil == null ? Status.Executing : Status.Postponed, 
                        Epoch: 0, 
                        leaseExpiration,
                        PostponedUntil: postponeUntil,
                        timestamp
                    ).ToDictionary(),
                    Conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") }
                }
            );    
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobAlreadyExists")
                throw;

            return false;
        }
        
        return true;
    }

    public async Task<StoredFunction?> RestartExecution(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        try
        {
            await blobClient
                .SetRfTags(
                    new RfTags(
                        functionId.TypeId.Value, 
                        Status.Executing, 
                        Epoch: expectedEpoch + 1, 
                        LeaseExpiration: leaseExpiration, 
                        PostponedUntil: null,
                        Timestamp: DateTime.UtcNow.Ticks
                    ),
                    expectedEpoch
                );
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode != "ConditionNotMet" && e.ErrorCode != "BlobNotFound")
                throw;

            return default;
        }

        var sf = await GetFunction(functionId);
        return sf?.Epoch == expectedEpoch + 1
            ? sf
            : default;
    }

    public async Task<bool> RenewLease(FunctionId functionId, int expectedEpoch, long leaseExpiration)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        try
        {
            await blobClient.SetRfTags(
                new RfTags(functionId.TypeId.Value, Status.Executing, Epoch: expectedEpoch, leaseExpiration, Timestamp: DateTime.UtcNow.Ticks, PostponedUntil: null),
                expectedEpoch
            );
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode != "ConditionNotMet")
                throw;

            return false;
        }

        return true;
    }

    public async Task<IEnumerable<StoredExecutingFunction>> GetCrashedFunctions(FunctionTypeId functionTypeId, long leaseExpiresBefore)
    {
        var executingBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId}' AND Status = '{(int) Status.Executing}' AND Epoch >= '0' AND LeaseExpiration < '{leaseExpiresBefore}'"
        );

        var executingFunctions = new List<StoredExecutingFunction>();
        await foreach (var executingBlob in executingBlobs)
        {
            var (_, _, instanceName, _) = Utils.SplitIntoParts(executingBlob.BlobName);
            var epoch = int.Parse(executingBlob.Tags["Epoch"]);
            var leaseExpiration = long.Parse(executingBlob.Tags["LeaseExpiration"]);
            var storedExecutingFunction = new StoredExecutingFunction(instanceName, epoch, leaseExpiration);
            
            executingFunctions.Add(storedExecutingFunction);
        }

        return executingFunctions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long isEligibleBefore)
    {
        var postponedBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId.Value}' AND Status = '{(int) Status.Postponed}' AND PostponedUntil <= '{isEligibleBefore}' AND Epoch >= '0'"
        );

        var postponedFunctions = new List<StoredPostponedFunction>();
        
        await foreach (var postponedBlob in postponedBlobs)
        {
            var (_, _, instanceName, _) = Utils.SplitIntoParts(postponedBlob.BlobName);
            var epoch = int.Parse(postponedBlob.Tags["Epoch"]);
            var postponedUntil = long.Parse(postponedBlob.Tags["PostponedUntil"]);
            
            var storedExecutingFunction = new StoredPostponedFunction(
                instanceName,
                epoch,
                postponedUntil
            );
            
            postponedFunctions.Add(storedExecutingFunction);
        }
        
        return postponedFunctions;
    }

    public async Task<bool> SetFunctionState(
        FunctionId functionId, 
        Status status, 
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, 
        StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch)
    {
        return await SetFunctionStateWithoutMessages(
            functionId,
            status,
            storedParameter,
            storedScrapbook,
            storedResult,
            storedException,
            postponeUntil,
            expectedEpoch,
            incrementEpoch: true,
            forceSetSuspendedTagIfSuspended: true
        );
    }

    private async Task<bool> SetFunctionStateWithoutMessages(
        FunctionId functionId, 
        Status status, 
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, 
        StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        int expectedEpoch,
        bool incrementEpoch,
        bool forceSetSuspendedTagIfSuspended
    )
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var (paramJson, paramType) = storedParameter;
        var (scrapbookJson, scrapbookType) = storedScrapbook;
        var (resultJson, resultType) = storedResult;

        var stateDictionary =
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
                { $"{nameof(StoredResult)}.{nameof(StoredResult.ResultJson)}", resultJson },
                { $"{nameof(StoredResult)}.{nameof(StoredResult.ResultType)}", resultType },
            };

        if (storedException != null)
        {
            var (exceptionMessage, exceptionStackTrace, exceptionType) = storedException;
            stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionMessage)}"] = exceptionMessage;
            stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionStackTrace)}"] = exceptionStackTrace;
            stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionType)}"] = exceptionType;
        }
        
        var content = SimpleDictionaryMarshaller.Serialize(stateDictionary);

        try
        {
            await blobClient.UploadAsync(
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'" },
                    Tags = new RfTags(
                        functionId.TypeId.Value,
                        status,
                        Epoch: incrementEpoch ? expectedEpoch + 1 : expectedEpoch,
                        LeaseExpiration: DateTime.UtcNow.Ticks,
                        Timestamp: DateTime.UtcNow.Ticks,
                        PostponedUntil: postponeUntil
                    ).ToDictionary()
                }
            );

            return true;
        }
        catch (RequestFailedException exception)
        {
            var tagNotAsExpected =
                exception.ErrorCode!.Equals("ConditionNotMet", StringComparison.OrdinalIgnoreCase) &&
                exception.Status == 412;
            if (tagNotAsExpected)
                return false;

            throw;
        }
    } 

    public async Task<bool> SaveScrapbookForExecutingFunction(
        FunctionId functionId,
        string _,
        int expectedEpoch,
        ComplimentaryState complementaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var (storedParamFunc, storedScrapbookFunc, leaseLength) = complementaryState;
        var (paramJson, paramType) = storedParamFunc();
        var (scrapbookJson, scrapbookType) = storedScrapbookFunc();
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
            }
        );

        try
        {
            await blobClient.UploadAsync(
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'" },
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        Status.Executing, 
                        expectedEpoch, 
                        LeaseExpiration: DateTime.UtcNow.Ticks + leaseLength,
                        PostponedUntil: null,
                        Timestamp: DateTime.UtcNow.Ticks
                    ).ToDictionary()
                }
            );
            
            return true;
        }
        catch (RequestFailedException exception)
        {
            var tagNotAsExpected =
                exception.ErrorCode!.Equals("ConditionNotMet", StringComparison.OrdinalIgnoreCase) &&
                exception.Status == 412;
            if (tagNotAsExpected) 
                return false;

            throw;
        }
    }
    
    public async Task<bool> SetParameters(
        FunctionId functionId, 
        StoredParameter storedParameter, 
        StoredScrapbook storedScrapbook, 
        StoredResult storedResult,
        int expectedEpoch)
    {
        var storedFunction = await GetFunction(functionId);
        if (storedFunction == null || storedFunction.Epoch != expectedEpoch)
            return false;

        return await SetFunctionState(
            functionId,
            storedFunction.Status,
            storedParameter,
            storedScrapbook,
            storedResult,
            storedFunction.Exception,
            storedFunction.PostponedUntil,
            expectedEpoch
        );
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string _, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var (resultJson, resultType) = result;
        var (storedParamFunc, storedScrapbookFunc, leaseLength) = complimentaryState;
        var (paramJson, paramType) = storedParamFunc();
        var (scrapbookJson, scrapbookType) = storedScrapbookFunc();
        
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
                { $"{nameof(StoredResult)}.{nameof(StoredResult.ResultJson)}", resultJson },
                { $"{nameof(StoredResult)}.{nameof(StoredResult.ResultType)}", resultType },
            }
        );

        try
        {
            await blobClient.UploadAsync( 
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        Status.Succeeded, 
                        Epoch: expectedEpoch, 
                        LeaseExpiration: DateTime.UtcNow.Ticks, 
                        PostponedUntil: null,
                        timestamp
                    ).ToDictionary(),
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'"}
                }
            );    
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobAlreadyExists")
                throw;

            return false;
        }
        
        return true;
    }

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string _, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var (storedParamFunc, storedScrapbookFunc, leaseLength) = complimentaryState;
        var (paramJson, paramType) = storedParamFunc();
        var (scrapbookJson, scrapbookType) = storedScrapbookFunc();
        
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
            }
        );

        try
        {
            await blobClient.UploadAsync( 
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        Status.Postponed, 
                        Epoch: expectedEpoch, 
                        LeaseExpiration: DateTime.UtcNow.Ticks,
                        postponeUntil,
                        timestamp
                    ).ToDictionary(),
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'"}
                }
            );    
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode is not ("BlobAlreadyExists" or "ConditionNotMet"))
                throw;

            return false;
        }
        
        return true;
    }

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string _, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var (storedParamFunc, storedScrapbookFunc, leaseLength) = complimentaryState;
        var (paramJson, paramType) = storedParamFunc();
        var (scrapbookJson, scrapbookType) = storedScrapbookFunc();
        
        var (exceptionMessage, exceptionStackTrace, exceptionType) = storedException;
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
                { $"{nameof(StoredException)}.{nameof(StoredException.ExceptionMessage)}", exceptionMessage },
                { $"{nameof(StoredException)}.{nameof(StoredException.ExceptionStackTrace)}", exceptionStackTrace },
                { $"{nameof(StoredException)}.{nameof(StoredException.ExceptionType)}", exceptionType },
            }
        );

        try
        {
            await blobClient.UploadAsync( 
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        Status.Failed, 
                        Epoch: expectedEpoch, 
                        LeaseExpiration: DateTime.UtcNow.Ticks,
                        PostponedUntil: null,
                        timestamp
                    ).ToDictionary(),
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'"}
                }
            );    
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobAlreadyExists")
                throw;

            return false;
        }
        
        return true;
    }

    public async Task<bool> SuspendFunction(FunctionId functionId, int expectedMessageCount, string scrapbookJson, long timestamp, int expectedEpoch, ComplimentaryState complimentaryState)
    {
        var success = await PostponeFunction(
            functionId,
            postponeUntil: DateTime.UtcNow.AddMinutes(1).Ticks,
            _: string.Empty,
            timestamp,
            expectedEpoch,
            complimentaryState
        );
        if (!success)
            return false;

        var messages = await MessageStore.GetMessages(functionId);
        if (messages.Count() != expectedMessageCount)
            return await PostponeFunction(
                functionId,
                postponeUntil: 0,
                _: string.Empty,
                timestamp,
                expectedEpoch,
                complimentaryState
            );

        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var (storedParamFunc, storedScrapbookFunc, leaseLength) = complimentaryState;
        var (paramJson, paramType) = storedParamFunc();
        var (_, scrapbookType) = storedScrapbookFunc();
        
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson },
            }
        );

        try
        {
            await blobClient.UploadAsync( 
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Tags = new RfTags(
                        functionId.TypeId.Value, 
                        Status.Suspended, 
                        Epoch: expectedEpoch, 
                        LeaseExpiration: DateTime.UtcNow.Ticks,
                        PostponedUntil: null,
                        timestamp
                    ).ToDictionary(),
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'"}
                }
            );    
        } catch (RequestFailedException e)
        {
            if (e.ErrorCode is not ("BlobAlreadyExists" or "ConditionNotMet"))
                throw;

            return false;
        }

        return true;
    }

    public async Task<StatusAndEpoch?> GetFunctionStatus(FunctionId functionId)
    {
        var sf = await GetFunction(functionId);
        if (sf == null) return null;
        
        return new StatusAndEpoch(sf.Status, sf.Epoch);
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        RfTags rfTags;
        try
        {
            var blobTags = await blobClient.GetTagsAsync();
            rfTags = RfTags.ConvertFrom(blobTags.Value.Tags);
        }
        catch (RequestFailedException exception)
        {
            if (exception.Status == 404) return null;
            throw;
        }

        BlobDownloadResult contentResponse;
        try
        {
            contentResponse = await blobClient.DownloadContentAsync(
                new BlobDownloadOptions
                {
                    Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{rfTags.Epoch}'" }
                }
            );
        }
        catch (RequestFailedException exception)
        {
            if (exception.Status == 404) return null;
            if (exception.ErrorCode == "ConditionNotMet")
            {
                await Task.Delay(250);
                return await GetFunction(functionId);
            }
            throw;
        }
        
        var marshalledContent = contentResponse.Content.ToString();

        var dictionary = SimpleDictionaryMarshaller.Deserialize(marshalledContent, expectedCount: 6);

        var storedParameter = new StoredParameter(
            ParamJson: dictionary[$"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}"]!,
            ParamType: dictionary[$"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}"]!
        );

        var storedScrapbook = new StoredScrapbook(
            ScrapbookJson: dictionary[$"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}"]!,
            ScrapbookType: dictionary[$"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}"]!
        );

        var storedResult = new StoredResult(
            ResultJson: dictionary.ContainsKey($"{nameof(StoredResult)}.{nameof(StoredResult.ResultJson)}")
                ? dictionary[$"{nameof(StoredResult)}.{nameof(StoredResult.ResultJson)}"]
                : null,
            ResultType: dictionary.ContainsKey($"{nameof(StoredResult)}.{nameof(StoredResult.ResultType)}")
                ? dictionary[$"{nameof(StoredResult)}.{nameof(StoredResult.ResultType)}"]
                : null
        );
        
        StoredException? storedException = default;
        if (dictionary.ContainsKey($"{nameof(StoredException)}.{nameof(StoredException.ExceptionMessage)}"))
        {
            var exceptionMessage = dictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionMessage)}"];
            var exceptionStackTrace = dictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionStackTrace)}"];
            var exceptionType = dictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionType)}"];

            storedException = new StoredException(
                exceptionMessage!,
                exceptionStackTrace,
                exceptionType!
            );
        }

        return new StoredFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            rfTags.Status,
            storedResult,
            Exception: storedException,
            PostponedUntil: rfTags.PostponedUntil,
            Epoch: rfTags.Epoch,
            LeaseExpiration: rfTags.LeaseExpiration,
            Timestamp: rfTags.Timestamp
        );
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        try
        {
            if (expectedEpoch.HasValue)
                await blobClient.DeleteIfExistsAsync(conditions: new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'" });
            else
                await blobClient.DeleteIfExistsAsync();

            return true;
        } catch (RequestFailedException exception)
        {
            var tagNotAsExpected =
                exception.ErrorCode!.Equals("ConditionNotMet", StringComparison.OrdinalIgnoreCase) &&
                exception.Status == 412;
            if (tagNotAsExpected) 
                return false;

            throw;
        }
    }
}