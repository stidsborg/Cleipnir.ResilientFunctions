using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobFunctionStore : IFunctionStore
{
    private readonly AzureBlobEventStore _eventStore;
    public IEventStore EventStore => _eventStore;
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
        _eventStore = new AzureBlobEventStore(_blobContainerClient);
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
        long signOfLifeFrequency,
        long initialSignOfLife)
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
                        Status.Executing, 
                        Epoch: 0, 
                        SignOfLife: initialSignOfLife, 
                        signOfLifeFrequency, 
                        PostponedUntil: null
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

    public async Task<bool> IncrementAlreadyPostponedFunctionEpoch(FunctionId functionId, int expectedEpoch)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var rfTags = await blobClient.GetRfTags();
        try
        {
            await blobClient
                .SetRfTags(
                    rfTags with { Epoch = expectedEpoch + 1, SignOfLife = 0 },
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

    public async Task<bool> RestartExecution(
        FunctionId functionId,
        int expectedEpoch, 
        long signOfLifeFrequency,
        long signOfLife)
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
                        signOfLife, signOfLifeFrequency, 
                        PostponedUntil: null
                    ),
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

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, long newSignOfLife, ComplimentaryState.UpdateSignOfLife complementaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        try
        {
            await blobClient.SetRfTags(
                new RfTags(functionId.TypeId.Value, Status.Executing, Epoch: expectedEpoch, newSignOfLife, SignOfLifeFrequency: complementaryState.SignOfLifeFrequency, PostponedUntil: null),
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

    public async Task<IEnumerable<StoredExecutingFunction>> GetExecutingFunctions(FunctionTypeId functionTypeId)
    {
        var executingBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId}' AND Status = '{(int) Status.Executing}' AND Epoch >= '0' AND SignOfLife >= '0' AND SignOfLifeFrequency >= '0'"
        );

        var executingFunctions = new List<StoredExecutingFunction>();
        await foreach (var executingBlob in executingBlobs)
        {
            var (_, _, instanceName, _) = Utils.SplitIntoParts(executingBlob.BlobName);
            var rfTags = RfTags.ConvertFrom(executingBlob.Tags);
            var storedExecutingFunction = new StoredExecutingFunction(
                instanceName,
                rfTags.Epoch,
                rfTags.SignOfLife,
                rfTags.SignOfLifeFrequency
            );
            
            executingFunctions.Add(storedExecutingFunction);
        }

        return executingFunctions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        var postponedBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId.Value}' AND Status = '{(int) Status.Postponed}' AND PostponedUntil <= '{expiresBefore}' AND Epoch >= '0'"
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
        ReplaceEvents? events,
        int expectedEpoch)
    {
        if (events != null)
            return await SetFunctionStateWithEvents(
                functionId,
                status,
                storedParameter,
                storedScrapbook,
                storedResult,
                storedException,
                postponeUntil,
                events,
                expectedEpoch
            );
        else
            return await SetFunctionStateWithoutEvents(
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

    private async Task<bool> SetFunctionStateWithoutEvents(
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

        if (status == Status.Suspended)
        {
            var epoch = incrementEpoch ? expectedEpoch + 1 : expectedEpoch;
            if (incrementEpoch)
                stateDictionary["SuspendedAtEpoch"] = epoch.ToString();
            else
                stateDictionary["SuspendedAtEpoch"] = expectedEpoch.ToString();
            
            if (forceSetSuspendedTagIfSuspended)
                await _eventStore.ForceSetSuspendedAtEpochTag(functionId, epoch, leaseId: null);
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
                        SignOfLife: Random.Shared.Next(0, int.MaxValue),
                        SignOfLifeFrequency: 0,
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

    private async Task<bool> SetFunctionStateWithEvents( 
        FunctionId functionId, 
        Status status, 
        StoredParameter storedParameter,
        StoredScrapbook storedScrapbook, 
        StoredResult storedResult, 
        StoredException? storedException, 
        long? postponeUntil,
        ReplaceEvents replaceEvents,
        int expectedEpoch)
    {
        BlobLeaseClient? stateLeaseClient = null;
        BlobLeaseClient? eventsLeaseClient = null;

        try
        {
            var stateBlobName = functionId.GetStateBlobName();
            var stateBlobClient = _blobContainerClient.GetBlobClient(stateBlobName);
            stateLeaseClient = stateBlobClient.GetBlobLeaseClient();
            var stateLeaseResponse = await stateLeaseClient.AcquireAsync(TimeSpan.FromSeconds(-1)); //acquire infinite state lease
            var stateLeaseId = stateLeaseResponse.Value.LeaseId;
            
            var eventsBlobName = functionId.GetEventsBlobName();
            var eventsBlobClient = _blobContainerClient.GetAppendBlobClient(eventsBlobName);
            await eventsBlobClient.CreateIfNotExistsAsync();
            
            eventsLeaseClient = eventsBlobClient.GetBlobLeaseClient();
            var eventsLeaseResponse = await eventsLeaseClient.AcquireAsync(TimeSpan.FromSeconds(-1)); //acquire infinite events lease
            var eventsLeaseId = eventsLeaseResponse.Value.LeaseId;
            
            var tags = await stateBlobClient
                .GetTagsAsync(conditions: new BlobRequestConditions { LeaseId = stateLeaseId }); 
            var rfTags = RfTags.ConvertFrom(tags.Value.Tags);
            if (rfTags.Epoch != expectedEpoch)
                return false;

            var (existingEvents, _, _) = await _eventStore.InnerGetEvents(functionId, offset: 0, leaseId: eventsLeaseId);
            if (replaceEvents.ExistingCount != existingEvents.Count)
                return false;

            await _eventStore.Replace(functionId, replaceEvents.Events, eventsLeaseId);

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
                stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionMessage)}"] =
                    exceptionMessage;
                stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionStackTrace)}"] =
                    exceptionStackTrace;
                stateDictionary[$"{nameof(StoredException)}.{nameof(StoredException.ExceptionType)}"] = exceptionType;
            }

            if (status == Status.Suspended)
            {
                stateDictionary[nameof(StoredFunction.SuspendedAtEpoch)] = (expectedEpoch + 1).ToString();
                await _eventStore.ForceSetSuspendedAtEpochTag(functionId, epoch: expectedEpoch + 1, leaseId: eventsLeaseId);
            }

            var content = SimpleDictionaryMarshaller.Serialize(stateDictionary);
            await stateBlobClient.UploadAsync(
                new BinaryData(content),
                new BlobUploadOptions
                {
                    Conditions = new BlobRequestConditions { LeaseId = stateLeaseId },
                    Tags = new RfTags(
                        functionId.TypeId.Value,
                        status,
                        Epoch: expectedEpoch + 1,
                        SignOfLife: Random.Shared.Next(0, int.MaxValue),
                        SignOfLifeFrequency: 0,
                        PostponedUntil: postponeUntil
                    ).ToDictionary()
                }
            );
        }
        finally
        {
            if (stateLeaseClient != null)
                await stateLeaseClient.ReleaseAsync();
            if (eventsLeaseClient != null)
                await eventsLeaseClient.ReleaseAsync();
        }

        return true;
    }

    public async Task<bool> SaveScrapbookForExecutingFunction(
        FunctionId functionId,
        string _,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complementaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var ((paramJson, paramType), (scrapbookJson, scrapbookType), signOfLifeFrequency) = complementaryState;
        
        var content = SimpleDictionaryMarshaller.Serialize(
            new Dictionary<string, string?>
            {
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}", paramType },
                { $"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}", paramJson },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}", scrapbookType },
                { $"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}", scrapbookJson }
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
                        SignOfLife: Random.Shared.Next(0, int.MaxValue),
                        signOfLifeFrequency,
                        PostponedUntil: null
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
        ReplaceEvents? events, 
        bool suspended,
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
            storedFunction.Result,
            storedFunction.Exception,
            storedFunction.PostponedUntil,
            events,
            expectedEpoch
        );
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string _, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var (resultJson, resultType) = result;
        var ((paramJson, paramType), (scrapbookJson, scrapbookType)) = complimentaryState;
        
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Succeeded, Epoch: expectedEpoch, SignOfLife: 0, SignOfLifeFrequency: 0, PostponedUntil: null).ToDictionary(),
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

    public async Task<bool> PostponeFunction(FunctionId functionId, long postponeUntil, string _, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var ((paramJson, paramType), (scrapbookJson, scrapbookType)) = complimentaryState;
        
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Postponed, Epoch: expectedEpoch, SignOfLife: 0, SignOfLifeFrequency: 0, postponeUntil).ToDictionary(),
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

    public async Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string _, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        var blobName = functionId.GetStateBlobName();
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var ((paramJson, paramType), (scrapbookJson, scrapbookType)) = complimentaryState;
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Failed, Epoch: expectedEpoch, SignOfLife: 0, SignOfLifeFrequency: 0, PostponedUntil: null).ToDictionary(),
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

    public async Task<SuspensionResult> SuspendFunction(FunctionId functionId, int expectedEventCount, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        var suspensionResult = await _eventStore.SetSuspendedAtEpochTag(functionId, expectedEpoch, expectedEventCount);
        if (suspensionResult is SuspensionResult.ConcurrentStateModification or SuspensionResult.EventCountMismatch)
            return suspensionResult;

        var success = await SetFunctionStateWithoutEvents(
            functionId,
            Status.Suspended,
            complimentaryState.StoredParameter,
            complimentaryState.StoredScrapbook,
            storedResult: new StoredResult(ResultJson: default, ResultType: default),
            storedException: null,
            postponeUntil: null,
            expectedEpoch: expectedEpoch,
            incrementEpoch: false,
            forceSetSuspendedTagIfSuspended: false
        );

        if (!success)
            return SuspensionResult.ConcurrentStateModification;
        
        return SuspensionResult.Success;
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

        var suspendedAtEpoch =
            dictionary.ContainsKey(nameof(StoredFunction.SuspendedAtEpoch))
                ? int.Parse(dictionary[nameof(StoredFunction.SuspendedAtEpoch)]!)
                : default(int?);

        return new StoredFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            rfTags.Status,
            storedResult,
            Exception: storedException,
            PostponedUntil: rfTags.PostponedUntil,  
            SuspendedAtEpoch: suspendedAtEpoch,
            Epoch: rfTags.Epoch,
            SignOfLife: rfTags.SignOfLife,
            SignOfLifeFrequency: rfTags.SignOfLifeFrequency
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