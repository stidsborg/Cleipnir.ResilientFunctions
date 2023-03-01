using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Messaging;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobFunctionStore : IFunctionStore 
{
    public IEventStore EventStore { get; }
    public ITimeoutStore TimeoutStore { get; }
    public Utilities Utilities { get; }

    private readonly string _connectionString;
    public string ContainerName { get; }
    private readonly BlobServiceClient _blobServiceClient;
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobFunctionStore(string connectionString, string prefix = "")
    {
        _connectionString = connectionString;
        ContainerName = $"{prefix}rfunctions";
        
        _blobServiceClient = new BlobServiceClient(connectionString);
        _blobContainerClient = _blobServiceClient.GetBlobContainerClient(ContainerName);
    }
    
    public async Task Initialize()
    {
        await _blobContainerClient.CreateIfNotExistsAsync();
    }

    public async Task<bool> CreateFunction(FunctionId functionId, StoredParameter param, StoredScrapbook scrapbook, long crashedCheckFrequency)
    {
        var blobName = GetBlobName(functionId);
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Executing, Epoch: 0, SignOfLife: 0, crashedCheckFrequency, PostponedUntil: null).ToDictionary(),
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
        var blobName = GetBlobName(functionId);
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
        long crashedCheckFrequency)
    {
        var blobName = GetBlobName(functionId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        try
        {
            await blobClient
                .SetRfTags(
                    new RfTags(functionId.TypeId.Value, Status.Executing, Epoch: expectedEpoch + 1, SignOfLife: 0, crashedCheckFrequency, PostponedUntil: null),
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

    public async Task<bool> UpdateSignOfLife(FunctionId functionId, int expectedEpoch, int newSignOfLife, ComplimentaryState.UpdateSignOfLife complementaryState)
    {
        var blobName = GetBlobName(functionId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        try
        {
            await blobClient.SetRfTags(
                new RfTags(functionId.TypeId.Value, Status.Executing, Epoch: expectedEpoch, newSignOfLife, CrashedCheckFrequency: complementaryState.CrashedCheckFrequency, PostponedUntil: null),
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
        //todo validate functiontypeId
        var executingBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId}' AND Status = '{(int) Status.Executing}' AND Epoch >= '0' AND SignOfLife >= '0' AND CrashedCheckFrequency >= '0'"
        );

        var executingFunctions = new List<StoredExecutingFunction>();
        await foreach (var executingBlob in executingBlobs)
        {
            var instanceName = executingBlob.BlobName.Split("@")[0];
            var rfTags = RfTags.ConvertFrom(executingBlob.Tags);
            var storedExecutingFunction = new StoredExecutingFunction(
                instanceName,
                rfTags.Epoch,
                rfTags.SignOfLife,
                rfTags.CrashedCheckFrequency
            );
            
            executingFunctions.Add(storedExecutingFunction);
        }

        return executingFunctions;
    }

    public async Task<IEnumerable<StoredPostponedFunction>> GetPostponedFunctions(FunctionTypeId functionTypeId, long expiresBefore)
    {
        var postponedBlobs = _blobContainerClient.FindBlobsByTagsAsync(
            tagFilterSqlExpression: $"FunctionType = '{functionTypeId.Value}' AND Status = '{(int) Status.Postponed}' AND PostponedUntil < '{expiresBefore}' AND Epoch >= '0'"
        );

        var postponedFunctions = new List<StoredPostponedFunction>();
        
        await foreach (var postponedBlob in postponedBlobs)
        {
            var instanceName = postponedBlob.BlobName.Split("@")[0];
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

    public Task<IEnumerable<StoredEligibleSuspendedFunction>> GetEligibleSuspendedFunctions(FunctionTypeId functionTypeId)
    {
        throw new NotImplementedException();
    }

    public Task<Epoch?> IsFunctionSuspendedAndEligibleForReInvocation(FunctionId functionId)
    {
        throw new NotImplementedException();
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
        if (events == null)
        {
            var blobName = GetBlobName(functionId);
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
                            Epoch: expectedEpoch + 1, 
                            SignOfLife: Random.Shared.Next(0, int.MaxValue),
                            CrashedCheckFrequency: 0,
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
        else
            throw new NotImplementedException();
    }

    public async Task<bool> SaveScrapbookForExecutingFunction(
        FunctionId functionId,
        string _,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complementaryState)
    {
        var blobName = GetBlobName(functionId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var ((paramJson, paramType), (scrapbookJson, scrapbookType), crashedCheckFrequency) = complementaryState;
        
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
                        crashedCheckFrequency,
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

    public Task<bool> SetParameters(FunctionId functionId, StoredParameter storedParameter, StoredScrapbook storedScrapbook, ReplaceEvents? events, int expectedEpoch)
    {
        throw new NotImplementedException();
    }

    public async Task<bool> SucceedFunction(FunctionId functionId, StoredResult result, string _, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        var blobName = GetBlobName(functionId);
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Succeeded, Epoch: 0, SignOfLife: 0, CrashedCheckFrequency: 0, PostponedUntil: null).ToDictionary(),
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
        var blobName = GetBlobName(functionId);
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Postponed, Epoch: 0, SignOfLife: 0, CrashedCheckFrequency: 0, postponeUntil).ToDictionary(),
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
        var blobName = GetBlobName(functionId);
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
                    Tags = new RfTags(functionId.TypeId.Value, Status.Failed, Epoch: 0, SignOfLife: 0, CrashedCheckFrequency: 0, PostponedUntil: null).ToDictionary(),
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

    public Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        throw new NotImplementedException();
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        var blobName = GetBlobName(functionId);
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
            throw;
        }
        
        var marshalledContent = contentResponse.Content.ToString();

        var dictionary = SimpleDictionaryMarshaller.Deserialize(marshalledContent, expectedCount: 6);

        var storedParameter = new StoredParameter(
            ParamJson: dictionary[$"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamJson)}"],
            ParamType: dictionary[$"{nameof(StoredParameter)}.{nameof(StoredParameter.ParamType)}"]
        );

        var storedScrapbook = new StoredScrapbook(
            ScrapbookJson: dictionary[$"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookJson)}"],
            ScrapbookType: dictionary[$"{nameof(StoredScrapbook)}.{nameof(StoredScrapbook.ScrapbookType)}"]
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
            SuspendedUntilEventSourceCount: default, //todo
            Epoch: rfTags.Epoch,
            SignOfLife: rfTags.SignOfLife,
            CrashedCheckFrequency: rfTags.CrashedCheckFrequency
        );
    }

    public async Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        var blobName = GetBlobName(functionId);
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

    private static string GetBlobName(FunctionId functionId)
    {
        functionId.Validate();
        return $"{functionId}_state";
    }
}