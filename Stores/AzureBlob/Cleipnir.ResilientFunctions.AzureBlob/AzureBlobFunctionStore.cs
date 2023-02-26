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

    public Task<bool> SetFunctionState(
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
        throw new NotImplementedException();
    }

    public Task<bool> SaveScrapbookForExecutingFunction(
        FunctionId functionId,
        string scrapbookJson,
        int expectedEpoch,
        ComplimentaryState.SaveScrapbookForExecutingFunction complementaryState)
    {
        throw new NotImplementedException();
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
            if (e.ErrorCode != "BlobAlreadyExists")
                throw;

            return false;
        }
        
        return true;
    }

    public Task<bool> FailFunction(FunctionId functionId, StoredException storedException, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        throw new NotImplementedException();
    }

    public Task<bool> SuspendFunction(FunctionId functionId, int suspendUntilEventSourceCountAtLeast, string scrapbookJson, int expectedEpoch, ComplimentaryState.SetResult complimentaryState)
    {
        throw new NotImplementedException();
    }

    public async Task<StoredFunction?> GetFunction(FunctionId functionId)
    {
        var blobName = GetBlobName(functionId);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        var blobTags = await blobClient.GetTagsAsync();
        var rfTags = RfTags.ConvertFrom(blobTags.Value.Tags);
        
        var blobContentTask = blobClient.DownloadContentAsync(
            new BlobDownloadOptions
            {
                Conditions = new BlobRequestConditions { TagConditions = $"Epoch = '{rfTags.Epoch}'" }
            }
        ); //todo download non-existing blob

        var contentResponse = await blobContentTask;
        var marshalledContent = contentResponse.Value.Content.ToString();

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
        
        /*
        var storedException = 
            dictionary.Keys.Any(k => k.StartsWith(nameof(StoredException)))
                ? new StoredException(
                    
                    );.ContainsKey($"{nameof(StoredException)}.{nameof(StoredException)}")
            
        StoredException*/

        return new StoredFunction(
            functionId,
            storedParameter,
            storedScrapbook,
            rfTags.Status,
            storedResult,
            Exception: default, //todo
            PostponedUntil: rfTags.PostponedUntil, //todo 
            SuspendedUntilEventSourceCount: default, //todo
            Epoch: rfTags.Epoch,
            SignOfLife: rfTags.SignOfLife,
            CrashedCheckFrequency: rfTags.CrashedCheckFrequency
        );
    }

    public Task<StoredFunctionStatus?> GetFunctionStatus(FunctionId functionId)
    {
        throw new NotImplementedException();
    }

    public Task<bool> DeleteFunction(FunctionId functionId, int? expectedEpoch = null)
    {
        throw new NotImplementedException();
    }

    private static string GetBlobName(FunctionId functionId)
    {
        functionId.Validate();
        return $"{functionId}_state";
    }
}