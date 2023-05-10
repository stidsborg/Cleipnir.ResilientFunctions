using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Cleipnir.ResilientFunctions.Helpers;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public class AzureBlobUnderlyingRegister : IUnderlyingRegister
{
    private readonly BlobContainerClient _blobContainerClient;

    public AzureBlobUnderlyingRegister(BlobContainerClient blobContainerClient)
    {
        _blobContainerClient = blobContainerClient;
    }

    public async Task<bool> SetIfEmpty(RegisterType registerType, string group, string name, string value)
    {
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        try
        {
            await blobClient.UploadAsync(overwrite: false, content: new BinaryData(value));
            return true;
        }
        catch (RequestFailedException e)
        {
            if (e.ErrorCode != "BlobAlreadyExists")
                throw;

            return false;
        }
    }

    public async Task<bool> CompareAndSwap(RegisterType registerType, string group, string name, string newValue, string expectedValue, bool setIfEmpty = true)
    {
        var currentResponse = await InnerGet(registerType, group, name);
        if (currentResponse == null && !setIfEmpty)
            return false;
        if (currentResponse == null) //setIfEmpty is true then
        {
            var success = await SetIfEmpty(registerType, group, name, newValue);
            if (success)
                return true;
            
            return await CompareAndSwap(registerType, group, name, newValue, expectedValue, setIfEmpty); //try again
        }

        if (currentResponse.Value.Content.ToString() != expectedValue)
            return false;
        
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        try
        {
            await blobClient.UploadAsync(
                new BinaryData(newValue),
                new BlobUploadOptions
                {
                    Conditions = new BlobRequestConditions { IfMatch = currentResponse.GetRawResponse().Headers.ETag }
                }
            );

            return true;
        }
        catch (RequestFailedException exception)
        {
            if (exception.ErrorCode == "ConditionNotMet")
                return await CompareAndSwap(registerType, group, name, newValue, expectedValue, setIfEmpty);
            
            throw;
        }
    }

    public async Task<string?> Get(RegisterType registerType, string group, string name)
    {
        return await InnerGet(registerType, group, name)
            .SelectAsync(r => r?.Value.Content.ToString());
    }

    private async Task<Response<BlobDownloadResult>?> InnerGet(RegisterType registerType, string group, string name)
    {
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        
        try
        {
            return await blobClient.DownloadContentAsync();
        }
        catch (RequestFailedException exception)
        {
            if (exception.Status == 404) return null;
            throw;
        }
    }

    public async Task<bool> Delete(RegisterType registerType, string group, string name, string expectedValue)
    {
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);

        var currentResponse = await InnerGet(registerType, group, name);
        if (currentResponse == null)
            return true;

        var currentValue = currentResponse.Value.Content.ToString();
        if (currentValue != expectedValue)
            return false;
        
        try
        {
            await blobClient
                .DeleteAsync(
                    conditions: new BlobRequestConditions
                    {
                        IfMatch = currentResponse.GetRawResponse().Headers.ETag
                    }
                );
            
            return true;
        }
        catch (RequestFailedException exception)
        {
            if (exception.Status == 404) return true;
            if (exception.ErrorCode == "ConditionNotMet")
                return await Delete(registerType, group, name, expectedValue);
            
            throw;
        }
    }

    public async Task Delete(RegisterType registerType, string group, string name)
    {
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        await blobClient.DeleteIfExistsAsync();
    }

    public async Task<bool> Exists(RegisterType registerType, string group, string name)
    {
        var blobName = Utils.GetUnderlyingRegisterName(registerType, group, name);
        var blobClient = _blobContainerClient.GetBlobClient(blobName);
        return await blobClient.ExistsAsync();
    }
}