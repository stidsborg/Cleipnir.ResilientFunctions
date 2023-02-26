using Azure.Storage.Blobs;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public static class AzureBlobHelper
{
    public static Task SetTags(this BlobContainerClient client, FunctionId functionId, RfTags tags)
    {
        throw new NotImplementedException();
    }

    public static Task<RfTags> GetTags(FunctionId functionId)
    {
        throw new NotImplementedException();
    }

    /*public static RfTags ToRfTags(this Dictionary<string, string> dictionary)
    {
        return new RfTags(
            
            );
    }*/
}