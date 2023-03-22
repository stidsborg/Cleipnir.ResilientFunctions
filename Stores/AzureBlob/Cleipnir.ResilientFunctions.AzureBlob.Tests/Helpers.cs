using System.Threading.Tasks;
using Azure.Storage.Blobs;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

public static class Helpers
{
    public static async Task DeleteAllBlobs(this BlobContainerClient blobContainerClient)
    {
        foreach (var blob in blobContainerClient.GetBlobs())
            await blobContainerClient.DeleteBlobAsync(blob.Name);
    }
}