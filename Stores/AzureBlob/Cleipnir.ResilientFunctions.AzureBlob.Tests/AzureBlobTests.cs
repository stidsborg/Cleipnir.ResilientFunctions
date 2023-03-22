using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Cleipnir.ResilientFunctions.Helpers;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.AzureBlob.Tests;

[TestClass]
public class AzureBlobTests
{
    private static readonly BlobContainerClient BlobContainerClient = null!;
    private static bool ShouldRun => Settings.ConnectionString != null; 

    static AzureBlobTests()
    {
        if (!ShouldRun) return;
        
        var blobServiceClient = new BlobServiceClient(Settings.ConnectionString);
        BlobContainerClient = blobServiceClient.GetBlobContainerClient(nameof(AzureBlobTests).ToLower());
        BlobContainerClient.CreateIfNotExists();

        BlobContainerClient.DeleteAllBlobs().GetAwaiter().GetResult();
    }

    [TestMethod]
    public async Task UploadToExistingBlobWhenNoFileWasExpectedFails()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetBlobClient(blobName);
        var content = "Hello world";
        var response = await blobClient.UploadAsync(
            new BinaryData(content),
            new BlobUploadOptions {Conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") }}
        );
        response.GetRawResponse().IsError.ShouldBeFalse();

        try
        {
            await blobClient.UploadAsync(
                new BinaryData(content),
                new BlobUploadOptions { Conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") } }
            );
            Assert.Fail("An exception should have been thrown on upload");
        }
        catch (RequestFailedException e)
        {
            e.ErrorCode.ShouldBe("BlobAlreadyExists");
            e.Status.ShouldBe(409);
        }
    }
    
    [TestMethod]
    public async Task OverwriteOfExistingBlobSucceedsWhenTagIsAsExpected()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetBlobClient(blobName);
        var response = await blobClient.UploadAsync(
            new BinaryData("Hello world"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "0" } },
                Conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") }
            }
        );
        response.GetRawResponse().IsError.ShouldBeFalse();
        
        response = await blobClient.UploadAsync(
            new BinaryData("Hello universe"),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "1" } },
                Conditions = new BlobRequestConditions { TagConditions = "Epoch = '0'"}
            }
        );
        response.GetRawResponse().IsError.ShouldBeFalse();

        var blob = await blobClient.DownloadContentAsync();
        blob.Value.Content.ConvertToString().ShouldBe("Hello universe");
        
        var tags = await blobClient.GetTagsAsync().SelectAsync(t => t.Value.Tags);
        tags.Count.ShouldBe(2);
        tags["Status"].ShouldBe("Executing");
        tags["Epoch"].ShouldBe("1");
    }
    
    [TestMethod]
    public async Task OverwriteOfExistingBlobFailsWasTagIsNotAsExpected()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetBlobClient(blobName);
        var content = "Hello world";
        var response = await blobClient.UploadAsync(
            new BinaryData(content),
            new BlobUploadOptions
            {
                Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "1" } },
                Conditions = new BlobRequestConditions { IfNoneMatch = new ETag("*") }
            }
        );
        response.GetRawResponse().IsError.ShouldBeFalse();

        try
        {
            await blobClient.UploadAsync(
                new BinaryData("Hello universe"),
                new BlobUploadOptions
                {
                    Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "1" } },
                    Conditions = new BlobRequestConditions { TagConditions = "Epoch = '0'" }
                }
            );
            Assert.Fail("An exception should have been thrown on upload");
        }
        catch (RequestFailedException e)
        {
            e.ErrorCode.ShouldBe("ConditionNotMet");
            e.Status.ShouldBe(412);
        }
        
        var blob = await blobClient.DownloadContentAsync();
        blob.Value.Content.ConvertToString().ShouldBe("Hello world");
        
        var tags = await blobClient.GetTagsAsync().SelectAsync(t => t.Value.Tags);
        tags.Count.ShouldBe(2);
        tags["Status"].ShouldBe("Executing");
        tags["Epoch"].ShouldBe("1");
    }

    [TestMethod]
    public async Task BlobAppendFailsWhenNoFileExist()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetAppendBlobClient(blobName);

        try
        {
            await blobClient.AppendBlockAsync("hello world".ConvertToStream());
            Assert.Fail("Appending to non-existing blob should have failed");
        }
        catch (RequestFailedException e)
        {
            e.ErrorCode.ShouldBe("BlobNotFound");
            e.Status.ShouldBe(404);
        }
    }

    [TestMethod]
    public async Task MultipleBlobAppendsToExistingFileSucceeds()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetAppendBlobClient(blobName);
        {
            var response = await blobClient.CreateIfNotExistsAsync();
            response.GetRawResponse().IsError.ShouldBeFalse();    
        }
        {
            var response = await blobClient.AppendBlockAsync("hello world\n".ConvertToStream());
            response.GetRawResponse().IsError.ShouldBeFalse();
        }
        {
            var response = await blobClient.AppendBlockAsync("hello universe".ConvertToStream());
            response.GetRawResponse().IsError.ShouldBeFalse();
        }
        {
            var response = await blobClient.DownloadContentAsync();
            var str = Encoding.UTF8.GetString(response.Value.Content.ToMemory().Span);
            var strs = str.Split("\n");
            strs.Length.ShouldBe(2);
            strs[0].ShouldBe("hello world");
            strs[1].ShouldBe("hello universe");
        }
    }
    
    [TestMethod]
    public async Task BlobAppendCanBeReadWithOffset()
    {
        if (!ShouldRun) return;
        
        var contentLength = 0;
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetAppendBlobClient(blobName);
        
        {
            var response = await blobClient.CreateIfNotExistsAsync();
            response.GetRawResponse().IsError.ShouldBeFalse();    
        }
        
        {
            var response = await blobClient.AppendBlockAsync("hello world".ConvertToStream());
            response.GetRawResponse().IsError.ShouldBeFalse();
        }
        
        {
            var response = await blobClient.DownloadContentAsync(new BlobDownloadOptions { Range = new HttpRange(offset: contentLength) });
            contentLength = response.GetRawResponse().Headers.ContentLength!.Value;
            var str = Encoding.UTF8.GetString(response.Value.Content.ToMemory().Span);
            str.ShouldBe("hello world");
        }

        {
            var response = await blobClient.AppendBlockAsync("hello universe".ConvertToStream());
            response.GetRawResponse().IsError.ShouldBeFalse();
        }
        
        {
            var response = await blobClient.DownloadContentAsync(new BlobDownloadOptions { Range = new HttpRange(offset: contentLength) });
            contentLength += response.GetRawResponse().Headers.ContentLength!.Value;
            var str = Encoding.UTF8.GetString(response.Value.Content.ToMemory().Span);
            str.ShouldBe("hello universe");
        }
        
        contentLength.ShouldBe(Encoding.UTF8.GetBytes("hello worldhello universe").Length);
    }
    
    [TestMethod]
    public async Task ListBlobsWithTags()
    {
        if (!ShouldRun) return;

        await BlobContainerClient.DeleteAllBlobs();
        
        var blobId = Guid.NewGuid().ToString("N");
        var blobName1 = "a" + blobId;
        var blobName2 = "b" + blobId;
        string eTag1;
        string eTag2;
        
        {
            var blobClient = BlobContainerClient.GetAppendBlobClient(blobName1);
            var response = await blobClient.CreateIfNotExistsAsync();
            response.GetRawResponse().IsError.ShouldBeFalse();
            eTag1 = response.Value.ETag.ToString().Replace("\"", "");
        }

        {
            var blobClient = BlobContainerClient.GetBlobClient(blobName2);
            var response = await blobClient.UploadAsync(
                content: new BinaryData("hello world"),
                new BlobUploadOptions { Tags = new Dictionary<string, string> { {"TestKey", "TestValue"} }}
            );
            response.GetRawResponse().IsError.ShouldBeFalse();
            eTag2 = response.Value.ETag.ToString().Replace("\"", "");
        }

        {
            var blobs = BlobContainerClient.GetBlobs(BlobTraits.Tags).OrderBy(b => b.Name).ToList();
            blobs[0].Name.ShouldBe(blobName1);
            blobs[0].Properties.ETag!.ToString().ShouldBe(eTag1);
            blobs[1].Name.ShouldBe(blobName2);
            blobs[1].Properties.ETag!.ToString().ShouldBe(eTag2);
            blobs[1].Tags.Count.ShouldBe(1);
            blobs[1].Tags["TestKey"].ShouldBe("TestValue");
        }
    }
    
    [TestMethod]
    public async Task UpdatedTagsCanBeFetchedImmediatelyAfterSuccessfully()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetBlobClient(blobName);
        var response = await blobClient.UploadAsync(
            new BinaryData("Hello world"),
            new BlobUploadOptions { Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "0" } } }
        );
        response.GetRawResponse().IsError.ShouldBeFalse();

        await blobClient.SetTagsAsync(
            new Dictionary<string, string> { { "Status", "Suspended" } },
            conditions: new BlobRequestConditions {TagConditions = "Status = 'Executing'"}
        );
        var tagsResponse = await blobClient.GetTagsAsync();
        var tags = tagsResponse.Value.Tags;
        tags["Status"].ShouldBe("Suspended");
    }
    
    [TestMethod]
    public async Task UpdatingTagsFailsWhenTagIsNotAsExpected()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetBlobClient(blobName);
        var response = await blobClient.UploadAsync(
            new BinaryData("Hello world"),
            new BlobUploadOptions { Tags = new Dictionary<string, string> { { "Status", "Executing" }, { "Epoch", "0" } } }
        );
        response.GetRawResponse().IsError.ShouldBeFalse();

        try
        {
            await blobClient.SetTagsAsync(
                new Dictionary<string, string> { { "Status", "Suspended" } },
                conditions: new BlobRequestConditions {TagConditions = "Status = 'Suspended'"}
            );
            throw new Exception("Expected SetTagsAsync-method invocation to fail");
        }
        catch (RequestFailedException exception)
        {
            if (exception.ErrorCode != "ConditionNotMet")
                throw;
        }
        
        var tagsResponse = await blobClient.GetTagsAsync();
        var tags = tagsResponse.Value.Tags;
        tags["Status"].ShouldBe("Executing");
    }
    
    [TestMethod]
    public async Task ETagReturnedAfterAppendIsAlsoETagWhenGettingAppendBlobAfterwards()
    {
        if (!ShouldRun) return;
        
        var blobName = Guid.NewGuid().ToString("N");
        var blobClient = BlobContainerClient.GetAppendBlobClient(blobName);
        await blobClient.CreateAsync();
        {
            var response = await blobClient.AppendBlockAsync("hello first".ConvertToStream());
            var eTag = response.GetRawResponse().Headers.ETag;
            var downloadResponse = await blobClient.DownloadContentAsync();
            var downloadEtag = downloadResponse.GetRawResponse().Headers.ETag;
            downloadEtag.ShouldBe(eTag);            
        }
        {
            var response = await blobClient.AppendBlockAsync("hello second".ConvertToStream());
            var eTag = response.GetRawResponse().Headers.ETag;
            var downloadResponse = await blobClient.DownloadContentAsync();
            var downloadEtag = downloadResponse.GetRawResponse().Headers.ETag;
            downloadEtag.ShouldBe(eTag);
        }
    }
}