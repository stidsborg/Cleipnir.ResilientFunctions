using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public readonly record struct RfTags(string FunctionType, Status Status, int Epoch, long LeaseExpiration, long? PostponedUntil, long Timestamp)
{
    public Dictionary<string, string> ToDictionary()
    {
        var dictionary = new Dictionary<string, string>
        {
            { nameof(FunctionType), FunctionType },
            { nameof(Status), ((int) Status).ToString() },
            { nameof(Epoch), Epoch.ToString() },
            { nameof(LeaseExpiration), LeaseExpiration.ToString() },
            { nameof(Timestamp), Timestamp.ToString() }
        };  
        
        if (PostponedUntil != null)
            dictionary[nameof(PostponedUntil)] = PostponedUntil.Value.ToString();

        return dictionary;
    } 

    public static RfTags ConvertFrom(IDictionary<string, string> tags)
        => new(
            FunctionType: tags[nameof(FunctionType)],
            Status: (Status)int.Parse(tags[nameof(Status)]),
            Epoch: int.Parse(tags[nameof(Epoch)]),
            LeaseExpiration: long.Parse(tags[nameof(LeaseExpiration)]),
            PostponedUntil: tags.ContainsKey(nameof(PostponedUntil)) 
                ? long.Parse(tags[nameof(PostponedUntil)]) 
                : default(long?),
            Timestamp: long.Parse(tags[nameof(Timestamp)])
        );
}

public static class RfTagsExtensions
{
    public static async Task<RfTags> GetRfTags(this BlobClient client)
    {
        var response = await client.GetTagsAsync();
        var tags = response.Value.Tags;

        var functionType = tags["FunctionType"];
        var status = (Status) int.Parse(tags["Status"]);
        var epoch = int.Parse(tags["Epoch"]);
        var leaseExpiration = long.Parse(tags["LeaseExpiration"]);
        var postponedUntil = tags.ContainsKey("PostponedUntil")
            ? long.Parse(tags["PostponedUntil"])
            : default;
        var timestamp = long.Parse(tags["Timestamp"]);

        return new RfTags(functionType, status, epoch, leaseExpiration, postponedUntil, timestamp);
    }

    public static Task SetRfTags(this BlobClient client, RfTags rfTags) 
        => client.SetTagsAsync(rfTags.ToDictionary());

    public static Task SetRfTags(this BlobClient client, RfTags rfTags, int expectedEpoch)
        => client.SetTagsAsync(
            rfTags.ToDictionary(),
            new BlobRequestConditions { TagConditions = $"Epoch = '{expectedEpoch}'" }
        );
}