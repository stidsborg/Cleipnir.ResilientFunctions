using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public static class Utils
{
    public static Task<string> ConvertToString(this Stream stream)
    {
        using var reader = new StreamReader(stream, Encoding.UTF8);
        return reader.ReadToEndAsync();
    }

    public static MemoryStream ConvertToStream(this string s)
        => new MemoryStream(Encoding.UTF8.GetBytes(s));

    public static string ConvertToString(this BinaryData binaryData) 
        => Encoding.UTF8.GetString(binaryData.ToMemory().Span);

    internal static string GetStateBlobName(this FunctionId functionId)
        => GetBlobName(type: "state", functionId.TypeId.Value, functionId.InstanceId.Value);

    internal static string GetEventsBlobName(this FunctionId functionId)
        => GetBlobName(type: "events", functionId.TypeId.Value, functionId.InstanceId.Value);

    internal static string GetTimeoutBlobName(this FunctionId functionId, string timeoutId)
        => GetBlobName(type: "timeout", functionId.TypeId.Value, functionId.InstanceId.Value, timeoutId);
    
    internal static string GetUnderlyingRegisterName(RegisterType registerType, string group, string name)
        => GetBlobName(registerType.ToString(), group, name);

    private static string GetBlobName(string type, string group, string instance, string? id = null)
        => SimpleMarshaller.Serialize(type, group, instance, id ?? "");
    public record FileNameParts(string Type, string Group, string Instance, string? Id);
    internal static FileNameParts SplitIntoParts(string blobName)
    {
        var parts = SimpleMarshaller.Deserialize(blobName, expectedCount: 4);
        return new FileNameParts(parts[0]!, parts[1]!, parts[2]!, parts[3]! == "" ? null : parts[3]);
    }
}