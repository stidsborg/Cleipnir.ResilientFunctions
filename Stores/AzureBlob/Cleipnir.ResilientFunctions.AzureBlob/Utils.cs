using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils;
using Base64 = Cleipnir.ResilientFunctions.Storage.Base64;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public static class Utils
{
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
    {
        var parts = id == null
            ? new[] { type, group, instance }
            : new[] { type, group, instance, id };

        return string.Join('|', parts.Select(Base64.Base64Encode));
    }
    public record FileNameParts(string Type, string Group, string Instance, string? Id);
    internal static FileNameParts SplitIntoParts(string blobName)
    {
        var parts = blobName
            .Split("|")
            .Select(Base64.Base64Decode)
            .ToArray();
        
        return new FileNameParts(Type: parts[0], Group: parts[1], Instance: parts[2], Id: parts.Length == 4 ? parts[3] : null);
    }
}