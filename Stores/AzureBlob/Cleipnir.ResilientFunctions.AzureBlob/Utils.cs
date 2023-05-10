using System.Text;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Utils;

namespace Cleipnir.ResilientFunctions.AzureBlob;

public static class Utils
{
    public static void Validate(this FunctionId functionId)
    {
        if (functionId.TypeId.Value.Contains("@") || functionId.InstanceId.Value.Contains("@"))
            throw new ArgumentException("'@'-sign not allowed in function id");
    }

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
    {
        functionId.Validate();
        return $"{functionId}_state";
    }

    internal static string GetEventsBlobName(this FunctionId functionId)
    {
        functionId.Validate();
        return $"{functionId}_events";
    }

    internal static string GetUnderlyingRegisterName(RegisterType registerType, string group, string name)
    {
        return $"{(int) registerType}{group.Length:000}_{group}_{name}";
    }

    public record RegisterName(RegisterType RegisterType, string Group, string Name);
    internal static RegisterName ConvertToRegisterName(string blobName)
    {
        var registerType = (RegisterType) int.Parse(blobName[0].ToString());
        var rest = blobName[1..]; //skip register type
        var groupLength = int.Parse(rest[..3]); 
        rest = rest[4..]; //skip group length and _
        var group = rest[..groupLength];
        rest = rest[(groupLength + 1)..]; //skip group name and trailing _
        var name = rest;

        return new RegisterName(registerType, group, name);
    }

    internal static FunctionId ConvertFromStateBlobNameToFunctionId(this string blobName)
    {
        var functionIdString = blobName[..^("_state".Length)];
        var split = functionIdString.Split("@");
        return new FunctionId(split[1], split[0]);
    }
    
    internal static FunctionId ConvertFromEventsBlobNameToFunctionId(this string blobName)
    {
        var functionIdString = blobName[..^("_events".Length)];
        var split = functionIdString.Split("@");
        return new FunctionId(split[1], split[0]);
    }
}