using System.Text;
using Cleipnir.ResilientFunctions.Domain;

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
    {
        return Encoding.UTF8.GetString(binaryData.ToMemory().Span);
    }
}