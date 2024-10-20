using System.Text.Json;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class Test
{
    public static byte[] SimpleStoredParameter { get; } = JsonSerializer.SerializeToUtf8Bytes("hello world");
}