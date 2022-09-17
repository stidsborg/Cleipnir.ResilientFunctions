using System.Threading;

namespace ConsoleApp.CorrelationId;

public class CorrelationId
{
    private static readonly AsyncLocal<string> AsyncLocal = new();
    public static string Get() => AsyncLocal.Value!;
    public static void Set(string value) => AsyncLocal.Value = value;
}