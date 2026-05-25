using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Helpers;

public static class ForeverTask
{
    public static Task Instance { get; } = new TaskCompletionSource().Task;
}
