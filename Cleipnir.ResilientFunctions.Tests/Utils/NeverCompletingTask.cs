using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class NeverCompletingTask
{
    public static Task<T> OfType<T>() => new Task<T>(() => default!);
    public static Task OfVoidType => new Task(() => {});
}