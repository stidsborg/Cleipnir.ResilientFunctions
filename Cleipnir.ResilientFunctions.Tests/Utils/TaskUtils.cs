using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public static class TaskUtils
{
    public static Task NeverCompleting => new Task(() => { });
}