using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task ScheduleReInvocation(string functionInstanceId, int expectedEpoch);