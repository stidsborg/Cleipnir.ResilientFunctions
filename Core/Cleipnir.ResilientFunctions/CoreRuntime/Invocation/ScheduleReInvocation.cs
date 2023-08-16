using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task ScheduleReInvocation(string functionInstanceId, int expectedEpoch, params Status[] expectedStatuses);