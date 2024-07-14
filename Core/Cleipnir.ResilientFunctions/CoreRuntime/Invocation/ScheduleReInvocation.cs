using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task ScheduleReInvocation(string flowInstance, int expectedEpoch);