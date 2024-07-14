using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task Restart(string instanceId, int expectedEpoch);