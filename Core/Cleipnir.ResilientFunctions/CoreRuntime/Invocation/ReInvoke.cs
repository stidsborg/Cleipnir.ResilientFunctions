using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task ReInvoke(string functionInstanceId, int expectedEpoch);