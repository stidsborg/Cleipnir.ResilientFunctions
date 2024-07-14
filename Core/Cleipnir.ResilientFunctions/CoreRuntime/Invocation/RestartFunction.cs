using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

internal delegate Task<RestartedFunction?> RestartFunction(FlowId flowId, int expectedEpoch);