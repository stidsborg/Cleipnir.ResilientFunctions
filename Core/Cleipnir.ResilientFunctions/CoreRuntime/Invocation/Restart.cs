using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public delegate Task Restart(StoredInstance instanceId, int expectedEpoch);