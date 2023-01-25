using Cleipnir.ResilientFunctions.Utils.Arbitrator;
using Cleipnir.ResilientFunctions.Utils.Monitor;
using Cleipnir.ResilientFunctions.Utils.Register;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Invocation;

public record Utilities(IMonitor Monitor, IRegister Register, IArbitrator Arbitrator);