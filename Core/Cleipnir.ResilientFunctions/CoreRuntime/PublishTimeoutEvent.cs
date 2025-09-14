using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Events;

namespace Cleipnir.ResilientFunctions.CoreRuntime;

public delegate Task PublishTimeoutEvent(TimeoutEvent timeoutId);