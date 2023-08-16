using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.CoreRuntime.Watchdogs;

public delegate Task WatchDogReInvokeFunc(FunctionInstanceId functionInstanceId, int expectedEpoch, params Status[] expectedStatuses);