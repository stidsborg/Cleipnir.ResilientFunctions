using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Watchdogs;

public delegate Task WatchDogReInvokeFunc(
    FunctionInstanceId functionInstanceId, 
    IEnumerable<Status> expectedStatuses, 
    int? expectedEpoch
);