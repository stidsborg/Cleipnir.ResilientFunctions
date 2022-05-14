using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task Schedule<in TParam>(string functionInstanceId, TParam param) where TParam : notnull;
public delegate Task ScheduleReInvocation(
    string functionInstanceId, 
    IEnumerable<Status> expectedStatuses, 
    int? expectedEpoch
);

public delegate Task ScheduleReInvocation<TScrapbook>(
    string functionInstanceId, 
    IEnumerable<Status> expectedStatuses, 
    int? expectedEpoch = null,
    Action<TScrapbook>? scrapbookUpdater = null
);