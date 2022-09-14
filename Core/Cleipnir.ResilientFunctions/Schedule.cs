using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task Schedule<in TParam, TScrapbook>(string functionInstanceId, TParam param, TScrapbook? scrapbook = null) 
    where TParam : notnull where TScrapbook : RScrapbook, new();
public delegate Task ScheduleReInvocation(
    string functionInstanceId, 
    IEnumerable<Status> expectedStatuses, 
    int? expectedEpoch = null,
    bool throwOnUnexpectedFunctionState = true
);

public delegate Task ScheduleReInvocation<TScrapbook>(
    string functionInstanceId, 
    IEnumerable<Status> expectedStatuses, 
    int? expectedEpoch = null,
    Action<TScrapbook>? scrapbookUpdater = null,
    bool throwOnUnexpectedFunctionState = true
);