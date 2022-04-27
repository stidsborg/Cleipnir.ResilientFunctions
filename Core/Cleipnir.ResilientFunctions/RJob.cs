using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task Retry(
    IEnumerable<Status> expectedStatuses,
    int? expectedEpoch = null
);

public class RJob
{
    public RJob(Func<Task> start, Retry retry)
    {
        Start = start;
        Retry = retry;
    }

    public Func<Task> Start { get; }
    public Retry Retry { get; }
}