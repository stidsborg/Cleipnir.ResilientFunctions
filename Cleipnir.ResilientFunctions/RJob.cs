using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task ForceContinuation(
    IEnumerable<Status> expectedStatuses,
    int? expectedEpoch = null
);

public class RJob
{
    public RJob(Func<Task> start, ForceContinuation forceContinuation)
    {
        Start = start;
        ForceContinuation = forceContinuation;
    }

    public Func<Task> Start { get; }
    public ForceContinuation ForceContinuation { get; }
}