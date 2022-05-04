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

public static class RJobExtensions
{
    public static Func<Nothing, TScrapbook, Task<Result>> ToNothingFunc<TScrapbook>(Func<TScrapbook, Task<Result>> inner)
    {
        return (_, scrapbook) => inner(scrapbook);
    } 
}

public class Nothing
{
    public static Nothing Instance = new Nothing();
}