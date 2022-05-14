using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions;

public delegate Task Retry<TScrapbook>(
    IEnumerable<Status> expectedStatuses,
    int? expectedEpoch = null,
    Action<TScrapbook>? scrapbookUpdater = null
);

public class RJob<TScrapbook>
{
    public RJob(Func<Task> start, Retry<TScrapbook> retry)
    {
        Start = start;
        Retry = retry;
    }

    public Func<Task> Start { get; }
    public Retry<TScrapbook> Retry { get; }
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
    public static readonly Nothing Instance = new Nothing();
}