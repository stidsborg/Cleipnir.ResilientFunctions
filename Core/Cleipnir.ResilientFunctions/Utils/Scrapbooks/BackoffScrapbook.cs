using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Utils.Scrapbooks;

public class BackoffScrapbook : Scrapbook, IBackoffScrapbook
{
    public int Retry { get; set; }
}