using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Utils.Scrapbooks;

public class BackoffScrapbook : RScrapbook, IBackoffScrapbook
{
    public int Retry { get; set; }
}