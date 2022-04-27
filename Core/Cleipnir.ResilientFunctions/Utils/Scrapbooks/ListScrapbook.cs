using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Utils.Scrapbooks;

public class ListScrapbook<T> : RScrapbook
{
    public List<T> List { get; set; } = new();
}