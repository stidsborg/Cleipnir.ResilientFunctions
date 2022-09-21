using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class ListScrapbook<T> : RScrapbook
{
    public List<T> List { get; set; } = new();
}