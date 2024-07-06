using System.Collections.Generic;
using Cleipnir.ResilientFunctions.Domain;

namespace Cleipnir.ResilientFunctions.Tests.Utils;

public class ListState<T> : FlowState
{
    public List<T> List { get; set; } = new();
}