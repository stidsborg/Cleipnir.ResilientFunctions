using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Sample.Holion.Ordering;

namespace Sample.Holion;

public class Flows
{
    public OrderFlows OrderFlows { get; }

    public Flows(IFunctionStore functionStore, Settings? settings = null)
    {
        var rFunctions = new RFunctions(functionStore, settings);
        OrderFlows = new OrderFlows(rFunctions);
    }
}