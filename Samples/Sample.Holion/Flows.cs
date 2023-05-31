using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Sample.Holion.A.Ordering;
using Sample.Holion.B.BankTransfer;

namespace Sample.Holion;

public class Flows
{
    public OrderFlows OrderFlows { get; }
    public TransferFlows TransferFlows { get; }

    public Flows(IFunctionStore functionStore, Settings? settings = null)
    {
        var rFunctions = new RFunctions(functionStore, settings);
        OrderFlows = new OrderFlows(rFunctions);
        TransferFlows = new TransferFlows(rFunctions);
    }
}