using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Domain;
using Cleipnir.ResilientFunctions.Storage;
using Sample.Holion.A.Ordering;
using Sample.Holion.B.BankTransfer;
using Sample.Holion.C.Newsletter;
using Sample.Holion.D.SupportTicket;
using Sample.Holion.E;

namespace Sample.Holion;

public class Flows
{
    public OrderFlows OrderFlows { get; }
    public TransferFlows TransferFlows { get; }
    public NewsletterFlows NewsletterFlows { get; }
    public SupportTicketFlows SupportTicketFlows { get; }
    public LoanApplicationFlows LoanApplicationFlows { get; }

    public Flows(IFunctionStore functionStore, Settings? settings = null)
    {
        var rFunctions = new RFunctions(functionStore, settings);
        OrderFlows = new OrderFlows(rFunctions);
        TransferFlows = new TransferFlows(rFunctions);
        NewsletterFlows = new NewsletterFlows(rFunctions);
        SupportTicketFlows = new SupportTicketFlows(rFunctions);
        LoanApplicationFlows = new LoanApplicationFlows(rFunctions);
    }
}