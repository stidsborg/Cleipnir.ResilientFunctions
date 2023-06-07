using Cleipnir.ResilientFunctions.CoreRuntime.Invocation;
using Sample.Holion.A.Ordering;
using Sample.Holion.A.Ordering.Clients;
using Sample.Holion.B.BankTransfer;
using Sample.Holion.C;
using Sample.Holion.C.Newsletter;
using Sample.Holion.D;
using Sample.Holion.D.SupportTicket;
using Sample.Holion.E;
using LoanApplicationFlow = Sample.Holion.Solutions.E.LoanApplicationFlow;

namespace Sample.Holion.Utils;

public class DependencyResolver : IDependencyResolver
{
    public static DependencyResolver Instance { get; } = new();
    private class ScopedDependencyResolver : IScopedDependencyResolver
    {
        public T Resolve<T>() where T : notnull
        {
            if (typeof(T) == typeof(OrderFlow))
                return (T) (object) new OrderFlow(new PaymentProviderClientStub(), new EmailClientStub(), new LogisticsClientStub());
            if (typeof(T) == typeof(TransferFlow))
                return (T) (object) new TransferFlow(new AccountService());
            if (typeof(T) == typeof(NewsletterFlow))
                return (T) (object) new NewsletterFlow();
            if (typeof(T) == typeof(SupportTicketFlow))
                return (T) (object) new SupportTicketFlow();
            if (typeof(T) == typeof(LoanApplicationFlow))
                return (T) (object) new LoanApplicationFlow();
            
            throw new ArgumentException($"Unable to resolve type: {typeof(T)}");
        }
        
        public void Dispose() { }
    }
    
    public IScopedDependencyResolver CreateScope()
    {
        return new ScopedDependencyResolver();
    }
}