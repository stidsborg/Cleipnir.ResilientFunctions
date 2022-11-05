using Sample.Kodedyret.V4;

namespace Sample.Kodedyret.V4StateMachineApproach;

public class OrderProcessor
{
    private readonly MessageBroker _messageBroker;

    public OrderProcessor(MessageBroker messageBroker)
    {
        _messageBroker = messageBroker;
    }

    private State _state = State.Initialized;
    private Order? _order;
    private Guid _transactionId = Guid.Empty;

    public void Initialize(Order order, Guid transactionId)
    {
        _order = order;
        _transactionId = transactionId;
    }

    public void Handle(FundsReserved fundsReserved)
    {
        _state = State.FundsReserved;
    }
    
    public void Handle(ProductsShipped productsShipped)
    {
        _state = State.ProductsShipped;
    }
    
    public void Handle(FundsCaptured fundsCaptured)
    {
        _state = State.FundsCaptured;
    }
    
    public void Handle(OrderConfirmationEmailSent orderConfirmationEmailSent)
    {
        _state = State.OrderConfirmationEmailSent;
    }

    public async Task ExecuteNextStep()
    {
        switch (_state)
        {
            case State.Initialized:
                await _messageBroker.Send(
                    new ReserveFunds(_order!.OrderId, _order.TotalPrice, _transactionId, _order.CustomerId)
                );
                break;
            case State.FundsReserved:
                await _messageBroker.Send(new ShipProducts(_order!.OrderId, _order.CustomerId, _order.ProductIds));
                break;
            case State.ProductsShipped:
                await _messageBroker.Send(new CaptureFunds(_order!.OrderId, _order.CustomerId!, _transactionId));
                break;
            case State.FundsCaptured:
                await _messageBroker.Send(new SendOrderConfirmationEmail(_order!.OrderId, _order.CustomerId));
                break;
            case State.OrderConfirmationEmailSent:
                break;
            case State.Completed:
                break;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
    
    private enum State
    {
        Initialized,
        FundsReserved,
        ProductsShipped,
        FundsCaptured,
        OrderConfirmationEmailSent,
        Completed
    }
}