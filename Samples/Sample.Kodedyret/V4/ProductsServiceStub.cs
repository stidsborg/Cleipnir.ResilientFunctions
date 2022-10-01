namespace Sample.Kodedyret.V4;

public class ProductsServiceStub 
{
    private readonly MessageBroker _messageBroker;

    public ProductsServiceStub(MessageBroker messageBroker)
    {
        _messageBroker = messageBroker;
        messageBroker.Subscribe(MessageHandler);
    }

    private async Task MessageHandler(EventsAndCommands message)
    {
        if (message is not GetProductsTotalPrice command)
            return;

        await Task.Delay(1_000);
        await _messageBroker.Send(
            new ProductsTotalPrice(
                command.OrderId,
                TotalPrice: command.ProductId.Count() * 25
            )
        );
    }
}