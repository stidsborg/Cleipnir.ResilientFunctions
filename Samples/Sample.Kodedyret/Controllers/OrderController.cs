using Microsoft.AspNetCore.Mvc;

namespace Sample.Kodedyret.Controllers;

[ApiController]
[Route("[controller]")]
public class OrderController : ControllerBase
{
    private readonly V0.OrderProcessor _orderProcessor;

    public OrderController(V0.OrderProcessor orderProcessor)
    {
        _orderProcessor = orderProcessor;
    }
    
    [HttpPost]
    public async Task Post(V0.Order order)
    {
        await _orderProcessor.ProcessOrder(order);
    }
}