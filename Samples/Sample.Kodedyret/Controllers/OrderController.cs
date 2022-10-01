using Microsoft.AspNetCore.Mvc;
using Sample.Kodedyret.V0;

namespace Sample.Kodedyret.Controllers;

[ApiController]
[Route("[controller]")]
public class OrderController : ControllerBase
{
    private readonly OrderProcessor _orderProcessor;

    public OrderController(OrderProcessor orderProcessor)
    {
        _orderProcessor = orderProcessor;
    }
    
    [HttpPost]
    public async Task Post(Order order)
    {
        await _orderProcessor.ProcessOrder(order);
    }
}