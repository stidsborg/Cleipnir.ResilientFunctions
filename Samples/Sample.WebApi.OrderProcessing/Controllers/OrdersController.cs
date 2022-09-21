﻿using Microsoft.AspNetCore.Mvc;
using Sample.WebApi.OrderProcessing.Domain;

namespace Sample.WebApi.OrderProcessing.Controllers;

[ApiController]
[Route("[controller]")]
public class OrdersController : ControllerBase
{
    private readonly BusinessLogic.MessageBased.OrderProcessor _orderProcessor;
    private readonly ILogger<OrdersController> _logger;

    public OrdersController(BusinessLogic.MessageBased.OrderProcessor orderProcessor, ILogger<OrdersController> logger)
    {
        _orderProcessor = orderProcessor;
        _logger = logger;
    }

    [HttpPost]
    public async Task Post(Order order)
    {
        try
        {
            await _orderProcessor.ProcessOrder(
                order.OrderId,
                order,
                new BusinessLogic.MessageBased.OrderProcessor.Scrapbook()
            );
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Failed order processing: '{OrderId}'", order.OrderId);
        }
    } 
}