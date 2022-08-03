using System.Text.Json;
using Dapper;

namespace Sample.WebApi.OrderProcessing.DataAccess;

public interface IOrdersRepository
{
    Task<bool> Insert(Domain.Order order);
    Task DeleteAllEntries();
}

public class OrdersRepository : IOrdersRepository
{
    private readonly SqlConnectionFactory _sqlConnectionFactory;
    public OrdersRepository(SqlConnectionFactory sqlConnectionFactory) => _sqlConnectionFactory = sqlConnectionFactory;

    public async Task<bool> Insert(Domain.Order order)
    {
        await using var conn = await _sqlConnectionFactory.Create();
        var affectedRows = await conn.ExecuteAsync(@"
            INSERT INTO orders 
                (order_id, products, customer_id)
            VALUES
                (@OrderId, @Products, @CustomerId)
            ON CONFLICT DO NOTHING;",
            new
            {
                order.OrderId, 
                Products = JsonSerializer.Serialize(order.ProductIds), 
                order.CustomerId
            }
        );

        return affectedRows == 1;
    }

    public async Task DeleteAllEntries()
    {
        await using var conn = await _sqlConnectionFactory.Create();
        await conn.ExecuteAsync("TRUNCATE TABLE orders");
    }
}