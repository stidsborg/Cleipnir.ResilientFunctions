using Npgsql;

namespace Sample.WebApi.OrderProcessing.DataAccess;

public class SqlConnectionFactory
{
    private readonly string _connectionString;

    public SqlConnectionFactory(string connectionString) => _connectionString = connectionString;

    public async Task<NpgsqlConnection> Create()
    {
        var conn = new NpgsqlConnection(_connectionString);
        await conn.OpenAsync();
        return conn;
    }

    public static Task<NpgsqlConnection> Create(string connectionString)
        => new SqlConnectionFactory(connectionString).Create();
}