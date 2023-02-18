using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL.Tests;

[TestClass]
public class BinaryDataTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.BinaryDataTests
{
    [TestMethod]
    public override async Task PersistAndRetrieveBinaryData()
    {
        await using var connection = new NpgsqlConnection(Sql.ConnectionString);
        await connection.OpenAsync();
        {
            await using var command = new NpgsqlCommand(
                @"CREATE TABLE binarytest (b bytea);",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        await PersistAndRetrieveBinaryData(
            save: async bytes =>
            {
                var cmd = new NpgsqlCommand("INSERT INTO binarytest (b) VALUES ($1)", connection)
                {
                    Parameters = { new() { Value = bytes } }
                };
                await cmd.ExecuteNonQueryAsync();
            },
            retrieve: async () =>
            {
                var cmd = new NpgsqlCommand("SELECT b FROM binarytest LIMIT 1", connection);
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var bytes = (byte[])reader[0];
                return bytes;
            }
        );
    }
}