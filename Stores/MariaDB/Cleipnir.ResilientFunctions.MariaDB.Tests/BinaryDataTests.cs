using Microsoft.VisualStudio.TestTools.UnitTesting;
using MySqlConnector;

namespace Cleipnir.ResilientFunctions.MariaDb.Tests;

[TestClass]
public class BinaryDataTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.BinaryDataTests
{
    [TestMethod]
    public override async Task PersistAndRetrieveBinaryData()
    {
        await using var connection = new MySqlConnection(Sql.ConnectionString);
        await connection.OpenAsync();
        {
            await using var command = new MySqlCommand(
                @"CREATE TABLE binarytest (b VARBINARY(65000));",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        await PersistAndRetrieveBinaryData(
            save: async bytes =>
            {
                var cmd = new MySqlCommand("INSERT INTO binarytest (b) VALUES (?)", connection)
                {
                    Parameters = { new() { Value = bytes } }
                };
                await cmd.ExecuteNonQueryAsync();
            },
            retrieve: async () =>
            {
                var cmd = new MySqlCommand("SELECT b FROM binarytest LIMIT 1", connection);
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var bytes = (byte[])reader[0];
                return bytes;
            }
        );
    }
}