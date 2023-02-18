using System.Data;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests;

[TestClass]
public class BinaryDataTests : Cleipnir.ResilientFunctions.Tests.TestTemplates.BinaryDataTests
{
    [TestMethod]
    public override async Task PersistAndRetrieveBinaryData()
    {
        await using var connection = new SqlConnection(Sql.ConnectionString);
        await connection.OpenAsync();
        {
            await using var command = new SqlCommand(
                @"CREATE TABLE binarydatatest (b VARBINARY(MAX));",
                connection
            );
            await command.ExecuteNonQueryAsync();
        }

        await PersistAndRetrieveBinaryData(
            save: async bytes =>
            {
                var cmd = new SqlCommand("INSERT INTO binarydatatest (b) VALUES (@BinaryData)", connection);
                var parameter = cmd.Parameters.Add("@BinaryData", SqlDbType.VarBinary);
                parameter.Value = bytes;
                await cmd.ExecuteNonQueryAsync();
            },
            retrieve: async () =>
            {
                var cmd = new SqlCommand("SELECT TOP(1) b FROM binarydatatest", connection);
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var bytes = (byte[])reader.GetValue(0);
                return bytes;
            }
        );
    }
}