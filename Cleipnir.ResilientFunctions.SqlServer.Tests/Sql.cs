using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace Cleipnir.ResilientFunctions.SqlServer.Tests
{
    public static class Sql
    {
        public static Func<Task<SqlConnection>> ConnFunc { get; }

        static Sql()
        {
            var connectionString = Environment.GetEnvironmentVariable("Cleipnir.RFunctions.SqlServer.Tests.ConnectionString");
            if (connectionString == null)
                throw new NullReferenceException("Environment variable 'Cleipnir.RFunctions.SqlServer.Tests.ConnectionString' was not set");

            ConnFunc = async () =>
            {
                var conn = new SqlConnection(connectionString);
                await conn.OpenAsync();
                return conn;
            };
        }
    }
}