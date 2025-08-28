using System.Collections.Generic;
using System.Threading.Tasks;
using Npgsql;

namespace Cleipnir.ResilientFunctions.PostgreSQL;

public static class DapperLight
{
    public static async Task<List<T>> Query<T>(this NpgsqlConnection conn, string sql, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var parameter in parameters)
            cmd.Parameters.AddWithValue(parameter);
        
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<T>();
        while (await reader.ReadAsync())
            rows.Add((T) reader.GetValue(0));
        
        return rows;
    }
    
    public static async Task<List<Row<T1, T2>>> Query<T1, T2>(this NpgsqlConnection conn, string sql, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var parameter in parameters)
            cmd.Parameters.AddWithValue(parameter);
        
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<Row<T1, T2>>();
        while (await reader.ReadAsync())
        {
            var t1 = (T1) reader.GetValue(0);            
            var t2 = (T2) reader.GetValue(1); 
            rows.Add(new Row<T1, T2>(t1, t2));
        }
        
        return rows;
    }
    
    public static async Task<List<Row<T1, T2, T3>>> Query<T1, T2, T3>(this NpgsqlConnection conn, string sql, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var parameter in parameters)
            cmd.Parameters.AddWithValue(parameter);
        
        await using var reader = await cmd.ExecuteReaderAsync();
        var rows = new List<Row<T1, T2, T3>>();
        while (await reader.ReadAsync())
        {
            var t1 = (T1) reader.GetValue(0);            
            var t2 = (T2) reader.GetValue(1); 
            var t3 = (T3) reader.GetValue(2); 
            rows.Add(new Row<T1, T2, T3>(t1, t2, t3));
        }
        
        return rows;
    }
    
    public static async Task<int> Execute(this NpgsqlConnection conn, string sql, params object[] parameters)
    {
        await using var cmd = new NpgsqlCommand(sql, conn);
        foreach (var parameter in parameters)
            cmd.Parameters.AddWithValue(parameter);

        return await cmd.ExecuteNonQueryAsync();        
    }
}

public record Row<T1, T2>(T1 Value1, T2 Value2);
public record Row<T1, T2, T3>(T1 Value1, T2 Value2, T3 Value3);