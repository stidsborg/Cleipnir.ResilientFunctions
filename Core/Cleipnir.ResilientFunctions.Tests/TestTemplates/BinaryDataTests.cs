using System;
using System.Text;
using System.Threading.Tasks;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.TestTemplates;

public abstract class BinaryDataTests
{
    public abstract Task PersistAndRetrieveBinaryData();

    public async Task PersistAndRetrieveBinaryData(Func<byte[], Task> save, Func<Task<byte[]>> retrieve)
    {
        var msg = "hello world";
        var msgBytes = Encoding.UTF8.GetBytes(msg);
        await save(msgBytes);
        var retrieved = await retrieve();
        var retrievedMsg = Encoding.UTF8.GetString(retrieved);
        retrievedMsg.ShouldBe(msg);
    }
}