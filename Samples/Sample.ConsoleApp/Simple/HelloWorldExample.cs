﻿using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions;
using Cleipnir.ResilientFunctions.Storage;

namespace ConsoleApp.Simple;

public class HelloWorldExample
{
    public static async Task Do()
    {
        var store = new InMemoryFunctionStore();
        var functions = new RFunctions(store, unhandledExceptionHandler: Console.WriteLine);

        var rFunc = functions.RegisterFunc(
            functionTypeId: "HelloWorld",
            inner: (string param) => param.ToUpper()
        ).Invoke;

        var returned = await rFunc(functionInstanceId: "", param: "hello world");
        Console.WriteLine($"Returned: '{returned}'");
    }
}