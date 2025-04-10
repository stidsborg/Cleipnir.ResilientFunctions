using System;
using System.Threading.Tasks;
using Cleipnir.ResilientFunctions.Domain.Exceptions.Commands;
using Cleipnir.ResilientFunctions.Helpers.Exposed;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Shouldly;

namespace Cleipnir.ResilientFunctions.Tests.UtilsTests;

[TestClass]
public class SuspensionExtensionTests
{
    [TestMethod]
    public async Task OnExceptionSuspendUntilSunshine()
    {
        await Task.CompletedTask
            .OnExceptionSuspendUntil(DateTime.Now);
    }  
    
    [TestMethod]
    public async Task OnExceptionWithResultSuspendUntilSunshine()
    {
        var result = await Task.FromResult("hello").OnExceptionSuspendUntil(DateTime.Now);
        result.ShouldBe("hello");
    }

    [TestMethod]
    public async Task OnExceptionSuspendUntilException()
    {
        var suspendUntil = DateTime.UtcNow.AddMinutes(10);
        try
        {
            await Task
                .FromException(new TimeoutException())
                .OnExceptionSuspendUntil(suspendUntil);
        }
        catch (PostponeInvocationException ex)
        {
            ex.PostponeUntil.ShouldBe(suspendUntil);
        }
    }  
    
    [TestMethod]
    public async Task OnExceptionWithResultSuspendUntilException()
    {
        var suspendUntil = DateTime.UtcNow.AddMinutes(10);
        try
        {
            await Task
                .FromException<string>(new TimeoutException())
                .OnExceptionSuspendUntil(suspendUntil);
        }
        catch (PostponeInvocationException ex)
        {
            ex.PostponeUntil.ShouldBe(suspendUntil);
        }
    }

    #region ValueTasks
    
    [TestMethod]
    public async Task OnExceptionSuspendUntilSunshineValueTask()
    {
        await ValueTask.CompletedTask
            .OnExceptionSuspendUntil(DateTime.Now);
    }  
    
    [TestMethod]
    public async Task OnExceptionWithResultSuspendUntilSunshineValueTask()
    {
        var result = await ValueTask.FromResult("hello").OnExceptionSuspendUntil(DateTime.Now);
        result.ShouldBe("hello");
    }
    
    [TestMethod]
    public async Task OnExceptionSuspendUntilExceptionValueTask()
    {
        var suspendUntil = DateTime.UtcNow.AddMinutes(10);
        try
        {
            await ValueTask
                .FromException(new TimeoutException())
                .OnExceptionSuspendUntil(suspendUntil);
        }
        catch (PostponeInvocationException ex)
        {
            ex.PostponeUntil.ShouldBe(suspendUntil);
        }
    }  
    
    [TestMethod]
    public async Task OnExceptionWithResultSuspendUntilExceptionValueTask()
    {
        var suspendUntil = DateTime.UtcNow.AddMinutes(10);
        try
        {
            await ValueTask
                .FromException<string>(new TimeoutException())
                .OnExceptionSuspendUntil(suspendUntil);
        }
        catch (PostponeInvocationException ex)
        {
            ex.PostponeUntil.ShouldBe(suspendUntil);
        }
    }

    #endregion
}