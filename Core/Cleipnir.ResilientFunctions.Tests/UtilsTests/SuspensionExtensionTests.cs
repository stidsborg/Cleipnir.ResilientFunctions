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
    public async Task OnExceptionSuspendForSunshine()
    {
        await Task.CompletedTask
            .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
    }  
    
    [TestMethod]
    public async Task OnExceptionSuspendUntilSunshine()
    {
        await Task.CompletedTask
            .OnExceptionSuspendUntil(DateTime.Now);
    }  
    
    [TestMethod]
    public static async Task OnExceptionSuspendForWithResultSunshine()
    {
        var result = await Task.FromResult("hello").OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        result.ShouldBe("hello");
    }
    
    [TestMethod]
    public static async Task OnExceptionWithResultSuspendUntilSunshine()
    {
        var result = await Task.FromResult("hello").OnExceptionSuspendUntil(DateTime.Now);
        result.ShouldBe("hello");
    }
    
    [TestMethod]
    public async Task OnExceptionSuspendForException()
    {
        var utcNow = DateTime.UtcNow;
        try
        {
            await Task
                .FromException(new TimeoutException())
                .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        }
        catch (PostponeInvocationException ex)
        {
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(10) > TimeSpan.Zero).ShouldBeTrue();
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(11) < TimeSpan.Zero).ShouldBeTrue();
        }
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
    public async Task OnExceptionWithResultSuspendForException()
    {
        var utcNow = DateTime.UtcNow;
        try
        {
            await Task
                .FromException<string>(new TimeoutException())
                .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        }
        catch (PostponeInvocationException ex)
        {
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(10) > TimeSpan.Zero).ShouldBeTrue();
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(11) < TimeSpan.Zero).ShouldBeTrue();
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
    public async Task OnExceptionSuspendForSunshineValueTask()
    {
        await ValueTask.CompletedTask
            .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
    }  
    
    [TestMethod]
    public async Task OnExceptionSuspendUntilSunshineValueTask()
    {
        await ValueTask.CompletedTask
            .OnExceptionSuspendUntil(DateTime.Now);
    }  
    
    [TestMethod]
    public static async Task OnExceptionSuspendForWithResultSunshineValueTask()
    {
        var result = await ValueTask.FromResult("hello").OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        result.ShouldBe("hello");
    }
    
    [TestMethod]
    public static async Task OnExceptionWithResultSuspendUntilSunshineValueTask()
    {
        var result = await ValueTask.FromResult("hello").OnExceptionSuspendUntil(DateTime.Now);
        result.ShouldBe("hello");
    }
    
    [TestMethod]
    public async Task OnExceptionSuspendForExceptionValueTask()
    {
        var utcNow = DateTime.UtcNow;
        try
        {
            await ValueTask
                .FromException(new TimeoutException())
                .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        }
        catch (PostponeInvocationException ex)
        {
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(10) > TimeSpan.Zero).ShouldBeTrue();
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(11) < TimeSpan.Zero).ShouldBeTrue();
        }
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
    public async Task OnExceptionWithResultSuspendForExceptionValueTask()
    {
        var utcNow = DateTime.UtcNow;
        try
        {
            await ValueTask
                .FromException<string>(new TimeoutException())
                .OnExceptionSuspendFor(TimeSpan.FromMinutes(10));
        }
        catch (PostponeInvocationException ex)
        {
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(10) > TimeSpan.Zero).ShouldBeTrue();
            ((ex.PostponeUntil - utcNow) - TimeSpan.FromMinutes(11) < TimeSpan.Zero).ShouldBeTrue();
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