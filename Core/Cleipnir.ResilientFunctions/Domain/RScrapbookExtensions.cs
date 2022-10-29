using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading.Tasks;

namespace Cleipnir.ResilientFunctions.Domain;

public static class RScrapbookExtensions
{
    private static readonly object Sync = new();
    private static readonly Dictionary<PropertyInfo, GetterAndSetter> GetterAndSetters = new();
        
    public static async Task DoAtMostOnce(
        this RScrapbook scrapbook, 
        string workId, 
        Func<Task> work, 
        bool flushCompletedStatusImmediately = true)
    {
        if (scrapbook.StateDictionary.ContainsKey(workId))
        {
            var value = scrapbook.StateDictionary[workId];
            var success = Enum.TryParse<WorkStatus>(value, ignoreCase: true, out var workStatus);
            if (!success)
                throw new InvalidOperationException($"Current value '{value}' could not be converted to {nameof(WorkStatus)} enum");

            if (workStatus == WorkStatus.Completed) return;
            if (workStatus == WorkStatus.Started) 
                throw new InvalidOperationException("Previous work was started but not completed");
        }

        scrapbook.StateDictionary[workId] = WorkStatus.Started.ToString();
        await scrapbook.Save();

        await work();
        
        scrapbook.StateDictionary[workId] = WorkStatus.Completed.ToString();
        if (flushCompletedStatusImmediately)
            await scrapbook.Save();
    }
    
    public static async Task DoAtMostOnce<TScrapbook>(
        this TScrapbook scrapbook, 
        Expression<Func<TScrapbook, WorkStatus>> workStatus, 
        Func<Task> work,
        bool flushCompletedStatusImmediately = true) where TScrapbook : RScrapbook
    {
        var getterAndSetter = GetOrCreateGetterAndSetter(workStatus);
        var getter = (Func<TScrapbook, WorkStatus>)getterAndSetter.Getter;
        var setter = (Action<TScrapbook, WorkStatus>)getterAndSetter.Setter;

        var workStatusValue = getter(scrapbook);
        if (workStatusValue == WorkStatus.Completed) return;
        if (workStatusValue == WorkStatus.Started)
            throw new InvalidOperationException("Previous work was started but not completed");

        setter(scrapbook, WorkStatus.Started);
        await scrapbook.Save(); 
        
        await work();
        
        setter(scrapbook, WorkStatus.Completed);
        if (flushCompletedStatusImmediately) await scrapbook.Save();
    }

    private static GetterAndSetter GetOrCreateGetterAndSetter<TScrapbook>(Expression<Func<TScrapbook, WorkStatus>> property)
    {
        //fast-path
        var existingGetterAndSetter = GetExistingGetterAndSetter(property);
        if (existingGetterAndSetter != null)
            return existingGetterAndSetter;
        
        //slow-path
        var propertyInfo = GetPropertyInfoAndValidatePropertyExpression(property);

        //generate getter and setter delegates
        var getter = propertyInfo.GetMethod!.CreateDelegate(typeof(Func<TScrapbook, WorkStatus>));
        var setter = propertyInfo.SetMethod!.CreateDelegate(typeof(Action<TScrapbook, WorkStatus>));
        var getterAndSetter = new GetterAndSetter(getter, setter);
        lock (Sync)
            GetterAndSetters[propertyInfo] = getterAndSetter;

        return getterAndSetter;
    }
    
    private static GetterAndSetter? GetExistingGetterAndSetter<TScrapbook>(Expression<Func<TScrapbook, WorkStatus>> propertyLambda)
    {
        var member = propertyLambda.Body as MemberExpression;
        if (member == null) return null;

        var propInfo = member.Member as PropertyInfo;
        if (propInfo == null) return null;

        lock (Sync)
            if (GetterAndSetters.ContainsKey(propInfo))
                return GetterAndSetters[propInfo];

        return null;
    }
    
    private static PropertyInfo GetPropertyInfoAndValidatePropertyExpression<TScrapbook>(Expression<Func<TScrapbook, WorkStatus>> propertyLambda)
    {
        var member = propertyLambda.Body as MemberExpression;
        if (member == null)
            throw new ArgumentException($"Expression '{propertyLambda}' must refer to a property of type {nameof(WorkStatus)}");

        var propInfo = member.Member as PropertyInfo;
        if (propInfo == null)
            throw new ArgumentException($"Expression '{propertyLambda}' must refer to a property of type {nameof(WorkStatus)}");
        
        var type = typeof(TScrapbook);
        if (type != propInfo.ReflectedType)
            throw new ArgumentException($"Expression '{propertyLambda}' must refer to a property of type {nameof(WorkStatus)}");
        if (propInfo.SetMethod == null)
            throw new ArgumentException($"Expression '{propertyLambda}' must refer to a settable property");
        if (propInfo.GetMethod == null)
            throw new ArgumentException($"Expression '{propertyLambda}' must refer to a gettable property");
        
        return propInfo;
    }

    private record GetterAndSetter(object Getter, object Setter);
}