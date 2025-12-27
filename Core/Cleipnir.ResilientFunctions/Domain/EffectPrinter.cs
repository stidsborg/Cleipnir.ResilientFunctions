using System.Collections.Generic;
using System.Linq;
using System.Text;
using Cleipnir.ResilientFunctions.Storage;

namespace Cleipnir.ResilientFunctions.Domain;

public static class EffectPrinter
{
    public static string Print(EffectResults effectResults)
    {
        var results = effectResults.Results;
        return Print(results);
    }

    public static string Print(Dictionary<EffectId, PendingEffectChange> results)
    {
        AddMissingStartedEffects(results);

        var sb = new StringBuilder();
        var rootEffects = results.Keys
            .Where(id => id.Value.Length == 1)
            .OrderBy(id => id.Value[0])
            .ToList();

        foreach (var rootEffect in rootEffects)
        {
            PrintEffect(results, rootEffect, "", true, sb);
        }

        return sb.ToString();
    }

    private static void PrintEffect(Dictionary<EffectId, PendingEffectChange> effectResults, EffectId effectId, string prefix, bool isLast, StringBuilder sb)
    {
        var pendingChange = effectResults[effectId];
        var storedEffect = pendingChange.StoredEffect;
        var isDirty = pendingChange.Operation != null;

        // Tree branch characters
        var connector = isLast ? "└─ " : "├─ ";
        sb.Append(prefix);
        sb.Append(connector);

        // Color for dirty effects (yellow)
        if (isDirty)
            sb.Append("\x1b[33m");

        // Status symbol
        var statusSymbol = storedEffect?.WorkStatus switch
        {
            WorkStatus.Completed => "✓",
            WorkStatus.Failed => "✗",
            WorkStatus.Started => "⋯",
            _ => "?"
        };
        sb.Append(statusSymbol);
        sb.Append(" ");

        // Effect ID (just the last number for clarity)
        sb.Append($"[{effectId.Id}]");

        // Alias if present
        if (!string.IsNullOrEmpty(storedEffect?.Alias))
        {
            sb.Append($" {storedEffect.Alias}");
        }

        // Additional info for failed effects
        if (storedEffect?.StoredException != null)
        {
            sb.Append($" ({storedEffect.StoredException.ExceptionType})");
        }

        // Reset color
        if (isDirty)
            sb.Append("\x1b[0m");

        sb.AppendLine();

        // Process children (direct children only, not all descendants)
        var children = effectResults
            .Values
            .Select(p => p.Id)
            .Where(id => effectId.IsChild(id))
            .OrderBy(id => id.Value[^1])
            .ToList();

        for (int i = 0; i < children.Count; i++)
        {
            var childPrefix = prefix + (isLast ? "   " : "│  ");
            PrintEffect(effectResults, children[i], childPrefix, i == children.Count - 1, sb);
        }
    }

    private static void AddMissingStartedEffects(Dictionary<EffectId, PendingEffectChange> effectResults)
    {
        foreach (var change in effectResults.Values.ToList())
            AddMissingAncestors(effectResults, change.Id);
    }

    private static void AddMissingAncestors(Dictionary<EffectId, PendingEffectChange> effectResults, EffectId effectId)
    {
        if (effectId.Value.Length <= 1)
            return; // Root effect, no parent needed

        var parentId = new EffectId(effectId.Context);

        if (!effectResults.ContainsKey(parentId))
        {
            // Create a missing parent with Started status
            var missingEffect = new StoredEffect(
                parentId,
                WorkStatus.Started,
                Result: null,
                StoredException: null,
                Alias: null
            );

            effectResults[parentId] = new PendingEffectChange(
                parentId,
                missingEffect,
                Operation: CrudOperation.Insert,
                Existing: false,
                Alias: null
            );
        }

        // Recursively ensure parent's ancestors exist
        AddMissingAncestors(effectResults, parentId);
    }
}
