# Tableau Migration SDK - Code Examples

This folder contains progressively complex examples showing how to use the Tableau Migration SDK, with a focus on **subscriptions** and **site/project mapping**.

---

## Example Files (In Order)

### 1. BasicMigrationSetup.cs
**What it shows:** The absolute minimum code to run a migration

**Key concepts:**
- How to configure source (Tableau Server)
- How to configure destination (Tableau Cloud)
- How to execute a migration
- How to check results

**When to use:** Understanding the basic structure

---

### 2. ProjectMappingExample.cs
**What it shows:** How to rename projects during migration

**Key concepts:**
- Creating a custom mapping class
- Dictionary-based mapping rules
- How projects get renamed
- How workbooks automatically follow to new projects

**Real-world use case:**
- Aggregating multiple sites → single Cloud site with prefixed project names
- Example: "Marketing" → "Cloud - Marketing"

---

### 3. UserMappingForSubscriptions.cs
**What it shows:** How to map users when email domains change

**Key concepts:**
- Email domain mapping (bulk change)
- Specific user mapping (individual overrides)
- Multiple mappings working together
- Why this is critical for subscriptions

**Real-world use case:**
- Company email domain changes during Cloud migration
- Example: john@oldcompany.com → john@newcompany.com
- Subscriptions need to find users by their new emails

---

### 4. CompleteSubscriptionMigration.cs
**What it shows:** Everything together - a production-ready example

**Key concepts:**
- Project mapping + User mapping combined
- Subscription filtering (exclude test subscriptions)
- Migration manifest (see what was migrated)
- Error handling and reporting

**Real-world use case:**
- Full migration from on-prem Server to Cloud
- Projects renamed with "Cloud -" prefix
- Email domain changes
- Test content filtered out
- Subscriptions automatically updated

---

## How Subscriptions Work in These Examples

### What Gets Migrated Automatically:
1. **Users** (with email mapping applied)
2. **Projects** (with name mapping applied)
3. **Workbooks** (moved to new project locations)
4. **Subscriptions** (references updated automatically)

### The Magic:
The SDK tracks all mappings and automatically updates subscription references:

```
SOURCE Subscription:
  User: alice@old.com
  Workbook: "Report" in project "Sales"
  Schedule: Every Monday

MAPPINGS APPLIED:
  User: alice@old.com → alice@new.com
  Project: "Sales" → "Cloud - Sales"

DESTINATION Subscription:
  User: alice@new.com  ← Updated!
  Workbook: "Report" in project "Cloud - Sales"  ← Updated!
  Schedule: Every Monday
```

You don't have to manually update subscriptions - the SDK does it!

---

## Running These Examples

### Prerequisites:
1. .NET 8 SDK installed
2. Tableau Migration SDK NuGet package
3. Access tokens for both source and destination

### Setup:
```bash
# Add the Tableau Migration SDK package
dotnet add package Tableau.Migration

# Restore packages
dotnet restore
```

### Configuration:
Replace these placeholders in the code:
- `https://tableau-server.company.com` → Your Tableau Server URL
- `https://10ax.online.tableau.com` → Your Tableau Cloud POD URL
- `"your-token-name"` → Your actual token names
- `"your-secret"` → Your actual token secrets

### Running:
```bash
# Run a specific example
dotnet run --project TableauMigrationExample
```

---

## Testing Safely

### Before Migrating Production:

1. **Use a Test Cloud Site**
   - Create a separate site for testing
   - Don't test on production!

2. **Start Small**
   - Migrate a single project first
   - Verify subscriptions work
   - Check user mappings

3. **Use Filters**
   - Exclude most content initially
   - Add more as you gain confidence

4. **Review the Manifest**
   - Check what was migrated
   - Verify counts match expectations
   - Look for errors

5. **Verify Subscriptions Manually**
   - Log into Cloud
   - Check that subscriptions exist
   - Verify schedules are correct
   - Test receiving emails

---

## Key Differences from Content Migration Tool (CMT)

| Feature | CMT | This SDK Approach |
|---------|-----|------------------|
| **Subscriptions** | ❌ Not supported | ✅ Fully automated |
| **User Migration** | ❌ Manual | ✅ Automatic with mapping |
| **Project Renaming** | Limited | ✅ Flexible mapping |
| **Email Updates** | ❌ Manual | ✅ Automatic domain mapping |
| **Cloud Migration** | ❌ Not recommended | ✅ Designed for it |
| **Customization** | Scripts only | ✅ Full C# code |

---

## Next Steps

1. **Read** `SUBSCRIPTIONS_AND_MAPPING.md` for conceptual understanding
2. **Study** these examples in order (1 → 2 → 3 → 4)
3. **Modify** the mapping rules for your environment
4. **Test** with a small subset of content
5. **Expand** to full migration

---

## Common Questions

### Q: Do I need to migrate subscriptions separately?
**A:** No! Subscriptions migrate automatically when you migrate workbooks/views.

### Q: What if a user doesn't exist in the destination?
**A:** The SDK will report an error. Make sure to migrate users first (SDK does this automatically).

### Q: Can I test without actually migrating?
**A:** Yes! You can build the plan and inspect it without executing, or migrate to a test site.

### Q: What if I need to map site names too?
**A:** You configure source and destination sites explicitly in the plan builder - that's your site mapping!

### Q: Can I migrate from Cloud to Cloud?
**A:** Yes, with some limitations. The SDK primarily targets Server → Cloud, but Cloud → Cloud is possible with customization.

---

## Getting Help

- **SDK Documentation:** https://help.tableau.com/current/api/migration_sdk/en-us/
- **GitHub Issues:** https://github.com/tableau/tableau-migration-sdk/issues
- **Code Samples:** https://tableau.github.io/migration-sdk/samples/

---

## Summary

These examples show you how to:
1. Set up a basic migration
2. Map projects (rename during migration)
3. Map users (change emails during migration)
4. Put it all together for subscription migration

The key insight: **You define the mapping rules, the SDK handles all the complexity of updating references, including subscriptions.**
