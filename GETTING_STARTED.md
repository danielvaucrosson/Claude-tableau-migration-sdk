# Getting Started with Tableau Migration SDK - Subscriptions Focus

## What We've Built

I've created a complete learning path for using the Tableau Migration SDK, focused specifically on **subscriptions** and **site/project mapping** for Cloud migrations.

---

## 📁 Files Created

### 1. Documentation Files

**TABLEAU_SDK_OVERVIEW.md**
- What the SDK is (vs APIs)
- Content types it can migrate
- Basic concepts (filters, mappings, transformers, hooks)
- Prerequisites and setup requirements

**PROJECT_PLAN.md**
- Original plan for GUI application
- Comparison of CMT vs SDK capabilities
- Architecture for future enhancements

**SUBSCRIPTIONS_AND_MAPPING.md** ⭐ **START HERE**
- Clear explanation of subscriptions in Tableau
- How mapping works (with real-world examples)
- Why mapping is critical for subscriptions
- The "pipeline" concept (Filter → Map → Transform → Hooks)

### 2. Code Examples

Located in `TableauMigrationExample/Examples/`

**1_BasicMigrationSetup.cs**
- Minimal code to run a migration
- Source/Destination configuration
- Execution and result checking

**2_ProjectMappingExample.cs**
- How to rename projects during migration
- Dictionary-based mapping rules
- Real scenario: "Campaign Analytics" → "Marketing - Campaigns"

**3_UserMappingForSubscriptions.cs** ⭐ **KEY FILE**
- Email domain mapping (bulk changes)
- Specific user mappings (individual overrides)
- Why this is critical for subscriptions

**4_CompleteSubscriptionMigration.cs** ⭐ **COMPLETE EXAMPLE**
- Everything together: projects + users + subscriptions
- Filtering test content
- Production-ready structure
- Manifest inspection (see what was migrated)

**Examples/README.md**
- Detailed guide to all examples
- How subscriptions work with mapping
- Testing strategies
- Common questions answered

---

## 🎯 What You Asked For

✅ **"Start with subscriptions"**
- Created comprehensive subscription examples
- Showed how SDK migrates them (CMT can't!)

✅ **"Account for site and project mapping"**
- Detailed examples of project renaming
- Site aggregation scenarios (multiple sites → one Cloud site)
- Real-world use cases

✅ **"Take it slowly, understand the process"**
- Progressive examples (1 → 2 → 3 → 4)
- Heavy comments explaining what happens
- Conceptual docs before code

✅ **"Start small"**
- Focused on one content type (subscriptions)
- Clear, isolated examples
- No GUI complexity to start

---

## 🚀 Next Steps - How to Use This

### Step 1: Read the Documentation (15 minutes)
```
1. Open: SUBSCRIPTIONS_AND_MAPPING.md
2. Read through the whole thing
3. Focus on the "Real-World Example" section
```

### Step 2: Study the Code Examples (30 minutes)
```
1. Open: TableauMigrationExample/Examples/README.md
2. Read through each example in order:
   - 1_BasicMigrationSetup.cs
   - 2_ProjectMappingExample.cs
   - 3_UserMappingForSubscriptions.cs
   - 4_CompleteSubscriptionMigration.cs
3. Read ALL the comments - they explain the SDK behavior
```

### Step 3: Customize for Your Environment (When Ready)
```
1. Identify your scenario:
   - What source sites do you have?
   - What should they be called in Cloud?
   - How do email domains change?

2. Modify the mappings in Example 4:
   - Update project mapping dictionary
   - Update email domain mapping
   - Add any specific user mappings

3. Add your credentials:
   - Source server URL and tokens
   - Destination Cloud URL and tokens
```

### Step 4: Test Safely
```
1. Create a TEST site in Tableau Cloud (not production!)
2. Migrate a SINGLE project first
3. Verify subscriptions work
4. Check user mappings are correct
5. Expand gradually
```

---

## 🔑 Key Insights

### What Makes Subscriptions Different:
Subscriptions have **dependencies**:
- **User**: Who receives the subscription?
- **Content**: Which workbook/view?
- **Schedule**: When does it run?

During migration:
- Users might have new emails (john@old.com → john@new.com)
- Content might be in different projects ("Sales" → "Cloud - Sales")
- Schedules might need adjustment (Cloud has different limits)

**The SDK handles all of this automatically**, you just provide the mapping rules!

### The "Magic" of the SDK:
```
You say: "john@old.com" is now "john@new.com"
SDK knows: Any subscriptions for john@old.com should go to john@new.com

You say: "Sales" project is now "Cloud - Sales"
SDK knows: Any workbooks in "Sales" are now in "Cloud - Sales"
SDK knows: Any subscriptions for those workbooks should use the new location

Result: Subscriptions "just work" in the new environment!
```

---

## 💡 Understanding the Process

### Traditional API Approach (Complex):
```
1. Get subscriptions from source REST API
2. For each subscription:
   a. Find the user in source
   b. Look up user in destination (handle email changes manually)
   c. Find the workbook in source
   d. Look up workbook in destination (handle project moves manually)
   e. Create subscription via destination REST API
   f. Handle errors (user doesn't exist? workbook not found?)
3. Repeat for every subscription
```

### SDK Approach (Simple):
```
1. Define mappings:
   - Project: "Sales" → "Cloud - Sales"
   - User: "@old.com" → "@new.com"

2. Run migration:
   migration.ExecuteAsync(plan)

3. SDK automatically:
   - Migrates users (with mapping)
   - Migrates projects (with mapping)
   - Migrates workbooks (to new projects)
   - Migrates subscriptions (with updated references)
   - Handles errors gracefully
```

**You write ~20 lines of mapping code instead of 200 lines of API calls!**

---

## ❓ Common Questions

**Q: Can I test this without credentials?**
A: The code structure is there to study, but you need real Tableau credentials to execute. You can use a trial Cloud account for testing.

**Q: What if I don't want to migrate everything?**
A: Use filters! Example 4 shows how to filter out test subscriptions.

**Q: Can I see what would be migrated before actually doing it?**
A: Yes - build the plan, inspect it, and run on a test site first. The manifest shows exactly what was migrated.

**Q: What if my scenario is more complex?**
A: The SDK supports transformers (modify content during migration) and hooks (run custom code). Start with these examples, then explore advanced features.

**Q: Do I need to know C# well?**
A: Not really! The examples are heavily commented and self-explanatory. If you understand APIs and dictionaries/mappings, you'll get it.

---

## 🎓 Learning Path

1. **Conceptual Understanding** (Today)
   - Read SUBSCRIPTIONS_AND_MAPPING.md
   - Understand the "pipeline" concept
   - See how mapping works

2. **Code Reading** (Today/Tomorrow)
   - Study the 4 examples in order
   - Focus on the comments
   - Understand the mapping classes

3. **Customization** (When Ready)
   - Map your actual environment
   - Define your project renames
   - Set up your email mappings

4. **Testing** (Before Production)
   - Test site in Cloud
   - Single project first
   - Verify subscriptions work

5. **Production Migration** (When Confident)
   - Full migration plan
   - Monitoring and logging
   - Validation and verification

---

## 📚 Additional Resources

- SDK Official Docs: https://help.tableau.com/current/api/migration_sdk/en-us/
- Code Samples: https://tableau.github.io/migration-sdk/samples/
- GitHub Repo: https://github.com/tableau/tableau-migration-sdk

---

## ✨ Summary

You now have:
- ✅ Complete conceptual understanding of subscriptions + mapping
- ✅ 4 progressively complex code examples
- ✅ Production-ready migration structure
- ✅ Testing strategies
- ✅ Clear next steps

**Start with reading SUBSCRIPTIONS_AND_MAPPING.md, then work through the code examples in the Examples/ folder.**

The SDK makes Cloud migration with subscriptions straightforward - you just define the rules, it handles the complexity!
