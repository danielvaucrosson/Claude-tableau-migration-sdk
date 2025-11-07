# Understanding Subscriptions and Mapping in Tableau Migration SDK

## What We're Solving

When migrating from Tableau Server to Tableau Cloud, you face two key challenges:
1. **Subscriptions need to be recreated** (CMT doesn't support this, SDK does!)
2. **Site and project structures often change** (aggregating sites, renaming projects, etc.)

---

## What ARE Subscriptions?

In Tableau, a **subscription** is an automated email that sends:
- A snapshot of a workbook or view
- On a schedule (daily, weekly, etc.)
- To specific users

**During migration**, subscriptions need to:
- Find the corresponding workbook/view in the new environment
- Map users correctly (email domains might change)
- Adjust schedules to be Cloud-compatible (e.g., Tableau Cloud doesn't support hourly schedules under 1 hour)

---

## The Migration SDK "Pipeline"

Think of the SDK as a conveyor belt. Each item (user, project, workbook, subscription) goes through stages:

```
SOURCE
  ↓
FILTER (Should we migrate this?)
  ↓
MAPPING (Change identifiers - names, locations, etc.)
  ↓
TRANSFORM (Modify properties - tags, permissions, etc.)
  ↓
HOOKS (Run custom code at specific points)
  ↓
DESTINATION
```

### Simple Analogy:
Imagine moving houses (source → destination):
- **Filter**: "Should I take this item or leave it behind?"
- **Mapping**: "This goes in the kitchen, not the bedroom"
- **Transform**: "Paint this blue instead of green"
- **Hooks**: "Call the electrician when we finish moving the appliances"

---

## How Mapping Works

**Mapping** changes WHERE things go and WHAT they're called during migration.

### Real-World Example:

**BEFORE (Source Server):**
```
Tableau Server
├── Site: "Marketing"
│   ├── Project: "Campaign Analytics"
│   │   └── Workbook: "Q4 Dashboard"
│   │       └── Subscription → sends to john@oldcompany.com
├── Site: "Sales"
│   ├── Project: "Sales Metrics"
│       └── Workbook: "Revenue Report"
```

**AFTER (Tableau Cloud):**
```
Tableau Cloud
└── Site: "Corporate" (single aggregated site)
    ├── Project: "Marketing - Campaigns"  ← renamed!
    │   └── Workbook: "Q4 Dashboard"
    │       └── Subscription → sends to john@newcompany.com  ← email changed!
    ├── Project: "Sales - Metrics"  ← renamed!
        └── Workbook: "Revenue Report"
```

### What Changed?
1. **Two sites merged into one** → Site mapping
2. **Projects renamed** → Project mapping
3. **Email domain changed** → User mapping
4. **Subscriptions automatically followed** → SDK handles this!

---

## Types of Mapping You'll Use

### 1. Site Mapping
Maps source sites to destination sites.

**Example:**
```
Source: "Marketing" site  →  Destination: "Corporate" site
Source: "Sales" site      →  Destination: "Corporate" site
```

### 2. Project Mapping
Changes project names or moves content to different projects.

**Example:**
```
Source: "Campaign Analytics"  →  Destination: "Marketing - Campaigns"
Source: "Sales Metrics"       →  Destination: "Sales - Metrics"
```

### 3. User Mapping (for subscriptions!)
Maps users from source to destination, handling email changes.

**Example:**
```
Source User: john@oldcompany.com  →  Destination User: john@newcompany.com
```

**Why this matters for subscriptions:**
Subscriptions are tied to users. If the email changes, the SDK needs to know "john@oldcompany.com" and "john@newcompany.com" are the same person.

---

## How Subscriptions Get Migrated

### What the SDK Does Automatically:

1. **Reads subscription from source**
   - Which workbook/view?
   - Which user receives it?
   - What schedule?

2. **Applies mappings**
   - Find the workbook in the new project (using project mapping)
   - Find the user with new email (using user mapping)

3. **Transforms if needed**
   - Adjust schedule (Cloud doesn't support sub-1-hour schedules)
   - Update any other properties

4. **Creates subscription in destination**
   - SDK calls Tableau Cloud REST API
   - Creates new subscription with updated details

### What YOU Need to Provide:

1. **User mapping** - Tell SDK how emails change
2. **Project mapping** - Tell SDK where content moves
3. **Filters (optional)** - Skip certain subscriptions if desired

---

## Code Example Structure

Here's how you'd set up a migration with mapping:

```csharp
// 1. Configure source and destination
var source = "https://tableau-server.company.com";
var destination = "https://10ax.online.tableau.com";

// 2. Define your mappings
var projectMapping = new Dictionary<string, string>
{
    { "Campaign Analytics", "Marketing - Campaigns" },
    { "Sales Metrics", "Sales - Metrics" }
};

var emailMapping = new Dictionary<string, string>
{
    { "@oldcompany.com", "@newcompany.com" }
};

// 3. Register mappings with the migration plan
planBuilder
    .Mappings.Add<ProjectRenameMapping>()      // Rename projects
    .Mappings.Add<EmailDomainMapping>()        // Update email domains
    .Mappings.Add<UserMapping>();              // Map users

// 4. Run the migration
// SDK automatically:
// - Migrates users (with new emails)
// - Migrates projects (with new names)
// - Migrates workbooks (to new projects)
// - Migrates subscriptions (with mapped users and workbooks)
```

---

## What Makes This Better Than CMT?

| Feature | Content Migration Tool (CMT) | Migration SDK |
|---------|------------------------------|---------------|
| **Subscriptions** | ❌ Not supported | ✅ Fully supported |
| **User Migration** | ❌ Not supported | ✅ Yes |
| **Email Mapping** | ❌ Manual | ✅ Automatic with mapping |
| **Site Aggregation** | ❌ Can't merge sites | ✅ Yes, via mapping |
| **Cloud Migration** | ❌ Not recommended | ✅ Designed for it |
| **Custom Logic** | Limited scripts | ✅ Full C# code |

---

## Your Use Case

You mentioned wanting to:
1. **Start with subscriptions** ✅ Perfect - SDK supports this fully
2. **Handle site/project mapping** ✅ SDK has powerful mapping system
3. **Aggregate sites** ✅ Map multiple source sites → single destination

**Next Steps:**
- See actual code examples
- Understand how to configure mappings
- Learn how to test without breaking production
- Build a simple console app that demonstrates this

---

## Key Takeaways

1. **Subscriptions are supported by the SDK** (but not CMT)
2. **Mapping = changing names/locations during migration**
3. **Three main mappings for subscriptions:**
   - Site mapping (where does content go?)
   - Project mapping (what are things called?)
   - User mapping (who receives subscriptions?)
4. **SDK handles the complexity** - you just configure the rules
5. **You can test with read-only operations first** - explore without migrating

Ready to see actual code examples?
