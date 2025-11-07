using Tableau.Migration;
using Tableau.Migration.Content;
using Tableau.Migration.Engine.Hooks.Mappings;
using Tableau.Migration.Engine.Hooks.Filters;

namespace TableauMigrationExample.Examples;

/// <summary>
/// Example 4: Complete Subscription Migration
/// Combines everything: Project mapping + User mapping + Subscription migration
/// This is a real-world scenario for migrating to Cloud.
/// </summary>

// PROJECT MAPPING: Rename projects
public class CloudProjectMapping : ContentMappingBase<IProject>
{
    private static readonly Dictionary<string, string> Mappings = new()
    {
        { "Marketing Campaigns", "Cloud - Marketing" },
        { "Sales Analytics", "Cloud - Sales" },
        { "Finance Reports", "Cloud - Finance" }
    };

    public override async Task<ContentMappingContext<IProject>?> MapAsync(
        ContentMappingContext<IProject> ctx,
        CancellationToken cancel)
    {
        var projectName = ctx.ContentItem.Location.Name;

        if (Mappings.TryGetValue(projectName, out var newName))
        {
            Console.WriteLine($"📁 Project: {projectName} → {newName}");
            return ctx.MapTo(ctx.ContentItem.Location.Rename(newName));
        }

        return ctx;
    }
}

// USER MAPPING: Update email domain
public class CloudUserMapping : ContentMappingBase<IUser>
{
    public override async Task<ContentMappingContext<IUser>?> MapAsync(
        ContentMappingContext<IUser> ctx,
        CancellationToken cancel)
    {
        var user = ctx.ContentItem;
        var email = user.Email;

        // Change domain from on-prem to cloud
        if (email.EndsWith("@onprem-company.com"))
        {
            var newEmail = email.Replace("@onprem-company.com", "@company.cloud");
            Console.WriteLine($"👤 User: {email} → {newEmail}");
            return ctx.MapTo(user.Location.WithUsername(newEmail));
        }

        return ctx;
    }
}

// OPTIONAL: Filter out test subscriptions
public class ExcludeTestSubscriptionsFilter : ContentFilterBase<ISubscription>
{
    public override async Task<bool> ShouldMigrateAsync(
        ContentMigrationItem<ISubscription> item,
        CancellationToken cancel)
    {
        var subscription = item.SourceItem;

        // Don't migrate subscriptions with "test" in the subject
        if (subscription.Subject?.Contains("TEST", StringComparison.OrdinalIgnoreCase) == true)
        {
            Console.WriteLine($"⏭️  Skipping test subscription: {subscription.Subject}");
            return false; // Don't migrate
        }

        return true; // Migrate this subscription
    }
}

// COMPLETE MIGRATION EXAMPLE
public class CompleteSubscriptionMigration
{
    public async Task RunCompleteSubscriptionMigrationAsync()
    {
        Console.WriteLine("=================================================");
        Console.WriteLine("  TABLEAU SUBSCRIPTION MIGRATION TO CLOUD");
        Console.WriteLine("=================================================\n");

        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                services.AddTableauMigration();
            })
            .Build();

        var migration = host.Services.GetRequiredService<IMigration>();

        Console.WriteLine("Building migration plan...");

        var plan = migration.CreatePlanBuilder()
            // SOURCE: On-premises Tableau Server
            .FromSourceTableauServer(
                serverUrl: new Uri("https://tableau-server.company.com"),
                siteContentUrl: "marketing",
                accessTokenName: "onprem-token",
                accessToken: "onprem-secret"
            )
            // DESTINATION: Tableau Cloud
            .ToDestinationTableauCloud(
                podUrl: new Uri("https://10ax.online.tableau.com"),
                siteContentUrl: "production",
                accessTokenName: "cloud-token",
                accessToken: "cloud-secret"
            )
            // MAPPINGS: Transform content during migration
            .Mappings.Add<CloudProjectMapping, IProject>()
            .Mappings.Add<CloudUserMapping, IUser>()

            // FILTERS: Exclude test content (optional)
            .Filters.Add<ExcludeTestSubscriptionsFilter, ISubscription>()

            .Build();

        Console.WriteLine("✓ Plan built successfully\n");

        // EXECUTE MIGRATION
        Console.WriteLine("Starting migration...\n");
        var result = await migration.ExecuteAsync(plan, CancellationToken.None);

        // REPORT RESULTS
        Console.WriteLine("\n=================================================");
        Console.WriteLine("  MIGRATION RESULTS");
        Console.WriteLine("=================================================\n");

        if (result.Status == MigrationCompletionStatus.Completed)
        {
            Console.WriteLine("✅ Migration completed successfully!\n");

            // You can inspect the manifest to see what was migrated
            var manifest = result.Manifest;

            Console.WriteLine($"📊 Migration Summary:");
            Console.WriteLine($"   Users:         {manifest.Entries.ForContentType<IUser>().Count()}");
            Console.WriteLine($"   Groups:        {manifest.Entries.ForContentType<IGroup>().Count()}");
            Console.WriteLine($"   Projects:      {manifest.Entries.ForContentType<IProject>().Count()}");
            Console.WriteLine($"   Data Sources:  {manifest.Entries.ForContentType<IDataSource>().Count()}");
            Console.WriteLine($"   Workbooks:     {manifest.Entries.ForContentType<IWorkbook>().Count()}");
            Console.WriteLine($"   Subscriptions: {manifest.Entries.ForContentType<ISubscription>().Count()}");
            Console.WriteLine();
        }
        else
        {
            Console.WriteLine($"❌ Migration failed: {result.Status}\n");

            // Show errors if any
            foreach (var error in result.Errors)
            {
                Console.WriteLine($"   Error: {error.Message}");
            }
        }

        Console.WriteLine("=================================================\n");
    }
}

/*
 * REAL-WORLD SCENARIO:
 *
 * SOURCE (Tableau Server - On-Premises):
 * ├── Site: "marketing"
 * ├── Users:
 * │   ├── alice@onprem-company.com
 * │   └── bob@onprem-company.com
 * ├── Projects:
 * │   ├── Marketing Campaigns
 * │   └── Sales Analytics
 * ├── Workbooks:
 * │   ├── "Q4 Campaign Report" (in Marketing Campaigns)
 * │   └── "Revenue Dashboard" (in Sales Analytics)
 * └── Subscriptions:
 *     ├── alice@onprem-company.com → "Q4 Campaign Report" (Every Monday)
 *     ├── bob@onprem-company.com → "Revenue Dashboard" (Every Friday)
 *     └── test@onprem-company.com → "TEST Dashboard" (Filtered out!)
 *
 * AFTER MIGRATION (Tableau Cloud):
 * ├── Site: "production"
 * ├── Users:
 * │   ├── alice@company.cloud  ← Email changed!
 * │   └── bob@company.cloud    ← Email changed!
 * ├── Projects:
 * │   ├── Cloud - Marketing  ← Renamed!
 * │   └── Cloud - Sales      ← Renamed!
 * ├── Workbooks:
 * │   ├── "Q4 Campaign Report" (in Cloud - Marketing)
 * │   └── "Revenue Dashboard" (in Cloud - Sales)
 * └── Subscriptions:
 *     ├── alice@company.cloud → "Q4 Campaign Report" (Every Monday)
 *     └── bob@company.cloud → "Revenue Dashboard" (Every Friday)
 *     (Test subscription was filtered out)
 *
 * THE SDK AUTOMATICALLY:
 * 1. Migrates users with new emails
 * 2. Migrates projects with new names
 * 3. Migrates workbooks to new project locations
 * 4. Migrates subscriptions with updated user references
 * 5. Adjusts schedules for Cloud compatibility
 * 6. Handles errors (e.g., if a user doesn't exist)
 *
 * WHAT YOU CONTROL:
 * - Mapping rules (how names change)
 * - Filters (what to exclude)
 * - Transformations (optional modifications)
 * - Error handling and logging
 *
 * TESTING BEFORE MIGRATING:
 * You can test this by:
 * 1. Using a test site on Cloud (not production)
 * 2. Migrating a small subset (use filters)
 * 3. Running in "dry run" mode (read-only, see what would happen)
 * 4. Reviewing the manifest before committing
 */
