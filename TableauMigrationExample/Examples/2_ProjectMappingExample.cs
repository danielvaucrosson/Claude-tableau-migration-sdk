using Tableau.Migration;
using Tableau.Migration.Content;
using Tableau.Migration.Engine.Hooks.Mappings;

namespace TableauMigrationExample.Examples;

/// <summary>
/// Example 2: Project Mapping
/// Shows how to RENAME projects during migration.
/// This is essential when your Cloud site has different naming conventions.
/// </summary>

// FIRST: Define a custom mapping class
public class CustomProjectMapping : ContentMappingBase<IProject>
{
    // This dictionary defines the mapping rules
    // Key = Source project name
    // Value = Destination project name
    private static readonly Dictionary<string, string> ProjectNameMappings = new()
    {
        // Format: { "Old Name", "New Name" }
        { "Campaign Analytics", "Marketing - Campaigns" },
        { "Sales Metrics", "Sales - Metrics" },
        { "Finance Reports", "Finance - Monthly Reports" }
    };

    // This method is called for EACH project during migration
    public override async Task<ContentMappingContext<IProject>?> MapAsync(
        ContentMappingContext<IProject> ctx,
        CancellationToken cancel)
    {
        // Get the current project from the context
        var project = ctx.ContentItem;

        // Get the project's current name (last part of the path)
        var currentName = project.Location.Name;

        Console.WriteLine($"🔍 Checking project: {currentName}");

        // Check if we have a mapping rule for this project
        if (ProjectNameMappings.TryGetValue(currentName, out var newName))
        {
            Console.WriteLine($"  ✏️  Renaming: {currentName} → {newName}");

            // Create new location with the new name
            var newLocation = project.Location.Rename(newName);

            // Return the mapped context with new location
            return ctx.MapTo(newLocation);
        }

        // No mapping needed, keep the same name
        Console.WriteLine($"  ✓ Keeping name: {currentName}");
        return ctx;
    }
}

// SECOND: Use the mapping in a migration
public class ProjectMappingExample
{
    public async Task RunMigrationWithProjectMappingAsync()
    {
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                services.AddTableauMigration();
            })
            .Build();

        var migration = host.Services.GetRequiredService<IMigration>();

        var plan = migration.CreatePlanBuilder()
            .FromSourceTableauServer(
                serverUrl: new Uri("https://tableau-server.company.com"),
                siteContentUrl: "marketing",
                accessTokenName: "server-token",
                accessToken: "server-secret"
            )
            .ToDestinationTableauCloud(
                podUrl: new Uri("https://10ax.online.tableau.com"),
                siteContentUrl: "corporate",
                accessTokenName: "cloud-token",
                accessToken: "cloud-secret"
            )
            // ⭐ HERE'S THE KEY: Register the project mapping
            .Mappings.Add<CustomProjectMapping, IProject>()
            .Build();

        Console.WriteLine("Starting migration with project mapping...");
        var result = await migration.ExecuteAsync(plan, CancellationToken.None);

        if (result.Status == MigrationCompletionStatus.Completed)
        {
            Console.WriteLine("✅ Migration completed with project renaming!");
        }
        else
        {
            Console.WriteLine($"❌ Migration failed: {result.Status}");
        }
    }
}

/*
 * WHAT HAPPENS NOW:
 *
 * SOURCE (Tableau Server - "marketing" site):
 * ├── Campaign Analytics
 * │   └── Q4 Dashboard.twbx
 * └── Sales Metrics
 *     └── Revenue Report.twbx
 *
 * DESTINATION (Tableau Cloud - "corporate" site):
 * ├── Marketing - Campaigns  ← Renamed!
 * │   └── Q4 Dashboard.twbx
 * └── Sales - Metrics  ← Renamed!
 *     └── Revenue Report.twbx
 *
 * IMPORTANT: Workbooks automatically follow to the new project names!
 * You don't need to map workbooks separately - SDK handles that.
 *
 * WHAT ABOUT SUBSCRIPTIONS?
 * - If "Q4 Dashboard.twbx" had a subscription on the source,
 * - The SDK will create a subscription for "Q4 Dashboard.twbx" in "Marketing - Campaigns"
 * - BUT we still need to map USERS (see next example)
 */
