using Tableau.Migration;
using Tableau.Migration.Engine.Endpoints;

namespace TableauMigrationExample.Examples;

/// <summary>
/// Example 1: Basic Migration Setup
/// This shows the absolute basics of setting up a migration.
/// NO mapping, NO filtering - just the bare minimum to understand the structure.
/// </summary>
public class BasicMigrationSetup
{
    public async Task RunMigrationAsync()
    {
        // STEP 1: Create the migration host
        // Think of this as the "engine" that runs migrations
        var host = Host.CreateDefaultBuilder()
            .ConfigureServices((context, services) =>
            {
                // Register the Tableau Migration SDK services
                services.AddTableauMigration();
            })
            .Build();

        // STEP 2: Get the migration service from dependency injection
        var migration = host.Services.GetRequiredService<IMigration>();

        // STEP 3: Build a migration plan
        // This is where you configure WHAT to migrate and HOW
        var plan = migration.CreatePlanBuilder()
            // Configure SOURCE (where content comes from)
            .FromSourceTableauServer(
                serverUrl: new Uri("https://tableau-server.company.com"),
                siteContentUrl: "marketing",  // Site name
                accessTokenName: "my-token-name",
                accessToken: "my-secret-token"
            )
            // Configure DESTINATION (where content goes to)
            .ToDestinationTableauCloud(
                podUrl: new Uri("https://10ax.online.tableau.com"),
                siteContentUrl: "corporate",  // Different site name!
                accessTokenName: "cloud-token-name",
                accessToken: "cloud-secret-token"
            )
            // Build the plan
            .Build();

        // STEP 4: Execute the migration
        Console.WriteLine("Starting migration...");
        var result = await migration.ExecuteAsync(plan, CancellationToken.None);

        // STEP 5: Check results
        if (result.Status == MigrationCompletionStatus.Completed)
        {
            Console.WriteLine("✅ Migration completed successfully!");
        }
        else
        {
            Console.WriteLine($"❌ Migration failed: {result.Status}");
        }
    }
}

/*
 * WHAT HAPPENS WHEN THIS RUNS:
 *
 * 1. SDK connects to source (Tableau Server)
 * 2. SDK reads ALL content types:
 *    - Users
 *    - Groups
 *    - Projects
 *    - Data Sources
 *    - Workbooks
 *    - Extract Refresh Tasks
 *    - Subscriptions
 *
 * 3. SDK migrates everything to destination (Tableau Cloud)
 * 4. Because we didn't add any mappings, it assumes names stay the same
 *
 * PROBLEM: What if project names are different? What if emails changed?
 * That's where MAPPING comes in (see next example)
 */
