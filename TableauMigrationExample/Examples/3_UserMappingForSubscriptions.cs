using Tableau.Migration;
using Tableau.Migration.Content;
using Tableau.Migration.Engine.Hooks.Mappings;

namespace TableauMigrationExample.Examples;

/// <summary>
/// Example 3: User Mapping for Subscriptions
/// Shows how to map users when email domains change.
/// This is CRITICAL for subscriptions to work after migration.
/// </summary>

// MAPPING 1: Change email domain
public class EmailDomainMapping : ContentMappingBase<IUser>
{
    private const string OldDomain = "@oldcompany.com";
    private const string NewDomain = "@newcompany.com";

    public override async Task<ContentMappingContext<IUser>?> MapAsync(
        ContentMappingContext<IUser> ctx,
        CancellationToken cancel)
    {
        var user = ctx.ContentItem;

        // Get current email
        var currentEmail = user.Email;

        Console.WriteLine($"👤 Processing user: {user.Name}");
        Console.WriteLine($"   Current email: {currentEmail}");

        // If email uses old domain, change it
        if (currentEmail.EndsWith(OldDomain, StringComparison.OrdinalIgnoreCase))
        {
            var newEmail = currentEmail.Replace(OldDomain, NewDomain);
            Console.WriteLine($"   📧 Updating email: {currentEmail} → {newEmail}");

            // Map to new location with updated email
            return ctx.MapTo(user.Location.WithUsername(newEmail));
        }

        Console.WriteLine($"   ✓ Email unchanged");
        return ctx;
    }
}

// MAPPING 2: Handle specific user renames
public class SpecificUserMapping : ContentMappingBase<IUser>
{
    // Some users might have completely different usernames
    private static readonly Dictionary<string, string> UsernameMappings = new()
    {
        { "john.smith@oldcompany.com", "j.smith@newcompany.com" },
        { "sarah.jones@oldcompany.com", "jones.s@newcompany.com" },
        // Add more specific mappings as needed
    };

    public override async Task<ContentMappingContext<IUser>?> MapAsync(
        ContentMappingContext<IUser> ctx,
        CancellationToken cancel)
    {
        var user = ctx.ContentItem;
        var currentEmail = user.Email;

        if (UsernameMappings.TryGetValue(currentEmail, out var newEmail))
        {
            Console.WriteLine($"👤 Specific mapping: {currentEmail} → {newEmail}");
            return ctx.MapTo(user.Location.WithUsername(newEmail));
        }

        return ctx; // No specific mapping, use original
    }
}

// PUTTING IT ALL TOGETHER
public class UserMappingExample
{
    public async Task RunMigrationWithUserMappingAsync()
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
            // ⭐ Register BOTH user mappings
            // Order matters: specific mappings first, then domain mapping
            .Mappings.Add<SpecificUserMapping, IUser>()
            .Mappings.Add<EmailDomainMapping, IUser>()
            .Build();

        Console.WriteLine("Starting migration with user mapping...");
        var result = await migration.ExecuteAsync(plan, CancellationToken.None);

        if (result.Status == MigrationCompletionStatus.Completed)
        {
            Console.WriteLine("✅ Migration completed with user mapping!");
        }
        else
        {
            Console.WriteLine($"❌ Migration failed: {result.Status}");
        }
    }
}

/*
 * WHAT HAPPENS WITH SUBSCRIPTIONS:
 *
 * SOURCE (Tableau Server):
 * ├── User: john.smith@oldcompany.com
 * ├── Workbook: "Q4 Dashboard"
 * └── Subscription:
 *     ├── To: john.smith@oldcompany.com
 *     ├── Content: "Q4 Dashboard"
 *     └── Schedule: Every Monday at 9 AM
 *
 * MIGRATION PROCESS:
 * 1. SDK migrates user → j.smith@newcompany.com (specific mapping)
 * 2. SDK migrates workbook → "Q4 Dashboard"
 * 3. SDK migrates subscription:
 *    ├── Finds user mapping: john.smith@oldcompany.com → j.smith@newcompany.com
 *    ├── Finds workbook: "Q4 Dashboard" (already migrated)
 *    └── Creates subscription with NEW user and NEW workbook reference
 *
 * DESTINATION (Tableau Cloud):
 * ├── User: j.smith@newcompany.com
 * ├── Workbook: "Q4 Dashboard"
 * └── Subscription:
 *     ├── To: j.smith@newcompany.com  ← Updated!
 *     ├── Content: "Q4 Dashboard"
 *     └── Schedule: Every Monday at 9 AM
 *
 * THE SDK AUTOMATICALLY:
 * - Tracks all user mappings
 * - Updates subscription references
 * - Adjusts schedules for Cloud compatibility
 * - Handles errors if users don't exist
 *
 * YOU JUST PROVIDE THE MAPPING RULES!
 */
