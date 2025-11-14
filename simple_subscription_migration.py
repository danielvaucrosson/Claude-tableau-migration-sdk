"""
Simple Subscription Migration - Single File
ONLY migrates subscriptions - skips users and workbooks (assumes they exist in Cloud).
"""

import logging
from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    TableauCloudUsernameMappingBase,
    ContentFilterBase,
    IUser,
    IWorkbook,
    IDataSource,
    IProject
)

# Configure logging - show subscription progress but suppress verbose HTTP/retry logs
logging.basicConfig(
    level=logging.INFO,  # Allow INFO messages through
    format='%(message)s'
)

# Silence the noisy loggers (HTTP requests, retries, etc.) but keep migration engine visible
logging.getLogger('System.Net.Http.HttpClient.DefaultHttpClient.LogicalHandler').setLevel(logging.CRITICAL)
logging.getLogger('System.Net.Http.HttpClient.DefaultHttpClient.ClientHandler').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Net.Logging.HttpActivityLogger').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Engine.Conversion.Schedules.ServerToCloudScheduleConverter').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Engine.Hooks.Transformers').setLevel(logging.CRITICAL)
logging.getLogger('Polly').setLevel(logging.CRITICAL)
logging.getLogger('System.Net.Http.HttpClient').setLevel(logging.CRITICAL)
# Keep Tableau.Migration.Engine at INFO to see subscription creation messages
logging.getLogger('Tableau.Migration.Engine').setLevel(logging.INFO)


# =============================================================================
# CONFIGURATION - EDIT THESE
# =============================================================================

# SOURCE: Tableau Server
SOURCE_SERVER_URL = "https://your-tableau-server.com"
SOURCE_SITE = ""  # Empty string for default site, or "site-name"
SOURCE_TOKEN_NAME = "your-token-name"
SOURCE_TOKEN = "your-token-secret"

# DESTINATION: Tableau Cloud
DEST_CLOUD_URL = "https://10ax.online.tableau.com"  # Change pod: 10ax, 10ay, etc.
DEST_SITE = "your-cloud-site"
DEST_TOKEN_NAME = "your-cloud-token-name"
DEST_TOKEN = "your-cloud-token-secret"


# =============================================================================
# USERNAME MAPPING - Just append @keyrus.com to find matching Cloud users
# =============================================================================

class SimpleUsernameMapping(TableauCloudUsernameMappingBase):
    """Append @keyrus.com to Server usernames to match Cloud users."""

    def map(self, ctx):
        username = ctx.content_item.name
        _tableau_user_domain = ctx.mapped_location.parent()

        # Already an email? Return as-is
        if "@" in username:
            return ctx.map_to(_tableau_user_domain.append(username))

        # Append @keyrus.com
        email = f"{username}@keyrus.com"
        print(f"👤 Mapping: {username} → {email}")

        # Return the mapped context with proper location object
        return ctx.map_to(_tableau_user_domain.append(email))


# =============================================================================
# FILTERS - Control what content actually gets migrated
# =============================================================================

class SkipUserMigration(ContentFilterBase[IUser]):
    """Don't migrate users - they should already exist in Cloud."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping user: {item.source_item.name}")
        return False  # Don't migrate users


class SkipProjectMigration(ContentFilterBase[IProject]):
    """Don't migrate projects - they should already exist in Cloud."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping project: {item.source_item.name}")
        return False  # Don't migrate projects


class SkipDataSourceMigration(ContentFilterBase[IDataSource]):
    """Don't migrate data sources - they should already exist in Cloud."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping data source: {item.source_item.name}")
        return False  # Don't migrate data sources


class SkipWorkbookMigration(ContentFilterBase[IWorkbook]):
    """Don't migrate workbooks - they should already exist in Cloud."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping workbook: {item.source_item.name}")
        return False  # Don't migrate workbooks


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_subscriptions():
    """Migrate subscriptions from Server to Cloud."""

    print("Starting subscription migration...")
    print(f"Source: {SOURCE_SERVER_URL} / {SOURCE_SITE if SOURCE_SITE else 'Default'}")
    print(f"Destination: {DEST_CLOUD_URL} / {DEST_SITE}\n")

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    plan_builder = (
        plan_builder
        .from_source_tableau_server(
            server_url=SOURCE_SERVER_URL,
            site_content_url=SOURCE_SITE,
            access_token_name=SOURCE_TOKEN_NAME,
            access_token=SOURCE_TOKEN
        )
        .to_destination_tableau_cloud(
            pod_url=DEST_CLOUD_URL,
            site_content_url=DEST_SITE,
            access_token_name=DEST_TOKEN_NAME,
            access_token=DEST_TOKEN
        )
        .for_server_to_cloud()
        .with_tableau_id_authentication_type()
        .with_tableau_cloud_usernames(lambda ctx: SimpleUsernameMapping().map(ctx))
    )

    # Add filters to skip migrating users, projects, data sources, and workbooks
    # Only subscriptions will be migrated
    print("Configuring filters to skip user/workbook migration...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipProjectMigration)
    plan_builder.filters.add(SkipDataSourceMigration)
    plan_builder.filters.add(SkipWorkbookMigration)

    # Build and execute
    print("Building migration plan...")
    plan = plan_builder.build()

    print("Starting migration (this may take a while)...\n")
    result = migration.execute(plan)

    # Results
    print("\n" + "="*50)
    if result.status.name == "Completed":
        print("✅ Migration completed!")
        print(f"   Check your Cloud site for migrated content")
    else:
        print(f"❌ Migration failed: {result.status}")
        if hasattr(result, 'errors') and result.errors:
            for error in result.errors:
                print(f"   {error}")
    print("="*50)


if __name__ == "__main__":
    migrate_subscriptions()
