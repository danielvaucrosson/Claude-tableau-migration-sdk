"""
Simple Subscription Migration - Single File
Just migrates subscriptions from Server to Cloud with user mapping.
"""

import asyncio
from tableau_migration import PyUser
from tableau_migration.migration import PyMigrationPlanBuilder
from tableau_migration.migration_engine_hooks_mappings import PyContentMappingContext


# =============================================================================
# CONFIGURATION - EDIT THESE
# =============================================================================

# SOURCE: Tableau Server
SOURCE_SERVER_URL = "https://your-tableau-server.com"
SOURCE_SITE = "your-site-name"  # or "" for default site
SOURCE_TOKEN_NAME = "your-token-name"
SOURCE_TOKEN = "your-token-secret"

# DESTINATION: Tableau Cloud
DEST_CLOUD_URL = "https://10ax.online.tableau.com"  # Change pod: 10ax, 10ay, etc.
DEST_SITE = "your-cloud-site"
DEST_TOKEN_NAME = "your-cloud-token-name"
DEST_TOKEN = "your-cloud-token-secret"

# USER MAPPINGS - Server username → Cloud email
USER_MAPPINGS = {
    "jsmith": "john.smith@company.com",
    "ajones": "alice.jones@company.com",
    # Add more users here...
}

# Default domain for unmapped users
DEFAULT_DOMAIN = "@company.com"


# =============================================================================
# USER MAPPING CLASS
# =============================================================================

class SimpleUserMapping:
    """Maps Server usernames to Cloud emails."""

    def map(self, ctx: PyContentMappingContext[PyUser]) -> PyContentMappingContext[PyUser]:
        username = ctx.content_item.name

        # Already an email? Keep it
        if "@" in username:
            return ctx

        # Check mapping dict
        if username in USER_MAPPINGS:
            email = USER_MAPPINGS[username]
            print(f"👤 Mapping: {username} → {email}")
            return ctx.map_to(ctx.content_item.location.with_username(email))

        # Default: append domain
        email = f"{username}{DEFAULT_DOMAIN}"
        print(f"👤 Default: {username} → {email}")
        return ctx.map_to(ctx.content_item.location.with_username(email))


# =============================================================================
# MIGRATION
# =============================================================================

async def migrate_subscriptions():
    """Migrate subscriptions from Server to Cloud."""

    print("Starting subscription migration...")
    print(f"Source: {SOURCE_SERVER_URL} / {SOURCE_SITE}")
    print(f"Destination: {DEST_CLOUD_URL} / {DEST_SITE}\n")

    # Build plan
    plan_builder = PyMigrationPlanBuilder()

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
    )

    # Add user mapping
    plan_builder.mappings.add(SimpleUserMapping())

    # Execute
    plan = plan_builder.build()
    result = await plan.execute_async()

    # Results
    print("\n" + "="*50)
    if result.status == "Completed":
        print("✅ Migration completed!")
        manifest = result.manifest
        print(f"   Subscriptions: {len(manifest.entries.get_by_type('Subscription'))}")
    else:
        print(f"❌ Migration failed: {result.status}")
        for error in result.errors:
            print(f"   {error}")
    print("="*50)


if __name__ == "__main__":
    asyncio.run(migrate_subscriptions())
