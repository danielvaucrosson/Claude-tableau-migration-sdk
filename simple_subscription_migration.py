"""
Simple Subscription Migration - Single File
Just migrates subscriptions from Server to Cloud with user mapping.
"""

from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    IUser
)
from tableau_migration.migration_engine_hooks_mappings import ContentMappingContext


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

# USER MAPPINGS - Server username → Cloud email
USER_MAPPINGS = {
    "jsmith": "john.smith@company.com",
    "ajones": "alice.jones@company.com",
    # Add more users here...
}

# Default email domain
DEFAULT_DOMAIN = "@company.com"


# =============================================================================
# USERNAME MAPPING CLASS
# =============================================================================

class SimpleUsernameMapping:
    """Maps Server usernames to Cloud emails."""

    def map(self, ctx: ContentMappingContext[IUser]) -> ContentMappingContext[IUser]:
        """
        Called for each user during migration.

        Args:
            ctx: The mapping context containing the user

        Returns:
            Mapped context with new username (email)
        """
        user = ctx.content_item
        username = user.name

        # Already an email? Return as-is
        if "@" in username:
            return ctx

        # Check mapping dict (access global)
        if username in globals()['USER_MAPPINGS']:
            email = globals()['USER_MAPPINGS'][username]
            print(f"👤 Mapping: {username} → {email}")
            # Use location.with_username() to create new location
            return ctx.map_to(user.location.with_username(email))

        # Default: append domain (access global)
        email = f"{username}{globals()['DEFAULT_DOMAIN']}"
        print(f"👤 Default: {username} → {email}")
        # Use location.with_username() to create new location
        return ctx.map_to(user.location.with_username(email))


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
    )

    # Register username mapping
    plan_builder.mappings.add(SimpleUsernameMapping)

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
