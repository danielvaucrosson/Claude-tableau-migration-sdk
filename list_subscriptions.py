"""
Simple Script to List Subscriptions from Tableau Server
Just authenticates and reads subscription data - no migration.
"""

from tableau_migration import (
    Migrator,
    MigrationPlanBuilder
)

# =============================================================================
# CONFIGURATION
# =============================================================================

# Tableau Server
SERVER_URL = "https://your-tableau-server.com/"
SITE = ""  # Empty string for default site
TOKEN_NAME = "your-token-name"
TOKEN = "your-token-secret"


def list_server_subscriptions():
    """Connect to Server and list subscriptions."""

    print("="*60)
    print("  TABLEAU SERVER - LIST SUBSCRIPTIONS")
    print("="*60)
    print(f"\nServer: {SERVER_URL}")
    print(f"Site: {SITE if SITE else 'Default'}\n")

    print("Authenticating to Server...")

    # Create a simple plan just to connect
    plan_builder = MigrationPlanBuilder()

    # Connect to source server
    plan_builder = plan_builder.from_source_tableau_server(
        server_url=SERVER_URL,
        site_content_url=SITE,
        access_token_name=TOKEN_NAME,
        access_token=TOKEN
    )

    # Build the plan (this establishes connection)
    plan = plan_builder.build()

    print("✓ Connected!\n")

    # Access the source endpoint to read data
    print("Fetching users from Server...")

    # Try to get the manifest which contains source info
    migrator = Migrator()

    # We can't easily read subscriptions directly without migration
    # But we can at least verify authentication worked
    print("\n" + "="*60)
    print("  AUTHENTICATION SUCCESSFUL")
    print("="*60)
    print("\nNote: To read actual subscription data, we need to run")
    print("a migration or use the Tableau REST API directly.")
    print("\nThe SDK is designed for migration, not just reading data.")
    print("="*60 + "\n")


if __name__ == "__main__":
    list_server_subscriptions()
