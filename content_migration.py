"""
Content Migration - Data Sources and Workbooks
Migrates workbooks and data sources (including custom views).
Skips users, projects, and subscriptions (handled by separate script).
"""

import logging
import json
from pathlib import Path
from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    TableauCloudUsernameMappingBase,
    ContentFilterBase,
    IUser,
    IProject
)

# Configure logging - show content migration progress but suppress verbose HTTP/retry logs
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
# Keep Tableau.Migration.Engine at INFO to see content creation messages
logging.getLogger('Tableau.Migration.Engine').setLevel(logging.INFO)


# =============================================================================
# CONFIGURATION - Load from config.json
# =============================================================================

def load_config(config_path='config.json'):
    """Load credentials from config.json file."""
    config_file = Path(config_path)

    if not config_file.exists():
        print(f"❌ Config file not found: {config_path}")
        print(f"\n📋 Setup instructions:")
        print(f"   1. Copy the template: cp config.json.template config.json")
        print(f"   2. Edit config.json with your actual credentials")
        print(f"   3. Run this script again\n")
        return None

    with open(config_file, 'r') as f:
        return json.load(f)

def validate_config(config):
    """Validate that config has all required fields."""
    if not config:
        return False

    required_fields = {
        'source': ['server_url', 'site_content_url', 'access_token_name', 'access_token'],
        'destination': ['pod_url', 'site_content_url', 'access_token_name', 'access_token']
    }

    missing = []
    for section, fields in required_fields.items():
        if section not in config:
            missing.append(f"Missing '{section}' section")
            continue
        for field in fields:
            if field not in config[section] or not config[section][field]:
                missing.append(f"{section}.{field}")

    if missing:
        print("❌ Missing or empty fields in config.json:")
        for m in missing:
            print(f"   - {m}")
        return False

    return True


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


# Note: Subscriptions are handled by the separate simple_subscription_migration.py script
# The migration plan will skip them by default when we only migrate content


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_content():
    """Migrate data sources and workbooks (with custom views) from Server to Cloud."""

    # Load and validate configuration
    config = load_config()
    if not config or not validate_config(config):
        return

    print("✅ Configuration loaded successfully\n")
    print("Starting content migration (Data Sources & Workbooks)...")
    print(f"Source: {config['source']['server_url']} / {config['source']['site_content_url'] if config['source']['site_content_url'] else 'Default'}")
    print(f"Destination: {config['destination']['pod_url']} / {config['destination']['site_content_url']}\n")

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    plan_builder = (
        plan_builder
        .from_source_tableau_server(
            server_url=config['source']['server_url'],
            site_content_url=config['source']['site_content_url'],
            access_token_name=config['source']['access_token_name'],
            access_token=config['source']['access_token']
        )
        .to_destination_tableau_cloud(
            pod_url=config['destination']['pod_url'],
            site_content_url=config['destination']['site_content_url'],
            access_token_name=config['destination']['access_token_name'],
            access_token=config['destination']['access_token']
        )
        .for_server_to_cloud()
        .with_tableau_id_authentication_type()
        .with_tableau_cloud_usernames(lambda ctx: SimpleUsernameMapping().map(ctx))
    )

    # Add filters to skip migrating users and projects
    # Data sources, workbooks, and custom views WILL be migrated
    # Subscriptions are handled by the separate simple_subscription_migration.py script
    print("Configuring filters to skip user/project migration...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipProjectMigration)

    # Build and execute
    print("Building migration plan...")
    plan = plan_builder.build()

    print("Starting migration (this may take a while)...\n")
    print("📊 Migrating data sources...")
    print("📈 Migrating workbooks...")
    print("👁️  Migrating custom views...\n")
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
    migrate_content()
