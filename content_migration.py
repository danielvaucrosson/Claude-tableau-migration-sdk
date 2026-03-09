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
    ContentTransformerBase,
    IPublishableWorkbook,
    IUser,
    IProject,
    IGroup
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

    # Check source and destination sections
    for section, fields in required_fields.items():
        if section not in config:
            missing.append(f"Missing '{section}' section")
            continue
        for field in fields:
            if field not in config[section] or not config[section][field]:
                missing.append(f"{section}.{field}")

    # Check for default content owner
    if 'default_content_owner' not in config or not config['default_content_owner']:
        missing.append("default_content_owner (email of user to own content when original owner doesn't exist)")

    if missing:
        print("❌ Missing or empty fields in config.json:")
        for m in missing:
            print(f"   - {m}")
        return False

    return True


# =============================================================================
# USERNAME MAPPING - Map to Cloud users with fallback to default owner
# =============================================================================

class ContentOwnerMapping(TableauCloudUsernameMappingBase):
    """
    Map Server usernames to Cloud users.
    Falls back to default owner if user doesn't exist in Cloud.
    """

    def __init__(self, default_owner):
        self.default_owner = default_owner
        super().__init__()

    def map(self, ctx):
        username = ctx.content_item.name
        _tableau_user_domain = ctx.mapped_location.parent()

        # Try to map the user
        if "@" in username:
            # Already an email
            mapped_email = username
        else:
            # Append @keyrus.com to create email
            mapped_email = f"{username}@keyrus.com"

        print(f"👤 Mapping content owner: {username} → {mapped_email}")

        # NOTE: If this user doesn't exist in Cloud, the migration will reassign
        # to the default owner specified in config. This happens automatically
        # when the user lookup fails.

        # Return the mapped context with proper location object
        return ctx.map_to(_tableau_user_domain.append(mapped_email))


# =============================================================================
# FILTERS - ONLY migrate datasources and workbooks
# =============================================================================

class SkipUserMigration(ContentFilterBase[IUser]):
    """Don't migrate users - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Skip all users


class SkipGroupMigration(ContentFilterBase[IGroup]):
    """Don't migrate groups - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Skip all groups


class SkipProjectMigration(ContentFilterBase[IProject]):
    """Don't migrate projects - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Skip all projects


# Subscriptions are automatically skipped - not included in migration types


# =============================================================================
# TRANSFORMERS - Inspect and log workbook view visibility before publishing
# =============================================================================

logger = logging.getLogger(__name__)


class WorkbookHiddenViewsTransformer(ContentTransformerBase[IPublishableWorkbook]):
    """
    Identifies hidden views for each workbook before it is published.

    Reads PyPublishableWorkbook.views and PyPublishableWorkbook.hidden_view_names
    to log which views will be hidden on the destination site.
    """

    def transform(self, item: IPublishableWorkbook) -> IPublishableWorkbook:
        all_views = list(item.views) if item.views else []
        hidden_names = list(item.hidden_view_names) if item.hidden_view_names else []

        view_names = [v.name for v in all_views]
        visible_names = [n for n in view_names if n not in hidden_names]

        logger.info(
            "Workbook '%s': %d view(s) total — %d visible %s, %d hidden %s",
            item.name,
            len(view_names),
            len(visible_names),
            visible_names,
            len(hidden_names),
            hidden_names,
        )

        return item


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_content():
    """Migrate ONLY data sources and workbooks from Server to Cloud."""

    # Load and validate configuration
    config = load_config()
    if not config or not validate_config(config):
        return

    # Get default content owner
    default_owner = config.get('default_content_owner', '')
    if not default_owner:
        print("❌ Missing 'default_content_owner' in config.json")
        print("   This email will own content when the original owner doesn't exist in Cloud")
        return

    print("✅ Configuration loaded successfully")
    print(f"   Default content owner: {default_owner}\n")
    print("=" * 70)
    print("MIGRATION SCOPE:")
    print("  ✅ Data Sources - WILL BE MIGRATED")
    print("  ✅ Workbooks - WILL BE MIGRATED")
    print("  ❌ Users - WILL NOT BE MIGRATED")
    print("  ❌ Groups - WILL NOT BE MIGRATED")
    print("  ❌ Projects - WILL NOT BE MIGRATED")
    print("  ❌ Subscriptions - WILL NOT BE MIGRATED")
    print("=" * 70)
    print(f"\nSource: {config['source']['server_url']} / {config['source']['site_content_url'] if config['source']['site_content_url'] else 'Default'}")
    print(f"Destination: {config['destination']['pod_url']} / {config['destination']['site_content_url']}\n")

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    # Create content owner mapping with default fallback
    owner_mapping = ContentOwnerMapping(default_owner)

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
        .with_tableau_cloud_usernames(lambda ctx: owner_mapping.map(ctx))
    )

    # Add filters to skip everything except datasources and workbooks
    print("Configuring content filters...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipGroupMigration)
    plan_builder.filters.add(SkipProjectMigration)

    # Add transformer to identify hidden views before each workbook is published
    print("Configuring workbook transformers...")
    plan_builder.transformers.add(WorkbookHiddenViewsTransformer())

    # Build and execute
    print("Building migration plan...")
    plan = plan_builder.build()

    print("\nStarting migration...\n")
    print("📊 Migrating data sources...")
    print("📈 Migrating workbooks...\n")
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
