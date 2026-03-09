"""
Workbook Analysis Script
Analyzes workbooks and their views (including hidden views) WITHOUT migrating.
Loads user mappings from CSV and analyzes how content would be mapped.
Uses Migration SDK to download and inspect workbooks, but filters prevent actual migration.
"""

import csv
import logging
import json
import time
import os
from pathlib import Path

# Configure SDK to fetch all content (increase page size)
os.environ['MigrationSDK__Network__FileChunkSizeKB'] = '4096'  # Larger chunks
os.environ['MigrationSDK__Network__DefaultPageSize'] = '1000'  # Fetch more items per page
from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    TableauCloudUsernameMappingBase,
    ContentFilterBase,
    ContentTransformerBase,
    IPublishableWorkbook,
    IDataSource,
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
logging.getLogger('tableauserverclient').setLevel(logging.CRITICAL)  # Suppress TSC REST API logs
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
    Map Server usernames to Cloud emails using user_mappings.csv.
    Falls back to default_content_owner for any user not in the CSV.
    """

    def __init__(self, default_owner, csv_path='user_mappings.csv', destination_config=None):
        self.default_owner = default_owner
        self.destination_config = destination_config
        self.cloud_users = set()  # Will store verified Cloud users
        self.mappings = self._load_csv(csv_path)
        self.mapping_results = []  # Track all mappings for summary
        super().__init__()

    def _get_cloud_users(self):
        """Fetch list of existing users from Tableau Cloud via REST API."""
        if not self.destination_config:
            return set()

        # Skip verification if using dummy destination (analysis mode)
        if (self.destination_config.get('access_token_name') == 'dummy' or
            self.destination_config.get('site_content_url') == 'dummy-site'):
            print("⏭️  Skipping Cloud user verification (analysis mode with dummy destination)\n")
            return set()

        try:
            import tableauserverclient as TSC
            import warnings

            # Completely disable TSC logging
            logging.disable(logging.CRITICAL)

            print("🔍 Connecting to Tableau Cloud to verify users...")

            # Create authentication using Personal Access Token
            tableau_auth = TSC.PersonalAccessTokenAuth(
                token_name=self.destination_config['access_token_name'],
                personal_access_token=self.destination_config['access_token'],
                site_id=self.destination_config['site_content_url']
            )

            # Connect to Tableau Cloud
            server = TSC.Server(self.destination_config['pod_url'], use_server_version=True)

            # Sign in and get users
            with server.auth.sign_in(tableau_auth):
                all_users = []
                # Paginate through all users
                for user in TSC.Pager(server.users):
                    all_users.append(user)

                # Extract usernames (which are emails in Tableau Cloud)
                user_emails = {user.name.lower() for user in all_users if user.name}

            # Re-enable logging after TSC operations
            logging.disable(logging.NOTSET)

            print(f"✅ Found {len(user_emails)} users in Tableau Cloud\n")
            return user_emails

        except Exception as e:
            print(f"⚠️  Could not verify Cloud users: {e}")
            print("   Proceeding without verification...\n")
            return set()

    def _load_csv(self, csv_path):
        path = Path(csv_path)
        if not path.exists():
            print(f"⚠️  user_mappings.csv not found at '{csv_path}' — all content will be assigned to default owner")
            return {}

        # Get list of existing Cloud users
        self.cloud_users = self._get_cloud_users()

        mappings = {}
        print(f"📄 Loading user mappings from {csv_path}:")
        with open(path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                username = row.get('ServerUsername', '').strip()
                email = row.get('CloudEmail', '').strip()

                if username and email:
                    mappings[username.lower()] = email

                    # Check if user exists in Cloud
                    if self.cloud_users:
                        if email.lower() in self.cloud_users:
                            print(f"   ✅ {username} → {email} (user exists in Cloud)")
                        else:
                            print(f"   ❌ {username} → {email} (user NOT found - will use default owner: {self.default_owner})")
                    else:
                        print(f"   ✅ {username} → {email}")

                    time.sleep(0.75)  # 0.75 second delay between items

                elif username or email:
                    print(f"   ⚠️  Skipped incomplete row: username='{username}', email='{email}'")
                    time.sleep(0.75)

        print(f"\n✅ Loaded {len(mappings)} user mapping(s)\n")
        return mappings

    def map(self, ctx):
        username = ctx.content_item.name
        domain = ctx.mapped_location.parent()

        if "@" in username:
            mapped_email = username
            source = "already email"
        elif username.lower() in self.mappings:
            mapped_email = self.mappings[username.lower()]
            # Check if mapped user exists in Cloud
            if self.cloud_users and mapped_email.lower() not in self.cloud_users:
                # User doesn't exist, fall back to default owner
                mapped_email = self.default_owner
                source = "CSV → default (user not found)"
            else:
                source = "CSV"
        else:
            mapped_email = self.default_owner
            source = "default owner"

        # Track this mapping
        self.mapping_results.append({
            'username': username,
            'mapped_email': mapped_email,
            'source': source
        })

        print(f"👤 {username} → {mapped_email} ({source})")
        return ctx.map_to(domain.append(mapped_email))

    def print_summary(self):
        """Print detailed mapping summary showing each user and their mapping status."""
        if not self.mapping_results:
            print("\n📊 No user mappings were performed")
            return

        print("\n" + "="*70)
        print("📊 USER MAPPING SUMMARY")
        print("="*70)

        # Group by source type
        csv_mapped = [r for r in self.mapping_results if r['source'] == 'CSV']
        already_email = [r for r in self.mapping_results if r['source'] == 'already email']
        default_owner = [r for r in self.mapping_results if r['source'] == 'default owner']

        # Show users with CSV mappings
        if csv_mapped:
            print(f"\n✅ Users with CSV mapping ({len(csv_mapped)}):")
            for result in csv_mapped:
                print(f"   • {result['username']} → {result['mapped_email']}")

        # Show users already in email format
        if already_email:
            print(f"\n📧 Users already in email format ({len(already_email)}):")
            for result in already_email:
                print(f"   • {result['username']}")

        # Show users mapped to default owner
        if default_owner:
            print(f"\n⚠️  Users without mapping (using default owner) ({len(default_owner)}):")
            for result in default_owner:
                print(f"   • {result['username']} → {result['mapped_email']} (default)")

        print(f"\nTotal users processed: {len(self.mapping_results)}")
        print("="*70)


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

    # Class-level counter for progress tracking
    workbook_count = 0

    @classmethod
    def reset_counter(cls):
        """Reset the workbook counter."""
        cls.workbook_count = 0

    def transform(self, item: IPublishableWorkbook) -> IPublishableWorkbook:
        WorkbookHiddenViewsTransformer.workbook_count += 1

        all_views = list(item.views) if item.views else []
        hidden_names = list(item.hidden_view_names) if item.hidden_view_names else []

        view_names = [v.name for v in all_views]
        visible_names = [n for n in view_names if n not in hidden_names]

        # Show progress with workbook number
        logger.info(
            "[%d] Workbook '%s': %d view(s) total — %d visible %s, %d hidden %s",
            WorkbookHiddenViewsTransformer.workbook_count,
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

def verify_source_connection(source_config):
    """Verify we can connect to the source Tableau Server."""
    import tableauserverclient as TSC

    print("🔍 Verifying source server connection...")

    try:
        # Temporarily disable logging for this test
        logging.disable(logging.CRITICAL)

        # Create authentication
        tableau_auth = TSC.PersonalAccessTokenAuth(
            token_name=source_config['access_token_name'],
            personal_access_token=source_config['access_token'],
            site_id=source_config.get('site_content_url', '')
        )

        # Try to connect
        server = TSC.Server(source_config['server_url'], use_server_version=True)

        with server.auth.sign_in(tableau_auth):
            print(f"✅ Successfully connected to source server")
            print(f"   Server version: {server.version}")
            print(f"   Site: {source_config.get('site_content_url', 'Default')}\n")

        # Re-enable logging
        logging.disable(logging.NOTSET)
        return True

    except Exception as e:
        logging.disable(logging.NOTSET)
        print(f"\n❌ Failed to connect to source server:")
        print(f"   Server: {source_config['server_url']}")
        print(f"   Site: {source_config.get('site_content_url', 'Default')}")
        print(f"   Error: {e}\n")
        print("💡 Check your config.json:")
        print("   - Is the server_url correct?")
        print("   - Is the site_content_url correct?")
        print("   - Are the access token credentials valid?")
        print("   - Has the token expired?\n")
        return False


def migrate_content():
    """Analyze workbooks and load user mappings WITHOUT migrating."""

    # Load configuration
    config = load_config()
    if not config:
        return

    # Get source configuration
    source = config.get('source', {})
    if not source or not source.get('server_url'):
        print("❌ Missing source server configuration in config.json")
        return

    # Verify source connection before proceeding
    if not verify_source_connection(source):
        return

    # Get default content owner (optional for analysis)
    default_owner = config.get('default_content_owner', 'analysis@example.com')

    # Destination is optional for analysis - use dummy if not configured
    destination = config.get('destination', {})
    if not destination or not destination.get('pod_url'):
        print("⚠️  Note: Destination not configured - using dummy (analysis only)\n")
        destination = {
            'pod_url': 'https://prod-useast-b.online.tableau.com',
            'site_content_url': 'dummy-site',
            'access_token_name': 'dummy',
            'access_token': 'dummy'
        }

    print("=" * 80)
    print("📊 WORKBOOK ANALYSIS MODE")
    print("=" * 80)
    print(f"Source: {source['server_url']} / {source.get('site_content_url', 'Default')}")
    print(f"Default content owner: {default_owner}")
    print("Mode: Analysis only - NO MIGRATION will occur")
    print("=" * 80)
    print()

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    # Create content owner mapping — loads user_mappings.csv with default fallback
    csv_path = Path(__file__).parent / 'user_mappings.csv'
    owner_mapping = ContentOwnerMapping(
        default_owner,
        csv_path=str(csv_path),
        destination_config=destination
    )

    plan_builder = (
        plan_builder
        .from_source_tableau_server(
            server_url=source['server_url'],
            site_content_url=source.get('site_content_url', ''),
            access_token_name=source['access_token_name'],
            access_token=source['access_token']
        )
        .to_destination_tableau_cloud(
            pod_url=destination['pod_url'],
            site_content_url=destination['site_content_url'],
            access_token_name=destination['access_token_name'],
            access_token=destination['access_token']
        )
        .for_server_to_cloud()
    )

    # Add filters to skip ALL migration (analysis only)
    print("⚙️  Configuring analysis mode (all migration disabled)...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipGroupMigration)
    plan_builder.filters.add(SkipProjectMigration)

    # Also skip workbooks and data sources from being migrated
    class SkipWorkbookMigration(ContentFilterBase[IPublishableWorkbook]):
        def should_migrate(self, item):
            return False

    class SkipDataSourceMigration(ContentFilterBase[IDataSource]):
        def should_migrate(self, item):
            return False

    # Skip large workbooks to avoid long downloads
    class SkipLargeWorkbooks(ContentFilterBase[IPublishableWorkbook]):
        MAX_SIZE_MB = 50  # Lower threshold for faster analysis
        MAX_SIZE_BYTES = MAX_SIZE_MB * 1024 * 1024  # 52428800 bytes

        def should_migrate(self, item):
            # Check if item has size attribute
            if hasattr(item, 'size') and item.size is not None:
                size_mb = item.size / (1024 * 1024)
                if item.size > self.MAX_SIZE_BYTES:
                    print(f"   ⏭️  Skipping large workbook: {item.name} ({size_mb:.1f} MB)")
                    return False
            return True  # Process workbooks under 50MB or without size info

    plan_builder.filters.add(SkipLargeWorkbooks)
    plan_builder.filters.add(SkipWorkbookMigration)
    plan_builder.filters.add(SkipDataSourceMigration)

    # Add transformer to analyze workbooks
    print("⚙️  Adding workbook analyzer...")
    WorkbookHiddenViewsTransformer.reset_counter()
    plan_builder.transformers.add(WorkbookHiddenViewsTransformer)

    # Build and execute
    print("🔍 Building analysis plan...")
    plan = plan_builder.build()

    print("\n📥 Fetching workbook list from server...")
    print("   (If this stops at 25, it's a pagination issue)")
    print("   ⏭️  Skipping workbooks larger than 50MB")
    print("   Press Ctrl+C to stop if needed\n")

    try:
        result = migration.execute(plan)

        # Show how many workbooks were actually found/processed
        print(f"\n📊 Total workbooks found by SDK: {WorkbookHiddenViewsTransformer.workbook_count}")
    except KeyboardInterrupt:
        print("\n\n⚠️  Analysis interrupted by user")
        print(f"   Processed {WorkbookHiddenViewsTransformer.workbook_count} workbook(s) before stopping\n")
        return

    # Show detailed mapping summary
    owner_mapping.print_summary()

    # Results
    print("\n" + "="*80)
    print(f"✅ Analysis complete - Status: {result.status.name}")
    if result.status.name != "Completed":
        print(f"⚠️  Note: Status is {result.status.name}, but this is normal for analysis-only mode")
    print("="*80)


if __name__ == "__main__":
    migrate_content()
