"""
Simple Subscription Migration - Single File
ONLY migrates subscriptions - skips all other content types.
Skips: users, projects, workbooks, data sources, custom views, and extract refresh tasks.
Assumes content and users already exist in Cloud.
Uses config.json and user_mappings.csv from parent directory.
"""

import logging
import json
import csv
import time
from pathlib import Path
from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    TableauCloudUsernameMappingBase,
    ContentFilterBase,
    IUser,
    IWorkbook,
    IDataSource,
    IProject,
    IServerExtractRefreshTask,
    ICustomView
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
# CONFIGURATION - Load from config.json
# =============================================================================

def load_config(config_path='../TableauMigrationPython/config.json'):
    """Load credentials from config.json file."""
    config_file = Path(config_path)

    if not config_file.exists():
        print(f"❌ Config file not found: {config_path}")
        print(f"\n📋 Setup instructions:")
        print(f"   1. Copy the template: cp TableauMigrationPython/config.json.template TableauMigrationPython/config.json")
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
            # site_content_url is allowed to be blank (empty string = default site)
            if field == 'site_content_url':
                if field not in config[section]:
                    missing.append(f"{section}.{field}")
            else:
                # All other fields must be present and non-empty
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

    def __init__(self, default_owner, csv_path='../TableauMigrationPython/user_mappings.csv', destination_config=None):
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


class SkipExtractRefreshTaskMigration(ContentFilterBase[IServerExtractRefreshTask]):
    """Don't migrate extract refresh tasks - only migrating subscriptions."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping extract refresh task: {item.source_item.id}")
        return False  # Don't migrate extract refresh tasks


class SkipCustomViewMigration(ContentFilterBase[ICustomView]):
    """Don't migrate custom views - only migrating subscriptions."""

    def should_migrate(self, item):
        print(f"⏭️  Skipping custom view: {item.source_item.name}")
        return False  # Don't migrate custom views


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_subscriptions():
    """Migrate subscriptions from Server to Cloud."""

    # Load configuration
    config = load_config()
    if not config or not validate_config(config):
        return

    # Get config sections
    source = config['source']
    destination = config['destination']
    default_owner = config['default_content_owner']

    print("="*70)
    print("📧 SUBSCRIPTION MIGRATION")
    print("="*70)
    print(f"Source: {source['server_url']} / {source.get('site_content_url', 'Default')}")
    print(f"Destination: {destination['pod_url']} / {destination['site_content_url']}")
    print(f"Default content owner: {default_owner}")
    print("="*70)
    print()

    # Create content owner mapping — loads user_mappings.csv with default fallback
    csv_path = Path(__file__).parent.parent / 'TableauMigrationPython' / 'user_mappings.csv'
    owner_mapping = ContentOwnerMapping(
        default_owner,
        csv_path=str(csv_path),
        destination_config=destination
    )

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

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
        .with_tableau_id_authentication_type()
        .with_tableau_cloud_usernames(lambda ctx: owner_mapping.map(ctx))
    )

    # Add filters to skip all content except subscriptions
    # Only subscriptions will be migrated
    print("Configuring filters to skip content migration (subscriptions only)...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipProjectMigration)
    plan_builder.filters.add(SkipDataSourceMigration)
    plan_builder.filters.add(SkipWorkbookMigration)
    plan_builder.filters.add(SkipExtractRefreshTaskMigration)
    plan_builder.filters.add(SkipCustomViewMigration)

    # Build and execute
    print("Building migration plan...")
    plan = plan_builder.build()

    print("Starting migration (this may take a while)...\n")
    result = migration.execute(plan)

    # Show detailed mapping summary
    owner_mapping.print_summary()

    # Results
    print("\n" + "="*70)
    print("📊 MIGRATION RESULTS")
    print("="*70)
    if result.status.name == "Completed":
        print("✅ Migration completed!")
        print(f"   Check your Cloud site for migrated subscriptions")
    else:
        print(f"❌ Migration failed: {result.status}")
        if hasattr(result, 'errors') and result.errors:
            for error in result.errors:
                print(f"   {error}")
    print("="*70)


if __name__ == "__main__":
    migrate_subscriptions()
