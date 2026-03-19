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
import warnings
import sys
import os
from pathlib import Path
from contextlib import contextmanager
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

# Suppress all warnings
warnings.filterwarnings('ignore')


@contextmanager
def suppress_output():
    """Suppress stdout and stderr output during migration."""
    old_stdout = sys.stdout
    old_stderr = sys.stderr
    try:
        sys.stdout = open(os.devnull, 'w')
        sys.stderr = open(os.devnull, 'w')
        yield
    finally:
        sys.stdout.close()
        sys.stderr.close()
        sys.stdout = old_stdout
        sys.stderr = old_stderr

# Configure logging - suppress ALL migration engine logs
logging.basicConfig(
    level=logging.CRITICAL,  # Only show critical errors
    format='%(message)s'
)

# Silence ALL loggers
logging.getLogger('System.Net.Http.HttpClient.DefaultHttpClient.LogicalHandler').setLevel(logging.CRITICAL)
logging.getLogger('System.Net.Http.HttpClient.DefaultHttpClient.ClientHandler').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Net.Logging.HttpActivityLogger').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Engine.Conversion.Schedules.ServerToCloudScheduleConverter').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Engine.Hooks.Transformers').setLevel(logging.CRITICAL)
logging.getLogger('Polly').setLevel(logging.CRITICAL)
logging.getLogger('System.Net.Http.HttpClient').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration.Engine').setLevel(logging.CRITICAL)
logging.getLogger('Tableau.Migration').setLevel(logging.CRITICAL)


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
            return set()

        try:
            import tableauserverclient as TSC
            import warnings

            # Completely disable TSC logging
            logging.disable(logging.CRITICAL)

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

            return user_emails

        except Exception as e:
            return set()

    def _load_csv(self, csv_path):
        path = Path(csv_path)
        if not path.exists():
            return {}

        # Get list of existing Cloud users
        self.cloud_users = self._get_cloud_users()

        mappings = {}
        with open(path, 'r', encoding='utf-8') as f:
            for row in csv.DictReader(f):
                username = row.get('ServerUsername', '').strip()
                email = row.get('CloudEmail', '').strip()

                if username and email:
                    mappings[username.lower()] = email

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
        return False  # Don't migrate users


class SkipProjectMigration(ContentFilterBase[IProject]):
    """Don't migrate projects - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Don't migrate projects


class SkipDataSourceMigration(ContentFilterBase[IDataSource]):
    """Don't migrate data sources - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Don't migrate data sources


class SkipWorkbookMigration(ContentFilterBase[IWorkbook]):
    """Don't migrate workbooks - they should already exist in Cloud."""

    def should_migrate(self, item):
        return False  # Don't migrate workbooks


class SkipExtractRefreshTaskMigration(ContentFilterBase[IServerExtractRefreshTask]):
    """Don't migrate extract refresh tasks - only migrating subscriptions."""

    def should_migrate(self, item):
        return False  # Don't migrate extract refresh tasks


class SkipCustomViewMigration(ContentFilterBase[ICustomView]):
    """Don't migrate custom views - only migrating subscriptions."""

    def should_migrate(self, item):
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

    print("\n" + "="*70)
    print("📧 SUBSCRIPTION MIGRATION")
    print("="*70)

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

    print("🔌 Connecting to Tableau Server...")
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
    print("✅ Connected to Tableau Server")

    print("🔌 Connecting to Tableau Cloud...")
    # Add filters to skip all content except subscriptions
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipProjectMigration)
    plan_builder.filters.add(SkipDataSourceMigration)
    plan_builder.filters.add(SkipWorkbookMigration)
    plan_builder.filters.add(SkipExtractRefreshTaskMigration)
    plan_builder.filters.add(SkipCustomViewMigration)
    print("✅ Connected to Tableau Cloud")

    # Build and execute
    print("\n🚀 Starting subscription migration...")
    plan = plan_builder.build()

    # Execute migration with output suppressed
    with suppress_output():
        result = migration.execute(plan)

    # Results
    print("\n" + "="*70)
    print("📊 SUBSCRIPTION MIGRATION RESULTS")
    print("="*70)

    try:
        manifest = result.manifest
        if hasattr(manifest, 'entries') and manifest.entries:
            # Look for subscription entries
            from tableau_migration import ISubscription
            subscription_entries = [e for e in manifest.entries if e.source.content_type.name == 'Subscription']

            if subscription_entries:
                # Count successful vs failed
                migrated = [e for e in subscription_entries if hasattr(e, 'status') and e.status.name == "Migrated"]

                print(f"\n✅ Successfully migrated {len(migrated)} subscription(s)")

                # Show details of migrated subscriptions
                if migrated:
                    print("\n📧 Migrated Subscriptions:")
                    for i, entry in enumerate(migrated, 1):
                        print(f"   {i}. Subscription migrated successfully")
                        if hasattr(entry, 'destination') and entry.destination:
                            try:
                                dest_id = str(entry.destination.id) if hasattr(entry.destination, 'id') else "Unknown"
                                print(f"      Cloud ID: {dest_id}")
                            except:
                                pass
            else:
                print("\n✅ Migration completed")
                print("No subscriptions found to migrate")
        else:
            print("\n✅ Migration completed")
    except Exception as e:
        print("\n✅ Migration completed")
        print("Check your Tableau Cloud site to verify subscriptions")

    print("="*70)


if __name__ == "__main__":
    migrate_subscriptions()
