"""
Simple Subscription Migration - Single File
ONLY migrates subscriptions - skips users and workbooks (assumes they exist in Cloud).
"""

import sys
import os

# Add parent directory to path so we can import migration_utils from repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
)
from migration_utils import (
    configure_logging,
    load_config,
    validate_config,
    EmailDomainMapping,
    SkipUserMigration,
    SkipProjectMigration,
    SkipDataSourceMigration,
    SkipWorkbookMigration,
)

configure_logging()


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_subscriptions():
    """Migrate subscriptions from Server to Cloud."""

    # Load config from repo root
    config_path = os.path.join(
        os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
        'config.json'
    )
    config = load_config(config_path)
    if not config or not validate_config(config):
        return

    email_domain = config['default_email_domain']

    print("Starting subscription migration...")
    print(f"Source: {config['source']['server_url']} / {config['source']['site_content_url'] if config['source']['site_content_url'] else 'Default'}")
    print(f"Destination: {config['destination']['pod_url']} / {config['destination']['site_content_url']}\n")

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    # Create username mapping once (not per-call)
    username_mapping = EmailDomainMapping(email_domain)

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
        .with_tableau_cloud_usernames(lambda ctx: username_mapping.map(ctx))
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
        print("Migration completed!")
        print(f"   Check your Cloud site for migrated content")
    else:
        print(f"Migration failed: {result.status}")
        if hasattr(result, 'errors') and result.errors:
            for error in result.errors:
                print(f"   {error}")
    print("="*50)


if __name__ == "__main__":
    migrate_subscriptions()
