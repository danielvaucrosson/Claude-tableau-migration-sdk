"""
Content Migration - Data Sources and Workbooks
Migrates workbooks and data sources (including custom views).
Skips users, projects, and subscriptions (handled by separate script).
"""

from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
)
from migration_utils import (
    configure_logging,
    load_config,
    validate_config,
    get_migration_scope,
    EmailDomainMapping,
    SkipUserMigration,
    SkipGroupMigration,
    SkipProjectMigration,
    SkipDataSourceMigration,
    SkipWorkbookMigration,
)

configure_logging()


# =============================================================================
# MIGRATION
# =============================================================================

def migrate_content():
    """Migrate data sources and workbooks from Server to Cloud."""

    # Load and validate configuration
    config = load_config()
    if not config or not validate_config(config):
        return

    # Get config values
    default_owner = config.get('default_content_owner', '')
    if not default_owner:
        print("Missing 'default_content_owner' in config.json")
        print("   This email will own content when the original owner doesn't exist in Cloud")
        return

    email_domain = config['default_email_domain']
    scope = get_migration_scope(config)

    print("Configuration loaded successfully")
    print(f"   Default content owner: {default_owner}")
    print(f"   Email domain: {email_domain}\n")
    print("=" * 70)
    print("MIGRATION SCOPE:")
    print(f"  {'YES' if scope['data_sources'] else 'NO ':>3} Data Sources - {'WILL BE MIGRATED' if scope['data_sources'] else 'WILL NOT BE MIGRATED'}")
    print(f"  {'YES' if scope['workbooks'] else 'NO ':>3} Workbooks - {'WILL BE MIGRATED' if scope['workbooks'] else 'WILL NOT BE MIGRATED'}")
    print("   NO Users - WILL NOT BE MIGRATED")
    print("   NO Groups - WILL NOT BE MIGRATED")
    print("   NO Projects - WILL NOT BE MIGRATED")
    print("   NO Subscriptions - WILL NOT BE MIGRATED")
    print("=" * 70)
    print(f"\nSource: {config['source']['server_url']} / {config['source']['site_content_url'] if config['source']['site_content_url'] else 'Default'}")
    print(f"Destination: {config['destination']['pod_url']} / {config['destination']['site_content_url']}\n")

    # Create migrator
    migration = Migrator()

    # Build plan
    plan_builder = MigrationPlanBuilder()

    # Create username mapping with email domain from config
    owner_mapping = EmailDomainMapping(email_domain, default_owner)

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

    # Always skip users, groups, projects
    print("Configuring content filters...")
    plan_builder.filters.add(SkipUserMigration)
    plan_builder.filters.add(SkipGroupMigration)
    plan_builder.filters.add(SkipProjectMigration)

    # Conditionally skip based on migration_scope config
    if not scope['data_sources']:
        plan_builder.filters.add(SkipDataSourceMigration)
    if not scope['workbooks']:
        plan_builder.filters.add(SkipWorkbookMigration)

    # Build and execute
    print("Building migration plan...")
    plan = plan_builder.build()

    print("\nStarting migration...\n")
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
    migrate_content()
