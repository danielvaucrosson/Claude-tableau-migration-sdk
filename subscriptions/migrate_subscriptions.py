"""
LLB Subscription Migration  |  Tableau Server -> Tableau Cloud
SDK v6.0  |  Migrates subscriptions only.

Prerequisites
-------------
  - Users already exist in Tableau Cloud
  - Projects already exist in Tableau Cloud
  - Workbooks / data sources already migrated (run content_migration.py first)

Scope filter (optional)
-----------------------
  Set SCOPE_PROJECT and/or SCOPE_WORKBOOK below to migrate only subscriptions
  whose content lives in a specific project or workbook.  Useful for scoped
  testing.  Leave both empty to migrate all subscriptions.

Usage
-----
  1. Ensure TableauMigrationPython/config.json is filled in.
  2. Ensure TableauMigrationPython/user_mappings.csv maps every Server username
     to a Cloud email (ServerUsername,CloudEmail columns).
  3. Run from the repo root:
       python subscriptions/migrate_subscriptions.py
"""

import json
import csv
import logging
from pathlib import Path

from tableau_migration import (
    Migrator,
    MigrationPlanBuilder,
    TableauCloudUsernameMappingBase,
    ContentFilterBase,
    IServerSubscription,
    IUser,
    IGroup,
    IProject,
    IWorkbook,
    IDataSource,
    IServerExtractRefreshTask,
    ICustomView,
)
from tableau_migration.migration_engine_hooks_initializemigration import PyInitializeMigrationHookResult
from Tableau.Migration.Api import IServerSessionProvider
from Tableau.Migration.Engine.Endpoints import ISourceEndpoint
from Tableau.Migration import TableauInstanceType
from System.Reflection import BindingFlags

# -- Scope filter (edit these to limit which subscriptions are migrated) --------
# Substring match against the subscription's content location path.
# Example: SCOPE_PROJECT = "Cloud", SCOPE_WORKBOOK = "Superstore"
# Leave empty ("") to migrate all subscriptions.
SCOPE_PROJECT  = ""
SCOPE_WORKBOOK = ""

# -- Logging --------------------------------------------------------------------
# Show INFO from the migration engine so progress is visible; silence HTTP noise.
logging.basicConfig(level=logging.INFO, format="%(message)s")
for _noisy in [
    "System.Net.Http",
    "Tableau.Migration.Net.Logging",
    "Tableau.Migration.Engine.Conversion.Schedules",
    "Tableau.Migration.Engine.Hooks.Transformers",
    "Polly",
    "tableauserverclient",
]:
    logging.getLogger(_noisy).setLevel(logging.CRITICAL)

# -- Paths ----------------------------------------------------------------------
_REPO_ROOT  = Path(__file__).parent.parent
CONFIG_PATH = _REPO_ROOT / "TableauMigrationPython" / "config.json"
CSV_PATH    = _REPO_ROOT / "TableauMigrationPython" / "user_mappings.csv"


# -- Config helpers -------------------------------------------------------------

def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(
            f"Config file not found: {CONFIG_PATH}\n"
            "  Fix: cp TableauMigrationPython/config.json.template "
            "TableauMigrationPython/config.json  then fill in your credentials."
        )
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def validate_config(cfg: dict) -> None:
    required = {
        "source":      ["server_url", "site_content_url", "access_token_name", "access_token"],
        "destination": ["pod_url", "site_content_url", "access_token_name", "access_token"],
    }
    missing = []
    for section, fields in required.items():
        for field in fields:
            val = cfg.get(section, {}).get(field)
            # site_content_url may be an empty string (default site) > that's fine
            if field != "site_content_url" and not val:
                missing.append(f"{section}.{field}")
            elif field == "site_content_url" and val is None:
                missing.append(f"{section}.{field}")
    if not cfg.get("default_content_owner"):
        missing.append("default_content_owner")
    if missing:
        raise ValueError(
            "Missing or empty fields in config.json:\n"
            + "\n".join(f"  - {m}" for m in missing)
        )


# -- User mapping ---------------------------------------------------------------

class SubscriptionUserMapping(TableauCloudUsernameMappingBase):
    """
    Resolves subscription owner references:
      Server username > Cloud email via user_mappings.csv
      Unmapped users  > default_content_owner from config.json

    The SDK calls map() for every user referenced in a subscription,
    so subscription ownership is automatically re-pointed to the correct
    Cloud user without any additional code.
    """

    def __init__(self, default_owner: str, csv_path: Path) -> None:
        self.default_owner = default_owner
        self.mappings = self._load_csv(csv_path)
        super().__init__()

    def _load_csv(self, csv_path: Path) -> dict:
        if not csv_path.exists():
            print(
                f"  WARNING: user_mappings.csv not found at {csv_path}\n"
                f"  All users will fall back to default owner: {self.default_owner}"
            )
            return {}
        mappings: dict = {}
        with open(csv_path, encoding="utf-8-sig") as f:
            for row in csv.DictReader(f):
                username = row.get("ServerUsername", "").strip()
                email    = row.get("CloudEmail", "").strip()
                if username and email:
                    mappings[username.lower()] = email
        print(f"  Loaded {len(mappings)} user mapping(s) from {csv_path.name}")
        return mappings

    def map(self, ctx):
        username = ctx.content_item.name

        # Pass through users already in email format
        if "@" in username:
            return ctx

        email = self.mappings.get(username.lower(), self.default_owner)
        domain = ctx.mapped_location.parent()
        return ctx.map_to(domain.append(email))


# -- Filters --------------------------------------------------------------------
# Skip every content type except subscriptions. Users, groups, projects,
# workbooks, and data sources were already migrated by content_migration.py.

class SkipUsers(ContentFilterBase[IUser]):
    def should_migrate(self, item): return False

class SkipGroups(ContentFilterBase[IGroup]):
    def should_migrate(self, item): return False

class SkipProjects(ContentFilterBase[IProject]):
    def should_migrate(self, item): return False

class SkipWorkbooks(ContentFilterBase[IWorkbook]):
    def should_migrate(self, item): return False

class SkipDataSources(ContentFilterBase[IDataSource]):
    def should_migrate(self, item): return False

class SkipCustomViews(ContentFilterBase[ICustomView]):
    def should_migrate(self, item): return False


class SubscriptionScopeFilter(ContentFilterBase[IServerSubscription]):
    """
    Optional scope filter: only migrate subscriptions whose content location
    matches SCOPE_PROJECT and/or SCOPE_WORKBOOK (case-insensitive substring).

    The SDK exposes content_url and location on IServerSubscription.
    Both are checked so the filter works regardless of which field the SDK
    populates for a given Server version.
    """
    def should_migrate(self, item) -> bool:
        if not SCOPE_PROJECT and not SCOPE_WORKBOOK:
            return True
        try:
            target = " ".join([
                str(getattr(item, "content_url", "") or ""),
                str(getattr(item, "location",    "") or ""),
            ]).lower()
            if SCOPE_PROJECT  and SCOPE_PROJECT.lower()  not in target:
                return False
            if SCOPE_WORKBOOK and SCOPE_WORKBOOK.lower() not in target:
                return False
            return True
        except Exception:
            return True   # migrate if we cannot inspect the item


# -- Instance type fix ----------------------------------------------------------
# Some Tableau Server configurations cause the SDK's serverinfo detection to
# return Unknown instead of TableauServer, which blocks server-specific APIs
# like get_ServerSubscriptions().  This hook runs after sign-in and forces
# the instance type to Server for any session provider that resolved as Unknown.

def _fix_unknown_instance_type(ctx: PyInitializeMigrationHookResult) -> PyInitializeMigrationHookResult:
    """
    The SDK creates separate DI scopes for source and destination endpoints.
    Each scope has its own IServerSessionProvider.  We must reach into the
    SOURCE endpoint's scope and fix its provider — not the migration scope's.
    """
    fixed = 0

    def _fix_provider(provider, label):
        nonlocal fixed
        if provider is None or "Unknown" not in str(provider.InstanceType):
            return
        prop = provider.GetType().GetProperty(
            "InstanceType",
            BindingFlags.Instance | BindingFlags.Public,
        )
        prop.SetValue(provider, TableauInstanceType.Server)
        fixed += 1
        print(f"  Fixed {label} instance type: Unknown -> Server")

    try:
        # 1. Fix the migration scope's provider (may not be the one that matters)
        migration_provider = ctx.scoped_services._get_service(IServerSessionProvider)
        _fix_provider(migration_provider, "migration-scope")

        # 2. Fix the source endpoint's provider (this is the one SitesApiClient uses)
        source_endpoint = ctx.scoped_services._get_service(ISourceEndpoint)
        if source_endpoint is not None:
            # Walk the type hierarchy to find the EndpointScope field
            t = source_endpoint.GetType()
            scope_field = None
            while t is not None and scope_field is None:
                scope_field = t.GetField(
                    "EndpointScope",
                    BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public,
                )
                t = t.BaseType

            if scope_field is not None:
                endpoint_scope = scope_field.GetValue(source_endpoint)
                if endpoint_scope is not None:
                    import clr
                    sp = endpoint_scope.ServiceProvider
                    src_provider = sp.GetService(clr.GetClrType(IServerSessionProvider))
                    _fix_provider(src_provider, "source-endpoint")
                else:
                    print("  Note: EndpointScope is null")
            else:
                print("  Note: EndpointScope field not found")
        else:
            print("  Note: ISourceEndpoint not available in scoped services")

    except Exception as e:
        print(f"  Note: Instance type hook: {e}")

    if fixed == 0:
        print("  Warning: No providers needed fixing — Unknown may persist")

    return ctx


# -- Migration ------------------------------------------------------------------

def migrate_subscriptions() -> None:
    print("\n" + "=" * 70)
    print("  LLB SUBSCRIPTION MIGRATION  |  Tableau Server -> Cloud  |  SDK v6.0")
    print("=" * 70)

    cfg = load_config()
    validate_config(cfg)

    src  = cfg["source"]
    dest = cfg["destination"]
    default_owner = cfg["default_content_owner"]

    print(f"\nSource:        {src['server_url']}")
    print(f"               site: '{src['site_content_url'] or 'Default'}'")
    print(f"Destination:   {dest['pod_url']}")
    print(f"               site: '{dest['site_content_url']}'")
    print(f"Default owner: {default_owner}")
    if SCOPE_PROJECT or SCOPE_WORKBOOK:
        scope_parts = []
        if SCOPE_PROJECT:  scope_parts.append(f"project='{SCOPE_PROJECT}'")
        if SCOPE_WORKBOOK: scope_parts.append(f"workbook='{SCOPE_WORKBOOK}'")
        print(f"Scope filter:  {', '.join(scope_parts)}")
    print()

    # Build user mapping (reads CSV immediately so any file issues surface early)
    user_mapping = SubscriptionUserMapping(default_owner, CSV_PATH)

    # Build the migration plan
    plan_builder = MigrationPlanBuilder()
    plan_builder = (
        plan_builder
        .from_source_tableau_server(
            server_url=src["server_url"],
            site_content_url=src["site_content_url"],
            access_token_name=src["access_token_name"],
            access_token=src["access_token"],
        )
        .to_destination_tableau_cloud(
            pod_url=dest["pod_url"],
            site_content_url=dest["site_content_url"],
            access_token_name=dest["access_token_name"],
            access_token=dest["access_token"],
        )
        .for_server_to_cloud()
        .with_saml_authentication_type(
            domain="llbean.com",
            idp_configuration_name="LLB_PRPROD_SAML",
        )
        .with_tableau_cloud_usernames(user_mapping.map)
    )

    # Skip all content types except subscriptions
    for filter_cls in (
        SkipUsers,
        SkipGroups,
        SkipProjects,
        SkipWorkbooks,
        SkipDataSources,
        SkipCustomViews,
    ):
        plan_builder.filters.add(filter_cls)

    # Use skip_content_type (not a filter) so the SDK never calls the Server
    # extract refresh tasks API — avoids TableauInstanceTypeNotSupportedException
    # on servers where instance type resolves as Unknown.
    plan_builder.skip_content_type(IServerExtractRefreshTask, pre_cache=False)

    # Hook to fix Unknown instance type after sign-in (LLB server workaround)
    plan_builder.hooks.add(PyInitializeMigrationHookResult, _fix_unknown_instance_type)

    # Optional: only migrate subscriptions matching the configured scope
    plan_builder.filters.add(SubscriptionScopeFilter)

    print("Building plan and connecting to Tableau Server...")
    plan = plan_builder.build()

    print("Running subscription migration (this may take several minutes)...\n")
    result = Migrator().execute(plan)

    # -- Results ----------------------------------------------------------------
    print("\n" + "=" * 70)
    print("  RESULTS")
    print("=" * 70)

    # Migrator.execute() wraps a .NET result. manifest.entries is a raw .NET
    # IEnumerable (not the Py-prefixed wrapper), so ContentType on the stub is
    # None.  Status is PascalCase on the .NET entry.
    # All non-subscription types were filtered to Skipped, so Migrated entries
    # are subscriptions and Error entries indicate migration problems.
    manifest = result.manifest

    def _entry_status(e):
        s = getattr(e, "Status", None)
        if s is None:
            return "unknown"
        # pythonnet IntEnum: str gives e.g. "MigrationManifestEntryStatus.Migrated"
        return str(s).split(".")[-1]

    try:
        all_entries = list(manifest.entries)
    except Exception:
        all_entries = []

    migrated_entries = [e for e in all_entries if _entry_status(e) == "Migrated"]
    skipped_entries  = [e for e in all_entries if _entry_status(e) == "Skipped"]
    error_entries    = [e for e in all_entries if _entry_status(e) == "Error"]

    print(f"\nContent items processed: {len(all_entries)}")
    print(f"  Migrated (subscriptions): {len(migrated_entries)}")
    print(f"  Skipped (filtered types): {len(skipped_entries)}")
    print(f"  Errors:                   {len(error_entries)}")

    if error_entries:
        print("\nFailed items:")
        for entry in error_entries:
            errs = list(getattr(entry, "Errors", None) or [])
            detail = "; ".join(str(e) for e in errs) if errs else "no detail available"
            src = getattr(entry, "Source", None)
            loc = getattr(src, "Location", None)
            print(f"  - {loc}: {detail}")

    # result.status is a Python IntEnum; .name gives the member name
    status_name = getattr(result.status, "name", str(result.status))
    print(f"\nOverall migration status: {status_name}")
    print("=" * 70 + "\n")


if __name__ == "__main__":
    migrate_subscriptions()
