# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

This repo contains Python tooling for migrating content from **Tableau Server (on-premises) to Tableau Cloud** using the [Tableau Migration SDK](https://help.tableau.com/current/api/migration_sdk/en-us/). There is also an archived C# equivalent in `archive/csharp-examples/`.

The production workflow is a **two-phase migration**:
1. **Phase 1** — `TableauMigrationPython/content_migration.py`: Migrate data sources and workbooks (users/projects/subscriptions must already exist in Cloud).
2. **Phase 2** — `subscriptions/simple_subscription_migration.py`: Migrate subscriptions only (assumes Phase 1 is complete).

## Setup

Requires **Python 3.10 or higher** (Python 3.9 deprecated in SDK v6.0).

```bash
cd TableauMigrationPython

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate        # Linux/Mac
venv\Scripts\activate           # Windows

# Install dependencies (SDK pinned to v6.x to avoid pythonnet conflicts)
pip install -r requirements.txt

# Verify SDK installed
python -c "import tableau_migration; print('OK')"
```

## Running the Migration

```bash
# Setup credentials (do this once)
cp TableauMigrationPython/config.json.template TableauMigrationPython/config.json
# Edit config.json with actual server/cloud credentials and default_content_owner email

# Generate user_mappings.csv automatically (matches Server users to Cloud emails)
python TableauMigrationPython/generate_user_mappings.py
# Review the output in Excel — filter NeedsReview=TRUE and fix uncertain rows, then save

# Phase 1: Migrate content (data sources + workbooks)
python TableauMigrationPython/content_migration.py

# Phase 2: Migrate subscriptions (SDK v6.0, run from repo root)
python subscriptions/migrate_subscriptions.py
```

All three scripts load config from `TableauMigrationPython/config.json` and user mappings from `TableauMigrationPython/user_mappings.csv`. These files are gitignored.

`subscriptions/simple_subscription_migration.py` is the previous v5.x script kept for reference.

To run an individual example:
```bash
python TableauMigrationPython/examples/1_basic_migration_setup.py
python TableauMigrationPython/examples/5_csv_based_user_mapping.py  # recommended
python TableauMigrationPython/examples/6_complete_subscription_migration.py  # production template
```

## Architecture

### Key SDK Extension Points

All customization is done by implementing classes with specific method signatures and registering them on the `plan_builder`:

| Extension | Interface | Method | Registration |
|-----------|-----------|--------|--------------|
| User/Project mapping | `TableauCloudUsernameMappingBase` or plain class | `map(self, ctx)` → return `ctx.map_to(...)` | `plan_builder.mappings.add(...)` |
| Content filtering | `ContentFilterBase[T]` | `should_migrate(self, item)` → `bool` | `plan_builder.filters.add(...)` |
| Content transformation | `ContentTransformerBase[T]` | `transform(self, item)` → `item` | `plan_builder.transformers.add(...)` |

### Two SDK API Styles

The codebase uses two layers of the same `tableau-migration` package:
- **`Py`-prefixed API** (`PyMigrationPlanBuilder`, `PyContentMappingContext`) — used in `examples/`, simpler wrappers.
- **Non-prefixed API** (`Migrator`, `MigrationPlanBuilder`, `ContentFilterBase`) — used in the production scripts; required for typed generics like `ContentFilterBase[IUser]`.

### Credential / Config Patterns

Three patterns exist (see `TableauMigrationPython/`):
- `config_json_file.py` — loads from `config.json` (used by production scripts)
- `config_env_vars.py` — loads from environment variables
- `.env.template` — for use with `python-dotenv`

### `content_migration.py` Specifics

- Runs the SDK in a **batch loop** — the SDK processes ~25 workbooks per run, so the script re-executes until no new workbooks are found.
- Contains `WorkbookHiddenViewsTransformer` which inspects hidden views without actually migrating them (analysis mode when destination is `dummy`).
- `ContentOwnerMapping` extends `TableauCloudUsernameMappingBase`, loads `user_mappings.csv`, optionally verifies users exist in Cloud via `tableauserverclient` REST API, and falls back to `default_content_owner` for unmapped users.
- Setting `destination` to dummy values activates analysis-only mode (no content is published).

## Configuration Reference

`config.json` fields:
```json
{
  "source": { "server_url", "site_content_url", "access_token_name", "access_token" },
  "destination": { "pod_url", "site_content_url", "access_token_name", "access_token" },
  "default_content_owner": "admin@yourcompany.com"
}
```

- `site_content_url` for source: `""` = default site, `"site-name"` for named sites.
- `pod_url` for destination: varies by Tableau Cloud region (`10ax`, `10ay`, `10az`, etc.).
- `default_content_owner`: Cloud email that owns content when the original owner has no mapping.

## `user_mappings.csv` Format

```csv
ServerUsername,CloudEmail,Notes
jsmith,john.smith@company.com,optional notes
```

Both production scripts look for this file at `TableauMigrationPython/user_mappings.csv` (path resolved relative to the script via `Path(__file__).parent`).
