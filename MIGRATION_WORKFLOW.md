# Tableau Migration Workflow

This repository contains two separate migration scripts for migrating from Tableau Server to Tableau Cloud.

## Scripts Overview

### 1. Content Migration (`content_migration.py`)
**Purpose:** Migrate data sources and workbooks (including custom views)

**What it migrates:**
- ✅ Data sources
- ✅ Workbooks
- ✅ Custom views

**What it skips:**
- ⏭️ Users (should already exist in Cloud)
- ⏭️ Projects (should already exist in Cloud)
- ⏭️ Subscriptions (handled by separate script)

**When to run:** FIRST - Run this to migrate your content before migrating subscriptions

### 2. Subscription Migration (`simple_subscription_migration.py`)
**Purpose:** Migrate subscriptions only

**What it migrates:**
- ✅ Subscriptions

**What it skips:**
- ⏭️ Users (should already exist in Cloud)
- ⏭️ Projects (should already exist in Cloud)
- ⏭️ Data sources (already migrated by content script)
- ⏭️ Workbooks (already migrated by content script)

**When to run:** SECOND - Run this after content migration is complete

## Migration Order

```
1. Ensure users and projects exist in Tableau Cloud (manual setup)
   ↓
2. Run content_migration.py
   - Migrates data sources
   - Migrates workbooks
   - Migrates custom views
   ↓
3. Run simple_subscription_migration.py
   - Migrates subscriptions
   - Links subscriptions to migrated content
```

## Configuration

Both scripts use the same configuration pattern:

```python
# SOURCE: Tableau Server
SOURCE_SERVER_URL = "https://your-tableau-server.com"
SOURCE_SITE = ""  # Empty string for default site
SOURCE_TOKEN_NAME = "your-token-name"
SOURCE_TOKEN = "your-token-secret"

# DESTINATION: Tableau Cloud
DEST_CLOUD_URL = "https://10ax.online.tableau.com"
DEST_SITE = "your-cloud-site"
DEST_TOKEN_NAME = "your-cloud-token-name"
DEST_TOKEN = "your-cloud-token-secret"
```

## User Mapping

Both scripts use `SimpleUsernameMapping` which appends `@keyrus.com` to Server usernames to match Cloud users.

**Example:**
- Server user: `jsmith`
- Cloud user: `jsmith@keyrus.com`

If users already have email format on Server, they are passed through unchanged.

## Usage

1. **Edit configuration** in both scripts with your credentials
2. **Run content migration first:**
   ```bash
   python content_migration.py
   ```
3. **Run subscription migration second:**
   ```bash
   python simple_subscription_migration.py
   ```

## Why Two Scripts?

This separation allows you to:
- Migrate content and subscriptions independently
- Re-run subscriptions without re-migrating content
- Test content migration without affecting subscriptions
- Keep working code isolated (subscription migration proven to work)

## Notes

- Custom views are attached to workbooks and migrate automatically with workbooks
- Subscriptions reference workbooks/data sources, so content MUST be migrated first
- User mapping is identical between both scripts for consistency
