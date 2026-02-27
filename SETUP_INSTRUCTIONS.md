# Tableau Migration Setup Instructions

## What Gets Migrated

By default, `content_migration.py` migrates:
- ✅ **Data Sources** (configurable via `migration_scope`)
- ✅ **Workbooks** (configurable via `migration_scope`)

It **DOES NOT** migrate:
- ❌ Users
- ❌ Groups
- ❌ Projects
- ❌ Subscriptions (use `subscriptions/simple_subscription_migration.py` instead)

## Prerequisites

Before running this migration:
1. **Users must already exist** in your Tableau Cloud site
2. **Projects must already exist** in your Tableau Cloud site
3. You need a **default content owner** (a Cloud user email) who will own content when the original owner doesn't exist

## Quick Start

### 1. Create your configuration file

```bash
cp config.json.template config.json
```

### 2. Edit config.json with your actual credentials

Open `config.json` and replace the placeholder values:

```json
{
  "source": {
    "server_url": "https://your-actual-tableau-server.com",
    "site_content_url": "",
    "access_token_name": "your-actual-token-name",
    "access_token": "your-actual-token-secret"
  },
  "destination": {
    "pod_url": "https://10ax.online.tableau.com",
    "site_content_url": "your-actual-site-name",
    "access_token_name": "your-cloud-token-name",
    "access_token": "your-cloud-token-secret"
  },
  "default_content_owner": "admin@yourcompany.com",
  "default_email_domain": "@yourcompany.com",
  "migration_scope": {
    "data_sources": true,
    "workbooks": true
  }
}
```

**Important notes:**
- `site_content_url` for source: Use `""` (empty string) for default site, or `"site-name"` for named sites
- `pod_url` for destination: Update the pod (10ax, 10ay, 10az, etc.) based on your Tableau Cloud instance
- `site_content_url` for destination: Your Tableau Cloud site name (NOT the full URL)
- `default_content_owner`: Email of a Cloud user who will own content when the original owner doesn't exist in Cloud
- `default_email_domain`: The email domain appended to Server usernames that aren't already emails (e.g., `"@yourcompany.com"` turns `jsmith` into `jsmith@yourcompany.com`)
- `migration_scope` (optional): Control which content types are migrated. Set `"data_sources": false` or `"workbooks": false` to exclude them. Defaults to both `true` if omitted

### 3. Verify your Tableau Cloud token

Common authentication issues:
- ✅ Token must be created for the correct site
- ✅ Token must have the correct permissions (Site Administrator Exploer or higher)
- ✅ Token name and secret must match exactly
- ✅ Site name must be the content URL (visible in your Tableau Cloud URL)

To verify your Tableau Cloud site name:
1. Log into Tableau Cloud
2. Look at the URL: `https://10ax.online.tableau.com/#/site/YOUR-SITE-NAME/`
3. Use `YOUR-SITE-NAME` as the `site_content_url` in config.json

### 4. Run the migration

```bash
python content_migration.py
```

## Security Notes

- ⚠️ `config.json` contains sensitive credentials and is in `.gitignore`
- ⚠️ Never commit `config.json` to version control
- ✅ Only commit `config.json.template` with placeholder values

## Troubleshooting

### "No such host is known"
- Check that your `server_url` is a valid, accessible URL
- Verify you can access the server from your network

### "The personal access token you provided is invalid"
- Verify the token is created for the correct site
- Check token name and secret are correct
- Ensure the token hasn't expired
- Verify the site name matches your Tableau Cloud site

### "Config file not found"
- Run: `cp config.json.template config.json`
- Make sure you're running the script from the correct directory
