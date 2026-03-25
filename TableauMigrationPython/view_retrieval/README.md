# View Retrieval

Retrieves views from Tableau Server using the [Tableau Server Client (TSC)](https://tableau.github.io/server-client-python/) library.

## Prerequisites

```bash
pip install tableauserverclient
```

## Setup

Open `view_retrieval.py` and fill in your credentials at the top of the file:

```python
SERVER_URL = "https://your-tableau-server.com"
SITE_CONTENT_URL = ""                # leave empty string for Default site
ACCESS_TOKEN_NAME = "your-token-name"
ACCESS_TOKEN = "your-token-secret"
```

## Usage

```bash
# List all views
python view_retrieval.py

# Filter by project
python view_retrieval.py --project "Finance"

# Filter by workbook
python view_retrieval.py --workbook "Sales Dashboard"

# Export to CSV
python view_retrieval.py --output views.csv
```

## Output

Prints a table of matching views to stdout:

```
Found 42 view(s):

Name                                     Project                   Content URL
------------------------------------------------------------------------------------------
Sales Overview                           Finance                   views/SalesDash/Overview
```

With `--output`, saves a CSV with columns:
`id, name, content_url, workbook_id, owner_id, project_name, created_at, updated_at`
