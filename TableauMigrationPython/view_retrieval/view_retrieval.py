"""
View Retrieval using Tableau Server Client (TSC)

Usage:
    python view_retrieval.py
    python view_retrieval.py --project "My Project"
    python view_retrieval.py --workbook "My Workbook"
    python view_retrieval.py --output views.csv

Requires:
    pip install tableauserverclient
"""

import csv
import argparse

import tableauserverclient as TSC


# ── Credentials ───────────────────────────────────────────────────────────────
SERVER_URL = "https://your-tableau-server.com"
SITE_CONTENT_URL = ""                # leave empty string for Default site
ACCESS_TOKEN_NAME = "your-token-name"
ACCESS_TOKEN = "your-token-secret"
# ──────────────────────────────────────────────────────────────────────────────


def connect() -> TSC.Server:
    """Sign in to Tableau Server and return an authenticated Server object."""
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=ACCESS_TOKEN_NAME,
        personal_access_token=ACCESS_TOKEN,
        site_id=SITE_CONTENT_URL,
    )
    server = TSC.Server(SERVER_URL, use_server_version=True)
    server.auth.sign_in(tableau_auth)
    return server


def get_all_views(
    server: TSC.Server,
    project_name: str = None,
    workbook_name: str = None,
) -> list[dict]:
    """
    Retrieve views from the server, with optional filters.

    Args:
        server:        Authenticated TSC Server object.
        project_name:  If set, only return views in this project.
        workbook_name: If set, only return views in this workbook.

    Returns:
        List of dicts with view metadata.
    """
    req_options = TSC.RequestOptions(pagesize=1000)
    all_views, _ = server.views.get(req_options)

    results = []
    for view in all_views:
        row = {
            'id': view.id,
            'name': view.name,
            'content_url': view.content_url,
            'workbook_id': view.workbook_id,
            'owner_id': view.owner_id,
            'project_name': view.project_name if hasattr(view, 'project_name') else '',
            'created_at': str(view.created_at) if view.created_at else '',
            'updated_at': str(view.updated_at) if view.updated_at else '',
        }

        if project_name and row['project_name'] != project_name:
            continue

        results.append(row)

    if workbook_name:
        wb_options = TSC.RequestOptions(pagesize=1000)
        all_workbooks, _ = server.workbooks.get(wb_options)
        matching_wb_ids = {
            wb.id for wb in all_workbooks if wb.name == workbook_name
        }
        results = [r for r in results if r['workbook_id'] in matching_wb_ids]

    return results


def print_views(views: list[dict]) -> None:
    if not views:
        print("No views found.")
        return

    print(f"\nFound {len(views)} view(s):\n")
    print(f"{'Name':<40} {'Project':<25} {'Content URL'}")
    print("-" * 90)
    for v in views:
        print(f"{v['name']:<40} {v['project_name']:<25} {v['content_url']}")


def save_to_csv(views: list[dict], output_path: str) -> None:
    if not views:
        print("Nothing to write.")
        return

    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=views[0].keys())
        writer.writeheader()
        writer.writerows(views)

    print(f"Saved {len(views)} view(s) to {output_path}")


def main():
    parser = argparse.ArgumentParser(description='Retrieve views from Tableau Server')
    parser.add_argument('--project', default=None, help='Filter by project name')
    parser.add_argument('--workbook', default=None, help='Filter by workbook name')
    parser.add_argument('--output', default=None, help='Save results to CSV file')
    args = parser.parse_args()

    print(f"Connecting to {SERVER_URL} ...")
    server = connect()
    try:
        views = get_all_views(server, project_name=args.project, workbook_name=args.workbook)
        print_views(views)
        if args.output:
            save_to_csv(views, args.output)
    finally:
        server.auth.sign_out()
        print("\nSigned out.")


if __name__ == '__main__':
    main()
