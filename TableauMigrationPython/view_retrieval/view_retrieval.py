"""
View Retrieval using Tableau Server Client (TSC)
Designed to run inside the Alteryx Python Tool.

Outputs a DataFrame with view names and PNG file paths to Alteryx anchor #1.

Setup (run once in the Python Tool before using this script):
    from ayx import Package
    Package.installPackages('tableauserverclient')
"""

import pandas as pd
from pathlib import Path
from ayx import Alteryx

import tableauserverclient as TSC


# -- Credentials ---------------------------------------------------------------
SERVER_URL = "https://your-tableau-server.com"
SITE_CONTENT_URL = ""                # leave empty string for Default site
ACCESS_TOKEN_NAME = "your-token-name"
ACCESS_TOKEN = "your-token-secret"
# -- Target workbook -----------------------------------------------------------
WORKBOOK_NAME = "Your Workbook Name"  # exact name as it appears in Tableau
# -- Output --------------------------------------------------------------------
IMAGE_OUTPUT_DIR = r"C:\path\to\your\output\images"
# ------------------------------------------------------------------------------


def connect() -> TSC.Server:
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=ACCESS_TOKEN_NAME,
        personal_access_token=ACCESS_TOKEN,
        site_id=SITE_CONTENT_URL,
    )
    server = TSC.Server(SERVER_URL, use_server_version=True)
    server.auth.sign_in(tableau_auth)
    return server


def get_workbook(server: TSC.Server) -> TSC.WorkbookItem:
    """Find the target workbook by name. Raises if not found."""
    req = TSC.RequestOptions(pagesize=1000)
    req.filter.add(TSC.Filter(
        TSC.RequestOptions.Field.Name,
        TSC.RequestOptions.Operator.Equals,
        WORKBOOK_NAME
    ))
    workbooks, _ = server.workbooks.get(req)

    if not workbooks:
        raise ValueError(f"Workbook '{WORKBOOK_NAME}' not found on {SERVER_URL}")

    return workbooks[0]


def get_views(server: TSC.Server, workbook: TSC.WorkbookItem) -> list:
    server.workbooks.populate_views(workbook)
    return workbook.views


def download_images(server: TSC.Server, views: list) -> list[dict]:
    """Download a high-res PNG for each view. Returns list of result rows."""
    out = Path(IMAGE_OUTPUT_DIR)
    out.mkdir(parents=True, exist_ok=True)

    image_req = TSC.ImageRequestOptions(
        imageresolution=TSC.ImageRequestOptions.Resolution.High
    )

    rows = []
    for view in views:
        safe_name = "".join(
            c if c.isalnum() or c in (' ', '-') else '_' for c in view.name
        ).strip()
        file_path = out / f"{safe_name}.png"
        try:
            server.views.populate_image(view, image_req)
            file_path.write_bytes(view.image)
            rows.append({
                'view_name': view.name,
                'file_path': str(file_path),
                'status': 'success',
            })
        except Exception as e:
            rows.append({
                'view_name': view.name,
                'file_path': str(file_path),
                'status': f'error: {e}',
            })

    return rows


# -- Main ----------------------------------------------------------------------
server = connect()
try:
    workbook = get_workbook(server)
    views = get_views(server, workbook)
    rows = download_images(server, views)
finally:
    server.auth.sign_out()

df = pd.DataFrame(rows)
Alteryx.write(df, 1)
