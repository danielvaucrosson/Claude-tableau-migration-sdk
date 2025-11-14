"""
List Subscriptions from Tableau Server using REST API
Simple authentication and data retrieval - no SDK complexity.
"""

import requests
import xml.etree.ElementTree as ET

# =============================================================================
# CONFIGURATION
# =============================================================================

SERVER_URL = "https://your-tableau-server.com"
SITE = ""  # Empty for default site
TOKEN_NAME = "your-token-name"
TOKEN = "your-token-secret"


def sign_in():
    """Sign in to Tableau Server and get auth token."""

    print(f"Signing in to {SERVER_URL}...")

    # Build sign-in request
    signin_url = f"{SERVER_URL}/api/3.21/auth/signin"

    payload = f"""
    <tsRequest>
        <credentials personalAccessTokenName="{TOKEN_NAME}" personalAccessTokenSecret="{TOKEN}">
            <site contentUrl="{SITE}" />
        </credentials>
    </tsRequest>
    """

    headers = {
        'Content-Type': 'application/xml',
        'Accept': 'application/xml'
    }

    response = requests.post(signin_url, data=payload, headers=headers)

    if response.status_code != 200:
        print(f"❌ Sign-in failed: {response.status_code}")
        print(response.text)
        return None, None

    # Parse response to get token and site ID
    root = ET.fromstring(response.content)

    # Find credentials element
    credentials = root.find('.//{http://tableau.com/api}credentials')
    if credentials is None:
        print("❌ No credentials in response")
        return None, None

    token = credentials.get('token')
    site_id = credentials.find('.//{http://tableau.com/api}site').get('id')

    print(f"✓ Signed in successfully!")
    print(f"  Site ID: {site_id}")

    return token, site_id


def list_subscriptions(token, site_id):
    """List all subscriptions on the site."""

    print(f"\nFetching subscriptions...")

    subscriptions_url = f"{SERVER_URL}/api/3.21/sites/{site_id}/subscriptions"

    headers = {
        'X-Tableau-Auth': token,
        'Accept': 'application/xml'
    }

    response = requests.get(subscriptions_url, headers=headers)

    if response.status_code != 200:
        print(f"❌ Failed to get subscriptions: {response.status_code}")
        print(response.text)
        return

    # Parse subscriptions
    root = ET.fromstring(response.content)
    subscriptions = root.findall('.//{http://tableau.com/api}subscription')

    print(f"\n{'='*60}")
    print(f"  FOUND {len(subscriptions)} SUBSCRIPTION(S)")
    print(f"{'='*60}\n")

    for i, sub in enumerate(subscriptions, 1):
        sub_id = sub.get('id')
        subject = sub.get('subject', 'N/A')

        # Get user info
        user = sub.find('.//{http://tableau.com/api}user')
        user_name = user.get('name') if user is not None else 'N/A'

        # Get content info
        content = sub.find('.//{http://tableau.com/api}content')
        content_type = content.get('type') if content is not None else 'N/A'

        print(f"{i}. Subscription ID: {sub_id}")
        print(f"   Subject: {subject}")
        print(f"   User: {user_name}")
        print(f"   Content Type: {content_type}")
        print()


def sign_out(token):
    """Sign out from Tableau Server."""

    signout_url = f"{SERVER_URL}/api/3.21/auth/signout"

    headers = {
        'X-Tableau-Auth': token
    }

    requests.post(signout_url, headers=headers)
    print("✓ Signed out")


def main():
    """Main function."""

    print("="*60)
    print("  TABLEAU SERVER - LIST SUBSCRIPTIONS")
    print("="*60)
    print(f"\nServer: {SERVER_URL}")
    print(f"Site: {SITE if SITE else 'Default'}\n")

    # Sign in
    token, site_id = sign_in()

    if not token:
        print("❌ Authentication failed. Exiting.")
        return

    try:
        # List subscriptions
        list_subscriptions(token, site_id)

    finally:
        # Always sign out
        sign_out(token)

    print("\n" + "="*60)
    print("  DONE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
