"""
Generate user_mappings.csv
──────────────────────────
Fetches users from Tableau Server and Tableau Cloud (credentials from
config.json), matches them using several heuristics, and writes
user_mappings.csv ready for content_migration.py / migrate_subscriptions.py.

Matching tiers (highest -> lowest confidence)
  1. exact_email    — server user's email field == cloud email           (1.00)
  2. exact_local    — server username local part == cloud email local part (0.95)
  3. full_name      — fullname fields match exactly (non-empty)          (0.90)
  4. abbreviation   — 'jsmith' is initials+last of 'john.smith'          (0.85)
  5. name_to_email  — fullname 'John Smith' -> expected 'john.smith'      (0.80)
  6. fuzzy          — SequenceMatcher ratio on normalised local parts     (0.60+)
  7. unmatched      — no match found; falls back to default_content_owner

Output columns
  ServerUsername  CloudEmail  MatchType  Confidence  NeedsReview
  ServerFullName  CloudFullName  Notes

NeedsReview = TRUE  when Confidence < 0.85 or MatchType == 'fuzzy'.
The migration scripts only read ServerUsername and CloudEmail; the extra
columns are for review in Excel and are harmlessly ignored at run time.

Usage
─────
  python TableauMigrationPython/generate_user_mappings.py
"""

import csv
import json
import re
import sys
import logging
from difflib import SequenceMatcher
from pathlib import Path

import tableauserverclient as TSC

# ── Paths ──────────────────────────────────────────────────────────────────────
_HERE       = Path(__file__).parent
CONFIG_PATH = _HERE / "config.json"
OUTPUT_PATH = _HERE / "user_mappings.csv"

# ── Logging ────────────────────────────────────────────────────────────────────
logging.basicConfig(level=logging.WARNING, format="%(message)s")
logging.getLogger("tableauserverclient").setLevel(logging.CRITICAL)


# ══════════════════════════════════════════════════════════════════════════════
# String helpers
# ══════════════════════════════════════════════════════════════════════════════

def strip_domain(username: str) -> str:
    """Remove Windows domain prefix.  DOMAIN\\jsmith -> jsmith"""
    return username.split("\\")[-1] if "\\" in username else username


def normalize(s: str) -> str:
    """Lowercase and strip all word-separators for comparison."""
    return re.sub(r"[.\-_ ]", "", s.lower())


def name_parts(s: str) -> list:
    """Split a name / email local part into words."""
    return [p for p in re.split(r"[.\-_ ]", s.lower()) if p]


def is_abbreviation(short: str, parts: list) -> bool:
    """
    Return True when 'short' is the Windows AD abbreviation of 'parts'.

    Pattern: first letter of every part except the last, then the full last part.
      [john, smith] -> j + smith  = jsmith  ok
      [jane, l, bean] -> j + l + bean = jlbean  ok
    """
    if len(parts) < 2:
        return False
    prefix = "".join(p[0] for p in parts[:-1])
    return short.lower() == prefix + parts[-1]


def fullname_to_local_candidates(fullname: str) -> list:
    """
    Generate plausible email local-part patterns from a display name.
      'John Smith' -> ['john.smith', 'jsmith', 'j.smith', 'johnsmith']
    """
    parts = name_parts(fullname)
    if len(parts) < 2:
        return []
    first, last = parts[0], parts[-1]
    return [
        f"{first}.{last}",
        f"{first[0]}{last}",
        f"{first[0]}.{last}",
        f"{first}{last}",
    ]


# ══════════════════════════════════════════════════════════════════════════════
# Tableau REST API helpers
# ══════════════════════════════════════════════════════════════════════════════

def _fetch_users(server_obj: TSC.Server) -> list:
    """Return list of UserItem for every user on the signed-in site."""
    return list(TSC.Pager(server_obj.users))


def get_server_users(src: dict) -> list:
    print("  Connecting to Tableau Server …")
    auth = TSC.PersonalAccessTokenAuth(
        token_name=src["access_token_name"],
        personal_access_token=src["access_token"],
        site_id=src.get("site_content_url", ""),
    )
    srv = TSC.Server(src["server_url"], use_server_version=True)
    with srv.auth.sign_in(auth):
        users = _fetch_users(srv)
    print(f"  Found {len(users)} user(s) on Tableau Server.")
    return users


def get_cloud_users(dest: dict) -> list:
    print("  Connecting to Tableau Cloud …")
    auth = TSC.PersonalAccessTokenAuth(
        token_name=dest["access_token_name"],
        personal_access_token=dest["access_token"],
        site_id=dest["site_content_url"],
    )
    srv = TSC.Server(dest["pod_url"], use_server_version=True)
    with srv.auth.sign_in(auth):
        users = _fetch_users(srv)
    print(f"  Found {len(users)} user(s) on Tableau Cloud.\n")
    return users


# ══════════════════════════════════════════════════════════════════════════════
# Matching
# ══════════════════════════════════════════════════════════════════════════════

def _build_cloud_indexes(cloud_users: list) -> tuple:
    """
    Build lookup structures for fast matching.
    Returns (by_email, by_norm_local, by_norm_fullname).
    """
    by_email        = {}  # lowercase email        -> UserItem
    by_norm_local   = {}  # normalised local part  -> UserItem
    by_norm_fullname = {} # normalised fullname    -> UserItem

    for cu in cloud_users:
        email = (cu.name or "").lower()
        if email:
            by_email[email] = cu
        if "@" in email:
            local_norm = normalize(email.split("@")[0])
            by_norm_local[local_norm] = cu
        fullname = (cu.fullname or "").strip()
        if fullname:
            by_norm_fullname[normalize(fullname)] = cu

    return by_email, by_norm_local, by_norm_fullname


def _match_one(server_user, cloud_users: list,
               by_email: dict, by_norm_local: dict, by_norm_fullname: dict):
    """
    Match a single server user to the best cloud user.
    Returns (cloud_UserItem | None, confidence: float, match_type: str, note: str).
    """
    raw_username = server_user.name or ""
    username     = strip_domain(raw_username)
    fullname     = (server_user.fullname or "").strip()
    srv_email    = (server_user.email    or "").strip().lower()

    # ── Tier 1: server email field exactly matches a cloud email ──────────────
    if srv_email and srv_email in by_email:
        cu = by_email[srv_email]
        return cu, 1.00, "exact_email", f"Server email '{srv_email}' == cloud email"

    # ── Tier 2: username local part == cloud email local part ─────────────────
    local = username.split("@")[0] if "@" in username else username
    local_norm = normalize(local)

    if local_norm and local_norm in by_norm_local:
        cu = by_norm_local[local_norm]
        return cu, 0.95, "exact_local", f"'{local}' matched cloud local '{cu.name.split('@')[0]}'"

    # ── Tier 3: full name exact match ─────────────────────────────────────────
    if fullname:
        fn_norm = normalize(fullname)
        if fn_norm in by_norm_fullname:
            cu = by_norm_fullname[fn_norm]
            return cu, 0.90, "full_name", f"Full name '{fullname}' matched"

    # ── Tier 4: abbreviation match ────────────────────────────────────────────
    for cu in cloud_users:
        email_local = cu.name.split("@")[0] if "@" in (cu.name or "") else (cu.name or "")
        cloud_parts = name_parts(email_local)
        if is_abbreviation(local, cloud_parts):
            return cu, 0.85, "abbreviation", (
                f"'{local}' is abbreviation of '{email_local}'"
            )
        # Reverse: cloud local is abbreviation of server name
        server_parts = name_parts(local)
        if is_abbreviation(email_local, server_parts):
            return cu, 0.85, "abbreviation", (
                f"Cloud '{email_local}' is abbreviation of server '{local}'"
            )

    # ── Tier 5: full name -> expected email pattern ────────────────────────────
    if fullname:
        for candidate in fullname_to_local_candidates(fullname):
            cand_norm = normalize(candidate)
            if cand_norm in by_norm_local:
                cu = by_norm_local[cand_norm]
                return cu, 0.80, "name_to_email", (
                    f"Full name '{fullname}' -> expected '{candidate}' matched cloud"
                )

    # ── Tier 6: fuzzy match ───────────────────────────────────────────────────
    best_cu    = None
    best_score = 0.0

    for cu in cloud_users:
        email_local = cu.name.split("@")[0] if "@" in (cu.name or "") else (cu.name or "")
        score = SequenceMatcher(None, local_norm, normalize(email_local)).ratio()

        # Also try full-name similarity when available
        if cu.fullname and fullname:
            name_score = SequenceMatcher(None, normalize(fullname), normalize(cu.fullname)).ratio()
            score = max(score, name_score * 0.95)

        if score > best_score:
            best_score = score
            best_cu = cu

    FUZZY_THRESHOLD = 0.65
    if best_score >= FUZZY_THRESHOLD and best_cu is not None:
        email_local = best_cu.name.split("@")[0] if "@" in (best_cu.name or "") else ""
        return (
            best_cu,
            round(best_score * 0.85, 2),   # cap fuzzy confidence below 0.85
            "fuzzy",
            f"Best fuzzy score {best_score:.2f} against '{email_local}'",
        )

    return None, 0.00, "unmatched", "No match found — will use default_content_owner"


# ══════════════════════════════════════════════════════════════════════════════
# Main
# ══════════════════════════════════════════════════════════════════════════════

def generate_mappings() -> None:
    print("\n" + "=" * 70)
    print("  GENERATE USER MAPPINGS  |  Tableau Server -> Cloud")
    print("=" * 70 + "\n")

    # ── Load config ───────────────────────────────────────────────────────────
    if not CONFIG_PATH.exists():
        print(f"ERROR: config.json not found at {CONFIG_PATH}")
        print("  Fix: cp config.json.template config.json  then fill in credentials.")
        sys.exit(1)
    cfg  = json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    src  = cfg["source"]
    dest = cfg["destination"]

    # ── Fetch users ───────────────────────────────────────────────────────────
    server_users = get_server_users(src)
    cloud_users  = get_cloud_users(dest)

    if not cloud_users:
        print("ERROR: No users found in Tableau Cloud. Check destination credentials.")
        sys.exit(1)

    # Build cloud lookup indexes
    by_email, by_norm_local, by_norm_fullname = _build_cloud_indexes(cloud_users)

    # ── Match each server user ────────────────────────────────────────────────
    print("Matching users …")
    rows = []
    matched_cloud_emails = set()

    for su in server_users:
        cu, confidence, match_type, note = _match_one(
            su, cloud_users, by_email, by_norm_local, by_norm_fullname
        )

        cloud_email = cu.name if cu else ""
        needs_review = (confidence < 0.85) or (match_type == "fuzzy")

        rows.append({
            "ServerUsername":  su.name or "",
            "CloudEmail":      cloud_email,
            "MatchType":       match_type,
            "Confidence":      f"{confidence:.2f}",
            "NeedsReview":     "TRUE" if needs_review else "FALSE",
            "ServerFullName":  (su.fullname or "").strip(),
            "CloudFullName":   (cu.fullname or "").strip() if cu else "",
            "Notes":           note,
        })

        if cloud_email:
            matched_cloud_emails.add(cloud_email.lower())

    # Sort: high-confidence first, then by confidence desc
    rows.sort(key=lambda r: (r["NeedsReview"] == "TRUE", -float(r["Confidence"])))

    # ── Write CSV ─────────────────────────────────────────────────────────────
    fieldnames = [
        "ServerUsername", "CloudEmail", "MatchType", "Confidence",
        "NeedsReview", "ServerFullName", "CloudFullName", "Notes",
    ]
    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    # ── Summary ───────────────────────────────────────────────────────────────
    counts = {}
    for r in rows:
        counts[r["MatchType"]] = counts.get(r["MatchType"], 0) + 1

    auto_ok    = sum(1 for r in rows if r["NeedsReview"] == "FALSE")
    needs_rev  = sum(1 for r in rows if r["NeedsReview"] == "TRUE")
    unmatched  = counts.get("unmatched", 0)

    unmatched_cloud = [
        cu.name for cu in cloud_users
        if (cu.name or "").lower() not in matched_cloud_emails
    ]

    print(f"\n{'=' * 70}")
    print("  RESULTS")
    print(f"{'=' * 70}")
    print(f"\nServer users:          {len(server_users)}")
    print(f"Cloud users:           {len(cloud_users)}")
    print(f"\nMatched (auto-ok):     {auto_ok}")
    print(f"Needs review:          {needs_rev}")
    print(f"Unmatched:             {unmatched}")
    print(f"\nBy match type:")
    for mtype, n in sorted(counts.items(), key=lambda x: -x[1]):
        print(f"  {mtype:<20} {n}")

    if unmatched_cloud:
        print(f"\nCloud users with no server match ({len(unmatched_cloud)}):")
        for email in unmatched_cloud:
            print(f"  {email}")

    print(f"\nOutput written to: {OUTPUT_PATH}")
    print(
        "\nNext steps:\n"
        "  1. Open user_mappings.csv in Excel\n"
        "  2. Filter NeedsReview = TRUE and review those rows\n"
        "  3. For unmatched rows, fill in CloudEmail manually (or leave blank\n"
        "     to use default_content_owner from config.json)\n"
        "  4. Save and run: python subscriptions/migrate_subscriptions.py"
    )
    print("=" * 70 + "\n")


if __name__ == "__main__":
    generate_mappings()
