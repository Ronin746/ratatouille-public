#!/usr/bin/env python3
"""
deploy_to_cloudflare.py — Deploy Archive/ to Cloudflare Pages.

Uses curl subprocess for reliable multipart/form-data uploads to:
  POST /accounts/{accountId}/pages/projects/{projectName}/deployments

Hash format: first 32 hex chars of SHA-256 — the exact format used by Wrangler CLI.
Uses only Python standard library + system curl — no pip required.

Credentials are read from env vars CF_ACCOUNT_ID / CF_API_TOKEN (GitHub Actions),
with hardcoded fallback for local use.
"""

import hashlib, json, os, subprocess, sys

# ── Config ────────────────────────────────────────────────────────────────────
ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "5d542bdd57fb4aac9391856b2f41a2a5")
API_TOKEN  = os.environ.get("CF_API_TOKEN",  "YmUMi-5FXmo1VXibR2bOavdvDcLz3_U1mhxaFJaW")
PROJECT    = "ratatouille-screener"
ROOT       = os.path.dirname(os.path.abspath(__file__))
SITE_DIR   = os.path.join(ROOT, "Archive")
CF_BASE    = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/pages/projects"
SITE_URL   = f"https://{PROJECT}.pages.dev"


# ── Helpers ───────────────────────────────────────────────────────────────────
def file_hash(path):
    """First 32 hex chars of SHA-256 — the format Wrangler CLI uses."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:32]


def collect_files():
    """
    Return {'rel/path': '/abs/path'} for every deployable file.
    Keys do NOT have a leading slash (Cloudflare API requirement).
    """
    out = {}
    for root, dirs, fnames in os.walk(SITE_DIR):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in fnames:
            if fname.startswith(".") or fname.endswith(".zip"):
                continue
            abs_path = os.path.join(root, fname)
            rel_path = os.path.relpath(abs_path, SITE_DIR).replace(os.sep, "/")
            out[rel_path] = abs_path
    return out


def cf_get(url):
    """GET request via curl, returns parsed JSON."""
    result = subprocess.run(
        ["curl", "-s", "-X", "GET",
         "-H", f"Authorization: Bearer {API_TOKEN}",
         url],
        capture_output=True, text=True
    )
    try:
        return json.loads(result.stdout)
    except json.JSONDecodeError:
        return {"success": False, "_raw": result.stdout, "_err": result.stderr}


# ── Main ──────────────────────────────────────────────────────────────────────
def deploy():
    print()
    print("─────────────────────────────────────────────────")
    print("  Cloudflare Pages — Deploy")
    print("─────────────────────────────────────────────────")

    # Check project exists
    res = cf_get(f"{CF_BASE}/{PROJECT}")
    if not res.get("success"):
        if "404" in str(res):
            print(f"  ✗  Project '{PROJECT}' not found — create it first in the Cloudflare dashboard.")
        else:
            print(f"  ✗  Error: {json.dumps(res, indent=2)[:500]}")
        sys.exit(1)
    print(f"  ✓  Project '{PROJECT}' ready")

    # Collect files
    files = collect_files()
    if not files:
        print("  ✗  No files found in Archive/")
        sys.exit(1)
    print(f"  Computing hashes for {len(files)} files...")

    # Build manifest + compute total size
    manifest = {}
    total_bytes = 0
    for rel, abs_path in files.items():
        manifest[rel] = file_hash(abs_path)
        total_bytes += os.path.getsize(abs_path)

    total_mb = total_bytes / 1024 / 1024
    print(f"  Uploading {len(files)} files ({total_mb:.1f} MB)...")

    # Build curl command with -F flags (exactly like Cloudflare docs)
    deploy_url = f"{CF_BASE}/{PROJECT}/deployments"
    cmd = [
        "curl", "-s", "-X", "POST",
        "-H", f"Authorization: Bearer {API_TOKEN}",
        "-F", "branch=main",
        "-F", f"manifest={json.dumps(manifest)}",
    ]

    # Add each unique file as a form field named by its hash
    seen_hashes = set()
    for rel, h in manifest.items():
        if h in seen_hashes:
            continue
        seen_hashes.add(h)
        abs_path = files[rel]
        # Use @file syntax for binary upload
        cmd.extend(["-F", f"{h}=@{abs_path}"])

    cmd.append(deploy_url)

    # Execute curl
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        print(f"  ✗  curl failed (exit {result.returncode}):")
        print(f"     {result.stderr[:500]}")
        sys.exit(1)

    try:
        response = json.loads(result.stdout)
    except json.JSONDecodeError:
        print(f"  ✗  Invalid JSON response:")
        print(f"     {result.stdout[:500]}")
        sys.exit(1)

    if response.get("success"):
        dep  = response["result"]
        live = dep.get("url") or SITE_URL
        print()
        print("─────────────────────────────────────────────────")
        print(f"  ✅  Deployed!")
        print(f"  🌐  {live}")
        print("─────────────────────────────────────────────────")
        print(f"__CF_URL__={live}")
        return live
    else:
        print(f"  ✗  Deploy failed:")
        print(f"     {json.dumps(response, indent=2)}")
        sys.exit(1)


if __name__ == "__main__":
    deploy()
