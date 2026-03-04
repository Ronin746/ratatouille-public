#!/usr/bin/env python3
"""
deploy_to_cloudflare.py — Deploy Archive/ to Cloudflare Pages.

Uses the Cloudflare Pages Direct Upload API (single multipart POST):
  POST /accounts/{accountId}/pages/projects/{projectName}/deployments
  Body: multipart/form-data with branch + manifest + one part per file (named by hash)

Hash format: first 32 hex chars of SHA-256 — the exact format used by Wrangler CLI.
Uses only Python standard library — no pip required.

Credentials are read from env vars CF_ACCOUNT_ID / CF_API_TOKEN (GitHub Actions),
with hardcoded fallback for local use.
"""

import hashlib, json, os, ssl, sys, time
from urllib.request import Request, urlopen
from urllib.error import HTTPError

# macOS Python.org installs often lack system CA certs — use unverified context.
_SSL_CTX = ssl.create_default_context()
_SSL_CTX.check_hostname = False
_SSL_CTX.verify_mode    = ssl.CERT_NONE

# ── Config ────────────────────────────────────────────────────────────────────
ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "5d542bdd57fb4aac9391856b2f41a2a5")
API_TOKEN  = os.environ.get("CF_API_TOKEN",  "YmUMi-5FXmo1VXibR2bOavdvDcLz3_U1mhxaFJaW")
PROJECT    = "ratatouille-screener"
ROOT       = os.path.dirname(os.path.abspath(__file__))
SITE_DIR   = os.path.join(ROOT, "Archive")
CF_BASE    = f"https://api.cloudflare.com/client/v4/accounts/{ACCOUNT_ID}/pages/projects"
SITE_URL   = f"https://{PROJECT}.pages.dev"

MIME = {
    ".html": "text/html; charset=utf-8",
    ".css":  "text/css",
    ".js":   "application/javascript",
    ".json": "application/json",
    ".png":  "image/png",
    ".jpg":  "image/jpeg",
    ".ico":  "image/x-icon",
    ".svg":  "image/svg+xml",
    ".txt":  "text/plain",
}


# ── Helpers ───────────────────────────────────────────────────────────────────
def file_hash(path):
    """First 32 hex chars of SHA-256 — the format Wrangler CLI uses."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()[:32]


def collect_files():
    """Return {'/rel/path': '/abs/path'} for every deployable file."""
    out = {}
    for root, dirs, fnames in os.walk(SITE_DIR):
        dirs[:] = [d for d in dirs if not d.startswith(".")]
        for fname in fnames:
            if fname.startswith(".") or fname.endswith(".zip"):
                continue
            abs_path = os.path.join(root, fname)
            rel_path = "/" + os.path.relpath(abs_path, SITE_DIR)
            out[rel_path] = abs_path
    return out


def build_multipart(files_dict):
    """
    Build multipart/form-data body:
      - field 'branch' = "main"
      - field 'manifest' = JSON {'/path': 'hash32', ...}
      - one field per unique file, named by its hash32, containing raw file bytes
    """
    boundary = ("CFDeploy" + str(int(time.time()))).encode()

    # Compute manifest
    manifest = {}
    for rel, abs_path in files_dict.items():
        manifest[rel] = file_hash(abs_path)

    parts = []

    # branch
    parts.append(
        b"--" + boundary + b"\r\n"
        b'Content-Disposition: form-data; name="branch"\r\n\r\n'
        b"main\r\n"
    )

    # manifest
    parts.append(
        b"--" + boundary + b"\r\n"
        b'Content-Disposition: form-data; name="manifest"\r\n'
        b"Content-Type: application/json\r\n\r\n"
        + json.dumps(manifest).encode() + b"\r\n"
    )

    # one part per unique file, named by hash
    seen = set()
    for rel, h in manifest.items():
        if h in seen:
            continue
        seen.add(h)
        abs_path = files_dict[rel]
        ext = os.path.splitext(abs_path)[1].lower()
        ct  = MIME.get(ext, "application/octet-stream")
        with open(abs_path, "rb") as f:
            content = f.read()
        parts.append(
            b"--" + boundary + b"\r\n"
            + f'Content-Disposition: form-data; name="{h}"\r\n'.encode()
            + f"Content-Type: {ct}\r\n\r\n".encode()
            + content + b"\r\n"
        )

    body      = b"".join(parts) + b"--" + boundary + b"--\r\n"
    ct_header = "multipart/form-data; boundary=" + boundary.decode()
    return body, ct_header, manifest


def cf_get(url):
    req = Request(url, method="GET", headers={"Authorization": f"Bearer {API_TOKEN}"})
    try:
        with urlopen(req, context=_SSL_CTX) as r:
            return json.loads(r.read())
    except HTTPError as e:
        return {"success": False, "_http": e.code, "_body": e.read().decode()}


# ── Main ──────────────────────────────────────────────────────────────────────
def deploy():
    print()
    print("─────────────────────────────────────────────────")
    print("  Cloudflare Pages — Deploy")
    print("─────────────────────────────────────────────────")

    # Check project exists
    res = cf_get(f"{CF_BASE}/{PROJECT}")
    if not res.get("success"):
        if res.get("_http") == 404:
            print(f"  ✗  Project '{PROJECT}' not found — create it first in the Cloudflare dashboard.")
        else:
            print(f"  ✗  Error: {res}")
        sys.exit(1)
    print(f"  ✓  Project '{PROJECT}' ready")

    # Collect files
    files = collect_files()
    if not files:
        print("  ✗  No files found in Archive/")
        sys.exit(1)
    print(f"  Computing hashes for {len(files)} files...")

    # Build multipart body
    body, ct_header, manifest = build_multipart(files)
    total_mb = len(body) / 1024 / 1024
    print(f"  Uploading {len(files)} files ({total_mb:.1f} MB)...")

    # POST deployment
    req = Request(
        f"{CF_BASE}/{PROJECT}/deployments",
        data=body,
        method="POST",
        headers={
            "Authorization": f"Bearer {API_TOKEN}",
            "Content-Type":  ct_header,
        },
    )

    try:
        with urlopen(req, context=_SSL_CTX) as r:
            result = json.loads(r.read())
    except HTTPError as e:
        err = e.read().decode()
        print(f"  ✗  Deploy failed (HTTP {e.code}):")
        print(f"     {err[:500]}")
        sys.exit(1)

    if result.get("success"):
        dep  = result["result"]
        live = dep.get("url") or SITE_URL
        print()
        print("─────────────────────────────────────────────────")
        print(f"  ✅  Deployed!")
        print(f"  🌐  {live}")
        print("─────────────────────────────────────────────────")
        print(f"__CF_URL__={live}")
        return live
    else:
        print(f"  ✗  Deploy failed: {json.dumps(result, indent=2)}")
        sys.exit(1)


if __name__ == "__main__":
    deploy()
