# M3U2 VOD Script Overview

This script keeps your **VOD library organized and current** by syncing an IPTV playlist (`VOD.m3u`) with your local media and `.strm` files.

---

## How It Works

1. **Scan Local Media**
   - Searches your existing movie and TV folders.
   - Builds a cache of everything you already have locally.

2. **Read the VOD Playlist**
   - Parses your IPTV `.m3u` file.
   - Lists all available titles and their stream URLs.

3. **Filter by Keywords** (TMDB country filtering removed)
   - Only excludes titles that match configured ignore keywords.
   - All other titles are allowed regardless of country of origin.
   - Excluded titles are logged to `excluded_entries.txt` for review.

4. **Compare and Sync**
   - If a title already exists locally, it's skipped.
   - If missing, the script creates a `.strm` file pointing to the IPTV stream.
   - Maintains a SQLite cache to track what's new, changed, or skipped.

5. **Clean Up**
   - Removes `.strm` files that are no longer valid or missing from the playlist.
   - Cleans up empty folders or NFO-only directories.

6. **Optional: Refresh Emby**
   - After updates, the script can automatically trigger an Emby library refresh.

---

## Result

- Only **new or missing** titles are added as `.strm` links.
- **Keyword-ignored** content is excluded (based on configuration).
- **Local media** remains untouched.
- **Emby** (if configured) updates automatically.

---

## Files and Roles

| File | Purpose |
|------|----------|
| `main.py` | Orchestrates the full process |
| `core.py` | Scans local media, normalizes titles, manages cache |
| `m3u_utils.py` | Parses `.m3u`, applies TMDb filters |
| `strm_utils.py` | Writes and cleans `.strm` files |
| `config.py` / `config.json` | Settings, paths, API keys, and filters |

---

## Example Output Structure
/media/m3u2strm/
│
├── Movies/
│ ├── Heat (1995)/Heat (1995).strm
│ └── Inception (2010)/Inception (2010).strm
│
└── TV Shows/
├── Breaking Bad (2008)/Season 01/Breaking Bad (2008) S01E01.strm
└── The Office (2005)/Season 02/The Office (2005) S02E03.strm