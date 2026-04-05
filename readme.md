# M3U2strm2026

Syncs an IPTV VOD playlist with local media using `.strm` files. Scans local media directories, compares against an M3U playlist, and creates `.strm` files for missing content.

## Features

- Scans local movie and TV directories to avoid duplicates
- Parses M3U playlists, deduplicates entries  
- Filters by keyword ignore lists (configurable per category)
- Creates `.strm` files pointing to IPTV stream URLs
- Cleans up orphaned `.strm` files
- Optional Emby library refresh after updates
- SQLite cache for performance

## Quick Start

1. **Clone and install**
   ```bash
   git clone https://github.com/evilgenx/M3U2strm2026.git
   cd M3U2strm2026
   ```

2. **Configure**
   - Copy `config.json.example` to `config.json`
   - Edit paths, directories, and keywords as needed

3. **Run**
   ```bash
   python main.py
   ```

## Configuration

`config.json` settings:

| Key | Purpose |
|-----|---------|
| `m3u` | Path to VOD.m3u playlist |
| `output_dir` | Where `.strm` files are written |
| `existing_media_dirs` | Directories to scan for local media |
| `ignore_keywords` | Keywords to exclude (movies/tvshows) |
| `emby_api_url` / `emby_api_key` | Optional Emby refresh |
| `dry_run` | Test mode (no files written) |

See `config.json.example` for all options.

## How It Works

1. **Scan local media** – Build cache of existing movies/TV shows
2. **Parse M3U** – Extract titles, URLs, categorize (movie/TV/doc)
3. **Filter** – Exclude entries matching ignore keywords
4. **Compare** – Skip titles already present locally
5. **Create `.strm` files** – Write stream links for missing content
6. **Clean up** – Remove orphaned `.strm` files
7. **Refresh Emby** – If configured, trigger library update

Excluded titles are logged to `excluded_entries.txt`.

## Project Structure

- `main.py` – Orchestrates the pipeline  
- `core.py` – Media scanning, title normalization, cache
- `m3u_utils.py` – M3U parsing, categorization
- `strm_utils.py` – `.strm` file creation and cleanup
- `config.py` – Configuration loader
- `config.json` – User settings

## Output Format

`.strm` files are organized by category:

```
/media/m3u2strm/
├── Movies/
│   └── Movie Title (Year)/
│       └── Movie Title (Year).strm
├── TV Shows/
│   └── Show Name (Year)/
│       └── Season 01/
│           └── Show Name (Year) S01E01.strm
└── Documentaries/
    └── Doc Title (Year)/
        └── Doc Title (Year).strm
```

## Notes

- **TMDB country filtering has been removed** – The `tmdb_api` field is kept for backward compatibility but is unused.
- Keywords in `ignore_keywords` are case-insensitive substring matches.
- The SQLite cache (`caches.db`) tracks processed entries between runs.
- Set `dry_run: true` to test configuration without writing files.