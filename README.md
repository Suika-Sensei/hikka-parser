# Hikka Parser

A Go tool that parses anime data from [Hikka](https://hikka.io) API and saves it to JSON and SQLite database.

## Features

- Fetches anime metadata and watch links from Hikka API
- Concurrent requests (20 pages + 50 watch requests simultaneously)
- Auto-retry on errors and rate limiting
- Progress saving for resumable parsing
- Outputs both JSON and SQLite formats

## Usage

```bash
go run hikka-parser.go
```

Or build and run:

```bash
go build -o hikka-parser hikka-parser.go
./hikka-parser
```

## Output

All files are saved to `output/` directory:

| File | Description |
|------|-------------|
| `hikka-full.json` | Full JSON with all anime and watch data |
| `hikka.db` | SQLite database with normalized tables |

## Database Schema

**`anime`** - Main anime information (slug, titles, score, status, etc.)

**`watch`** - Episode links (slug, source, team, episode, video_url)

Tables are linked by `slug` field.

## Requirements

- Go 1.21+
- `github.com/mattn/go-sqlite3`

Install dependencies:

```bash
go mod tidy
```
