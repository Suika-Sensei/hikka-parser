package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	HikkaAPI          = "https://api.hikka.io/anime"
	WatchAPI          = "https://api.hikka-features.pp.ua/watch"
	PageSize          = 100
	ConcurrentPages   = 20
	ConcurrentWatch   = 50
	RequestTimeout    = 5 * time.Second
	DelayBetweenBatch = 10 * time.Millisecond
)

var (
	outputDir     = "output"
	outputPath    = filepath.Join(outputDir, "hikka-full.json")
	progressPath  = filepath.Join(outputDir, "parser-progress.json")
	animeDBPath   = filepath.Join(outputDir, "anime.db")
	watchDBPath   = filepath.Join(outputDir, "watch.db")
	httpClient    = &http.Client{Timeout: RequestTimeout}
)

type Pagination struct {
	Pages int `json:"pages"`
	Total int `json:"total"`
}

type AnimePageResponse struct {
	Pagination Pagination       `json:"pagination"`
	List       []map[string]any `json:"list"`
}

type Progress struct {
	CurrentPage int              `json:"currentPage"`
	TotalPages  int              `json:"totalPages"`
	AnimeList   []map[string]any `json:"animeList"`
}

type Output struct {
	UpdatedAt string           `json:"updated_at"`
	Total     int              `json:"total"`
	List      []map[string]any `json:"list"`
}

type Episode struct {
	Episode  int    `json:"episode"`
	VideoURL string `json:"video_url"`
}

// ==================== HTTP Functions ====================

func fetchAnimePage(page int) (*AnimePageResponse, error) {
	url := fmt.Sprintf("%s?page=%d&size=%d", HikkaAPI, page, PageSize)

	req, _ := http.NewRequest("POST", url, bytes.NewBuffer([]byte("{}")))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Content-Type", "application/json")

	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := httpClient.Do(req)
		if err != nil {
			if attempt == 3 {
				return nil, err
			}
			time.Sleep(time.Duration(attempt*500) * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			time.Sleep(5 * time.Second)
			continue
		}

		if resp.StatusCode != 200 {
			if attempt == 3 {
				return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
			}
			time.Sleep(time.Duration(attempt*500) * time.Millisecond)
			continue
		}

		body, _ := io.ReadAll(resp.Body)
		var result AnimePageResponse
		if err := json.Unmarshal(body, &result); err != nil {
			return nil, err
		}
		return &result, nil
	}
	return nil, fmt.Errorf("max retries exceeded")
}

func fetchWatch(slug string) map[string]any {
	url := fmt.Sprintf("%s/%s", WatchAPI, slug)

	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("Accept", "application/json")

	for attempt := 1; attempt <= 3; attempt++ {
		resp, err := httpClient.Do(req)
		if err != nil {
			if attempt == 3 {
				return nil
			}
			time.Sleep(time.Duration(attempt*300) * time.Millisecond)
			continue
		}
		defer resp.Body.Close()

		if resp.StatusCode == 429 {
			time.Sleep(3 * time.Second)
			continue
		}

		if resp.StatusCode != 200 {
			return nil
		}

		body, _ := io.ReadAll(resp.Body)
		var result map[string]any
		if err := json.Unmarshal(body, &result); err != nil {
			return nil
		}
		return result
	}
	return nil
}

// ==================== Progress Functions ====================

func loadProgress() *Progress {
	data, err := os.ReadFile(progressPath)
	if err != nil {
		return nil
	}
	var progress Progress
	if err := json.Unmarshal(data, &progress); err != nil {
		return nil
	}
	return &progress
}

func saveProgress(progress *Progress) {
	data, _ := json.Marshal(progress)
	os.WriteFile(progressPath, data, 0644)
}

func saveJSON(animeList []map[string]any) {
	output := Output{
		UpdatedAt: time.Now().UTC().Format(time.RFC3339),
		Total:     len(animeList),
		List:      animeList,
	}
	data, _ := json.MarshalIndent(output, "", "  ")
	os.WriteFile(outputPath, data, 0644)
}

// ==================== Batch Processing ====================

func processPageBatch(pages []int, watchSem chan struct{}) []map[string]any {
	var wg sync.WaitGroup
	var mu sync.Mutex
	allAnimes := make([]map[string]any, 0)

	pageChan := make(chan *AnimePageResponse, len(pages))

	for _, page := range pages {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			data, err := fetchAnimePage(p)
			if err != nil {
				fmt.Printf("  [ERROR] Page %d: %v\n", p, err)
				return
			}
			pageChan <- data
		}(page)
	}

	go func() {
		wg.Wait()
		close(pageChan)
	}()

	for pageData := range pageChan {
		if pageData != nil {
			allAnimes = append(allAnimes, pageData.List...)
		}
	}

	var watchWg sync.WaitGroup
	var completed int64
	total := len(allAnimes)
	results := make([]map[string]any, len(allAnimes))

	for i, anime := range allAnimes {
		watchWg.Add(1)
		go func(idx int, a map[string]any) {
			defer watchWg.Done()

			watchSem <- struct{}{}
			defer func() { <-watchSem }()

			slug, _ := a["slug"].(string)
			watchData := fetchWatch(slug)

			if watchData == nil {
				watchData = make(map[string]any)
			}
			a["watch"] = watchData

			mu.Lock()
			results[idx] = a
			mu.Unlock()

			c := atomic.AddInt64(&completed, 1)
			if c%100 == 0 || int(c) == total {
				fmt.Printf("\r  Watch: %d/%d", c, total)
			}
		}(i, anime)
	}

	watchWg.Wait()
	fmt.Println()

	filtered := make([]map[string]any, 0, len(results))
	for _, r := range results {
		if r != nil {
			filtered = append(filtered, r)
		}
	}

	return filtered
}

// ==================== Database Functions ====================

func createDatabases(animeList []map[string]any, skipWatch bool) error {
	fmt.Println("\n[DB] Creating SQLite databases...")

	os.Remove(animeDBPath)
	os.Remove(watchDBPath)

	// Create anime.db
	animeDB, err := sql.Open("sqlite3", animeDBPath)
	if err != nil {
		return fmt.Errorf("error opening anime database: %v", err)
	}
	defer animeDB.Close()

	_, err = animeDB.Exec(`
		CREATE TABLE IF NOT EXISTS anime (
			slug TEXT PRIMARY KEY,
			data_type TEXT,
			title_en TEXT,
			title_ja TEXT,
			title_ua TEXT,
			image TEXT,
			media_type TEXT,
			source TEXT,
			status TEXT,
			rating TEXT,
			season TEXT,
			episodes_released INTEGER,
			episodes_total INTEGER,
			start_date INTEGER,
			end_date INTEGER,
			score REAL,
			scored_by INTEGER,
			native_score REAL,
			native_scored_by INTEGER,
			translated_ua BOOLEAN
		)
	`)
	if err != nil {
		return fmt.Errorf("error creating anime table: %v", err)
	}

	// Create watch.db
	watchDB, err := sql.Open("sqlite3", watchDBPath)
	if err != nil {
		return fmt.Errorf("error opening watch database: %v", err)
	}
	defer watchDB.Close()

	var createWatchSQL string
	if skipWatch {
		createWatchSQL = `
			CREATE TABLE IF NOT EXISTS watch (
				slug TEXT NOT NULL,
				team TEXT NOT NULL,
				PRIMARY KEY (slug, team)
			)
		`
	} else {
		createWatchSQL = `
			CREATE TABLE IF NOT EXISTS watch (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				slug TEXT NOT NULL,
				source TEXT NOT NULL,
				team TEXT NOT NULL,
				episode INTEGER NOT NULL,
				video_url TEXT NOT NULL
			)
		`
	}

	_, err = watchDB.Exec(createWatchSQL)
	if err != nil {
		return fmt.Errorf("error creating watch table: %v", err)
	}

	// Create indexes for watch.db
	if skipWatch {
		watchDB.Exec("CREATE INDEX idx_watch_slug ON watch(slug)")
		watchDB.Exec("CREATE INDEX idx_watch_team ON watch(team)")
	} else {
		watchDB.Exec("CREATE INDEX idx_watch_slug ON watch(slug)")
		watchDB.Exec("CREATE INDEX idx_watch_source ON watch(source)")
		watchDB.Exec("CREATE INDEX idx_watch_team ON watch(team)")
		watchDB.Exec("CREATE INDEX idx_watch_episode ON watch(episode)")
	}

	// Begin transactions
	animeTx, err := animeDB.Begin()
	if err != nil {
		return fmt.Errorf("error starting anime transaction: %v", err)
	}

	watchTx, err := watchDB.Begin()
	if err != nil {
		return fmt.Errorf("error starting watch transaction: %v", err)
	}

	var watchStmt *sql.Stmt
	if skipWatch {
		watchStmt, err = watchTx.Prepare(`
			INSERT OR IGNORE INTO watch (slug, team)
			VALUES (?, ?)
		`)
	} else {
		watchStmt, err = watchTx.Prepare(`
			INSERT INTO watch (slug, source, team, episode, video_url)
			VALUES (?, ?, ?, ?, ?)
		`)
	}
	if err != nil {
		return fmt.Errorf("error preparing watch statement: %v", err)
	}
	defer watchStmt.Close()

	animeStmt, err := animeTx.Prepare(`
		INSERT OR REPLACE INTO anime (
			slug, data_type, title_en, title_ja, title_ua, image, media_type,
			source, status, rating, season, episodes_released, episodes_total,
			start_date, end_date, score, scored_by, native_score, native_scored_by,
			translated_ua
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("error preparing anime statement: %v", err)
	}
	defer animeStmt.Close()

	animeCount := 0
	watchCount := 0

	for _, item := range animeList {
		slug, _ := item["slug"].(string)
		if slug == "" {
			continue
		}

		_, err = animeStmt.Exec(
			slug,
			item["data_type"],
			item["title_en"],
			item["title_ja"],
			item["title_ua"],
			item["image"],
			item["media_type"],
			item["source"],
			item["status"],
			item["rating"],
			item["season"],
			item["episodes_released"],
			item["episodes_total"],
			item["start_date"],
			item["end_date"],
			item["score"],
			item["scored_by"],
			item["native_score"],
			item["native_scored_by"],
			item["translated_ua"],
		)
		if err != nil {
			fmt.Printf("  [WARN] anime %s: %v\n", slug, err)
			continue
		}
		animeCount++

		// Process watch data
		watchData, ok := item["watch"].(map[string]any)
		if !ok {
			continue
		}

		for sourceName, teamsRaw := range watchData {
			teamsMap, ok := teamsRaw.(map[string]any)
			if !ok {
				continue
			}

			for teamName, episodesRaw := range teamsMap {
				episodes, ok := episodesRaw.([]any)
				if !ok {
					continue
				}

				if skipWatch {
					_, err = watchStmt.Exec(slug, teamName)
					if err == nil {
						watchCount++
					}
				} else {
					for _, epRaw := range episodes {
						ep, ok := epRaw.(map[string]any)
						if !ok {
							continue
						}

						episode, _ := ep["episode"].(float64)
						videoURL, _ := ep["video_url"].(string)
						_, err = watchStmt.Exec(
							slug,
							sourceName,
							teamName,
							int(episode),
							videoURL,
						)
						if err != nil {
							continue
						}
						watchCount++
					}
				}
			}
		}
	}

	if err := animeTx.Commit(); err != nil {
		return fmt.Errorf("error committing anime transaction: %v", err)
	}

	fmt.Printf("[DB] Created: %s\n", animeDBPath)
	fmt.Printf("[DB] Anime records: %d\n", animeCount)

	if err := watchTx.Commit(); err != nil {
		return fmt.Errorf("error committing watch transaction: %v", err)
	}
	fmt.Printf("[DB] Created: %s\n", watchDBPath)
	fmt.Printf("[DB] Watch records: %d\n", watchCount)

	return nil
}

// ==================== Main ====================

func loadJSON() ([]map[string]any, error) {
	data, err := os.ReadFile(outputPath)
	if err != nil {
		return nil, err
	}
	var output Output
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, err
	}
	return output.List, nil
}

func printStats(animeList []map[string]any) {
	withWatch, withTeams := 0, 0
	providers := make(map[string]int)

	for _, anime := range animeList {
		watch, ok := anime["watch"].(map[string]any)
		if !ok || len(watch) == 0 {
			continue
		}

		hasProvider := false
		for key, val := range watch {
			if key == "type" {
				continue
			}
			providers[key]++
			hasProvider = true

			if prov, ok := val.(map[string]any); ok {
				if _, hasTeams := prov["teams"]; hasTeams {
					withTeams++
				}
			}
		}
		if hasProvider {
			withWatch++
		}
	}

	fmt.Printf("\nTotal: %d anime\n", len(animeList))
	fmt.Printf("With watch: %d\n", withWatch)
	fmt.Printf("With teams: %d\n", withTeams)
	fmt.Printf("Providers: %v\n", providers)
}

func main() {
	// Command line flags
	skipParse := flag.Bool("skip-parse", false, "Skip parsing, use existing hikka-full.json")
	skipDB := flag.Bool("skip-db", false, "Skip database creation")
	skipWatch := flag.Bool("skip-watch", false, "Skip watch data (video URLs)")
	dbOnly := flag.Bool("db-only", false, "Only create databases from existing hikka-full.json")
	jsonOnly := flag.Bool("json-only", false, "Only parse, skip database creation")
	flag.Parse()

	// Handle conflicting flags
	if *dbOnly {
		*skipParse = true
		*skipDB = false
	}
	if *jsonOnly {
		*skipDB = true
	}

	// Create output directory
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		fmt.Printf("[FATAL] Cannot create output directory: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("==================================================")
	fmt.Println("  HIKKA PARSER + DATABASE")
	fmt.Println("==================================================")
	fmt.Printf("  Output directory: %s\n", outputDir)
	if !*skipParse {
		fmt.Printf("  Concurrent pages: %d\n", ConcurrentPages)
		fmt.Printf("  Concurrent watch: %d\n", ConcurrentWatch)
	}
	fmt.Println()

	var animeList []map[string]any

	if *skipParse {
		// Load from existing JSON
		fmt.Printf("Loading from %s...\n", outputPath)
		var err error
		animeList, err = loadJSON()
		if err != nil {
			fmt.Printf("[FATAL] Cannot load JSON: %v\n", err)
			os.Exit(1)
		}
		fmt.Printf("Loaded %d anime\n", len(animeList))
		printStats(animeList)
	} else {
		// Parse from API
		startPage := 1
		totalPages := 0

		// Check progress
		if progress := loadProgress(); progress != nil {
			fmt.Printf("Found progress: page %d/%d, %d anime\n", progress.CurrentPage, progress.TotalPages, len(progress.AnimeList))
			fmt.Print("Continue? (y/n): ")
			var answer string
			fmt.Scanln(&answer)
			if answer == "y" || answer == "Y" {
				animeList = progress.AnimeList
				startPage = progress.CurrentPage
				totalPages = progress.TotalPages
				fmt.Println("Continuing...\n")
			} else {
				fmt.Println("Starting fresh...\n")
			}
		}

		// Get total pages
		if totalPages == 0 {
			fmt.Println("Getting info...")
			firstPage, err := fetchAnimePage(1)
			if err != nil {
				fmt.Printf("[FATAL] %v\n", err)
				os.Exit(1)
			}
			totalPages = firstPage.Pagination.Pages
			fmt.Printf("Total: %d pages, %d anime\n\n", totalPages, firstPage.Pagination.Total)
		}

		watchSem := make(chan struct{}, ConcurrentWatch)
		startTime := time.Now()

		// Process batches
		for batchStart := startPage; batchStart <= totalPages; batchStart += ConcurrentPages {
			batchEnd := batchStart + ConcurrentPages - 1
			if batchEnd > totalPages {
				batchEnd = totalPages
			}

			pages := make([]int, 0, batchEnd-batchStart+1)
			for p := batchStart; p <= batchEnd; p++ {
				pages = append(pages, p)
			}

			elapsed := time.Since(startTime).Seconds()
			fmt.Printf("\n[%.1fs] Pages %d-%d / %d\n", elapsed, batchStart, batchEnd, totalPages)

			batchResults := processPageBatch(pages, watchSem)
			animeList = append(animeList, batchResults...)

			saveProgress(&Progress{
				CurrentPage: batchEnd + 1,
				TotalPages:  totalPages,
				AnimeList:   animeList,
			})
			fmt.Printf("  Saved: %d anime\n", len(animeList))

			if batchEnd < totalPages {
				time.Sleep(DelayBetweenBatch)
			}
		}

		// Save JSON result
		totalTime := time.Since(startTime).Seconds()
		fmt.Println("\n==================================================")
		fmt.Printf("PARSING DONE in %.1fs\n", totalTime)
		fmt.Println("==================================================")

		saveJSON(animeList)
		os.Remove(progressPath)
		printStats(animeList)
		fmt.Printf("\nJSON: %s\n", outputPath)
	}

	// Create databases
	if !*skipDB {
		fmt.Println("\n==================================================")
		fmt.Println("CREATING DATABASES")
		fmt.Println("==================================================")

		if err := createDatabases(animeList, *skipWatch); err != nil {
			fmt.Printf("[ERROR] Database creation failed: %v\n", err)
			os.Exit(1)
		}
	}

	fmt.Println("\n==================================================")
	fmt.Println("ALL DONE!")
	fmt.Println("==================================================")
	if !*skipParse {
		fmt.Printf("JSON file:  %s\n", outputPath)
	}
	if !*skipDB {
		fmt.Printf("Anime DB:   %s\n", animeDBPath)
		fmt.Printf("Watch DB:   %s\n", watchDBPath)
	}
}
