// benchmark-charts reads JSON benchmark results and generates SVG comparison
// charts plus a markdown comparison report.
//
// Usage:
//
//	go run ./cmd/benchmark-charts -results-dir bench/results/20260328-231500 -output-dir bench/results/20260328-231500
package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

var (
	resultsDir = flag.String("results-dir", ".", "Directory containing JSON result files")
	outputDir  = flag.String("output-dir", ".", "Directory for SVG and markdown output")
)

type benchResult struct {
	Name    string  `json:"name"`
	Iters   int     `json:"iters"`
	Errors  int     `json:"errors"`
	MeanUs  float64 `json:"mean_us"`
	P50Us   float64 `json:"p50_us"`
	P95Us   float64 `json:"p95_us"`
	P99Us   float64 `json:"p99_us"`
	QPS     float64 `json:"qps"`
	AvgRows float64 `json:"avg_rows"`
}

type report struct {
	Target   string        `json:"target"`
	Endpoint string        `json:"endpoint"`
	Nodes    int           `json:"nodes"`
	Edges    int           `json:"edges"`
	Date     string        `json:"date"`
	Results  []benchResult `json:"results"`
}

type resourceStats struct {
	PeakRSSMiB float64 `json:"peak_rss_mib"`
	AvgCPUPct  float64 `json:"avg_cpu_pct"`
	Samples    int     `json:"samples"`
}

func loadReport(path string) (*report, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var r report
	return &r, json.Unmarshal(data, &r)
}

func loadStats(path string) (*resourceStats, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s resourceStats
	return &s, json.Unmarshal(data, &s)
}

// findBench finds a benchmark result by name.
func findBench(results []benchResult, name string) *benchResult {
	for i := range results {
		if results[i].Name == name {
			return &results[i]
		}
	}
	return nil
}

// fmtUs formats microseconds into a human-readable duration.
func fmtUs(us float64) string {
	if us < 1000 {
		return fmt.Sprintf("%.0fus", us)
	}
	if us < 1_000_000 {
		return fmt.Sprintf("%.1fms", us/1000)
	}
	return fmt.Sprintf("%.2fs", us/1_000_000)
}

// generateBarChartSVG creates a horizontal bar chart SVG comparing values across configs.
func generateBarChartSVG(title, unit string, labels []string, values []float64, colors []string) string {
	if len(labels) == 0 {
		return ""
	}

	barHeight := 40
	barGap := 15
	labelWidth := 200
	chartWidth := 600
	rightPad := 80
	topPad := 50
	bottomPad := 20

	totalHeight := topPad + len(labels)*(barHeight+barGap) + bottomPad
	totalWidth := labelWidth + chartWidth + rightPad

	maxVal := 0.0
	for _, v := range values {
		if v > maxVal {
			maxVal = v
		}
	}
	if maxVal == 0 {
		maxVal = 1
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf(`<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" font-family="system-ui,-apple-system,sans-serif">`, totalWidth, totalHeight))
	sb.WriteString("\n")

	// Background.
	sb.WriteString(fmt.Sprintf(`  <rect width="%d" height="%d" fill="#ffffff"/>`, totalWidth, totalHeight))
	sb.WriteString("\n")

	// Title.
	sb.WriteString(fmt.Sprintf(`  <text x="%d" y="30" font-size="16" font-weight="bold" fill="#1a1a2e">%s</text>`, labelWidth, title))
	sb.WriteString("\n")

	// Bars.
	for i, label := range labels {
		y := topPad + i*(barHeight+barGap)
		barW := int(math.Round(float64(chartWidth) * values[i] / maxVal))
		if barW < 2 {
			barW = 2
		}
		color := "#4361ee"
		if i < len(colors) {
			color = colors[i]
		}

		// Label.
		sb.WriteString(fmt.Sprintf(`  <text x="%d" y="%d" font-size="13" fill="#333" text-anchor="end" dominant-baseline="middle">%s</text>`,
			labelWidth-10, y+barHeight/2, label))
		sb.WriteString("\n")

		// Bar.
		sb.WriteString(fmt.Sprintf(`  <rect x="%d" y="%d" width="%d" height="%d" rx="4" fill="%s"/>`,
			labelWidth, y, barW, barHeight, color))
		sb.WriteString("\n")

		// Value label.
		var valStr string
		if unit == "us" {
			valStr = fmtUs(values[i])
		} else if unit == "MiB" {
			valStr = fmt.Sprintf("%.0f MiB", values[i])
		} else {
			valStr = fmt.Sprintf("%.0f %s", values[i], unit)
		}
		sb.WriteString(fmt.Sprintf(`  <text x="%d" y="%d" font-size="12" fill="#666" dominant-baseline="middle">%s</text>`,
			labelWidth+barW+8, y+barHeight/2, valStr))
		sb.WriteString("\n")
	}

	sb.WriteString("</svg>\n")
	return sb.String()
}

func main() {
	flag.Parse()

	// Load available result files.
	configs := map[string]*report{}
	stats := map[string]*resourceStats{}

	patterns := map[string]string{
		"loveliness-single":  "loveliness-single.json",
		"loveliness-cluster": "loveliness-cluster.json",
		"neo4j":              "neo4j.json",
	}
	for name, file := range patterns {
		path := filepath.Join(*resultsDir, file)
		r, err := loadReport(path)
		if err != nil {
			fmt.Fprintf(os.Stderr, "skip %s: %v\n", name, err)
			continue
		}
		configs[name] = r
	}

	// Load resource stats.
	statsPatterns := map[string]string{
		"loveliness-single":  "loveliness-single-stats.json",
		"loveliness-cluster": "loveliness-cluster-c1-stats.json", // Use node 1 as representative.
		"neo4j":              "neo4j-stats.json",
	}
	for name, file := range statsPatterns {
		path := filepath.Join(*resultsDir, file)
		s, err := loadStats(path)
		if err != nil {
			continue
		}
		stats[name] = s
	}

	// Also load cluster nodes 2 and 3 for total RSS.
	var clusterTotalRSS float64
	for _, suffix := range []string{"c1", "c2", "c3"} {
		s, err := loadStats(filepath.Join(*resultsDir, fmt.Sprintf("loveliness-cluster-%s-stats.json", suffix)))
		if err == nil {
			clusterTotalRSS += s.PeakRSSMiB
		}
	}

	if len(configs) == 0 {
		fmt.Fprintln(os.Stderr, "no result files found")
		os.Exit(1)
	}

	fmt.Printf("Loaded %d configurations\n", len(configs))

	// Key benchmarks for comparison charts.
	keyBenchmarks := []string{
		"point_lookup", "1_hop_traversal", "2_hop_traversal",
		"count_all_nodes", "aggregate_avg_age", "single_write",
	}

	// Generate memory comparison chart.
	if len(stats) > 0 {
		var labels []string
		var values []float64
		colors := []string{"#4361ee", "#3a0ca3", "#f72585"}

		order := []string{"loveliness-single", "loveliness-cluster", "neo4j"}
		displayNames := map[string]string{
			"loveliness-single":  "Loveliness (1 node)",
			"loveliness-cluster": "Loveliness (3 nodes, per-node)",
			"neo4j":              "Neo4j CE",
		}
		for _, name := range order {
			if s, ok := stats[name]; ok {
				labels = append(labels, displayNames[name])
				values = append(values, s.PeakRSSMiB)
			}
		}

		svg := generateBarChartSVG("Peak Memory Usage (per node)", "MiB", labels, values, colors)
		path := filepath.Join(*outputDir, "memory-comparison.svg")
		if err := os.WriteFile(path, []byte(svg), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "write memory chart: %v\n", err)
		} else {
			fmt.Printf("  memory chart: %s\n", path)
		}
	}

	// Generate latency comparison chart for key benchmarks.
	configOrder := []string{"loveliness-single", "loveliness-cluster", "neo4j"}
	displayOrder := []string{"Loveliness (1)", "Loveliness (3)", "Neo4j CE"}
	barColors := []string{"#4361ee", "#3a0ca3", "#f72585"}

	for _, bench := range keyBenchmarks {
		var labels []string
		var values []float64
		var colors []string

		for i, name := range configOrder {
			r, ok := configs[name]
			if !ok {
				continue
			}
			b := findBench(r.Results, bench)
			if b == nil {
				continue
			}
			labels = append(labels, displayOrder[i])
			values = append(values, b.P50Us)
			colors = append(colors, barColors[i])
		}

		if len(labels) > 0 {
			svg := generateBarChartSVG(fmt.Sprintf("P50 Latency: %s", bench), "us", labels, values, colors)
			path := filepath.Join(*outputDir, fmt.Sprintf("latency-%s.svg", bench))
			if err := os.WriteFile(path, []byte(svg), 0644); err != nil {
				fmt.Fprintf(os.Stderr, "write latency chart %s: %v\n", bench, err)
			} else {
				fmt.Printf("  latency chart: %s\n", path)
			}
		}
	}

	// Generate markdown comparison report.
	var md strings.Builder
	md.WriteString("# Benchmark Comparison Report\n\n")

	// Date and config.
	for _, name := range configOrder {
		if r, ok := configs[name]; ok {
			md.WriteString(fmt.Sprintf("**Date:** %s | **Nodes:** %d | **Edges:** %d\n\n", r.Date, r.Nodes, r.Edges))
			break
		}
	}

	// Resource usage table.
	if len(stats) > 0 {
		md.WriteString("## Resource Usage\n\n")
		md.WriteString("| Configuration | Peak RSS (per node) | Avg CPU % |\n")
		md.WriteString("|---|---|---|\n")
		for i, name := range configOrder {
			if s, ok := stats[name]; ok {
				md.WriteString(fmt.Sprintf("| %s | **%.0f MiB** | %.1f%% |\n", displayOrder[i], s.PeakRSSMiB, s.AvgCPUPct))
			}
		}
		if clusterTotalRSS > 0 {
			md.WriteString(fmt.Sprintf("| Loveliness (3) total | **%.0f MiB** | — |\n", clusterTotalRSS))
		}
		md.WriteString("\n")
	}

	// Latency comparison table.
	md.WriteString("## Query Latency (P50)\n\n")
	md.WriteString("| Benchmark |")
	for _, d := range displayOrder {
		if _, ok := configs[configOrder[sort.SearchStrings(displayOrder, d)]]; ok {
			md.WriteString(fmt.Sprintf(" %s |", d))
		}
	}
	md.WriteString("\n|---|")
	for range configOrder {
		md.WriteString("---|")
	}
	md.WriteString("\n")

	allBenchNames := []string{}
	for _, name := range configOrder {
		if r, ok := configs[name]; ok {
			for _, b := range r.Results {
				found := false
				for _, existing := range allBenchNames {
					if existing == b.Name {
						found = true
						break
					}
				}
				if !found {
					allBenchNames = append(allBenchNames, b.Name)
				}
			}
			break // Use first config's order.
		}
	}

	for _, bench := range allBenchNames {
		md.WriteString(fmt.Sprintf("| %s |", bench))
		for _, name := range configOrder {
			r, ok := configs[name]
			if !ok {
				md.WriteString(" — |")
				continue
			}
			b := findBench(r.Results, bench)
			if b == nil {
				md.WriteString(" — |")
				continue
			}
			md.WriteString(fmt.Sprintf(" **%s** |", fmtUs(b.P50Us)))
		}
		md.WriteString("\n")
	}
	md.WriteString("\n")

	// QPS comparison.
	md.WriteString("## Throughput (QPS)\n\n")
	md.WriteString("| Benchmark |")
	for _, d := range displayOrder {
		md.WriteString(fmt.Sprintf(" %s |", d))
	}
	md.WriteString("\n|---|")
	for range configOrder {
		md.WriteString("---|")
	}
	md.WriteString("\n")

	for _, bench := range allBenchNames {
		md.WriteString(fmt.Sprintf("| %s |", bench))
		for _, name := range configOrder {
			r, ok := configs[name]
			if !ok {
				md.WriteString(" — |")
				continue
			}
			b := findBench(r.Results, bench)
			if b == nil {
				md.WriteString(" — |")
				continue
			}
			md.WriteString(fmt.Sprintf(" **%.0f** |", b.QPS))
		}
		md.WriteString("\n")
	}
	md.WriteString("\n")

	// Write report.
	mdPath := filepath.Join(*outputDir, "comparison.md")
	if err := os.WriteFile(mdPath, []byte(md.String()), 0644); err != nil {
		fmt.Fprintf(os.Stderr, "write report: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("  report: %s\n", mdPath)
}
