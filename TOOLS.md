# Tools Reference

This document describes all MCP tools provided by `maltese-competition-mcp`.

All tools use the prefix `mt_comp_`.

## Search & Retrieval Tools

### `mt_comp_search_decisions`

Full-text search across MCCAA enforcement decisions.

**Parameters:**
- `query` *(required)*: Search query (e.g., `'abuse of dominance'`, `'price fixing'`)
- `type` *(optional)*: Filter by decision type — `abuse_of_dominance`, `cartel`, `merger`, `sector_inquiry`
- `sector` *(optional)*: Filter by sector ID (e.g., `financial_services`, `gaming`)
- `outcome` *(optional)*: Filter by outcome — `prohibited`, `cleared`, `cleared_with_conditions`, `fine`
- `limit` *(optional)*: Maximum results, defaults to 20

**Returns:** Array of matching decisions with `_citation` per item.

---

### `mt_comp_get_decision`

Retrieve a single MCCAA decision by case number.

**Parameters:**
- `case_number` *(required)*: e.g., `CA/001/2023`, `CA/005/2022`

**Returns:** Full decision record with `_citation` and `_meta`.

---

### `mt_comp_search_mergers`

Search MCCAA merger control decisions.

**Parameters:**
- `query` *(required)*: Search query
- `sector` *(optional)*: Filter by sector ID
- `outcome` *(optional)*: `cleared`, `cleared_phase1`, `cleared_with_conditions`, `prohibited`
- `limit` *(optional)*: Maximum results, defaults to 20

**Returns:** Array of matching mergers with `_citation` per item.

---

### `mt_comp_get_merger`

Retrieve a single MCCAA merger control decision by case number.

**Parameters:**
- `case_number` *(required)*: e.g., `M/001/2023`

**Returns:** Full merger record with `_citation` and `_meta`.

---

### `mt_comp_list_sectors`

List all sectors with MCCAA enforcement activity.

**Parameters:** None

**Returns:** Array of sectors with decision and merger counts.

---

## Meta Tools

### `mt_comp_list_sources`

List all data sources ingested into this MCP, with provenance and licensing.

**Parameters:** None

**Returns:** Array of source records.

---

### `mt_comp_check_data_freshness`

Check the freshness of ingested data.

**Parameters:** None

**Returns:** Last-ingest date, record counts, latest decision date, and staleness flag.

---

### `mt_comp_about`

Return server metadata including version, description, coverage summary, and tool list.

**Parameters:** None

**Returns:** Server metadata object.

---

## Response Structure

All responses include a `_meta` block:

```json
{
  "_meta": {
    "disclaimer": "Research tool only — not regulatory or legal advice...",
    "data_age": "2026-03-23",
    "copyright": "© Malta Competition and Consumer Affairs Authority",
    "source_url": "https://www.mccaa.org.mt/"
  }
}
```

Retrieval responses include `_citation`:

```json
{
  "_citation": {
    "canonical_ref": "CA/001/2023",
    "display_text": "CA/001/2023",
    "lookup": {
      "tool": "mt_comp_get_decision",
      "args": { "case_number": "CA/001/2023" }
    }
  }
}
```

Error responses include `_error_type` (`not_found`, `tool_error`, `unknown_tool`, `execution_error`).
