/**
 * Ingestion crawler for the MCCAA (Malta Competition and Consumer Affairs Authority)
 * MCP server.
 *
 * Scrapes competition enforcement decisions, merger control (concentration)
 * decisions, and sector data from mccaa.org.mt and populates the SQLite
 * database.
 *
 * Data sources:
 *   - Office for Competition section (sectionId=1060)
 *     • Individual decision pages (/Section/Content?contentId=NNNN)
 *     • Notification of concentration pages
 *     • Court judgement pages
 *   - News listing (/Section/AllNews) — discovers decision/merger links
 *   - Judgements & Administrative Decisions (contentId=1187)
 *   - Decision PDFs under /media/ (metadata extraction only — titles/dates)
 *
 * MCCAA URL conventions:
 *   - Section index:  /Section/index?sectionId=NNNN
 *   - Content pages:  /Section/Content?contentId=NNNN
 *   - All news:       /Section/AllNews  (paginated via ?page=N)
 *   - Media/PDFs:     /media/NNNN/filename.pdf
 *
 * MCCAA case number formats:
 *   - COMP/MCCAA/NN/YYYY   (competition enforcement)
 *   - CONC/MCCAA/NN/YYYY   (concentrations/mergers)
 *   - OC/NNN/YYYY          (Office for Competition legacy)
 *   - Fallback: MCCAA-NNNN (generated from contentId when no case number found)
 *
 * Usage:
 *   npx tsx scripts/ingest-mccaa.ts
 *   npx tsx scripts/ingest-mccaa.ts --dry-run
 *   npx tsx scripts/ingest-mccaa.ts --resume
 *   npx tsx scripts/ingest-mccaa.ts --force
 *   npx tsx scripts/ingest-mccaa.ts --max-pages 5
 */

import Database from "better-sqlite3";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  unlinkSync,
  writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import * as cheerio from "cheerio";
import { SCHEMA_SQL } from "../src/db.js";

// ---------------------------------------------------------------------------
// Configuration
// ---------------------------------------------------------------------------

const DB_PATH = process.env["MCCAA_DB_PATH"] ?? "data/mccaa.db";
const STATE_FILE = join(dirname(DB_PATH), "ingest-state.json");
const BASE_URL = "https://mccaa.org.mt";
const RATE_LIMIT_MS = 1500;
const MAX_RETRIES = 3;
const RETRY_DELAY_MS = 5000;
const USER_AGENT =
  "AnsvarMCCAACrawler/1.0 (+https://github.com/Ansvar-Systems/maltese-competition-mcp)";

/**
 * Entry points on mccaa.org.mt that list or link to competition content.
 *
 * The MCCAA website uses a CMS with Section/Content URLs. Content discovery
 * works in two phases:
 *   1. Crawl known listing pages to discover contentId links
 *   2. Fetch each content page and parse structured data
 */
const LISTING_SOURCES = [
  {
    id: "office-for-competition",
    url: "/Section/index?sectionId=1060",
    description: "Office for Competition — main section index",
  },
  {
    id: "judgements-admin-decisions",
    url: "/Section/Content?contentId=1187",
    description: "Judgements and Administrative Decisions listing",
  },
  {
    id: "news-all",
    url: "/Section/AllNews",
    description: "All MCCAA news (paginated)",
    paginated: true,
    maxPages: 30,
  },
  {
    id: "news",
    url: "/news",
    description: "MCCAA news feed",
  },
] as const;

/**
 * Known content IDs discovered through web search that point to
 * competition decisions, merger notifications, or court judgements.
 * This seeds the crawl with pages we know exist, in case the listing
 * pages do not link to all of them.
 */
const SEED_CONTENT_IDS = [
  3153,  // Office for Competition — Decision (St Edward's College / In Design)
  5694,  // Concentrations — Decision
  7431,  // Concentration notification (888 / William Hill)
  7563,  // Office for Competition — Decision (Lidl Phase II investigation)
  10851, // Notification of Concentration (easyJet / SR Technics Malta)
  11245, // Office for Competition — Press Release
  13225, // Office for Competition — Court Judgement
  13282, // Office for Competition — Decision
  13855, // Notification of Concentration (Jetho / I.V. Portelli)
  1295,  // Competition section content
];

// CLI flags
const dryRun = process.argv.includes("--dry-run");
const resume = process.argv.includes("--resume");
const force = process.argv.includes("--force");
const maxPagesArg = process.argv.find((_, i, a) => a[i - 1] === "--max-pages");
const maxPagesOverride = maxPagesArg ? parseInt(maxPagesArg, 10) : null;

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

interface IngestState {
  processedUrls: string[];
  lastRun: string;
  decisionsIngested: number;
  mergersIngested: number;
  errors: string[];
}

interface ParsedDecision {
  case_number: string;
  title: string;
  date: string | null;
  type: string | null;
  sector: string | null;
  parties: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  fine_amount: number | null;
  gwb_articles: string | null;
  status: string;
}

interface ParsedMerger {
  case_number: string;
  title: string;
  date: string | null;
  sector: string | null;
  acquiring_party: string | null;
  target: string | null;
  summary: string | null;
  full_text: string;
  outcome: string | null;
  turnover: number | null;
}

interface SectorAccumulator {
  [id: string]: {
    name: string;
    name_en: string | null;
    description: string | null;
    decisionCount: number;
    mergerCount: number;
  };
}

// ---------------------------------------------------------------------------
// HTTP fetching with rate limiting and retries
// ---------------------------------------------------------------------------

let lastRequestTime = 0;

async function rateLimitedFetch(url: string): Promise<string | null> {
  const now = Date.now();
  const elapsed = now - lastRequestTime;
  if (elapsed < RATE_LIMIT_MS) {
    await sleep(RATE_LIMIT_MS - elapsed);
  }

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    try {
      lastRequestTime = Date.now();
      const response = await fetch(url, {
        headers: {
          "User-Agent": USER_AGENT,
          Accept:
            "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
          "Accept-Language": "en-GB,en;q=0.9,mt;q=0.8",
        },
        redirect: "follow",
        signal: AbortSignal.timeout(30_000),
      });

      if (response.status === 403 || response.status === 429) {
        console.warn(
          `  [WARN] HTTP ${response.status} for ${url} (attempt ${attempt}/${MAX_RETRIES})`,
        );
        if (attempt < MAX_RETRIES) {
          await sleep(RETRY_DELAY_MS * attempt);
          continue;
        }
        return null;
      }

      if (!response.ok) {
        console.warn(`  [WARN] HTTP ${response.status} for ${url}`);
        return null;
      }

      return await response.text();
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.warn(
        `  [WARN] Fetch error for ${url} (attempt ${attempt}/${MAX_RETRIES}): ${message}`,
      );
      if (attempt < MAX_RETRIES) {
        await sleep(RETRY_DELAY_MS * attempt);
      }
    }
  }

  return null;
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ---------------------------------------------------------------------------
// State management (for --resume)
// ---------------------------------------------------------------------------

function loadState(): IngestState {
  if (resume && existsSync(STATE_FILE)) {
    try {
      const raw = readFileSync(STATE_FILE, "utf-8");
      return JSON.parse(raw) as IngestState;
    } catch {
      console.warn("[WARN] Could not read state file, starting fresh.");
    }
  }
  return {
    processedUrls: [],
    lastRun: new Date().toISOString(),
    decisionsIngested: 0,
    mergersIngested: 0,
    errors: [],
  };
}

function saveState(state: IngestState): void {
  state.lastRun = new Date().toISOString();
  const dir = dirname(STATE_FILE);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }
  writeFileSync(STATE_FILE, JSON.stringify(state, null, 2), "utf-8");
}

// ---------------------------------------------------------------------------
// URL discovery — find content page URLs from listing/index pages
// ---------------------------------------------------------------------------

/**
 * Extract content page URLs from a listing/index page.
 *
 * MCCAA links to content pages using two URL patterns:
 *   - /Section/Content?contentId=NNNN
 *   - /section/content?contentId=NNNN  (case-insensitive CMS)
 *
 * We filter for links that appear to reference Office for Competition
 * decisions, merger notifications, or court judgements based on link text
 * and surrounding context.
 */
function extractContentUrls($: cheerio.CheerioAPI): string[] {
  const urls: string[] = [];

  $("a[href]").each((_i, el) => {
    const href = $(el).attr("href");
    if (!href) return;

    // Match content page links (case-insensitive)
    const contentMatch = href.match(
      /\/[Ss]ection\/[Cc]ontent\?contentId=(\d+)/,
    );
    if (contentMatch) {
      const contentId = contentMatch[1]!;
      const fullUrl = `${BASE_URL}/Section/Content?contentId=${contentId}`;
      if (!urls.includes(fullUrl)) {
        urls.push(fullUrl);
      }
      return;
    }

    // Match section index links (sub-sections of competition)
    const sectionMatch = href.match(
      /\/[Ss]ection\/index\?sectionId=(\d+)/,
    );
    if (sectionMatch) {
      const sectionId = parseInt(sectionMatch[1]!, 10);
      // Only follow sub-sections near the competition section range
      if (sectionId >= 1060 && sectionId <= 1200) {
        const fullUrl = `${BASE_URL}/Section/index?sectionId=${sectionId}`;
        if (!urls.includes(fullUrl)) {
          urls.push(fullUrl);
        }
      }
    }
  });

  return urls;
}

/**
 * Crawl listing pages (including pagination) to discover content URLs.
 */
async function discoverContentUrls(
  state: IngestState,
): Promise<string[]> {
  const allUrls = new Set<string>();

  // Phase 1: Seed with known content IDs
  for (const id of SEED_CONTENT_IDS) {
    allUrls.add(`${BASE_URL}/Section/Content?contentId=${id}`);
  }
  console.log(`  Seeded ${SEED_CONTENT_IDS.length} known content URLs`);

  // Phase 2: Crawl listing sources
  for (const source of LISTING_SOURCES) {
    console.log(`\n  Crawling ${source.id} (${source.description})...`);

    if ("paginated" in source && source.paginated) {
      const maxPages = maxPagesOverride
        ? Math.min(maxPagesOverride, source.maxPages)
        : source.maxPages;

      for (let page = 1; page <= maxPages; page++) {
        const pageUrl =
          page === 1
            ? `${BASE_URL}${source.url}`
            : `${BASE_URL}${source.url}?page=${page}`;

        if (page % 5 === 1 || page === 1) {
          console.log(
            `    Fetching page ${page}/${maxPages}... (${allUrls.size} URLs total)`,
          );
        }

        const html = await rateLimitedFetch(pageUrl);
        if (!html) {
          console.warn(`    [WARN] Could not fetch page ${page} of ${source.id}`);
          continue;
        }

        const $ = cheerio.load(html);
        const pageUrls = extractContentUrls($);

        if (pageUrls.length === 0 && page > 1) {
          console.log(
            `    No new URLs on page ${page} — stopping pagination for ${source.id}`,
          );
          break;
        }

        for (const url of pageUrls) {
          allUrls.add(url);
        }
      }
    } else {
      const html = await rateLimitedFetch(`${BASE_URL}${source.url}`);
      if (!html) {
        console.warn(`    [WARN] Could not fetch ${source.id}`);
        continue;
      }

      const $ = cheerio.load(html);
      const foundUrls = extractContentUrls($);

      // Also crawl any sub-section links found on index pages
      const subSections = foundUrls.filter((u) => u.includes("sectionId"));
      for (const subUrl of subSections) {
        const subHtml = await rateLimitedFetch(subUrl);
        if (subHtml) {
          const $sub = cheerio.load(subHtml);
          const subContentUrls = extractContentUrls($sub);
          for (const u of subContentUrls) {
            allUrls.add(u);
          }
        }
      }

      for (const url of foundUrls) {
        if (url.includes("contentId")) {
          allUrls.add(url);
        }
      }
    }

    console.log(`    ${source.id}: ${allUrls.size} total URLs discovered`);
  }

  // Filter out already-processed URLs when resuming
  const processedSet = new Set(state.processedUrls);
  const filtered = [...allUrls].filter((url) => !processedSet.has(url));

  console.log(
    `\n  Discovery complete: ${allUrls.size} total, ${filtered.length} new (${processedSet.size} already processed)`,
  );

  return filtered;
}

// ---------------------------------------------------------------------------
// Content page parsing
// ---------------------------------------------------------------------------

/**
 * Determine whether a content page is competition-related.
 *
 * MCCAA publishes content from multiple offices (Competition, Consumer Affairs,
 * Standards, Metrology). We only want competition and merger content.
 */
function isCompetitionContent($: cheerio.CheerioAPI): boolean {
  const bodyText = $("body").text().toLowerCase();
  const title = $("title").text().toLowerCase();
  const breadcrumb = $(".breadcrumb, nav[aria-label='breadcrumb']")
    .text()
    .toLowerCase();

  const competitionSignals = [
    "office for competition",
    "competition act",
    "cap. 379",
    "cap 379",
    "concentration",
    "merger",
    "acquisition",
    "anti-competitive",
    "anticompetitive",
    "dominant position",
    "abuse of dominan",
    "cartel",
    "concerted practice",
    "market share",
    "article 5",
    "article 9",
    "s.l. 379",
    "s.l.379",
    "antitrust",
    "competition law",
    "office for fair competition",
  ];

  // Check breadcrumb first (most reliable)
  if (
    breadcrumb.includes("competition") ||
    breadcrumb.includes("office for competition")
  ) {
    return true;
  }

  // Check title
  if (
    title.includes("competition") ||
    title.includes("concentration") ||
    title.includes("merger")
  ) {
    return true;
  }

  // Check body for multiple competition signals
  const matchCount = competitionSignals.filter((s) =>
    bodyText.includes(s),
  ).length;
  return matchCount >= 2;
}

/**
 * Determine whether a content page describes a merger/concentration
 * (as opposed to an enforcement decision).
 */
function isMergerContent($: cheerio.CheerioAPI): boolean {
  const bodyText = $("body").text().toLowerCase();
  const title = $("title").text().toLowerCase();
  const all = `${title} ${bodyText.slice(0, 3000)}`;

  const mergerSignals = [
    "notification of concentration",
    "notification of a concentration",
    "control of concentrations",
    "concentrations regulations",
    "proposed acquisition",
    "proposed merger",
    "acquisition of sole control",
    "acquisition of joint control",
    "joint venture",
    "phase i",
    "phase ii",
    "phase 1",
    "phase 2",
    "non-opposition decision",
    "clearance decision",
    "the acquiring",
    "the target",
    "acquiring party",
    "target undertaking",
    "s.l. 379.08",
    "s.l.379.08",
  ];

  const matchCount = mergerSignals.filter((s) =>
    all.includes(s),
  ).length;
  return matchCount >= 2;
}

/**
 * Extract the page title from MCCAA content pages.
 *
 * MCCAA content pages typically use:
 *   - An <h1> or <h2> within the main content area
 *   - A breadcrumb trail ending with the page title
 *   - The <title> element (often prefixed with "MCCAA")
 */
function extractTitle($: cheerio.CheerioAPI): string {
  // Try main content heading first
  const h1 = $("main h1, .content h1, article h1, #content h1").first().text().trim();
  if (h1 && h1.length > 5 && !h1.toLowerCase().startsWith("mccaa")) {
    return h1;
  }

  const h2 = $("main h2, .content h2, article h2").first().text().trim();
  if (h2 && h2.length > 5) {
    return h2;
  }

  // Try page title (strip MCCAA prefix)
  const pageTitle = $("title").text().trim();
  const cleaned = pageTitle
    .replace(/^MCCAA\s*[-–|]\s*/i, "")
    .replace(/\s*[-–|]\s*MCCAA$/i, "")
    .trim();
  if (cleaned.length > 5) {
    return cleaned;
  }

  // Try the first significant heading anywhere
  const anyH = $("h1, h2, h3").first().text().trim();
  if (anyH && anyH.length > 5) {
    return anyH;
  }

  return pageTitle || "Untitled MCCAA content";
}

/**
 * Extract the main body text from MCCAA content pages, excluding
 * navigation, footer, and sidebar elements.
 */
function extractBodyText($: cheerio.CheerioAPI): string {
  // Remove navigation, footer, sidebar, and script elements
  const $clone = cheerio.load($.html() ?? "");
  $clone("nav, footer, header, script, style, .sidebar, .menu, .navigation, .footer, .cookie-banner").remove();

  // Try specific content containers
  const containers = [
    "main .content",
    "main article",
    ".page-content",
    "#content",
    "main",
    "article",
    ".content-area",
    ".entry-content",
  ];

  for (const selector of containers) {
    const text = $clone(selector).text().trim();
    if (text.length > 100) {
      return normaliseWhitespace(text);
    }
  }

  // Fallback: body text
  return normaliseWhitespace($clone("body").text().trim());
}

/**
 * Collapse consecutive whitespace into single spaces and trim.
 */
function normaliseWhitespace(text: string): string {
  return text.replace(/[\s\n\r\t]+/g, " ").trim();
}

/**
 * Extract an MCCAA case number from the page content.
 *
 * Known formats:
 *   - COMP/MCCAA/NN/YYYY
 *   - CONC/MCCAA/NN/YYYY
 *   - OC/NNN/YYYY
 *   - Case No. NNN/YYYY
 *   - Reference: MCCAA-NNNN
 */
function extractCaseNumber(
  $: cheerio.CheerioAPI,
  bodyText: string,
  contentId: string,
): string {
  // Pattern 1: COMP/MCCAA/NN/YYYY (competition enforcement)
  const compMatch = bodyText.match(
    /COMP\/MCCAA\/(\d{1,3})\/(\d{4})/i,
  );
  if (compMatch) {
    return compMatch[0].toUpperCase();
  }

  // Pattern 2: CONC/MCCAA/NN/YYYY (concentrations/mergers)
  const concMatch = bodyText.match(
    /CONC\/MCCAA\/(\d{1,3})\/(\d{4})/i,
  );
  if (concMatch) {
    return concMatch[0].toUpperCase();
  }

  // Pattern 3: OC/NNN/YYYY (Office for Competition legacy)
  const ocMatch = bodyText.match(/OC\/(\d{1,4})\/(\d{4})/i);
  if (ocMatch) {
    return ocMatch[0].toUpperCase();
  }

  // Pattern 4: Case No. NNN/YYYY or Case Number NNN/YYYY
  const caseNoMatch = bodyText.match(
    /[Cc]ase\s+[Nn](?:o|umber)[.\s]*[:.]?\s*(\d{1,4}\/\d{4})/,
  );
  if (caseNoMatch?.[1]) {
    return `OC/${caseNoMatch[1]}`;
  }

  // Pattern 5: Reference number in metadata-like structures
  const labels = ["reference", "case ref", "ref", "case number"];
  $("dt, .field-label, strong, b, th").each((_i, el) => {
    const labelText = $(el).text().trim().toLowerCase().replace(/:$/, "");
    if (labels.some((l) => labelText.includes(l))) {
      const value =
        $(el).next("dd, .field-item, td").text().trim() ||
        $(el).parent().text().replace($(el).text(), "").trim();
      if (value && value.match(/\d/)) {
        // Already found via earlier patterns — skip
      }
    }
  });

  // Fallback: generate from contentId
  return `MCCAA-${contentId}`;
}

/**
 * Extract date from MCCAA content pages.
 *
 * Looks for dates in multiple locations:
 *   - Meta tags (article:published_time, date, etc.)
 *   - Labelled fields (Date:, Published:, Decision date:)
 *   - Inline date patterns in the body text
 */
function extractDate($: cheerio.CheerioAPI, bodyText: string): string | null {
  // Meta tags
  const metaDate =
    $("meta[property='article:published_time']").attr("content") ||
    $("meta[name='date']").attr("content") ||
    $("meta[name='DC.date']").attr("content") ||
    $("time[datetime]").first().attr("datetime");

  if (metaDate) {
    const parsed = parseDate(metaDate);
    if (parsed) return parsed;
  }

  // Labelled fields in the page
  const dateLabels = [
    "date",
    "decision date",
    "published",
    "date of decision",
    "notification date",
    "filed on",
  ];

  let labelledDate: string | null = null;
  $("dt, strong, b, .field-label, th, span.label").each((_i, el) => {
    if (labelledDate) return;
    const label = $(el).text().trim().toLowerCase().replace(/:$/, "");
    if (dateLabels.some((d) => label.includes(d))) {
      const value =
        $(el).next("dd, .field-item, td, span").text().trim() ||
        $(el).parent().text().replace($(el).text(), "").trim();
      if (value) {
        labelledDate = parseDate(value);
      }
    }
  });
  if (labelledDate) return labelledDate;

  // Date patterns in body text (first 1500 chars)
  const head = bodyText.slice(0, 1500);
  const dateFromBody = parseDate(head);
  return dateFromBody;
}

/**
 * Parse a date string into ISO format (YYYY-MM-DD).
 *
 * Handles:
 *   - ISO: 2024-09-19, 2024-09-19T00:00:00Z
 *   - European: 19/09/2024, 19.09.2024
 *   - English textual: 19 September 2024, September 19, 2024
 *   - Short month: 19 Sep 2024
 */
function parseDate(raw: string): string | null {
  if (!raw) return null;

  // ISO format
  const isoMatch = raw.match(/(\d{4})-(\d{2})-(\d{2})/);
  if (isoMatch) {
    return isoMatch[0];
  }

  // European dd/mm/yyyy or dd.mm.yyyy
  const euroMatch = raw.match(/(\d{1,2})[./](\d{1,2})[./](\d{4})/);
  if (euroMatch) {
    const [, day, month, year] = euroMatch;
    return `${year}-${month!.padStart(2, "0")}-${day!.padStart(2, "0")}`;
  }

  // English textual: "19 September 2024" or "September 19, 2024"
  const months: Record<string, string> = {
    january: "01", jan: "01",
    february: "02", feb: "02",
    march: "03", mar: "03",
    april: "04", apr: "04",
    may: "05",
    june: "06", jun: "06",
    july: "07", jul: "07",
    august: "08", aug: "08",
    september: "09", sep: "09", sept: "09",
    october: "10", oct: "10",
    november: "11", nov: "11",
    december: "12", dec: "12",
  };

  // "19 September 2024"
  const dmy = raw.match(
    /(\d{1,2})\s+(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+(\d{4})/i,
  );
  if (dmy) {
    const monthNum = months[dmy[2]!.toLowerCase()];
    if (monthNum) {
      return `${dmy[3]}-${monthNum}-${dmy[1]!.padStart(2, "0")}`;
    }
  }

  // "September 19, 2024"
  const mdy = raw.match(
    /(january|february|march|april|may|june|july|august|september|october|november|december|jan|feb|mar|apr|jun|jul|aug|sep|sept|oct|nov|dec)\s+(\d{1,2}),?\s+(\d{4})/i,
  );
  if (mdy) {
    const monthNum = months[mdy[1]!.toLowerCase()];
    if (monthNum) {
      return `${mdy[3]}-${monthNum}-${mdy[2]!.padStart(2, "0")}`;
    }
  }

  return null;
}

/**
 * Extract parties from the page content.
 *
 * MCCAA decision pages mention parties in several ways:
 *   - "between X and Y"
 *   - "filed by X"
 *   - "against X"
 *   - Labelled fields: "Parties:", "Notifying party:", "Acquiring party:"
 *   - Concentration format: "X (location) and Y (location)"
 */
function extractParties($: cheerio.CheerioAPI, bodyText: string): string[] {
  const parties: string[] = [];

  // Look for labelled party fields
  const partyLabels = [
    "parties",
    "notifying party",
    "acquiring party",
    "target",
    "target undertaking",
    "undertaking",
    "complainant",
    "respondent",
  ];

  $("dt, strong, b, .field-label, th").each((_i, el) => {
    const label = $(el).text().trim().toLowerCase().replace(/:$/, "");
    if (partyLabels.some((p) => label.includes(p))) {
      const value =
        $(el).next("dd, .field-item, td").text().trim() ||
        $(el).parent().text().replace($(el).text(), "").trim();
      if (value && value.length > 2 && value.length < 300) {
        parties.push(value);
      }
    }
  });

  if (parties.length > 0) return parties;

  // Pattern: "between X and Y" in concentration notifications
  const betweenMatch = bodyText.match(
    /(?:between|filed by)\s+(.+?)\s+and\s+(.+?)(?:\s+was\s+filed|\s*,)/i,
  );
  if (betweenMatch) {
    // Clean up location suffixes like "(Luton, United Kingdom)"
    const clean = (s: string) =>
      s.replace(/\s*\([^)]+\)\s*$/, "").trim();
    if (betweenMatch[1]) parties.push(clean(betweenMatch[1]));
    if (betweenMatch[2]) parties.push(clean(betweenMatch[2]));
  }

  return parties;
}

/**
 * Extract a summary from the first substantial paragraph of the content.
 */
function extractSummary($: cheerio.CheerioAPI, bodyText: string): string | null {
  // Look for explicit summary or abstract sections
  const summaryLabels = ["summary", "abstract", "overview", "background"];
  let explicitSummary: string | null = null;

  $("h2, h3, h4, strong, b").each((_i, el) => {
    if (explicitSummary) return;
    const heading = $(el).text().trim().toLowerCase();
    if (summaryLabels.some((l) => heading.includes(l))) {
      const nextText = $(el).nextAll("p").first().text().trim();
      if (nextText && nextText.length > 50) {
        explicitSummary = nextText;
      }
    }
  });

  if (explicitSummary) return explicitSummary;

  // Take the first substantial paragraph from the body text.
  // Skip very short sentences (navigation text, dates, labels).
  const sentences = bodyText.split(/(?<=[.!?])\s+/);
  const substantive: string[] = [];
  let charCount = 0;
  const maxSummaryChars = 500;

  for (const sentence of sentences) {
    if (sentence.length < 20) continue;
    if (charCount + sentence.length > maxSummaryChars) break;
    substantive.push(sentence);
    charCount += sentence.length;
  }

  return substantive.length > 0 ? substantive.join(" ") : null;
}

/**
 * Extract legal articles cited in the decision.
 *
 * Maltese competition law references:
 *   - Competition Act (Cap. 379)  — Article 5 (agreements), Article 9 (abuse of dominance)
 *   - Control of Concentrations Regulations (S.L. 379.08) — Article 10, etc.
 *   - EU Treaty — Articles 101, 102 TFEU
 */
function extractLegalArticles(bodyText: string): string[] {
  const articles = new Set<string>();
  let m: RegExpExecArray | null;

  // Competition Act Cap. 379 articles
  const capPattern =
    /(?:Competition\s+Act|Cap\.?\s*379|Chapter\s+379)\s*,?\s*(?:Art(?:icle)?\.?\s*)(\d+)(?:\((\d+)\))?/gi;
  while ((m = capPattern.exec(bodyText)) !== null) {
    const artNum = m[1]!;
    const sub = m[2] ? `(${m[2]})` : "";
    articles.add(`Competition Act Cap. 379, Article ${artNum}${sub}`);
  }

  // Standalone "Article N" near competition context
  const standaloneArt = /Article\s+(\d{1,2})(?:\((\d+)\))?/gi;
  while ((m = standaloneArt.exec(bodyText)) !== null) {
    const num = parseInt(m[1]!, 10);
    // Only capture articles commonly referenced in Maltese competition law
    if ([5, 6, 9, 10, 11, 12, 14, 15, 25].includes(num)) {
      const sub = m[2] ? `(${m[2]})` : "";
      articles.add(`Competition Act Cap. 379, Article ${num}${sub}`);
    }
  }

  // S.L. 379.08 (Control of Concentrations Regulations)
  const slPattern =
    /S\.?L\.?\s*379\.08\s*,?\s*(?:(?:Art(?:icle)?|Regulation)\.?\s*)?(\d+)/gi;
  while ((m = slPattern.exec(bodyText)) !== null) {
    articles.add(`S.L. 379.08, Article ${m[1]}`);
  }

  // EU Treaty articles (TFEU 101 / 102)
  const euPattern =
    /(?:TFEU|Treaty|Article)\s*(101|102)\s*(?:TFEU|of\s+the\s+Treaty)?/gi;
  while ((m = euPattern.exec(bodyText)) !== null) {
    articles.add(`TFEU Article ${m[1]}`);
  }

  // "Art. 101" / "Art. 102"
  const artDot = /Art\.?\s*(101|102)/gi;
  while ((m = artDot.exec(bodyText)) !== null) {
    articles.add(`TFEU Article ${m[1]}`);
  }

  return [...articles];
}

/**
 * Extract fine/penalty amounts from Maltese competition decision text.
 *
 * Handles EUR amounts in various formats:
 *   - EUR 1,200,000 / EUR 1.200.000 / EUR 1 200 000
 *   - Euro 1.2 million / EUR 2.4 million
 *   - a fine of EUR X
 */
function extractFineAmount(bodyText: string): number | null {
  const patterns = [
    // "EUR N million" / "Euro N million"
    /(?:EUR|Euro|€)\s*([\d,.]+)\s*million/gi,
    // "fine of EUR N" / "penalty of EUR N" / "EUR N"
    /(?:fine|penalty|penalt)\w*\s+(?:of\s+)?(?:EUR|Euro|€)\s*([\d,.\s]+)/gi,
    // Standalone "EUR N" with large numbers
    /(?:EUR|Euro|€)\s*([\d,.\s]+)/gi,
  ];

  for (const pattern of patterns) {
    const match = pattern.exec(bodyText);
    if (match?.[1]) {
      let numStr = match[1].trim();

      // "N million"
      if (pattern.source.includes("million")) {
        numStr = numStr.replace(/[,\s]/g, "").replace(/\.(?=\d{3})/g, "");
        // Handle European decimal: 1,2 million = 1.2 million
        numStr = numStr.replace(",", ".");
        const val = parseFloat(numStr);
        if (!isNaN(val) && val > 0) return val * 1_000_000;
      }

      // Direct amount: normalise separators
      // European format uses dot or space as thousands separator, comma as decimal
      // English format uses comma as thousands separator, dot as decimal
      // Detect: if pattern is N.NNN.NNN (dots as thousands) => European
      if (numStr.match(/\d\.\d{3}\./)) {
        // European thousands separator
        numStr = numStr.replace(/\./g, "").replace(",", ".");
      } else {
        // English thousands separator (commas) or spaces
        numStr = numStr.replace(/[,\s]/g, "");
      }

      const val = parseFloat(numStr);
      if (!isNaN(val) && val > 1000) return val;
    }
  }

  return null;
}

/**
 * Classify the decision type based on page content.
 */
function classifyDecisionType(
  title: string,
  bodyText: string,
): { type: string | null; outcome: string | null } {
  const all = `${title} ${bodyText.slice(0, 4000)}`.toLowerCase();

  // --- Type ---
  let type: string | null = null;

  if (
    all.includes("cartel") ||
    all.includes("concerted practice") ||
    all.includes("price fixing") ||
    all.includes("bid rigging") ||
    all.includes("market sharing") ||
    all.includes("information exchange") ||
    all.includes("coordination")
  ) {
    type = "cartel";
  } else if (
    all.includes("abuse of dominan") ||
    all.includes("dominant position") ||
    all.includes("predatory pricing") ||
    all.includes("margin squeeze") ||
    all.includes("exclusionary") ||
    all.includes("exclusive dealing") ||
    all.includes("foreclosure")
  ) {
    type = "abuse_of_dominance";
  } else if (
    all.includes("sector inquiry") ||
    all.includes("sector investigation") ||
    all.includes("market study") ||
    all.includes("market investigation")
  ) {
    type = "sector_inquiry";
  } else if (
    all.includes("commitment") ||
    all.includes("undertakings offered")
  ) {
    type = "commitment_decision";
  } else if (all.includes("interim measures")) {
    type = "interim_measures";
  } else if (all.includes("court judgement") || all.includes("court judgment")) {
    type = "court_judgement";
  } else {
    type = "decision";
  }

  // --- Outcome ---
  let outcome: string | null = null;

  if (
    all.includes("fine") ||
    all.includes("penalty") ||
    all.includes("penalised") ||
    all.includes("fined")
  ) {
    outcome = "fine";
  } else if (
    all.includes("prohibited") ||
    all.includes("blocked") ||
    all.includes("prohibition decision")
  ) {
    outcome = "prohibited";
  } else if (
    all.includes("commitment") &&
    (all.includes("accepted") || all.includes("binding"))
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("cleared with conditions") ||
    all.includes("approved with conditions") ||
    all.includes("conditional clearance") ||
    all.includes("subject to conditions")
  ) {
    outcome = "cleared_with_conditions";
  } else if (
    all.includes("cleared") ||
    all.includes("approved") ||
    all.includes("no competition concerns") ||
    all.includes("not likely to create significant impediment")
  ) {
    outcome = "cleared";
  } else if (
    all.includes("dismissed") ||
    all.includes("rejected") ||
    all.includes("insufficient grounds")
  ) {
    outcome = "dismissed";
  } else if (
    all.includes("closed") ||
    all.includes("no further action")
  ) {
    outcome = "closed";
  }

  return { type, outcome };
}

/**
 * Classify merger outcome.
 */
function classifyMergerOutcome(
  title: string,
  bodyText: string,
): string | null {
  const all = `${title} ${bodyText}`.toLowerCase();

  if (
    all.includes("prohibited") ||
    all.includes("blocked") ||
    all.includes("prohibition decision")
  ) {
    return "blocked";
  }
  if (
    all.includes("condition") ||
    all.includes("cleared with") ||
    all.includes("subject to") ||
    all.includes("remedy") ||
    all.includes("divestiture") ||
    all.includes("commitment")
  ) {
    return "cleared_with_conditions";
  }
  if (all.includes("withdrawn") || all.includes("abandoned")) {
    return "withdrawn";
  }
  if (all.includes("phase ii") || all.includes("phase 2") || all.includes("in-depth")) {
    if (
      all.includes("cleared") ||
      all.includes("approved") ||
      all.includes("non-opposition")
    ) {
      return "cleared_phase2";
    }
    return "cleared_phase2";
  }
  if (
    all.includes("cleared") ||
    all.includes("approved") ||
    all.includes("non-opposition") ||
    all.includes("no competition concerns")
  ) {
    return "cleared_phase1";
  }

  // Default: most MCCAA merger notifications are cleared
  return "cleared_phase1";
}

/**
 * Classify sector from content.
 */
function classifySector(
  title: string,
  bodyText: string,
): string | null {
  const text = `${title} ${bodyText.slice(0, 3000)}`.toLowerCase();

  const sectorMapping: Array<{ id: string; patterns: string[] }> = [
    {
      id: "financial_services",
      patterns: [
        "bank",
        "banking",
        "insurance",
        "payment",
        "financial",
        "investment",
        "securities",
        "credit",
        "savings",
        "fintech",
      ],
    },
    {
      id: "gaming",
      patterns: [
        "gaming",
        "igaming",
        "i-gaming",
        "betting",
        "casino",
        "gambling",
        "lotteries",
        "lottery",
        "mga",
        "malta gaming authority",
      ],
    },
    {
      id: "telecommunications",
      patterns: [
        "telecom",
        "broadband",
        "mobile",
        "internet service",
        "fibre",
        "fiber",
        "go plc",
        "melita",
        "vodafone malta",
        "epic",
      ],
    },
    {
      id: "energy",
      patterns: [
        "energy",
        "electricity",
        "petroleum",
        "fuel",
        "gas",
        "lng",
        "enemalta",
        "renewable",
        "solar",
      ],
    },
    {
      id: "tourism",
      patterns: [
        "hotel",
        "tourism",
        "hospitality",
        "restaurant",
        "travel",
        "airline",
        "car hire",
        "ferry",
        "cruise",
      ],
    },
    {
      id: "healthcare",
      patterns: [
        "health",
        "pharma",
        "hospital",
        "medical",
        "clinic",
        "dental",
        "optician",
        "care home",
      ],
    },
    {
      id: "retail",
      patterns: [
        "retail",
        "supermarket",
        "grocery",
        "shop",
        "convenience store",
        "lidl",
        "pavi",
        "scott",
        "food retail",
      ],
    },
    {
      id: "construction",
      patterns: [
        "construction",
        "property",
        "real estate",
        "building",
        "cement",
        "quarry",
        "aggregate",
        "development",
      ],
    },
    {
      id: "transport",
      patterns: [
        "transport",
        "shipping",
        "logistics",
        "freight",
        "port",
        "maritime",
        "aviation",
        "bus",
      ],
    },
    {
      id: "digital_economy",
      patterns: [
        "software",
        "platform",
        "digital",
        "technology",
        "it service",
        "data centre",
        "cloud",
        "saas",
      ],
    },
    {
      id: "media",
      patterns: [
        "media",
        "broadcasting",
        "television",
        "radio",
        "press",
        "newspaper",
        "advertising",
      ],
    },
    {
      id: "professional_services",
      patterns: [
        "audit",
        "accountancy",
        "legal",
        "consulting",
        "advisory",
        "notary",
      ],
    },
    {
      id: "manufacturing",
      patterns: [
        "manufacturing",
        "factory",
        "production",
        "industrial",
        "processing",
      ],
    },
  ];

  for (const { id, patterns } of sectorMapping) {
    const matchCount = patterns.filter((p) => text.includes(p)).length;
    if (matchCount >= 2) return id;
  }

  // Single strong match
  for (const { id, patterns } of sectorMapping) {
    if (patterns.some((p) => text.includes(p))) return id;
  }

  return null;
}

/**
 * Extract acquiring party and target from merger/concentration content.
 */
function extractMergerParties(
  $: cheerio.CheerioAPI,
  bodyText: string,
): { acquiring: string | null; target: string | null } {
  let acquiring: string | null = null;
  let target: string | null = null;

  // Look for labelled fields
  $("dt, strong, b, .field-label, th").each((_i, el) => {
    const label = $(el).text().trim().toLowerCase().replace(/:$/, "");
    const value =
      $(el).next("dd, .field-item, td").text().trim() ||
      $(el).parent().text().replace($(el).text(), "").trim();

    if (!value || value.length < 2 || value.length > 300) return;

    if (
      label.includes("acquiring") ||
      label.includes("notifying party") ||
      label.includes("acquirer") ||
      label.includes("purchaser")
    ) {
      acquiring = value;
    }
    if (
      label.includes("target") ||
      label.includes("acquired undertaking") ||
      label.includes("target undertaking")
    ) {
      target = value;
    }
  });

  if (acquiring && target) return { acquiring, target };

  // Pattern: "acquisition of X by Y" or "acquisition by Y of X"
  const byOfMatch = bodyText.match(
    /acquisition\s+(?:of\s+(?:sole\s+|joint\s+)?(?:control\s+(?:of|over)\s+)?)?(.+?)\s+by\s+(.+?)(?:\.|,|\s+was\s)/i,
  );
  if (byOfMatch) {
    if (!target) target = byOfMatch[1]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
    if (!acquiring) acquiring = byOfMatch[2]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
  }

  // Pattern: "between X and Y"
  if (!acquiring || !target) {
    const betweenMatch = bodyText.match(
      /(?:between|notification.*?filed.*?by)\s+(.+?)\s+and\s+(.+?)(?:\s+was|\s*[.,])/i,
    );
    if (betweenMatch) {
      if (!acquiring) acquiring = betweenMatch[1]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
      if (!target) target = betweenMatch[2]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
    }
  }

  // Pattern: "merger of X and Y"
  if (!acquiring || !target) {
    const mergerMatch = bodyText.match(
      /merger\s+(?:of|between)\s+(.+?)\s+and\s+(.+?)(?:\s+was|\s*[.,])/i,
    );
    if (mergerMatch) {
      if (!acquiring) acquiring = mergerMatch[1]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
      if (!target) target = mergerMatch[2]?.trim().replace(/\s*\([^)]+\)\s*$/, "") ?? null;
    }
  }

  return { acquiring, target };
}

/**
 * Extract a contentId from a URL.
 */
function contentIdFromUrl(url: string): string {
  const match = url.match(/contentId=(\d+)/);
  return match?.[1] ?? "0";
}

// ---------------------------------------------------------------------------
// Main page processing
// ---------------------------------------------------------------------------

/**
 * Process a single MCCAA content page and return a parsed decision or merger,
 * or null if the page is not relevant competition content.
 */
async function processContentPage(
  url: string,
): Promise<
  | { kind: "decision"; data: ParsedDecision }
  | { kind: "merger"; data: ParsedMerger }
  | null
> {
  const html = await rateLimitedFetch(url);
  if (!html) return null;

  const $ = cheerio.load(html);

  // Skip non-competition content
  if (!isCompetitionContent($)) {
    return null;
  }

  const contentId = contentIdFromUrl(url);
  const title = extractTitle($);
  const bodyText = extractBodyText($);

  if (bodyText.length < 50) {
    console.warn(`    [SKIP] Page too short: ${url}`);
    return null;
  }

  const date = extractDate($, bodyText);
  const sector = classifySector(title, bodyText);
  const summary = extractSummary($, bodyText);
  const legalArticles = extractLegalArticles(bodyText);
  const caseNumber = extractCaseNumber($, bodyText, contentId);

  // Classify as merger or decision
  if (isMergerContent($)) {
    const { acquiring, target } = extractMergerParties($, bodyText);
    const outcome = classifyMergerOutcome(title, bodyText);

    const merger: ParsedMerger = {
      case_number: caseNumber,
      title,
      date,
      sector,
      acquiring_party: acquiring,
      target,
      summary,
      full_text: bodyText,
      outcome,
      turnover: null, // MCCAA does not typically publish turnover figures on web pages
    };

    return { kind: "merger", data: merger };
  }

  // Competition enforcement decision
  const parties = extractParties($, bodyText);
  const { type, outcome } = classifyDecisionType(title, bodyText);
  const fineAmount = extractFineAmount(bodyText);

  const decision: ParsedDecision = {
    case_number: caseNumber,
    title,
    date,
    type,
    sector,
    parties: parties.length > 0 ? JSON.stringify(parties) : null,
    summary,
    full_text: bodyText,
    outcome,
    fine_amount: fineAmount,
    gwb_articles: legalArticles.length > 0 ? JSON.stringify(legalArticles) : null,
    status: "final",
  };

  return { kind: "decision", data: decision };
}

// ---------------------------------------------------------------------------
// Database operations
// ---------------------------------------------------------------------------

function initDb(): Database.Database {
  const dir = dirname(DB_PATH);
  if (!existsSync(dir)) {
    mkdirSync(dir, { recursive: true });
  }

  if (force && existsSync(DB_PATH)) {
    unlinkSync(DB_PATH);
    console.log(`Deleted existing database: ${DB_PATH}`);
  }

  const db = new Database(DB_PATH);
  db.pragma("journal_mode = WAL");
  db.pragma("foreign_keys = ON");
  db.exec(SCHEMA_SQL);
  return db;
}

function upsertDecision(db: Database.Database, d: ParsedDecision): boolean {
  const existing = db
    .prepare("SELECT id FROM decisions WHERE case_number = ?")
    .get(d.case_number) as { id: number } | undefined;

  if (existing) {
    db.prepare(
      `UPDATE decisions SET title=?, date=?, type=?, sector=?, parties=?,
       summary=?, full_text=?, outcome=?, fine_amount=?, gwb_articles=?, status=?
       WHERE case_number=?`,
    ).run(
      d.title,
      d.date,
      d.type,
      d.sector,
      d.parties,
      d.summary,
      d.full_text,
      d.outcome,
      d.fine_amount,
      d.gwb_articles,
      d.status,
      d.case_number,
    );
    return false; // updated, not new
  }

  db.prepare(
    `INSERT INTO decisions (case_number, title, date, type, sector, parties,
     summary, full_text, outcome, fine_amount, gwb_articles, status)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    d.case_number,
    d.title,
    d.date,
    d.type,
    d.sector,
    d.parties,
    d.summary,
    d.full_text,
    d.outcome,
    d.fine_amount,
    d.gwb_articles,
    d.status,
  );
  return true; // new
}

function upsertMerger(db: Database.Database, m: ParsedMerger): boolean {
  const existing = db
    .prepare("SELECT id FROM mergers WHERE case_number = ?")
    .get(m.case_number) as { id: number } | undefined;

  if (existing) {
    db.prepare(
      `UPDATE mergers SET title=?, date=?, sector=?, acquiring_party=?, target=?,
       summary=?, full_text=?, outcome=?, turnover=?
       WHERE case_number=?`,
    ).run(
      m.title,
      m.date,
      m.sector,
      m.acquiring_party,
      m.target,
      m.summary,
      m.full_text,
      m.outcome,
      m.turnover,
      m.case_number,
    );
    return false;
  }

  db.prepare(
    `INSERT INTO mergers (case_number, title, date, sector, acquiring_party, target,
     summary, full_text, outcome, turnover)
     VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  ).run(
    m.case_number,
    m.title,
    m.date,
    m.sector,
    m.acquiring_party,
    m.target,
    m.summary,
    m.full_text,
    m.outcome,
    m.turnover,
  );
  return true;
}

function updateSectorCounts(db: Database.Database, sectors: SectorAccumulator): void {
  const upsert = db.prepare(
    `INSERT INTO sectors (id, name, name_en, description, decision_count, merger_count)
     VALUES (?, ?, ?, ?, ?, ?)
     ON CONFLICT(id) DO UPDATE SET
       decision_count = excluded.decision_count,
       merger_count = excluded.merger_count`,
  );

  const transaction = db.transaction(() => {
    for (const [id, s] of Object.entries(sectors)) {
      upsert.run(id, s.name, s.name_en, s.description, s.decisionCount, s.mergerCount);
    }
  });

  transaction();
}

// ---------------------------------------------------------------------------
// Sector accumulation
// ---------------------------------------------------------------------------

const SECTOR_DEFINITIONS: Record<
  string,
  { name: string; name_en: string; description: string }
> = {
  financial_services: {
    name: "Financial services",
    name_en: "Financial services",
    description:
      "Banking, insurance, payment services, investment products, and capital markets in Malta.",
  },
  gaming: {
    name: "Gaming",
    name_en: "Gaming",
    description:
      "Online gaming operators, gaming software providers, and B2B gaming services licensed by the Malta Gaming Authority.",
  },
  telecommunications: {
    name: "Telecommunications",
    name_en: "Telecommunications",
    description:
      "Mobile communications, fixed broadband, internet services, and data centre infrastructure.",
  },
  energy: {
    name: "Energy",
    name_en: "Energy",
    description:
      "Electricity generation and distribution, petroleum imports, LNG, and renewable energy.",
  },
  tourism: {
    name: "Tourism and hospitality",
    name_en: "Tourism and hospitality",
    description:
      "Hotels, tour operators, airlines, car hire, restaurants, and related tourism services.",
  },
  healthcare: {
    name: "Healthcare",
    name_en: "Healthcare",
    description:
      "Hospitals, pharmacies, medical devices, pharmaceutical distribution, and health services.",
  },
  retail: {
    name: "Retail",
    name_en: "Retail",
    description:
      "Supermarkets, grocery retail, convenience stores, and general retail distribution.",
  },
  construction: {
    name: "Construction and property",
    name_en: "Construction and property",
    description:
      "Building construction, property development, quarrying, cement, and aggregate supplies.",
  },
  transport: {
    name: "Transport and logistics",
    name_en: "Transport and logistics",
    description:
      "Shipping, freight, port services, public transport, and logistics providers.",
  },
  digital_economy: {
    name: "Digital economy",
    name_en: "Digital economy",
    description:
      "Software, IT services, data centres, digital platforms, and cloud services.",
  },
  media: {
    name: "Media and broadcasting",
    name_en: "Media and broadcasting",
    description:
      "Television, radio, press, online media, and advertising services.",
  },
  professional_services: {
    name: "Professional services",
    name_en: "Professional services",
    description:
      "Audit, accountancy, legal services, consulting, and advisory firms.",
  },
  manufacturing: {
    name: "Manufacturing",
    name_en: "Manufacturing",
    description:
      "Industrial manufacturing, processing, and production facilities in Malta.",
  },
};

function buildSectorAccumulator(
  decisions: ParsedDecision[],
  mergers: ParsedMerger[],
): SectorAccumulator {
  const acc: SectorAccumulator = {};

  // Initialise all known sectors
  for (const [id, def] of Object.entries(SECTOR_DEFINITIONS)) {
    acc[id] = {
      name: def.name,
      name_en: def.name_en,
      description: def.description,
      decisionCount: 0,
      mergerCount: 0,
    };
  }

  for (const d of decisions) {
    if (d.sector && acc[d.sector]) {
      acc[d.sector]!.decisionCount++;
    }
  }

  for (const m of mergers) {
    if (m.sector && acc[m.sector]) {
      acc[m.sector]!.mergerCount++;
    }
  }

  // Remove sectors with zero activity
  for (const [id, s] of Object.entries(acc)) {
    if (s.decisionCount === 0 && s.mergerCount === 0) {
      delete acc[id];
    }
  }

  return acc;
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

async function main(): Promise<void> {
  console.log("=== MCCAA Ingestion Crawler ===\n");
  console.log(`  Database: ${DB_PATH}`);
  console.log(`  Dry run:  ${dryRun}`);
  console.log(`  Resume:   ${resume}`);
  console.log(`  Force:    ${force}`);
  if (maxPagesOverride) {
    console.log(`  Max pages: ${maxPagesOverride}`);
  }
  console.log();

  const state = loadState();
  if (resume) {
    console.log(
      `  Resuming from previous run (${state.processedUrls.length} URLs already processed)`,
    );
  }

  // Phase 1: Discover content URLs
  console.log("\n--- Phase 1: URL Discovery ---");
  const contentUrls = await discoverContentUrls(state);

  if (contentUrls.length === 0) {
    console.log("\n  No new URLs to process. Use --force to re-process all.");
    return;
  }

  // Phase 2: Fetch and parse each content page
  console.log(`\n--- Phase 2: Processing ${contentUrls.length} content pages ---`);

  const allDecisions: ParsedDecision[] = [];
  const allMergers: ParsedMerger[] = [];
  let skipped = 0;
  let errors = 0;

  let db: Database.Database | null = null;
  if (!dryRun) {
    db = initDb();
  }

  for (let i = 0; i < contentUrls.length; i++) {
    const url = contentUrls[i]!;
    const progress = `[${i + 1}/${contentUrls.length}]`;

    try {
      const result = await processContentPage(url);

      if (!result) {
        skipped++;
        if (i % 10 === 0) {
          console.log(
            `  ${progress} Skipped (not competition content): ${url}`,
          );
        }
        state.processedUrls.push(url);
        continue;
      }

      if (result.kind === "decision") {
        console.log(
          `  ${progress} DECISION: ${result.data.case_number} — ${result.data.title.slice(0, 60)}`,
        );
        allDecisions.push(result.data);

        if (db) {
          const isNew = upsertDecision(db, result.data);
          if (isNew) state.decisionsIngested++;
        }
      } else {
        console.log(
          `  ${progress} MERGER: ${result.data.case_number} — ${result.data.title.slice(0, 60)}`,
        );
        allMergers.push(result.data);

        if (db) {
          const isNew = upsertMerger(db, result.data);
          if (isNew) state.mergersIngested++;
        }
      }

      state.processedUrls.push(url);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`  ${progress} ERROR processing ${url}: ${message}`);
      state.errors.push(`${url}: ${message}`);
      errors++;
    }

    // Save state periodically (every 20 pages)
    if (i % 20 === 0 && !dryRun) {
      saveState(state);
    }
  }

  // Phase 3: Update sector counts
  if (db && (allDecisions.length > 0 || allMergers.length > 0)) {
    console.log("\n--- Phase 3: Updating sector counts ---");
    const sectors = buildSectorAccumulator(allDecisions, allMergers);
    updateSectorCounts(db, sectors);
    console.log(`  Updated ${Object.keys(sectors).length} sectors`);
  }

  // Save final state
  if (!dryRun) {
    saveState(state);
  }

  // Close database
  if (db) {
    const dCnt = (
      db.prepare("SELECT count(*) as cnt FROM decisions").get() as {
        cnt: number;
      }
    ).cnt;
    const mCnt = (
      db.prepare("SELECT count(*) as cnt FROM mergers").get() as {
        cnt: number;
      }
    ).cnt;
    const sCnt = (
      db.prepare("SELECT count(*) as cnt FROM sectors").get() as {
        cnt: number;
      }
    ).cnt;

    db.close();

    console.log(`\n--- Summary ---`);
    console.log(`  Database totals: ${dCnt} decisions, ${mCnt} mergers, ${sCnt} sectors`);
  } else {
    console.log(`\n--- Summary (dry run) ---`);
  }

  console.log(`  This run: ${allDecisions.length} decisions, ${allMergers.length} mergers`);
  console.log(`  Skipped: ${skipped} (not competition content)`);
  console.log(`  Errors: ${errors}`);
  console.log(`  State file: ${STATE_FILE}`);

  if (state.errors.length > 0) {
    console.log(`\n  Recent errors:`);
    for (const e of state.errors.slice(-5)) {
      console.log(`    - ${e}`);
    }
  }

  if (dryRun) {
    console.log("\n  [DRY RUN] No data was written to the database.");
  }

  console.log(`\nDone.`);
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
