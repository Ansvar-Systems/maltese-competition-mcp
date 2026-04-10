#!/usr/bin/env node

/**
 * HTTP Server Entry Point for Docker Deployment
 *
 * Provides Streamable HTTP transport for remote MCP clients.
 * Use src/index.ts for local stdio-based usage.
 *
 * Endpoints:
 *   GET  /health  — liveness probe
 *   POST /mcp     — MCP Streamable HTTP (session-aware)
 */

import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { join, dirname } from "node:path";
import { fileURLToPath } from "node:url";
import { randomUUID } from "node:crypto";
import { Server } from "@modelcontextprotocol/sdk/server/index.js";
import { StreamableHTTPServerTransport } from "@modelcontextprotocol/sdk/server/streamableHttp.js";
import {
  CallToolRequestSchema,
  ListToolsRequestSchema,
} from "@modelcontextprotocol/sdk/types.js";
import { z } from "zod";
import {
  searchDecisions,
  getDecision,
  searchMergers,
  getMerger,
  listSectors,
} from "./db.js";
import { buildCitation } from "./utils/citation.js";

const __filename = fileURLToPath(import.meta.url);
const __dirname = dirname(__filename);

const PORT = parseInt(process.env["PORT"] ?? "3000", 10);
const SERVER_NAME = "maltese-competition-mcp";

let pkgVersion = "0.1.0";
try {
  const pkg = JSON.parse(
    readFileSync(join(__dirname, "..", "package.json"), "utf8"),
  ) as { version: string };
  pkgVersion = pkg.version;
} catch {
  // fallback
}

// --- Tool definitions (shared with index.ts) ---------------------------------

const TOOLS = [
  {
    name: "mt_comp_search_decisions",
    description:
      "Full-text search across MCCAA (Malta Competition and Consumer Affairs Authority) enforcement decisions covering abuse of dominance, cartel enforcement, and sector inquiries under the Malta Competition Act (Cap. 379). Returns matching decisions with case number, parties, outcome, fine amount, and legal basis cited.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'abuse of dominance', 'price fixing', 'market power')" },
        type: {
          type: "string",
          enum: ["abuse_of_dominance", "cartel", "merger", "sector_inquiry"],
          description: "Filter by decision type. Optional.",
        },
        sector: { type: "string", description: "Filter by sector ID (e.g., 'financial_services', 'telecommunications', 'gaming'). Optional." },
        outcome: {
          type: "string",
          enum: ["prohibited", "cleared", "cleared_with_conditions", "fine"],
          description: "Filter by outcome. Optional.",
        },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "mt_comp_get_decision",
    description:
      "Get a specific MCCAA decision by case number (e.g., 'CA/001/2023', 'CA/005/2022').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: { type: "string", description: "MCCAA case number (e.g., 'CA/001/2023', 'CA/005/2022')" },
      },
      required: ["case_number"],
    },
  },
  {
    name: "mt_comp_search_mergers",
    description:
      "Search MCCAA merger control decisions. Returns merger cases with acquiring party, target, sector, and outcome.",
    inputSchema: {
      type: "object" as const,
      properties: {
        query: { type: "string", description: "Search query (e.g., 'banking acquisition', 'gaming sector concentration', 'telecommunications merger')" },
        sector: { type: "string", description: "Filter by sector ID (e.g., 'financial_services', 'gaming', 'telecommunications'). Optional." },
        outcome: {
          type: "string",
          enum: ["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"],
          description: "Filter by merger outcome. Optional.",
        },
        limit: { type: "number", description: "Maximum number of results to return. Defaults to 20." },
      },
      required: ["query"],
    },
  },
  {
    name: "mt_comp_get_merger",
    description:
      "Get a specific MCCAA merger control decision by case number (e.g., 'M/001/2023').",
    inputSchema: {
      type: "object" as const,
      properties: {
        case_number: { type: "string", description: "MCCAA merger case number (e.g., 'M/001/2023')" },
      },
      required: ["case_number"],
    },
  },
  {
    name: "mt_comp_list_sectors",
    description:
      "List all sectors with MCCAA enforcement activity, including decision counts and merger counts per sector.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "mt_comp_list_sources",
    description:
      "List all data sources ingested into this MCP, with provenance, licensing, and last-updated metadata.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "mt_comp_check_data_freshness",
    description:
      "Check the freshness of the ingested data. Returns last-updated timestamp, record counts, and whether data may be stale.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
  {
    name: "mt_comp_about",
    description:
      "Return metadata about this MCP server: version, data source, coverage, and tool list.",
    inputSchema: { type: "object" as const, properties: {}, required: [] },
  },
];

// --- Zod schemas -------------------------------------------------------------

const SearchDecisionsArgs = z.object({
  query: z.string().min(1),
  type: z.enum(["abuse_of_dominance", "cartel", "merger", "sector_inquiry"]).optional(),
  sector: z.string().optional(),
  outcome: z.enum(["prohibited", "cleared", "cleared_with_conditions", "fine"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetDecisionArgs = z.object({
  case_number: z.string().min(1),
});

const SearchMergersArgs = z.object({
  query: z.string().min(1),
  sector: z.string().optional(),
  outcome: z.enum(["cleared", "cleared_phase1", "cleared_with_conditions", "prohibited"]).optional(),
  limit: z.number().int().positive().max(100).optional(),
});

const GetMergerArgs = z.object({
  case_number: z.string().min(1),
});

// --- MCP server factory ------------------------------------------------------

function createMcpServer(): Server {
  const server = new Server(
    { name: SERVER_NAME, version: pkgVersion },
    { capabilities: { tools: {} } },
  );

  server.setRequestHandler(ListToolsRequestSchema, async () => ({
    tools: TOOLS,
  }));

  server.setRequestHandler(CallToolRequestSchema, async (request) => {
    const { name, arguments: args = {} } = request.params;

    function responseMeta() {
      return {
        disclaimer: "Research tool only — not regulatory or legal advice. Verify all references against primary sources.",
        data_age: "2026-03-23",
        copyright: "© Malta Competition and Consumer Affairs Authority",
        source_url: "https://www.mccaa.org.mt/",
      };
    }

    function textContent(data: unknown) {
      const payload = typeof data === "object" && data !== null ? data : { value: data };
      return {
        content: [{ type: "text" as const, text: JSON.stringify({ ...payload as object, _meta: responseMeta() }, null, 2) }],
      };
    }

    function errorContent(message: string, errorType = "tool_error") {
      return {
        content: [{ type: "text" as const, text: JSON.stringify({ error: message, _meta: responseMeta(), _error_type: errorType }) }],
        isError: true as const,
      };
    }

    try {
      switch (name) {
        case "mt_comp_search_decisions": {
          const parsed = SearchDecisionsArgs.parse(args);
          const results = searchDecisions({
            query: parsed.query,
            type: parsed.type,
            sector: parsed.sector,
            outcome: parsed.outcome,
            limit: parsed.limit,
          });
          const resultsWithCitation = results.map((r) => ({
            ...r,
            _citation: buildCitation(
              r.case_number,
              r.title,
              "mt_comp_get_decision",
              { case_number: r.case_number },
            ),
          }));
          return textContent({ results: resultsWithCitation, count: results.length });
        }

        case "mt_comp_get_decision": {
          const parsed = GetDecisionArgs.parse(args);
          const decision = getDecision(parsed.case_number);
          if (!decision) {
            return errorContent(`Decision not found: ${parsed.case_number}`, "not_found");
          }
          const d = decision as Record<string, unknown>;
          return textContent({
            ...decision,
            _citation: buildCitation(
              String(d.case_number || parsed.case_number),
              String(d.title || d.case_number || parsed.case_number),
              "mt_comp_get_decision",
              { case_number: parsed.case_number },
              d.source_url as string | undefined,
            ),
          });
        }

        case "mt_comp_search_mergers": {
          const parsed = SearchMergersArgs.parse(args);
          const results = searchMergers({
            query: parsed.query,
            sector: parsed.sector,
            outcome: parsed.outcome,
            limit: parsed.limit,
          });
          const resultsWithCitation = results.map((r) => ({
            ...r,
            _citation: buildCitation(
              r.case_number,
              r.title,
              "mt_comp_get_merger",
              { case_number: r.case_number },
            ),
          }));
          return textContent({ results: resultsWithCitation, count: results.length });
        }

        case "mt_comp_get_merger": {
          const parsed = GetMergerArgs.parse(args);
          const merger = getMerger(parsed.case_number);
          if (!merger) {
            return errorContent(`Merger case not found: ${parsed.case_number}`, "not_found");
          }
          const m = merger as Record<string, unknown>;
          return textContent({
            ...merger,
            _citation: buildCitation(
              String(m.case_number || parsed.case_number),
              String(m.title || m.case_number || parsed.case_number),
              "mt_comp_get_merger",
              { case_number: parsed.case_number },
              m.source_url as string | undefined,
            ),
          });
        }

        case "mt_comp_list_sectors": {
          const sectors = listSectors();
          return textContent({ sectors, count: sectors.length });
        }

        case "mt_comp_list_sources": {
          return textContent({
            sources: [
              {
                id: "mccaa_decisions",
                name: "MCCAA Enforcement Decisions",
                authority: "Malta Competition and Consumer Affairs Authority",
                url: "https://www.mccaa.org.mt/",
                license: "Public domain — official regulatory publications",
                coverage: "Abuse of dominance, cartel enforcement, sector inquiries under Competition Act (Cap. 379)",
                last_updated: "2026-03-23",
              },
              {
                id: "mccaa_mergers",
                name: "MCCAA Merger Control Decisions",
                authority: "Malta Competition and Consumer Affairs Authority",
                url: "https://www.mccaa.org.mt/",
                license: "Public domain — official regulatory publications",
                coverage: "Merger control decisions — Phase I and Phase II",
                last_updated: "2026-03-23",
              },
            ],
          });
        }

        case "mt_comp_check_data_freshness": {
          const db = (await import("./db.js")).getDb();
          const decisionCount = (db.prepare("SELECT COUNT(*) as count FROM decisions").get() as { count: number }).count;
          const mergerCount = (db.prepare("SELECT COUNT(*) as count FROM mergers").get() as { count: number }).count;
          const latestDecision = db.prepare("SELECT MAX(date) as latest FROM decisions").get() as { latest: string | null };
          return textContent({
            last_ingest: "2026-03-23",
            records: {
              decisions: decisionCount,
              mergers: mergerCount,
            },
            latest_decision_date: latestDecision.latest,
            is_stale: false,
          });
        }

        case "mt_comp_about": {
          return textContent({
            name: SERVER_NAME,
            version: pkgVersion,
            description:
              "MCCAA (Malta Competition and Consumer Affairs Authority) MCP server. Provides access to Maltese competition law enforcement decisions, merger control cases, and sector enforcement data under the Competition Act (Cap. 379).",
            data_source: "MCCAA (https://www.mccaa.org.mt/)",
            coverage: {
              decisions: "Abuse of dominance, cartel enforcement, and sector inquiries under Malta Competition Act (Cap. 379)",
              mergers: "Merger control decisions — Phase I and Phase II",
              sectors: "Financial services, gaming, telecommunications, retail, tourism, construction, media",
            },
            tools: TOOLS.map((t) => ({ name: t.name, description: t.description })),
          });
        }

        default:
          return errorContent(`Unknown tool: ${name}`);
      }
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      return errorContent(`Error executing ${name}: ${message}`);
    }
  });

  return server;
}

// --- HTTP server -------------------------------------------------------------

async function main(): Promise<void> {
  const sessions = new Map<
    string,
    { transport: StreamableHTTPServerTransport; server: Server }
  >();

  const httpServer = createServer((req, res) => {
    handleRequest(req, res, sessions).catch((err) => {
      console.error(`[${SERVER_NAME}] Unhandled error:`, err);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ error: "Internal server error" }));
      }
    });
  });

  async function handleRequest(
    req: import("node:http").IncomingMessage,
    res: import("node:http").ServerResponse,
    activeSessions: Map<
      string,
      { transport: StreamableHTTPServerTransport; server: Server }
    >,
  ): Promise<void> {
    const url = new URL(req.url ?? "/", `http://localhost:${PORT}`);

    if (url.pathname === "/health") {
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ status: "ok", server: SERVER_NAME, version: pkgVersion }));
      return;
    }

    if (url.pathname === "/mcp") {
      const sessionId = req.headers["mcp-session-id"] as string | undefined;

      if (sessionId && activeSessions.has(sessionId)) {
        const session = activeSessions.get(sessionId)!;
        await session.transport.handleRequest(req, res);
        return;
      }

      const mcpServer = createMcpServer();
      const transport = new StreamableHTTPServerTransport({
        sessionIdGenerator: () => randomUUID(),
      });

      // eslint-disable-next-line @typescript-eslint/no-explicit-any -- SDK type mismatch with exactOptionalPropertyTypes
      await mcpServer.connect(transport as any);

      transport.onclose = () => {
        if (transport.sessionId) {
          activeSessions.delete(transport.sessionId);
        }
        mcpServer.close().catch(() => {});
      };

      await transport.handleRequest(req, res);

      if (transport.sessionId) {
        activeSessions.set(transport.sessionId, { transport, server: mcpServer });
      }
      return;
    }

    res.writeHead(404, { "Content-Type": "application/json" });
    res.end(JSON.stringify({ error: "Not found" }));
  }

  httpServer.listen(PORT, () => {
    console.error(`${SERVER_NAME} v${pkgVersion} (HTTP) listening on port ${PORT}`);
    console.error(`MCP endpoint:  http://localhost:${PORT}/mcp`);
    console.error(`Health check:  http://localhost:${PORT}/health`);
  });

  process.on("SIGTERM", () => {
    console.error("Received SIGTERM, shutting down...");
    httpServer.close(() => process.exit(0));
  });
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
