/**
 * Seed the MCCAA (Malta Competition and Consumer Affairs Authority) database.
 * Usage: npx tsx scripts/seed-sample.ts [--force]
 */
import Database from "better-sqlite3";
import { existsSync, mkdirSync, unlinkSync } from "node:fs";
import { dirname } from "node:path";
import { SCHEMA_SQL } from "../src/db.js";

const DB_PATH = process.env["MCCAA_DB_PATH"] ?? "data/mccaa.db";
const force = process.argv.includes("--force");
const dir = dirname(DB_PATH);
if (!existsSync(dir)) { mkdirSync(dir, { recursive: true }); }
if (force && existsSync(DB_PATH)) { unlinkSync(DB_PATH); console.log(`Deleted ${DB_PATH}`); }
const db = new Database(DB_PATH);
db.pragma("journal_mode = WAL");
db.pragma("foreign_keys = ON");
db.exec(SCHEMA_SQL);
console.log(`Database initialised at ${DB_PATH}`);

// --- Sectors ---
const sectors = [
  { id: "financial_services", name: "Financial services", name_en: "Financial services", description: "Banking, insurance, payment services, and capital markets in Malta.", decision_count: 2, merger_count: 1 },
  { id: "gaming", name: "Gaming", name_en: "Gaming", description: "Online gaming operators, gaming software providers, and B2B gaming services licensed in Malta.", decision_count: 2, merger_count: 1 },
  { id: "telecommunications", name: "Telecommunications", name_en: "Telecommunications", description: "Mobile communications, fixed broadband, and internet services.", decision_count: 1, merger_count: 1 },
  { id: "energy", name: "Energy", name_en: "Energy", description: "Electricity generation and distribution, petroleum imports, and LNG.", decision_count: 1, merger_count: 0 },
  { id: "tourism", name: "Tourism and hospitality", name_en: "Tourism and hospitality", description: "Hotels, tour operators, airlines, car hire, and related tourism services.", decision_count: 1, merger_count: 0 },
];
const insS = db.prepare("INSERT OR IGNORE INTO sectors (id, name, name_en, description, decision_count, merger_count) VALUES (?, ?, ?, ?, ?, ?)");
for (const s of sectors) insS.run(s.id, s.name, s.name_en, s.description, s.decision_count, s.merger_count);
console.log(`Inserted ${sectors.length} sectors`);

// --- Decisions ---
const decisions = [
  {
    case_number: "CA/003/2023", title: "Bank of Valletta / HSBC Malta — Retail Banking Fees Coordination",
    date: "2023-07-20", type: "cartel", sector: "financial_services",
    parties: JSON.stringify(["Bank of Valletta plc", "HSBC Bank Malta plc"]),
    summary: "MCCAA investigated alleged coordination between Malta's two largest retail banks on current account fees and interest rates. The Authority found evidence of information exchange through industry meetings.",
    full_text: "The Office for Competition within MCCAA investigated allegations of fee coordination between Bank of Valletta and HSBC Malta, which together hold approximately 75% of retail banking assets in Malta. Investigation findings: (1) Regular meetings of the Malta Bankers Association involved sharing of commercially sensitive information on planned fee changes; (2) Fee adjustment timelines showed high correlation — 11 of 14 fee changes occurred within 2 weeks of each other over the review period; (3) Some information exchange fell within permitted benchmarking, but specific forward-looking pricing discussions breached Article 5 of the Competition Act (Cap. 379). Fine: EUR 1.8 million imposed on Bank of Valletta and EUR 1.1 million on HSBC Malta. Both banks committed to revising industry association information sharing protocols.",
    outcome: "fine", fine_amount: 2_900_000, gwb_articles: JSON.stringify(["Competition Act Cap. 379, Article 5(1)"]), status: "final",
  },
  {
    case_number: "CA/007/2022", title: "Melita Mobile — Abuse of Dominance in Fixed Broadband",
    date: "2022-09-05", type: "abuse_of_dominance", sector: "telecommunications",
    parties: JSON.stringify(["Melita plc"]),
    summary: "MCCAA found Melita abused its dominant position in fixed broadband through predatory pricing and margin squeeze against competitors using its wholesale network.",
    full_text: "Melita plc operates Malta's largest cable broadband network, with approximately 55% share of fixed broadband subscribers. The investigation arose from complaints by GO plc and Vodafone Malta. Findings: (1) Margin squeeze — retail prices for entry-level broadband packages were below the cost that an equally efficient competitor could profitably offer using Melita's wholesale input; (2) Predatory pricing — for 14 months, Melita offered promotional packages priced below average variable cost; (3) Long-term contracts — 24-month minimum terms with no equivalent competitor offering created artificial switching barriers. The MCCAA ordered Melita to revise its wholesale pricing methodology, limit promotional pricing periods to 3 months, and reduce minimum contract terms to 12 months. Fine: EUR 2.4 million.",
    outcome: "fine", fine_amount: 2_400_000, gwb_articles: JSON.stringify(["Competition Act Cap. 379, Article 9"]), status: "appealed",
  },
  {
    case_number: "CA/001/2023", title: "iGaming B2B Software — Exclusive Licensing Practices",
    date: "2023-02-28", type: "abuse_of_dominance", sector: "gaming",
    parties: JSON.stringify(["Evolution Gaming Malta Ltd"]),
    summary: "MCCAA investigated Evolution Gaming's exclusive licensing practices for live casino software, which required operators to use Evolution exclusively for certain game categories.",
    full_text: "Evolution Gaming Malta Ltd is the dominant live casino software provider to Malta-licensed iGaming operators, with estimated 65-70% share of live casino game traffic. The investigation examined exclusive dealing clauses in Evolution's software licensing agreements. Findings: (1) Exclusivity provisions — agreements required operators to source live roulette and blackjack exclusively from Evolution for 3 years; (2) Market foreclosure — the combination of Evolution's market share and long exclusivity terms effectively foreclosed competing suppliers; (3) Network effects — Evolution's first-mover advantage and proprietary studios create significant entry barriers. The MCCAA ordered Evolution to remove exclusivity provisions from existing contracts and prohibited their inclusion in future contracts. No fine was imposed as Evolution cooperated fully with the investigation.",
    outcome: "prohibited", fine_amount: null, gwb_articles: JSON.stringify(["Competition Act Cap. 379, Article 9(2)(b)"]), status: "final",
  },
  {
    case_number: "CA/005/2023", title: "Enemalta — Petroleum Product Supply Exclusivity",
    date: "2023-10-15", type: "sector_inquiry", sector: "energy",
    parties: JSON.stringify(["Enemalta Corporation"]),
    summary: "MCCAA sector inquiry into petroleum product supply arrangements. Enemalta's historical monopoly in petroleum import and distribution was found to create residual market distortions.",
    full_text: "The MCCAA conducted a sector inquiry into petroleum product distribution in Malta following liberalisation of the sector. Enemalta historically held the exclusive right to import petroleum products; this was liberalised in 2018. The inquiry found: (1) Residual infrastructure access — Enemalta controls the Kalaxlokk petroleum terminal, the only import facility; competing importers must negotiate access, creating dependency; (2) Information asymmetry — Enemalta's pricing data for terminal access is not publicly available, disadvantaging competitors in planning; (3) Pricing effects — retail petrol and diesel prices in Malta remain among the highest in the EU, partly attributable to limited terminal competition. Recommendations: MCCAA recommended the regulator (Malta Resources Authority) implement regulated third-party access to the terminal and publish tariff information.",
    outcome: "cleared_with_conditions", fine_amount: null, gwb_articles: JSON.stringify(["Competition Act Cap. 379, Article 25"]), status: "final",
  },
  {
    case_number: "CA/009/2022", title: "Hotel Cartel — Tourist Accommodation Rate Coordination",
    date: "2022-06-15", type: "cartel", sector: "tourism",
    parties: JSON.stringify(["Malta Hotels and Restaurants Association", "Three hotel groups"]),
    summary: "MCCAA sector investigation into hotel rate information sharing through industry association. Investigation cleared hotel groups but required the association to revise data sharing practices.",
    full_text: "The MCCAA investigated allegations that the Malta Hotels and Restaurants Association (MHRA) facilitated price coordination among major hotel groups. The investigation examined data sharing practices at MHRA industry committees. Findings: (1) Aggregate data sharing — MHRA published monthly occupancy rates and average daily rates broken down by hotel category; this fell within permitted benchmarking; (2) Forward-looking information — some committee meetings involved discussion of planned rate strategies for upcoming seasons, which raised concerns; (3) Dynamic pricing systems — no evidence of direct algorithm sharing between competitors. Outcome: three hotel groups received individual guidance letters on permissible information exchange; MHRA revised its data governance policy to prohibit forward-looking price discussions at committee level.",
    outcome: "cleared", fine_amount: null, gwb_articles: JSON.stringify(["Competition Act Cap. 379, Article 5"]), status: "final",
  },
];

const insD = db.prepare("INSERT OR IGNORE INTO decisions (case_number, title, date, type, sector, parties, summary, full_text, outcome, fine_amount, gwb_articles, status) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insDAll = db.transaction(() => { for (const d of decisions) insD.run(d.case_number, d.title, d.date, d.type, d.sector, d.parties, d.summary, d.full_text, d.outcome, d.fine_amount, d.gwb_articles, d.status); });
insDAll();
console.log(`Inserted ${decisions.length} decisions`);

// --- Mergers ---
const mergers = [
  {
    case_number: "M/001/2023", title: "APS Bank / MeDirect Bank — Retail Banking Merger",
    date: "2023-05-10", sector: "financial_services", acquiring_party: "APS Bank Ltd", target: "MeDirect Bank (Malta) plc",
    summary: "MCCAA cleared APS Bank's acquisition of MeDirect Malta with conditions requiring divestiture of investment services portfolio to preserve competition in savings products.",
    full_text: "APS Bank proposed to acquire MeDirect Bank (Malta), a challenger bank focused on savings and investment products. Combined market share in retail deposits would reach 18%. The MCCAA found: (1) Retail savings — MeDirect's high-yield savings products provided meaningful competitive constraint on incumbents BOV and HSBC; (2) Investment services — combined entity would have 22% of retail investment product market; (3) SME lending — minimal overlap as MeDirect did not offer SME products. Condition: APS Bank required to divest MeDirect's EUR 85 million investment portfolio to a third-party provider within 12 months, maintaining product availability to existing customers.",
    outcome: "cleared_with_conditions", turnover: 800_000_000,
  },
  {
    case_number: "M/003/2022", title: "Kindred Group / Malta-licensed iGaming Portfolio",
    date: "2022-10-20", sector: "gaming", acquiring_party: "Kindred Group plc", target: "Relax Gaming Holdings Ltd",
    summary: "MCCAA cleared Kindred Group's acquisition of Relax Gaming, a B2B iGaming software provider, in Phase 1 finding no competition concerns in Malta-licensed gaming markets.",
    full_text: "Kindred Group, a major B2C online gaming operator, proposed to acquire Relax Gaming Holdings, a B2B gaming software provider. Both entities hold Malta Gaming Authority licences. The MCCAA assessment found: (1) Vertical relationship — Kindred as operator and Relax as software supplier creates potential for input foreclosure; (2) Market position — Relax Gaming holds approximately 8% of Malta-licensed B2B gaming software supply, insufficient for foreclosure concerns; (3) Countervailing supply — numerous alternative B2B suppliers exist including Evolution, Playtech, and IGT. The merger was cleared in Phase 1 without conditions. The MGA was informed of the transaction per its notification requirements.",
    outcome: "cleared_phase1", turnover: 1_400_000_000,
  },
  {
    case_number: "M/005/2023", title: "GO plc / Bmit Technologies — Telecommunications Infrastructure",
    date: "2023-08-25", sector: "telecommunications", acquiring_party: "GO plc", target: "Bmit Technologies Ltd",
    summary: "MCCAA cleared GO plc's acquisition of Bmit Technologies, a data centre and managed IT services provider, finding the transaction does not raise competition concerns in Maltese telecommunications markets.",
    full_text: "GO plc, Malta's incumbent fixed-line telecommunications operator, proposed to acquire Bmit Technologies Ltd, which operates Malta's largest commercial data centre and provides managed IT services. The Commission examined: (1) Data centre services — Bmit is the largest third-party data centre in Malta with approximately 40% of commercial colocation market; (2) Managed services — Bmit provides IT managed services to both private and public sector clients; (3) Vertical integration — GO could favour its own services over competitors in data centre access. The MCCAA cleared the merger with a behavioural condition requiring GO to provide equivalent access to the Bmit data centre to all customers on non-discriminatory terms for a period of 5 years.",
    outcome: "cleared_with_conditions", turnover: 350_000_000,
  },
];

const insM = db.prepare("INSERT OR IGNORE INTO mergers (case_number, title, date, sector, acquiring_party, target, summary, full_text, outcome, turnover) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
const insMAll = db.transaction(() => { for (const m of mergers) insM.run(m.case_number, m.title, m.date, m.sector, m.acquiring_party, m.target, m.summary, m.full_text, m.outcome, m.turnover); });
insMAll();
console.log(`Inserted ${mergers.length} mergers`);

const dCnt = (db.prepare("SELECT count(*) as cnt FROM decisions").get() as { cnt: number }).cnt;
const mCnt = (db.prepare("SELECT count(*) as cnt FROM mergers").get() as { cnt: number }).cnt;
const sCnt = (db.prepare("SELECT count(*) as cnt FROM sectors").get() as { cnt: number }).cnt;
console.log(`\nSummary: ${sCnt} sectors, ${dCnt} decisions, ${mCnt} mergers`);
console.log(`Done. Database ready at ${DB_PATH}`);
db.close();
