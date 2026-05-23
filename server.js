import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "2mb" }));

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  if (req.method === "OPTIONS") return res.sendStatus(204);
  next();
});

const PORT = process.env.PORT || 10000;
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "cleanslate-c9be5";
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN || "";
const CLEANSLATE_USER_ID = process.env.CLEANSLATE_USER_ID || "";
const POLL_MS = Number(process.env.POLL_MS || 5000);
const SCAN_PAGE_SIZE = Number(process.env.SCAN_PAGE_SIZE || 500);
const SCAN_MESSAGE_CONCURRENCY = Number(process.env.SCAN_MESSAGE_CONCURRENCY || 12);
const MAX_SCAN_PAGES_PER_CYCLE = Number(process.env.MAX_SCAN_PAGES_PER_CYCLE || 10);
const MAX_JOBS_PER_CYCLE = Number(process.env.MAX_JOBS_PER_CYCLE || 10);
const MAX_MESSAGES_PER_SENDER = Number(process.env.MAX_MESSAGES_PER_SENDER || 25000);

let scanActive = false;
let cleanupActive = false;

if (!admin.apps.length) {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!raw) console.error("Missing FIREBASE_SERVICE_ACCOUNT_JSON");
  else {
    try {
      const serviceAccount = JSON.parse(raw);
      admin.initializeApp({ credential: admin.credential.cert(serviceAccount), projectId: PROJECT_ID });
      console.log("Firebase Admin initialized");
    } catch (error) {
      console.error("Firebase Admin initialization failed:", error.message || error);
    }
  }
}

function hasFirebase() { return admin.apps.length > 0; }
function hasGoogleOAuth() { return Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET && GOOGLE_REFRESH_TOKEN); }
function hasUser() { return Boolean(CLEANSLATE_USER_ID); }
function db() { return admin.firestore(); }

function assertReady() {
  if (!hasFirebase()) throw new Error("Firebase Admin is not initialized.");
  if (!hasGoogleOAuth()) throw new Error("Missing Google OAuth variables.");
  if (!hasUser()) throw new Error("Missing CLEANSLATE_USER_ID.");
}

function oauthClient() {
  const client = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET);
  client.setCredentials({ refresh_token: GOOGLE_REFRESH_TOKEN });
  return client;
}

async function gmailClient() {
  return google.gmail({ version: "v1", auth: oauthClient() });
}

function header(headers, name) {
  return (headers || []).find(h => (h.name || "").toLowerCase() === name.toLowerCase())?.value || "";
}

function parseFrom(value) {
  const input = value || "";
  const match = input.match(/"?([^"<]+)"?\s*<([^>]+)>/);
  if (match) return { name: match[1].trim().replace(/^"|"$/g, ""), email: match[2].trim().toLowerCase() };
  const email = (input.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i) || [""])[0].toLowerCase();
  return { name: email ? email.split("@")[0] : input || "Unknown", email: email || input.toLowerCase() };
}

function domainOf(email) {
  return String(email || "").split("@").pop().toLowerCase().replace(/^www\./, "");
}

function cleanKey(value) {
  return String(value || "unknown").toLowerCase().replace(/[.#$/\[\]]/g, "_").slice(0, 180);
}

function extractUnsubscribe(headers) {
  const raw = header(headers, "List-Unsubscribe");
  const oneClick = Boolean(header(headers, "List-Unsubscribe-Post"));
  const bracketUrls = [...String(raw || "").matchAll(/<([^>]+)>/g)].map(m => m[1]);
  const splitUrls = String(raw || "").split(",").map(x => x.trim());
  const urls = [...bracketUrls, ...splitUrls].filter(Boolean);
  return {
    unsubUrl: urls.find(u => /^https?:\/\//i.test(u) && !/example\.com/i.test(u)) || "",
    unsubMailto: urls.find(u => /^mailto:/i.test(u)) || "",
    oneClick
  };
}

const protectedWords = [
  "bank","credit","capital one","fidelity","mortgage","loan","insurance","geico","progressive",
  "doctor","medical","hospital","mychart","pharmacy","irs","tax","payroll","w2","receipt",
  "order","invoice","payment","statement","bill","utility","honda","acura","regal","google",
  "apple","amazon","walmart","netflix","school","daycare","paypal","zelle","wallet"
];

function classifySender(sender) {
  const text = `${sender.name} ${sender.email} ${sender.domain}`.toLowerCase();
  if (protectedWords.some(word => text.includes(word))) return "safe";
  if (sender.unsubUrl || sender.unsubMailto) return "cleanup";
  return "review";
}

async function mapConcurrent(items, limit, fn) {
  let index = 0;
  const workers = Array.from({ length: Math.min(limit, items.length) }, async () => {
    while (index < items.length) {
      const item = items[index++];
      await fn(item);
    }
  });
  await Promise.all(workers);
}

async function saveScanState(state) {
  await db().collection("users").doc(CLEANSLATE_USER_ID).collection("state").doc("scan").set({
    ...state,
    updatedAt: new Date().toISOString()
  }, { merge: true });
}

async function loadScanState() {
  const ref = db().collection("users").doc(CLEANSLATE_USER_ID).collection("state").doc("scan");
  const snap = await ref.get();
  if (!snap.exists) {
    const initial = { total: 0, pages: 0, nextPageToken: "", running: false, done: false };
    await ref.set({ ...initial, updatedAt: new Date().toISOString() }, { merge: true });
    return initial;
  }
  return snap.data() || {};
}

async function upsertSender(sender) {
  const key = sender.key;
  const ref = db().collection("users").doc(CLEANSLATE_USER_ID).collection("senders").doc(key);
  await db().runTransaction(async tx => {
    const snap = await tx.get(ref);
    const old = snap.exists ? snap.data() : {};
    const merged = {
      ...old,
      ...sender,
      count: Number(old.count || 0) + Number(sender.count || 1),
      bucket: old.protected ? "safe" : classifySender({ ...old, ...sender }),
      updatedAt: new Date().toISOString()
    };
    tx.set(ref, merged, { merge: true });
  });
}

async function processScanCycle() {
  assertReady();
  let state = await loadScanState();
  if (!state.running || state.done) return { scanned: 0, reason: "not_running" };

  const gmail = await gmailClient();
  let scanned = 0;

  for (let pageNum = 0; pageNum < MAX_SCAN_PAGES_PER_CYCLE; pageNum++) {
    state = await loadScanState();
    if (!state.running || state.done) break;

    const list = await gmail.users.messages.list({
      userId: "me",
      maxResults: SCAN_PAGE_SIZE,
      pageToken: state.nextPageToken || undefined
    });

    const messages = list.data.messages || [];
    if (!messages.length) {
      state.done = true;
      state.running = false;
      await saveScanState(state);
      break;
    }

    await mapConcurrent(messages, SCAN_MESSAGE_CONCURRENCY, async (m) => {
      try {
        const msg = await gmail.users.messages.get({
          userId: "me",
          id: m.id,
          format: "metadata",
          metadataHeaders: ["From", "List-Unsubscribe", "List-Unsubscribe-Post"]
        });
        const headers = msg.data.payload?.headers || [];
        const from = header(headers, "From");
        if (!from) return;
        const parsed = parseFrom(from);
        const domain = domainOf(parsed.email || parsed.name);
        const unsub = extractUnsubscribe(headers);
        const key = cleanKey(parsed.email || domain || parsed.name);
        await upsertSender({
          key,
          name: parsed.name,
          email: parsed.email,
          domain,
          count: 1,
          ...unsub
        });
      } catch (error) {
        console.error("Message scan failed:", error.message || error);
      }
    });

    scanned += messages.length;
    state.total = Number(state.total || 0) + messages.length;
    state.pages = Number(state.pages || 0) + 1;
    state.nextPageToken = list.data.nextPageToken || "";
    state.running = true;
    state.done = !state.nextPageToken;
    if (state.done) state.running = false;
    await saveScanState(state);
    if (!state.nextPageToken) break;
  }

  return { scanned, state };
}

function senderQuery(job) {
  if (job.email) return `from:${job.email}`;
  if (job.domain) return `from:${job.domain}`;
  return "";
}

async function listAllMessageIds(gmail, q, limit = MAX_MESSAGES_PER_SENDER) {
  const ids = [];
  let pageToken = undefined;
  while (ids.length < limit) {
    const res = await gmail.users.messages.list({ userId: "me", maxResults: 500, q, pageToken });
    for (const message of res.data.messages || []) {
      ids.push(message.id);
      if (ids.length >= limit) break;
    }
    pageToken = res.data.nextPageToken;
    if (!pageToken) break;
  }
  return ids;
}

async function batchModify(gmail, ids, addLabelIds = [], removeLabelIds = []) {
  for (let i = 0; i < ids.length; i += 1000) {
    await gmail.users.messages.batchModify({
      userId: "me",
      requestBody: { ids: ids.slice(i, i + 1000), addLabelIds, removeLabelIds }
    });
  }
}

async function processCleanupQueueOnce() {
  assertReady();
  const gmail = await gmailClient();
  const queue = await db()
    .collection("users").doc(CLEANSLATE_USER_ID)
    .collection("cleanupQueue")
    .where("status", "in", ["queued", "retry"])
    .limit(MAX_JOBS_PER_CYCLE)
    .get();

  let processed = 0;
  let skippedSafe = 0;

  for (const docSnap of queue.docs) {
    const job = docSnap.data();
    if (job.bucket === "safe" || job.protected) {
      await docSnap.ref.set({ status: "skipped_safe", workerStatus: "skipped_safe", updatedAt: new Date().toISOString() }, { merge: true });
      skippedSafe++;
      continue;
    }

    const q = senderQuery(job);
    if (!q) continue;

    await docSnap.ref.set({ status: "processing", workerStatus: "processing", updatedAt: new Date().toISOString() }, { merge: true });

    const ids = await listAllMessageIds(gmail, q);
    if (ids.length) await batchModify(gmail, ids, ["TRASH"], []);

    await docSnap.ref.set({
      status: "complete",
      workerStatus: "complete",
      deleteCount: ids.length,
      updatedAt: new Date().toISOString()
    }, { merge: true });

    await db().collection("users").doc(CLEANSLATE_USER_ID).collection("cleanupHistory").doc(docSnap.id).set({
      ...job,
      deleteCount: ids.length,
      processedAt: new Date().toISOString()
    }, { merge: true });

    processed++;
  }

  return { processed, skippedSafe };
}

function healthPayload() {
  return {
    ok: true,
    service: "CleanSlate Render Worker",
    projectId: PROJECT_ID,
    hasFirebase: hasFirebase(),
    hasGoogleOAuth: hasGoogleOAuth(),
    hasUser: hasUser(),
    scanActive,
    cleanupActive,
    pollMs: POLL_MS,
    maxScanPagesPerCycle: MAX_SCAN_PAGES_PER_CYCLE,
    scanMessageConcurrency: SCAN_MESSAGE_CONCURRENCY
  };
}

app.get("/", (req, res) => res.json(healthPayload()));
app.get("/status", (req, res) => res.json(healthPayload()));

app.post("/scan/start", async (req, res) => {
  try {
    assertReady();
    const state = await loadScanState();
    await saveScanState({ ...state, running: true, done: false });
    if (!scanActive) runScanLoop();
    res.json({ ok: true, message: "scan_started" });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || String(error) });
  }
});

app.post("/scan/pause", async (req, res) => {
  try {
    assertReady();
    const state = await loadScanState();
    await saveScanState({ ...state, running: false });
    res.json({ ok: true, message: "scan_pause_requested" });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || String(error) });
  }
});

app.post("/scan/reset", async (req, res) => {
  try {
    assertReady();
    const state = { total: 0, pages: 0, nextPageToken: "", running: false, done: false };
    await saveScanState(state);
    res.json({ ok: true, message: "scan_reset" });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || String(error) });
  }
});

app.post("/run-once", async (req, res) => {
  try {
    const result = await processCleanupQueueOnce();
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || String(error) });
  }
});

async function runScanLoop() {
  if (scanActive) return;
  scanActive = true;
  try {
    while (true) {
      const state = await loadScanState();
      if (!state.running || state.done) break;
      const result = await processScanCycle();
      console.log("Scan cycle:", result.scanned);
      if (!result.scanned) break;
    }
  } catch (error) {
    console.error("Scan loop error:", error.message || error);
  } finally {
    scanActive = false;
  }
}

async function runCleanupLoop() {
  if (cleanupActive) return;
  cleanupActive = true;
  try {
    const result = await processCleanupQueueOnce();
    if (result.processed || result.skippedSafe) console.log("Cleanup cycle:", result);
  } catch (error) {
    console.error("Cleanup loop error:", error.message || error);
  } finally {
    cleanupActive = false;
  }
}

app.listen(PORT, () => {
  console.log(`CleanSlate server-side worker listening on ${PORT}`);
});

setInterval(async () => {
  try {
    if (!hasFirebase() || !hasGoogleOAuth() || !hasUser()) return;
    const state = await loadScanState();
    if (state.running && !state.done) runScanLoop();
    runCleanupLoop();
  } catch (error) {
    console.error("Background interval error:", error.message || error);
  }
}, POLL_MS);
