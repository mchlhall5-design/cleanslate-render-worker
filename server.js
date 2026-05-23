import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";

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
const WORKER_SECRET = process.env.WORKER_SECRET || "change-me";
const POLL_MS = Number(process.env.POLL_MS || 15000);
const MAX_JOBS_PER_CYCLE = Number(process.env.MAX_JOBS_PER_CYCLE || 10);
const MAX_MESSAGES_PER_SENDER = Number(process.env.MAX_MESSAGES_PER_SENDER || 25000);

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

function db() { return admin.firestore(); }
function hasFirebase() { return admin.apps.length > 0; }
function hasGoogleOAuth() { return Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET && GOOGLE_REFRESH_TOKEN); }
function hasUser() { return Boolean(CLEANSLATE_USER_ID); }

function healthPayload() {
  return {
    ok: true,
    service: "CleanSlate Render Worker",
    projectId: PROJECT_ID,
    hasFirebase: hasFirebase(),
    hasGoogleOAuth: hasGoogleOAuth(),
    hasUser: hasUser(),
    pollMs: POLL_MS,
    maxJobsPerCycle: MAX_JOBS_PER_CYCLE,
    maxMessagesPerSender: MAX_MESSAGES_PER_SENDER
  };
}

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

async function processQueueOnce() {
  assertReady();
  const gmail = await gmailClient();
  const queue = await db()
    .collection("users")
    .doc(CLEANSLATE_USER_ID)
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

    if (ids.length) {
      await batchModify(gmail, ids, ["TRASH"], []);
    }

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

app.get("/", (req, res) => res.json(healthPayload()));
app.get("/status", (req, res) => res.json(healthPayload()));

app.post("/run-once", async (req, res) => {
  try {
    const authRequired = WORKER_SECRET && WORKER_SECRET !== "change-me";
    if (authRequired && req.headers.authorization !== `Bearer ${WORKER_SECRET}`) {
      return res.status(401).json({ ok: false, error: "Unauthorized" });
    }
    const result = await processQueueOnce();
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: error.message || String(error) });
  }
});

app.listen(PORT, () => console.log(`CleanSlate worker listening on ${PORT}`));

setInterval(async () => {
  try {
    if (!hasFirebase() || !hasGoogleOAuth() || !hasUser()) return;
    const result = await processQueueOnce();
    if (result.processed || result.skippedSafe) console.log("Queue cycle:", result);
  } catch (error) {
    console.error("Background queue error:", error.message || error);
  }
}, POLL_MS);
