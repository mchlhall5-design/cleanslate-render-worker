import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";

const app = express();

app.use(express.json({ limit: "2mb" }));

app.use((req, res, next) => {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");

  if (req.method === "OPTIONS") {
    return res.sendStatus(204);
  }

  next();
});

const PORT = process.env.PORT || 10000;

const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "cleanslate-c9be5";

const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";

const CLEANSLATE_USER_ID = process.env.CLEANSLATE_USER_ID || "";

const WORKER_URL = (
  process.env.WORKER_URL ||
  "https://cleanslate-render-worker.onrender.com"
).replace(/\/$/, "");

const POLL_MS = Number(process.env.POLL_MS || 3000);

const SCAN_PAGE_SIZE = Number(process.env.SCAN_PAGE_SIZE || 500);

const SCAN_MESSAGE_CONCURRENCY = Number(
  process.env.SCAN_MESSAGE_CONCURRENCY || 8
);

const MAX_SCAN_PAGES_PER_CYCLE = Number(
  process.env.MAX_SCAN_PAGES_PER_CYCLE || 3
);

const MESSAGE_DELAY_MS = Number(
  process.env.MESSAGE_DELAY_MS || 120
);

const RATE_LIMIT_BACKOFF_MS = Number(
  process.env.RATE_LIMIT_BACKOFF_MS || 70000
);

const MAX_JOBS_PER_CYCLE = Number(
  process.env.MAX_JOBS_PER_CYCLE || 5
);

const MAX_MESSAGES_PER_SENDER = Number(
  process.env.MAX_MESSAGES_PER_SENDER || 25000
);

let scanActive = false;
let cleanupActive = false;

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

function isRateLimit(error) {
  const text = String(
    error?.message ||
    error?.response?.data?.error?.message ||
    error ||
    ""
  ).toLowerCase();

  const code = error?.code || error?.response?.status;

  return (
    code === 429 ||
    text.includes("quota exceeded") ||
    text.includes("rate limit") ||
    text.includes("user-rate-limit-exceeded")
  );
}

if (!admin.apps.length) {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;

  if (!raw) {
    console.error("Missing FIREBASE_SERVICE_ACCOUNT_JSON");
  } else {
    try {
      const sa = JSON.parse(raw);

      admin.initializeApp({
        credential: admin.credential.cert(sa),
        projectId: PROJECT_ID,
      });

      console.log("Firebase Admin initialized");
    } catch (e) {
      console.error(
        "Firebase Admin initialization failed:",
        e.message || e
      );
    }
  }
}

const hasFirebase = () => admin.apps.length > 0;

const hasGoogleClient = () =>
  Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET);

const hasUser = () =>
  Boolean(CLEANSLATE_USER_ID);

const db = () => admin.firestore();

const userDoc = () =>
  db().collection("users").doc(CLEANSLATE_USER_ID);

const stateRef = () =>
  userDoc().collection("state").doc("scan");

const oauthRef = () =>
  userDoc().collection("secrets").doc("gmailOAuth");

async function getStoredRefreshToken() {
  if (!hasFirebase() || !hasUser()) return "";

  const s = await oauthRef().get();

  return s.exists ? (s.data().refreshToken || "") : "";
}

async function saveScanState(patch) {
  await stateRef().set(
    {
      ...patch,
      updatedAt: new Date().toISOString(),
    },
    { merge: true }
  );
}

async function loadScanState() {
  const s = await stateRef().get();

  if (!s.exists) {
    const i = {
      total: 0,
      pages: 0,
      nextPageToken: "",
      running: false,
      done: false,
      lastError: "",
    };

    await saveScanState(i);

    return i;
  }

  return s.data() || {};
}

function assertReadyForFirebase() {
  if (!hasFirebase()) {
    throw new Error("Firebase Admin is not initialized.");
  }

  if (!hasUser()) {
    throw new Error("Missing CLEANSLATE_USER_ID.");
  }
}

async function assertReadyForGmail() {
  assertReadyForFirebase();

  if (!hasGoogleClient()) {
    throw new Error(
      "Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET."
    );
  }

  if (!(await getStoredRefreshToken())) {
    throw new Error(
      "Gmail is not connected to Render yet."
    );
  }
}

function oauthClient(refreshToken = "") {
  const c = new google.auth.OAuth2(
    GOOGLE_CLIENT_ID,
    GOOGLE_CLIENT_SECRET,
    `${WORKER_URL}/oauth/callback`
  );

  if (refreshToken) {
    c.setCredentials({
      refresh_token: refreshToken,
    });
  }

  return c;
}

async function gmailClient() {
  return google.gmail({
    version: "v1",
    auth: oauthClient(await getStoredRefreshToken()),
  });
}

async function gmailCall(fn, label) {
  while (true) {
    try {
      return await fn();
    } catch (e) {
      if (isRateLimit(e)) {

        const msg =
          `Rate limit hit at ${label}. ` +
          `Waiting ${Math.round(
            RATE_LIMIT_BACKOFF_MS / 1000
          )} seconds then continuing automatically.`;

        console.warn(msg);

        await saveScanState({
          lastError: msg,
          running: true,
          rateLimitedAt: new Date().toISOString(),
        }).catch(() => {});

        await sleep(RATE_LIMIT_BACKOFF_MS);

        continue;
      }

      throw e;
    }
  }
}

async function processOneScanPage(gmail, state) {

  const list = await gmailCall(
    () =>
      gmail.users.messages.list({
        userId: "me",
        maxResults: SCAN_PAGE_SIZE,
        pageToken: state.nextPageToken || undefined,
      }),
    "message list"
  );

  const messages = list.data.messages || [];

  if (!messages.length) {
    await saveScanState({
      running: false,
      done: true,
      lastError: "",
    });

    return {
      scanned: 0,
      done: true,
    };
  }

  let processed = 0;

  for (const m of messages) {

    await sleep(MESSAGE_DELAY_MS);

    try {

      await gmailCall(
        () =>
          gmail.users.messages.get({
            userId: "me",
            id: m.id,
            format: "metadata",
            metadataHeaders: [
              "From",
              "List-Unsubscribe",
            ],
          }),
        "message metadata"
      );

      processed++;

      if (processed % 50 === 0) {
        await saveScanState({
          running: true,
          livePageProgress: processed,
          lastError: "",
        });
      }

    } catch (e) {

      if (isRateLimit(e)) {
        throw e;
      }

      console.error(
        "Message scan failed:",
        e.message || e
      );
    }
  }

  const total =
    Number(state.total || 0) + processed;

  const pages =
    Number(state.pages || 0) + 1;

  const nextPageToken =
    list.data.nextPageToken || "";

  const done = !nextPageToken;

  await saveScanState({
    total,
    pages,
    nextPageToken,
    running: !done,
    done,
    livePageProgress: 0,
    lastPageScanned: processed,
    lastError: "",
  });

  return {
    scanned: processed,
    done,
  };
}

async function processScanCycle() {

  await assertReadyForGmail();

  const gmail = await gmailClient();

  let scanned = 0;

  for (
    let p = 0;
    p < MAX_SCAN_PAGES_PER_CYCLE;
    p++
  ) {

    const s = await loadScanState();

    if (!s.running || s.done) {
      return {
        scanned,
        reason: "not_running_or_done",
      };
    }

    const r = await processOneScanPage(
      gmail,
      s
    );

    scanned += r.scanned || 0;

    if (r.done || !r.scanned) {
      break;
    }
  }

  return { scanned };
}

app.get("/", async (req, res) => {
  res.json({
    ok: true,
    service: "CleanSlate Auto Continue Worker",
    scanActive,
    cleanupActive,
  });
});

app.post("/scan/start", async (req, res) => {

  try {

    await assertReadyForGmail();

    const s = await loadScanState();

    await saveScanState({
      running: true,
      done: false,
      lastError: "",
      startedAt:
        s.startedAt ||
        new Date().toISOString(),
    });

    runScanLoop();

    res.json({
      ok: true,
      message:
        "scan_started_auto_continue",
    });

  } catch (e) {

    await saveScanState({
      lastError: e.message || String(e),
      running: false,
    }).catch(() => {});

    res.status(500).json({
      ok: false,
      error: e.message || String(e),
    });
  }
});

async function runScanLoop() {

  if (scanActive) return;

  scanActive = true;

  try {

    while (true) {

      const s = await loadScanState();

      if (!s.running || s.done) {
        break;
      }

      const r = await processScanCycle();

      console.log("Scan cycle:", r);

      if (!r.scanned) {
        await sleep(10000);
      }
    }

  } catch (e) {

    if (isRateLimit(e)) {

      await saveScanState({
        lastError:
          `Rate limit hit. Waiting ` +
          `${Math.round(
            RATE_LIMIT_BACKOFF_MS / 1000
          )} seconds then continuing automatically.`,
        running: true,
      }).catch(() => {});

      await sleep(RATE_LIMIT_BACKOFF_MS);

      scanActive = false;

      return runScanLoop();
    }

    console.error(
      "Scan loop error:",
      e.message || e
    );

    await saveScanState({
      lastError: e.message || String(e),
      running: false,
    }).catch(() => {});
  } finally {
    scanActive = false;
  }
}

app.listen(PORT, () => {
  console.log(
    `CleanSlate auto-continue worker listening on ${PORT}`
  );
});

setInterval(async () => {

  try {

    if (
      !hasFirebase() ||
      !hasGoogleClient() ||
      !hasUser()
    ) {
      return;
    }

    const s = await loadScanState();

    if (s.running && !s.done) {
      runScanLoop();
    }

  } catch (e) {

    console.error(
      "Background interval error:",
      e.message || e
    );

    await saveScanState({
      lastError: e.message || String(e),
      running: false,
    }).catch(() => {});
  }

}, POLL_MS);
