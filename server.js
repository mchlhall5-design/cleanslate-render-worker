import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";
import fetch from "node-fetch";

const app = express();
app.use(express.json({ limit: "2mb" }));

const PORT = process.env.PORT || 10000;
const PROJECT_ID = process.env.FIREBASE_PROJECT_ID || "cleanslate-c9be5";
const GOOGLE_CLIENT_ID = process.env.GOOGLE_CLIENT_ID || "";
const GOOGLE_CLIENT_SECRET = process.env.GOOGLE_CLIENT_SECRET || "";
const GOOGLE_REFRESH_TOKEN = process.env.GOOGLE_REFRESH_TOKEN || "";
const CLEANSLATE_USER_ID = process.env.CLEANSLATE_USER_ID || "";
const WORKER_SECRET = process.env.WORKER_SECRET || "change-me";
const POLL_MS = Number(process.env.POLL_MS || 15000);
const MAX_JOBS_PER_CYCLE = Number(process.env.MAX_JOBS_PER_CYCLE || 10);

if (!admin.apps.length) {
  const raw = process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if (!raw) {
    console.error("Missing FIREBASE_SERVICE_ACCOUNT_JSON");
  } else {
    const serviceAccount = JSON.parse(raw);
    admin.initializeApp({
      credential: admin.credential.cert(serviceAccount),
      projectId: PROJECT_ID
    });
  }
}

const db = () => admin.firestore();

function assertReady() {
  if (!admin.apps.length) throw new Error("Firebase Admin is not initialized.");
  if (!GOOGLE_CLIENT_ID || !GOOGLE_CLIENT_SECRET || !GOOGLE_REFRESH_TOKEN) {
    throw new Error("Missing Google OAuth env vars: GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET, GOOGLE_REFRESH_TOKEN.");
  }
  if (!CLEANSLATE_USER_ID) throw new Error("Missing CLEANSLATE_USER_ID.");
}

function oauthClient() {
  const client = new google.auth.OAuth2(GOOGLE_CLIENT_ID, GOOGLE_CLIENT_SECRET);
  client.setCredentials({ refresh_token: GOOGLE_REFRESH_TOKEN });
  return client;
}

function senderQuery(job) {
  if (job.email) return `from:${job.email}`;
  if (job.domain) return `from:${job.domain}`;
  return "";
}

async function gmailClient() {
  const auth = oauthClient();
  return google.gmail({ version: "v1", auth });
}

async function listAllMessageIds(gmail, q, limit = 10000) {
  const ids = [];
  let pageToken = undefined;
  while (ids.length < limit) {
    const res = await gmail.users.messages.list({
      userId: "me",
      maxResults: 500,
      q,
      pageToken
    });
    for (const m of res.data.messages || []) ids.push(m.id);
    pageToken = res.data.nextPageToken;
    if (!pageToken) break;
  }
  return ids;
}

async function batchModify(gmail, ids, addLabelIds = [], removeLabelIds = []) {
  for (let i = 0; i < ids.length; i += 1000) {
    await gmail.users.messages.batchModify({
      userId: "me",
      requestBody: {
        ids: ids.slice(i, i + 1000),
        addLabelIds,
        removeLabelIds
      }
    });
  }
}

async function sendMailtoUnsubscribe(gmail, mailto) {
  const url = mailto.replace(/^mailto:/i, "");
  const [toPart, qs] = url.split("?");
  const params = new URLSearchParams(qs || "");
  const to = decodeURIComponent(toPart);
  const subject = params.get("subject") || "Unsubscribe";
  const body = params.get("body") || "Please unsubscribe me from this mailing list.";

  const rawMessage = [
    `To: ${to}`,
    `Subject: ${subject}`,
    "",
    body
  ].join("\r\n");

  const raw = Buffer.from(rawMessage)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");

  await gmail.users.messages.send({
    userId: "me",
    requestBody: { raw }
  });
}

async function attemptUrlUnsubscribe(url, oneClick) {
  if (!url) return { status: "no_url" };
  try {
    const res = await fetch(url, {
      method: oneClick ? "POST" : "GET",
      redirect: "follow",
      headers: {
        "User-Agent": "CleanSlate unsubscribe worker",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
      }
    });
    return { status: res.ok ? "submitted_url" : "url_response_not_ok", httpStatus: res.status };
  } catch (error) {
    return { status: "manual_or_blocked_url", error: String(error.message || error) };
  }
}

async function processJob(jobRef, job) {
  const gmail = await gmailClient();
  const startedAt = admin.firestore.FieldValue.serverTimestamp();

  await jobRef.set({
    workerStatus: "processing",
    workerStartedAt: startedAt,
    updatedAt: new Date().toISOString()
  }, { merge: true });

  const result = {
    deleteCount: 0,
    archiveCount: 0,
    unsubscribeStatus: "not_attempted",
    manualNeeded: false,
    processedAt: new Date().toISOString()
  };

  try {
    if (job.unsubMailto) {
      await sendMailtoUnsubscribe(gmail, job.unsubMailto);
      result.unsubscribeStatus = "completed_mailto";
    } else if (job.unsubUrl) {
      const urlResult = await attemptUrlUnsubscribe(job.unsubUrl, job.oneClick);
      result.unsubscribeStatus = urlResult.status;
      result.unsubscribeHttpStatus = urlResult.httpStatus || null;
      result.manualNeeded = ["manual_or_blocked_url", "url_response_not_ok"].includes(urlResult.status);
    } else {
      result.unsubscribeStatus = "no_unsubscribe_header";
      result.manualNeeded = true;
    }

    const q = senderQuery(job);
    if (q) {
      const ids = await listAllMessageIds(gmail, q, Number(process.env.MAX_MESSAGES_PER_SENDER || 25000));
      if (ids.length) {
        if ((job.action || "").includes("archive")) {
          await batchModify(gmail, ids, [], ["INBOX"]);
          result.archiveCount = ids.length;
        } else {
          await batchModify(gmail, ids, ["TRASH"], []);
          result.deleteCount = ids.length;
        }
      }
    }

    await jobRef.set({
      workerStatus: "complete",
      status: "complete",
      result,
      updatedAt: new Date().toISOString()
    }, { merge: true });

    const histRef = db()
      .collection("users").doc(CLEANSLATE_USER_ID)
      .collection("cleanupHistory").doc(jobRef.id);

    await histRef.set({
      ...job,
      result,
      processedAt: new Date().toISOString()
    }, { merge: true });

    return result;
  } catch (error) {
    await jobRef.set({
      workerStatus: "error",
      status: "error",
      error: String(error.message || error),
      updatedAt: new Date().toISOString()
    }, { merge: true });
    throw error;
  }
}

async function processQueueOnce() {
  assertReady();

  const queue = await db()
    .collection("users").doc(CLEANSLATE_USER_ID)
    .collection("cleanupQueue")
    .where("status", "in", ["queued", "retry"])
    .limit(MAX_JOBS_PER_CYCLE)
    .get();

  let processed = 0;
  for (const docSnap of queue.docs) {
    const job = docSnap.data();
    if (job.bucket === "safe" || job.protected) {
      await docSnap.ref.set({
        status: "skipped_safe",
        workerStatus: "skipped_safe",
        updatedAt: new Date().toISOString()
      }, { merge: true });
      continue;
    }
    await processJob(docSnap.ref, job);
    processed++;
  }
  return { processed };
}

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "CleanSlate Render Worker",
    projectId: PROJECT_ID,
    hasFirebase: admin.apps.length > 0,
    hasGoogleOAuth: Boolean(GOOGLE_CLIENT_ID && GOOGLE_CLIENT_SECRET && GOOGLE_REFRESH_TOKEN),
    hasUser: Boolean(CLEANSLATE_USER_ID)
  });
});

app.post("/run-once", async (req, res) => {
  try {
    if (req.headers.authorization !== `Bearer ${WORKER_SECRET}`) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const result = await processQueueOnce();
    res.json({ ok: true, ...result });
  } catch (error) {
    res.status(500).json({ ok: false, error: String(error.message || error) });
  }
});

app.listen(PORT, () => {
  console.log(`CleanSlate worker listening on ${PORT}`);
});

setInterval(async () => {
  try {
    const result = await processQueueOnce();
    if (result.processed) console.log("Processed jobs:", result.processed);
  } catch (error) {
    console.error("Queue loop error:", error.message || error);
  }
}, POLL_MS);
