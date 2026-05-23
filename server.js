import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";

const app = express();
app.use(express.json({ limit: "2mb" }));
app.use((req,res,next)=>{
  res.setHeader("Access-Control-Allow-Origin","*");
  res.setHeader("Access-Control-Allow-Headers","Content-Type, Authorization");
  res.setHeader("Access-Control-Allow-Methods","GET, POST, OPTIONS");
  if(req.method==="OPTIONS") return res.sendStatus(204);
  next();
});

const PORT=process.env.PORT||10000;
const PROJECT_ID=process.env.FIREBASE_PROJECT_ID||"cleanslate-c9be5";
const GOOGLE_CLIENT_ID=process.env.GOOGLE_CLIENT_ID||"";
const GOOGLE_CLIENT_SECRET=process.env.GOOGLE_CLIENT_SECRET||"";
const CLEANSLATE_USER_ID=process.env.CLEANSLATE_USER_ID||"";
const WORKER_URL=(process.env.WORKER_URL||"https://cleanslate-render-worker.onrender.com").replace(/\/$/,"");

const POLL_MS=Number(process.env.POLL_MS||3000);
const SCAN_PAGE_SIZE=Number(process.env.SCAN_PAGE_SIZE||500);
const SCAN_MESSAGE_CONCURRENCY=Number(process.env.SCAN_MESSAGE_CONCURRENCY||8);
const MAX_SCAN_PAGES_PER_CYCLE=Number(process.env.MAX_SCAN_PAGES_PER_CYCLE||3);
const MESSAGE_DELAY_MS=Number(process.env.MESSAGE_DELAY_MS||120);
const RATE_LIMIT_BACKOFF_MS=Number(process.env.RATE_LIMIT_BACKOFF_MS||70000);
const MAX_JOBS_PER_CYCLE=Number(process.env.MAX_JOBS_PER_CYCLE||5);
const MAX_MESSAGES_PER_SENDER=Number(process.env.MAX_MESSAGES_PER_SENDER||25000);

let scanActive=false, cleanupActive=false;

if(!admin.apps.length){
  const raw=process.env.FIREBASE_SERVICE_ACCOUNT_JSON;
  if(!raw) console.error("Missing FIREBASE_SERVICE_ACCOUNT_JSON");
  else {
    try{
      const sa=JSON.parse(raw);
      admin.initializeApp({credential:admin.credential.cert(sa),projectId:PROJECT_ID});
      console.log("Firebase Admin initialized");
    }catch(e){ console.error("Firebase Admin initialization failed:",e.message||e); }
  }
}

const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const hasFirebase=()=>admin.apps.length>0;
const hasGoogleClient=()=>Boolean(GOOGLE_CLIENT_ID&&GOOGLE_CLIENT_SECRET);
const hasUser=()=>Boolean(CLEANSLATE_USER_ID);
const db=()=>admin.firestore();
const userDoc=()=>db().collection("users").doc(CLEANSLATE_USER_ID);
const stateRef=()=>userDoc().collection("state").doc("scan");
const oauthRef=()=>userDoc().collection("secrets").doc("gmailOAuth");

function isRateLimit(error){
  const text=String(error?.message||error?.response?.data?.error?.message||error||"").toLowerCase();
  const code=error?.code||error?.response?.status;
  return code===429 || text.includes("quota exceeded") || text.includes("rate limit") || text.includes("user-rate-limit-exceeded");
}

async function getStoredRefreshToken(){
  if(!hasFirebase()||!hasUser()) return "";
  const s=await oauthRef().get();
  return s.exists ? (s.data().refreshToken||"") : "";
}

async function saveScanState(patch){
  await stateRef().set({...patch,updatedAt:new Date().toISOString()},{merge:true});
}

async function loadScanState(){
  const s=await stateRef().get();
  if(!s.exists){
    const i={total:0,pages:0,nextPageToken:"",running:false,done:false,lastError:""};
    await saveScanState(i);
    return i;
  }
  return s.data()||{};
}

function assertReadyForFirebase(){
  if(!hasFirebase()) throw new Error("Firebase Admin is not initialized.");
  if(!hasUser()) throw new Error("Missing CLEANSLATE_USER_ID.");
}

async function assertReadyForGmail(){
  assertReadyForFirebase();
  if(!hasGoogleClient()) throw new Error("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET.");
  if(!(await getStoredRefreshToken())) throw new Error("Gmail is not connected to Render yet. Use the Connect Gmail To Render Worker button.");
}

function oauthClient(refreshToken=""){
  const c=new google.auth.OAuth2(GOOGLE_CLIENT_ID,GOOGLE_CLIENT_SECRET,`${WORKER_URL}/oauth/callback`);
  if(refreshToken) c.setCredentials({refresh_token:refreshToken});
  return c;
}

async function gmailClient(){
  return google.gmail({version:"v1",auth:oauthClient(await getStoredRefreshToken())});
}

const header=(hs,n)=>(hs||[]).find(h=>(h.name||"").toLowerCase()===n.toLowerCase())?.value||"";

function parseFrom(v){
  const input=v||"",m=input.match(/"?([^"<]+)"?\s*<([^>]+)>/);
  if(m) return {name:m[1].trim().replace(/^"|"$/g,""),email:m[2].trim().toLowerCase()};
  const email=(input.match(/[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}/i)||[""])[0].toLowerCase();
  return {name:email?email.split("@")[0]:input||"Unknown",email:email||input.toLowerCase()};
}
const domainOf=e=>String(e||"").split("@").pop().toLowerCase().replace(/^www\./,"");
const cleanKey=v=>String(v||"unknown").toLowerCase().replace(/[.#$/\[\]]/g,"_").slice(0,180);

function extractUnsubscribe(hs){
  const raw=header(hs,"List-Unsubscribe");
  const oneClick=Boolean(header(hs,"List-Unsubscribe-Post"));
  const br=[...String(raw||"").matchAll(/<([^>]+)>/g)].map(m=>m[1]);
  const sp=String(raw||"").split(",").map(x=>x.trim());
  const urls=[...br,...sp].filter(Boolean);
  return {
    unsubUrl:urls.find(u=>/^https?:\/\//i.test(u)&&!/example\.com/i.test(u))||"",
    unsubMailto:urls.find(u=>/^mailto:/i.test(u))||"",
    oneClick
  };
}

const protectedWords=["bank","credit","capital one","fidelity","mortgage","loan","insurance","geico","progressive","doctor","medical","hospital","mychart","pharmacy","irs","tax","payroll","w2","receipt","order","invoice","payment","statement","bill","utility","honda","acura","regal","google","apple","amazon","walmart","netflix","school","daycare","paypal","zelle","wallet"];

function classifySender(s){
  const t=`${s.name} ${s.email} ${s.domain}`.toLowerCase();
  if(protectedWords.some(w=>t.includes(w))) return "safe";
  if(s.unsubUrl||s.unsubMailto) return "cleanup";
  return "review";
}

function mergeSender(map,s){
  const e=map.get(s.key)||{...s,count:0};
  e.count+=s.count||1;
  e.name=s.name||e.name;
  e.email=s.email||e.email;
  e.domain=s.domain||e.domain;
  e.unsubUrl=e.unsubUrl||s.unsubUrl||"";
  e.unsubMailto=e.unsubMailto||s.unsubMailto||"";
  e.oneClick=e.oneClick||s.oneClick||false;
  e.bucket=classifySender(e);
  map.set(s.key,e);
}

async function batchUpsertSenders(map){
  const entries=[...map.values()];
  for(let i=0;i<entries.length;i+=400){
    const b=db().batch();
    entries.slice(i,i+400).forEach(s=>b.set(userDoc().collection("senders").doc(s.key),{...s,updatedAt:new Date().toISOString()},{merge:true}));
    await b.commit();
  }
}

async function gmailCall(fn, label){
  while(true){
    try {
      return await fn();
    } catch(e) {
      if(isRateLimit(e)){
        const msg=`Rate limit hit at ${label}. Waiting ${Math.round(RATE_LIMIT_BACKOFF_MS/1000)} seconds, then continuing automatically.`;
        console.warn(msg);
        await saveScanState({lastError:msg,running:true,rateLimitedAt:new Date().toISOString()}).catch(()=>{});
        await sleep(RATE_LIMIT_BACKOFF_MS);
        continue;
      }
      throw e;
    }
  }
}

async function processOneScanPage(gmail,state){
  const list=await gmailCall(()=>gmail.users.messages.list({
    userId:"me",
    maxResults:SCAN_PAGE_SIZE,
    pageToken:state.nextPageToken||undefined
  }),"message list");

  const messages=list.data.messages||[];
  if(!messages.length){
    await saveScanState({running:false,done:true,lastError:""});
    return {scanned:0,done:true};
  }

  const map=new Map();
  let completed=0;

  const worker = async () => {
    while(messages.length){
      const m=messages.shift();
      await sleep(MESSAGE_DELAY_MS);
      try{
        const msg=await gmailCall(()=>gmail.users.messages.get({
          userId:"me",
          id:m.id,
          format:"metadata",
          metadataHeaders:["From","List-Unsubscribe","List-Unsubscribe-Post"]
        }),"message metadata");

        const hs=msg.data.payload?.headers||[];
        const from=header(hs,"From");
        if(from){
          const p=parseFrom(from),domain=domainOf(p.email||p.name),unsub=extractUnsubscribe(hs),key=cleanKey(p.email||domain||p.name);
          mergeSender(map,{key,name:p.name,email:p.email,domain,count:1,...unsub});
        }
      }catch(e){
        if(isRateLimit(e)) throw e;
        console.error("Message scan failed:",e.message||e);
      }finally{
        completed++;
        if(completed%50===0) await saveScanState({livePageProgress:completed,running:true,lastError:""}).catch(()=>{});
      }
    }
  };

  await Promise.all(Array.from({length:SCAN_MESSAGE_CONCURRENCY},worker));
  await batchUpsertSenders(map);

  const total=Number(state.total||0)+completed;
  const pages=Number(state.pages||0)+1;
  const nextPageToken=list.data.nextPageToken||"";
  const done=!nextPageToken;

  await saveScanState({
    total,pages,nextPageToken,running:!done,done,livePageProgress:0,
    lastPageScanned:completed,lastSenderGroupsWritten:map.size,lastError:""
  });

  return {scanned:completed,senderGroups:map.size,done};
}

async function processScanCycle(){
  await assertReadyForGmail();
  const gmail=await gmailClient();
  let scanned=0,groups=0;

  for(let p=0;p<MAX_SCAN_PAGES_PER_CYCLE;p++){
    const s=await loadScanState();
    if(!s.running||s.done) return {scanned,senderGroups:groups,reason:"not_running_or_done"};
    const r=await processOneScanPage(gmail,s);
    scanned+=r.scanned||0;
    groups+=r.senderGroups||0;
    if(r.done||!r.scanned) break;
  }
  return {scanned,senderGroups:groups};
}

function senderQuery(j){ if(j.email)return`from:${j.email}`; if(j.domain)return`from:${j.domain}`; return""; }

async function listAllMessageIds(gmail,q,limit=MAX_MESSAGES_PER_SENDER){
  const ids=[]; let pageToken;
  while(ids.length<limit){
    const r=await gmailCall(()=>gmail.users.messages.list({userId:"me",maxResults:500,q,pageToken}),"cleanup list");
    for(const m of r.data.messages||[]){ ids.push(m.id); if(ids.length>=limit) break; }
    pageToken=r.data.nextPageToken;
    if(!pageToken) break;
  }
  return ids;
}

async function batchModify(gmail,ids,addLabelIds=[],removeLabelIds=[]){
  for(let i=0;i<ids.length;i+=1000){
    await gmailCall(()=>gmail.users.messages.batchModify({userId:"me",requestBody:{ids:ids.slice(i,i+1000),addLabelIds,removeLabelIds}}),"cleanup batch");
    await sleep(1000);
  }
}

async function processCleanupQueueOnce(){
  await assertReadyForGmail();
  const gmail=await gmailClient();
  const q=await userDoc().collection("cleanupQueue").where("status","in",["queued","retry"]).limit(MAX_JOBS_PER_CYCLE).get();
  let processed=0,skippedSafe=0;

  for(const d of q.docs){
    const job=d.data();
    if(job.bucket==="safe"||job.protected){
      await d.ref.set({status:"skipped_safe",workerStatus:"skipped_safe",updatedAt:new Date().toISOString()},{merge:true});
      skippedSafe++; continue;
    }
    const query=senderQuery(job);
    if(!query) continue;
    await d.ref.set({status:"processing",workerStatus:"processing",updatedAt:new Date().toISOString()},{merge:true});
    const ids=await listAllMessageIds(gmail,query);
    if(ids.length) await batchModify(gmail,ids,["TRASH"],[]);
    await d.ref.set({status:"complete",workerStatus:"complete",deleteCount:ids.length,updatedAt:new Date().toISOString()},{merge:true});
    processed++;
  }
  return {processed,skippedSafe};
}

async function healthPayload(){
  let hasStoredGmailToken=false;
  try{hasStoredGmailToken=Boolean(await getStoredRefreshToken());}catch{}
  return {
    ok:true,service:"CleanSlate Auto Continue Worker",projectId:PROJECT_ID,
    hasFirebase:hasFirebase(),hasGoogleClient:hasGoogleClient(),hasStoredGmailToken,hasUser:hasUser(),
    scanActive,cleanupActive,pollMs:POLL_MS,scanPageSize:SCAN_PAGE_SIZE,
    maxScanPagesPerCycle:MAX_SCAN_PAGES_PER_CYCLE,scanMessageConcurrency:SCAN_MESSAGE_CONCURRENCY,
    messageDelayMs:MESSAGE_DELAY_MS,rateLimitBackoffMs:RATE_LIMIT_BACKOFF_MS,
    oauthCallback:`${WORKER_URL}/oauth/callback`
  };
}

app.get("/",async(req,res)=>res.json(await healthPayload()));
app.get("/status",async(req,res)=>res.json(await healthPayload()));

app.get("/oauth/start",async(req,res)=>{
  try{
    assertReadyForFirebase();
    if(!hasGoogleClient()) throw new Error("Missing GOOGLE_CLIENT_ID or GOOGLE_CLIENT_SECRET.");
    const uid=String(req.query.uid||CLEANSLATE_USER_ID);
    const returnUrl=String(req.query.returnUrl||"https://mchlhall5-design.github.io/cleanslate/");
    const state=Buffer.from(JSON.stringify({uid,returnUrl})).toString("base64url");
    const url=oauthClient().generateAuthUrl({
      access_type:"offline",prompt:"consent",
      scope:["https://www.googleapis.com/auth/gmail.readonly","https://www.googleapis.com/auth/gmail.modify","https://www.googleapis.com/auth/gmail.send"],
      state
    });
    res.redirect(url);
  }catch(e){ res.status(500).send(`<pre>OAuth start failed:\n${e.message||e}</pre>`); }
});

app.get("/oauth/callback",async(req,res)=>{
  try{
    assertReadyForFirebase();
    if(!req.query.code) throw new Error("Missing authorization code.");
    let state={};
    try{state=JSON.parse(Buffer.from(String(req.query.state||""),"base64url").toString("utf8"));}catch{}
    const {tokens}=await oauthClient().getToken(String(req.query.code));
    if(!tokens.refresh_token) throw new Error("Google did not return a refresh token. Try Connect Gmail again and approve consent.");
    await oauthRef().set({refreshToken:tokens.refresh_token,scope:tokens.scope||"",connectedAt:new Date().toISOString(),connectedUid:state.uid||CLEANSLATE_USER_ID},{merge:true});
    await saveScanState({lastError:"",gmailConnectedAt:new Date().toISOString()});
    const returnUrl=state.returnUrl||"https://mchlhall5-design.github.io/cleanslate/";
    res.send(`<!doctype html><html><head><meta name="viewport" content="width=device-width,initial-scale=1"><title>CleanSlate Connected</title></head><body style="font-family:Arial;padding:24px;background:#07111f;color:#fff"><h2>Gmail connected to Render successfully.</h2><p>Return to CleanSlate and start the background scan.</p><p><a style="color:#67e8f9;font-size:20px" href="${returnUrl}">Return to CleanSlate</a></p></body></html>`);
  }catch(e){
    await saveScanState({lastError:e.message||String(e),running:false}).catch(()=>{});
    res.status(500).send(`<pre>OAuth callback failed:\n${e.message||e}</pre>`);
  }
});

app.post("/scan/start",async(req,res)=>{
  try{
    await assertReadyForGmail();
    const s=await loadScanState();
    const patch={running:true,done:false,lastError:"",startedAt:s.startedAt||new Date().toISOString()};
    if(req.body?.restart===true){ patch.total=0; patch.pages=0; patch.nextPageToken=""; patch.startedAt=new Date().toISOString(); }
    await saveScanState(patch);
    runScanLoop();
    res.json({ok:true,message:"scan_started_auto_continue",currentTotal:s.total||0});
  }catch(e){
    await saveScanState({lastError:e.message||String(e),running:false}).catch(()=>{});
    res.status(500).json({ok:false,error:e.message||String(e)});
  }
});

app.post("/scan/pause",async(req,res)=>{
  try{assertReadyForFirebase();await saveScanState({running:false});res.json({ok:true,message:"scan_pause_requested"});}
  catch(e){res.status(500).json({ok:false,error:e.message||String(e)});}
});

app.post("/scan/reset",async(req,res)=>{
  try{
    assertReadyForFirebase();
    await saveScanState({total:0,pages:0,nextPageToken:"",running:false,done:false,livePageProgress:0,lastError:"",startedAt:new Date().toISOString()});
    res.json({ok:true,message:"scan_reset"});
  }catch(e){res.status(500).json({ok:false,error:e.message||String(e)});}
});

app.post("/run-once",async(req,res)=>{
  try{res.json({ok:true,...await processCleanupQueueOnce()});}
  catch(e){res.status(500).json({ok:false,error:e.message||String(e)});}
});

async function runScanLoop(){
  if(scanActive) return;
  scanActive=true;
  try{
    while(true){
      const s=await loadScanState();
      if(!s.running||s.done) break;
      const r=await processScanCycle();
      console.log("Scan cycle:",r);
      if(!r.scanned){
        await sleep(10000);
      }
    }
  }catch(e){
    if(isRateLimit(e)){
      await saveScanState({lastError:`Rate limit hit. Waiting ${Math.round(RATE_LIMIT_BACKOFF_MS/1000)} seconds, then continuing automatically.`,running:true}).catch(()=>{});
      await sleep(RATE_LIMIT_BACKOFF_MS);
      scanActive=false;
      return runScanLoop();
    }
    console.error("Scan loop error:",e.message||e);
    await saveScanState({lastError:e.message||String(e),running:false}).catch(()=>{});
  }finally{ scanActive=false; }
}

async function runCleanupLoop(){
  if(cleanupActive) return;
  cleanupActive=true;
  try{
    const r=await processCleanupQueueOnce();
    if(r.processed||r.skippedSafe) console.log("Cleanup cycle:",r);
  }catch(e){ console.error("Cleanup loop error:",e.message||e); }
  finally{cleanupActive=false;}
}

app.listen(PORT,()=>console.log(`CleanSlate auto-continue worker listening on ${PORT}`));

setInterval(async()=>{
  try{
    if(!hasFirebase()||!hasGoogleClient()||!hasUser()) return;
    const s=await loadScanState();
    if(s.running&&!s.done) runScanLoop();
    if(await getStoredRefreshToken()) runCleanupLoop();
  }catch(e){
    console.error("Background interval error:",e.message||e);
    await saveScanState({lastError:e.message||String(e),running:false}).catch(()=>{});
  }
},POLL_MS);
