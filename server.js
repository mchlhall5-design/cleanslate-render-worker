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

const SPEEDS = {
  safe: { concurrency: 12, maxPagesPerCycle: 5, messageDelayMs: 80, backoffMs: 60000 },
  fast: { concurrency: 24, maxPagesPerCycle: 8, messageDelayMs: 25, backoffMs: 75000 },
  max: { concurrency: 45, maxPagesPerCycle: 12, messageDelayMs: 0, backoffMs: 90000 }
};

const POLL_MS=3000;
const SCAN_PAGE_SIZE=500;
const MAX_JOBS_PER_CYCLE=5;
const MAX_MESSAGES_PER_SENDER=25000;

let scanActive=false, cleanupActive=false;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));

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

const hasFirebase=()=>admin.apps.length>0;
const hasGoogleClient=()=>Boolean(GOOGLE_CLIENT_ID&&GOOGLE_CLIENT_SECRET);
const hasUser=()=>Boolean(CLEANSLATE_USER_ID);
const db=()=>admin.firestore();
const FieldValue=admin.firestore.FieldValue;
const FieldPath=admin.firestore.FieldPath;
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
    const i={total:0,pages:0,nextPageToken:"",running:false,done:false,lastError:"",speedMode:"fast"};
    await saveScanState(i);
    return i;
  }
  return s.data()||{};
}

async function getSpeed(){
  const s=await loadScanState();
  const mode = SPEEDS[s.speedMode] ? s.speedMode : "fast";
  return { mode, ...SPEEDS[mode] };
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

async function gmailCall(fn,label){
  while(true){
    try{return await fn();}
    catch(e){
      if(isRateLimit(e)){
        const sp=await getSpeed();
        const msg=`Rate limit hit at ${label}. Waiting ${Math.round(sp.backoffMs/1000)} seconds, then continuing automatically.`;
        await saveScanState({lastError:msg,running:true,rateLimitedAt:new Date().toISOString()}).catch(()=>{});
        await sleep(sp.backoffMs);
        continue;
      }
      throw e;
    }
  }
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

const protectedWords=["bank","credit","capital one","fidelity","mortgage","loan","insurance","geico","progressive","doctor","medical","hospital","mychart","pharmacy","irs","tax","payroll","w2","statement","bill","utility","honda","acura","regal","school","daycare","paypal","zelle","wallet"];
const cleanupWords=["promo","deal","sale","coupon","newsletter","marketing","offers","discount","shop","store","rewards","daily","weekly","digest","notification","notify","no-reply","noreply","donotreply","do-not-reply","mailer","campaign","updates"];
function scoreSender(s){
  const t=`${s.name||""} ${s.email||""} ${s.domain||""} ${s.subjectSample||""} ${s.listId||""}`.toLowerCase();
  if(protectedWords.some(w=>t.includes(w))) return {bucket:"safe",confidence:"protected",cleanupScore:0,scoreReasons:["protected keyword"]};
  let score=0,reasons=[];
  if(s.unsubUrl||s.unsubMailto){score+=35;reasons.push("unsubscribe available");}
  if(s.oneClick){score+=20;reasons.push("one-click unsubscribe");}
  if(s.listId){score+=25;reasons.push("mailing list");}
  if(s.bulkHeader){score+=20;reasons.push("bulk/list mail");}
  if(s.noreply){score+=15;reasons.push("no-reply sender");}
  if(Number(s.promoCount||0)>0){score+=25;reasons.push("promotions label");}
  if(Number(s.count||0)>=25){score+=10;reasons.push("repeated sender");}
  if(Number(s.count||0)>=100){score+=15;reasons.push("high volume");}
  if(Number(s.count||0)>=500){score+=20;reasons.push("very high volume");}
  if(cleanupWords.some(w=>t.includes(w))){score+=12;reasons.push("marketing keyword");}
  let bucket="review",confidence="low";
  if(score>=70){bucket="cleanup";confidence="high";} else if(score>=40){bucket="cleanup";confidence="medium";}
  return {bucket,confidence,cleanupScore:score,scoreReasons:[...new Set(reasons)].slice(0,8)};
}
function classifySender(s){return scoreSender(s).bucket;}

function mergeSender(map,s){
  const e=map.get(s.key)||{...s,count:0};
  e.count+=s.count||1;
  e.name=s.name||e.name;
  e.email=s.email||e.email;
  e.domain=s.domain||e.domain;
  e.unsubUrl=e.unsubUrl||s.unsubUrl||"";
  e.unsubMailto=e.unsubMailto||s.unsubMailto||"";
  e.oneClick=e.oneClick||s.oneClick||false;
  e.subjectSample=e.subjectSample||s.subjectSample||"";
  e.listId=e.listId||s.listId||"";
  e.bulkHeader=e.bulkHeader||s.bulkHeader||false;
  e.noreply=e.noreply||s.noreply||false;
  e.promoCount=(e.promoCount||0)+(s.promoCount||0);
  const scored=scoreSender(e); Object.assign(e,scored);
  map.set(s.key,e);
}

async function batchUpsertSenders(map){
  const entries=[...map.values()];
  for(let i=0;i<entries.length;i+=400){
    const b=db().batch();
    entries.slice(i,i+400).forEach(s=>b.set(userDoc().collection("senders").doc(s.key),{key:s.key,name:s.name||"",email:s.email||"",domain:s.domain||"",subjectSample:s.subjectSample||"",listId:s.listId||"",unsubUrl:s.unsubUrl||"",unsubMailto:s.unsubMailto||"",oneClick:Boolean(s.oneClick),bulkHeader:Boolean(s.bulkHeader),noreply:Boolean(s.noreply),count:FieldValue.increment(Number(s.count||0)),promoCount:FieldValue.increment(Number(s.promoCount||0)),cleanupScore:s.cleanupScore||0,bucket:s.bucket||"review",confidence:s.confidence||"low",scoreReasons:s.scoreReasons||[],updatedAt:new Date().toISOString()},{merge:true}));
    await b.commit();
  }
}

async function updateProfileEstimate(gmail){
  try{
    const profile=await gmail.users.getProfile({userId:"me"});
    const messagesTotal=Number(profile.data.messagesTotal||0);
    if(messagesTotal){
      await saveScanState({messagesTotal,estimatedTotal:messagesTotal});
    }
  }catch(e){console.warn("profile estimate failed",e.message||e);}
}

async function processOneScanPage(gmail,state){
  const sp=await getSpeed();
  const started=Date.now();
  const list=await gmailCall(()=>gmail.users.messages.list({
    userId:"me",maxResults:SCAN_PAGE_SIZE,pageToken:state.nextPageToken||undefined
  }),"message list");

  const messages=list.data.messages||[];
  if(!messages.length){
    await saveScanState({running:false,done:true,lastError:""});
    return {scanned:0,done:true};
  }

  const senderMap=new Map();
  let cursor=0, processed=0;

  async function worker(){
    while(cursor<messages.length){
      const m=messages[cursor++];
      await sleep(sp.messageDelayMs);
      try{
        const full=await gmailCall(()=>gmail.users.messages.get({
          userId:"me",
          id:m.id,
          format:"metadata",
          metadataHeaders:["From","Subject","List-Unsubscribe","List-Unsubscribe-Post","List-ID","Precedence","Auto-Submitted","Reply-To","Sender"]
        }),"message metadata");

        const hs=full.data.payload?.headers||[];
        const from=header(hs,"From");
        if(from){
          const p=parseFrom(from),domain=domainOf(p.email||p.name),unsub=extractUnsubscribe(hs),key=cleanKey(p.email||domain||p.name);
          const labels=full.data.labelIds||[];
          const subject=header(hs,"Subject"), listId=header(hs,"List-ID"), precedence=header(hs,"Precedence"), autoSub=header(hs,"Auto-Submitted"), replyTo=header(hs,"Reply-To"), senderHeader=header(hs,"Sender");
          const lower=`${p.email} ${from} ${replyTo} ${senderHeader}`.toLowerCase();
          mergeSender(senderMap,{key,name:p.name,email:p.email,domain,count:1,subjectSample:subject,listId,bulkHeader:Boolean(listId||/bulk|list|junk/i.test(precedence)||/auto/i.test(autoSub)),noreply:/no-?reply|donotreply|do-not-reply|noreply/i.test(lower),promoCount:labels.includes("CATEGORY_PROMOTIONS")?1:0,...unsub});
        }
      }catch(e){
        if(isRateLimit(e)) throw e;
        console.error("Message scan failed:",e.message||e);
      }finally{
        processed++;
        if(processed%50===0){
          await saveScanState({running:true,livePageProgress:processed,lastError:""}).catch(()=>{});
        }
      }
    }
  }

  await Promise.all(Array.from({length:sp.concurrency},()=>worker()));
  await batchUpsertSenders(senderMap);

  const total=Number(state.total||0)+processed;
  const pages=Number(state.pages||0)+1;
  const nextPageToken=list.data.nextPageToken||"";
  const done=!nextPageToken;
  const estimatedTotal=Number(state.estimatedTotal||state.messagesTotal||0);
  const elapsedSec=Math.max(1,(Date.now()-started)/1000);
  const emailsPerMinute=Math.round((processed/elapsedSec)*60);
  const etaSeconds=estimatedTotal && emailsPerMinute ? Math.max(0,Math.round(((estimatedTotal-total)/emailsPerMinute)*60)) : null;
  const percentComplete=estimatedTotal ? Math.min(100,Math.round((total/estimatedTotal)*1000)/10) : null;

  await saveScanState({
    total,pages,nextPageToken,running:!done,done,livePageProgress:0,
    lastPageScanned:processed,lastSenderGroupsWritten:senderMap.size,lastError:"",
    emailsPerMinute,etaSeconds,percentComplete,
    speedMode:sp.mode,
    estimatedTotal: estimatedTotal || null
  });

  return {scanned:processed,senderGroups:senderMap.size,done};
}

async function processScanCycle(){
  await assertReadyForGmail();
  const gmail=await gmailClient();
  await updateProfileEstimate(gmail);
  const sp=await getSpeed();
  let scanned=0,groups=0;

  for(let p=0;p<sp.maxPagesPerCycle;p++){
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

function parseMailto(mailto){const clean=String(mailto||"").replace(/^mailto:/i,"");const [addr,qs]=clean.split("?");const params=new URLSearchParams(qs||"");return {to:decodeURIComponent(addr||""),subject:params.get("subject")||"Unsubscribe",body:params.get("body")||"Please unsubscribe me from this mailing list."};}
function toBase64Url(str){return Buffer.from(str).toString("base64").replace(/\+/g,"-").replace(/\//g,"_").replace(/=+$/g,"");}
async function sendMailtoUnsubscribe(gmail,mailto){const m=parseMailto(mailto);if(!m.to)return {ok:false,status:"missing_mailto"};const raw=[`To: ${m.to}`,`Subject: ${m.subject}`,"",m.body].join("\r\n");await gmail.users.messages.send({userId:"me",requestBody:{raw:toBase64Url(raw)}});return {ok:true,status:"mailto_sent"};}
async function attemptUrlUnsubscribe(url,oneClick){if(!url)return {ok:false,status:"missing_url"};try{const res=await fetch(url,oneClick?{method:"POST",headers:{"Content-Type":"application/x-www-form-urlencoded","User-Agent":"CleanSlate"},body:"List-Unsubscribe=One-Click"}:{method:"GET",headers:{"User-Agent":"CleanSlate"}});return {ok:res.ok,status:res.ok?"url_submitted":"url_http_error",httpStatus:res.status};}catch(e){return {ok:false,status:"url_failed",error:e.message||String(e)};}}
async function unsubscribeSender(gmail,job){if(job.unsubUrl)return attemptUrlUnsubscribe(job.unsubUrl,Boolean(job.oneClick));if(job.unsubMailto)return sendMailtoUnsubscribe(gmail,job.unsubMailto);return {ok:false,status:"no_unsubscribe_method"};}

async function processCleanupQueueOnce(){
  await assertReadyForGmail();
  const gmail=await gmailClient();
  const q=await userDoc().collection("cleanupQueue").where("status","in",["queued","retry"]).limit(MAX_JOBS_PER_CYCLE).get();
  let processed=0,skippedSafe=0,trashCount=0,archiveCount=0,unsubscribeAttempts=0;
  for(const d of q.docs){
    const job=d.data();
    if(job.bucket==="safe"||job.protected){await d.ref.set({status:"skipped_safe",workerStatus:"skipped_safe",updatedAt:new Date().toISOString()},{merge:true});skippedSafe++;continue;}
    const action=job.action||"unsubscribe_and_trash";
    const query=senderQuery(job);
    await d.ref.set({status:"processing",workerStatus:"processing",updatedAt:new Date().toISOString()},{merge:true});
    let unsubResult={status:"not_requested"};
    if(action.includes("unsubscribe")){unsubscribeAttempts++;unsubResult=await unsubscribeSender(gmail,job);}
    let ids=[],changed=0;
    if(query&&(action.includes("trash")||action.includes("delete")||action.includes("archive"))){ids=await listAllMessageIds(gmail,query);if(ids.length&&(action.includes("trash")||action.includes("delete"))){await batchModify(gmail,ids,["TRASH"],[]);trashCount+=ids.length;changed=ids.length;}else if(ids.length&&action.includes("archive")){await batchModify(gmail,ids,[],["INBOX"]);archiveCount+=ids.length;changed=ids.length;}}
    await d.ref.set({status:"complete",workerStatus:"complete",unsubscribeResult:unsubResult,affectedCount:changed,updatedAt:new Date().toISOString()},{merge:true});
    await userDoc().collection("cleanupHistory").doc(d.id).set({...job,unsubscribeResult:unsubResult,affectedCount:changed,processedAt:new Date().toISOString()},{merge:true});
    processed++;
  }
  return {processed,skippedSafe,trashCount,archiveCount,unsubscribeAttempts};
}

async function reclassifySenders(){
  assertReadyForFirebase();
  let last=null,updated=0,cleanup=0,safe=0,review=0;
  while(true){
    let q=userDoc().collection("senders").orderBy(FieldPath.documentId()).limit(400);
    if(last) q=q.startAfter(last);
    const snap=await q.get();
    if(snap.empty) break;
    const b=db().batch();
    snap.docs.forEach(doc=>{const s=doc.data();const scored=scoreSender(s);if(scored.bucket==="cleanup")cleanup++;else if(scored.bucket==="safe")safe++;else review++;b.set(doc.ref,{...scored,updatedAt:new Date().toISOString()},{merge:true});updated++;});
    await b.commit();
    last=snap.docs[snap.docs.length-1];
    if(snap.size<400) break;
  }
  return {updated,cleanup,safe,review};
}

async function healthPayload(){
  let hasStoredGmailToken=false;
  try{hasStoredGmailToken=Boolean(await getStoredRefreshToken());}catch{}
  const sp = await getSpeed().catch(()=>({mode:"fast"}));
  return {
    ok:true,service:"CleanSlate Scroll Fixed Max Speed Worker",projectId:PROJECT_ID,
    hasFirebase:hasFirebase(),hasGoogleClient:hasGoogleClient(),hasStoredGmailToken,hasUser:hasUser(),
    scanActive,cleanupActive,pollMs:POLL_MS,scanPageSize:SCAN_PAGE_SIZE,
    speedMode:sp.mode, speedSettings:sp,
    oauthCallback:`${WORKER_URL}/oauth/callback`
  };
}

app.get("/",async(req,res)=>res.json(await healthPayload()));
app.get("/status",async(req,res)=>res.json(await healthPayload()));

app.post("/scan/speed",async(req,res)=>{
  try{
    assertReadyForFirebase();
    const mode = SPEEDS[req.body?.mode] ? req.body.mode : "fast";
    await saveScanState({speedMode:mode,lastError:`Speed changed to ${mode}.`});
    res.json({ok:true,speedMode:mode,settings:SPEEDS[mode]});
  }catch(e){res.status(500).json({ok:false,error:e.message||String(e)});}
});

app.post("/tools/reclassify",async(req,res)=>{
  try{res.json({ok:true,...await reclassifySenders()});}
  catch(e){res.status(500).json({ok:false,error:e.message||String(e)});}
});

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
    res.json({ok:true,message:"scan_started",currentTotal:s.total||0});
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
    await saveScanState({total:0,pages:0,nextPageToken:"",running:false,done:false,livePageProgress:0,lastError:"",startedAt:new Date().toISOString(),speedMode:"fast"});
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
      if(!r.scanned) await sleep(10000);
    }
  }catch(e){
    if(isRateLimit(e)){
      const sp=await getSpeed();
      await saveScanState({lastError:`Rate limit hit. Waiting ${Math.round(sp.backoffMs/1000)} seconds, then continuing automatically.`,running:true}).catch(()=>{});
      await sleep(sp.backoffMs);
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

app.listen(PORT,()=>console.log(`CleanSlate speed-percent worker listening on ${PORT}`));

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
