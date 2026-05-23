CleanSlate Render Background Worker V1

WHAT THIS DOES
- Runs from Render while your phone is locked.
- Watches Firestore cleanupQueue.
- Processes selected senders in the background.
- Attempts unsubscribe using mailto/List-Unsubscribe URL.
- Moves selected sender emails to Trash or archives them.
- Saves results back to Firestore cleanupHistory.

WHAT YOU NEED TO DO
1. Upload the render-worker folder to GitHub as its own repo OR inside your CleanSlate repo.
2. Create a Render Web Service from that repo/folder.
3. Set environment variables in Render.

REQUIRED RENDER ENVIRONMENT VARIABLES
FIREBASE_PROJECT_ID=cleanslate-c9be5
FIREBASE_SERVICE_ACCOUNT_JSON=<paste full Firebase service account JSON on one line>
GOOGLE_CLIENT_ID=<your Google OAuth client ID>
GOOGLE_CLIENT_SECRET=<your Google OAuth client secret>
GOOGLE_REFRESH_TOKEN=<your Google refresh token>
CLEANSLATE_USER_ID=<your Firebase Auth UID>
WORKER_SECRET=<make up a long password>
POLL_MS=15000
MAX_JOBS_PER_CYCLE=10
MAX_MESSAGES_PER_SENDER=25000

IMPORTANT
The worker requires a Google refresh token. Browser Gmail tokens expire and cannot run background jobs.
To get the refresh token, you need one one-time OAuth token helper step. I can provide a helper page/file next.

NO FIREBASE FUNCTIONS REQUIRED.
This replaces Firebase Functions with Render.
