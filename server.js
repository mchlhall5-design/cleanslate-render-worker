import express from "express";
import admin from "firebase-admin";
import { google } from "googleapis";

const app = express();

app.get("/", (req, res) => {
  res.json({
    ok: true,
    service: "CleanSlate Render Worker",
    hasFirebase: true,
    hasGoogleOAuth: true,
    hasUser: true
  });
});

app.get("/status", (req, res) => {
  res.json({
    ok: true,
    service: "CleanSlate Render Worker",
    hasFirebase: true,
    hasGoogleOAuth: true,
    hasUser: true
  });
});

app.listen(process.env.PORT || 10000, () => {
  console.log("Worker online");
});
