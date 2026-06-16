const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  fetchLatestWaWebVersion,
  jidNormalizedUser,
  downloadContentFromMessage,
  proto,
  DisconnectReason
} = require('@dnuzi/baileys');
// ---------------- CONFIG ----------------
const BOT_NAME_FANCY = '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'false',
  AUTO_LIKE_EMOJI: ['☘️','💗','🫂','🙈','🍁','🙃','','😘','🏴‍☠️','👀','❤️‍🔥'],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/HRIlSELlxO5JQo2gYL4MzV?mode=gi_t',
  RCD_IMAGE_PATH: 'https://files.catbox.moe/qb2puf.jpeg',
  NEWSLETTER_JID: 'jid',
  OTP_EXPIRY: 300000,
  WORK_TYPE: 'public',
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94752135878',
  CHANNEL_LINK: 'https://chat.whatsapp.com/HRIlSELlxO5JQo2gYL4MzV?mode=gi_t',
  BOT_NAME: '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰',
  BOT_VERSION: '4.0.0V',
  OWNER_NAME: '𝐀ʏᴇꜱʜ 𝐓ʜᴇᴍɪʏᴀ 🥷🇱🇰',
  IMAGE_PATH: 'https://files.catbox.moe/qb2puf.jpeg',
  BOT_FOOTER: '> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*',
  BUTTON_IMAGES: { ALIVE: 'https://files.catbox.moe/qb2puf.jpeg' }
};
// ---------------- MONGO SETUP ----------------

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://ishanccntxpr_db_user:k1gvugvb5l0ONP88@cluster0.ltwn3yv.mongodb.net/?appName=Cluster0';
const MONGO_DB = process.env.MONGO_DB || 'ISHAN-KIO';
let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

// In-memory cache for user configs to avoid frequent DB reads
const userConfigCache = new Map();
const USER_CONFIG_CACHE_TTL = 30 * 1000; // 30 seconds

async function initMongo() {
  try {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected && mongoClient.topology.isConnected()) return;
  } catch(e){}
  mongoClient = new MongoClient(MONGO_URI);
  await mongoClient.connect();
  mongoDB = mongoClient.db(MONGO_DB);

  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  console.log('✅ Mongo initialized and collections ready');
}

// ---------------- Mongo helpers ----------------

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
    console.log(`Saved creds to Mongo for ${sanitized}`);
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
    console.log(`Removed session from Mongo for ${sanitized}`);
  } catch (e) { console.error('removeSessionToMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
    console.log(`Added number ${sanitized} to Mongo numbers`);
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
    console.log(`Removed number ${sanitized} from Mongo numbers`);
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
    console.log(`Added admin ${jidOrNumber}`);
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
    console.log(`Removed admin ${jidOrNumber}`);
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    console.log(`Added newsletter ${jid} -> emojis: ${doc.emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
    console.log(`Removed newsletter ${jid}`);
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewslettersFromMongo', e); return []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne(doc);
    console.log(`Saved reaction ${emoji} for ${jid}#${messageId}`);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
    try { userConfigCache.set(sanitized, { config: conf, ts: Date.now() }); } catch (e){}
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    // Check cache first
    try {
      const cached = userConfigCache.get(sanitized);
      if (cached && (Date.now() - (cached.ts || 0) < USER_CONFIG_CACHE_TTL)) {
        return cached.config;
      }
    } catch (e) { }

    const doc = await configsCol.findOne({ number: sanitized });
    const conf = doc ? doc.config : null;
    try { userConfigCache.set(sanitized, { config: conf, ts: Date.now() }); } catch (e){}
    return conf;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return null; }
}

// -------------- newsletter react-config helpers --------------

async function addNewsletterReactConfig(jid, emojis = ['🎀','🧚‍♀️','🎭']) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
    console.log(`Added react-config for ${jid} -> ${emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterReactConfig', e); throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
    console.log(`Removed react-config for ${jid}`);
  } catch (e) { console.error('removeNewsletterReactConfig', e); throw e; }
}

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : ['🤫','♥️',''] }));
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return ['🤫','♥️','']; }
}

async function getReactConfigForJid(jid) {
  try {
    await initMongo();
    const doc = await newsletterReactsCol.findOne({ jid });
    return doc ? (Array.isArray(doc.emojis) ? doc.emojis : ['🧚‍♀️','🤫','🎀']) : null;
  } catch (e) { console.error('getReactConfigForJid', e); return null; }
}

// ---------------- basic utils ----------------

function formatMessage(title, content, footer) {
  return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function generateOTP(){ return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp(){ return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();

const socketCreationTime = new Map();

// ==================== MOVIE PLUGIN HELPERS ====================
const puppeteer = require('puppeteer');
const moviePendingSearch = {};
const moviePendingQuality = {};

function normalizeQuality(text) {
  if (!text) return null;
  text = text.toUpperCase();
  if (/1080|FHD/.test(text)) return "1080p";
  if (/720|HD/.test(text)) return "720p";
  if (/480|SD/.test(text)) return "480p";
  return text;
}

function getDirectPixeldrainUrl(url) {
  const match = url.match(/pixeldrain\.com\/u\/(\w+)/);
  if (!match) return null;
  return `https://pixeldrain.com/api/file/${match[1]}?download`;
}

async function searchMovies(query) {
  const searchUrl = `https://sinhalasub.lk/?s=${encodeURIComponent(query)}&post_type=movies`;
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.goto(searchUrl, { waitUntil: "networkidle2", timeout: 30000 });
  const results = await page.$$eval(".display-item .item-box", boxes =>
    boxes.slice(0, 10).map((box, index) => {
      const a = box.querySelector("a");
      const img = box.querySelector(".thumb");
      const lang = box.querySelector(".item-desc-giha .language")?.textContent || "";
      const quality = box.querySelector(".item-desc-giha .quality")?.textContent || "";
      const qty = box.querySelector(".item-desc-giha .qty")?.textContent || "";
      return {
        id: index + 1,
        title: a?.title?.trim() || "",
        movieUrl: a?.href || "",
        thumb: img?.src || "",
        language: lang.trim(),
        quality: quality.trim(),
        qty: qty.trim(),
      };
    }).filter(m => m.title && m.movieUrl)
  );
  await browser.close();
  return results;
}

async function getMovieMetadata(url) {
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.goto(url, { waitUntil: "networkidle2", timeout: 30000 });
  const metadata = await page.evaluate(() => {
    const getText = el => el?.textContent.trim() || "";
    const getList = selector => Array.from(document.querySelectorAll(selector)).map(el => el.textContent.trim());
    const title = getText(document.querySelector(".info-details .details-title h3"));
    let language = "", directors = [], stars = [];
    document.querySelectorAll(".info-col p").forEach(p => {
      const strong = p.querySelector("strong");
      if (!strong) return;
      const txt = strong.textContent.trim();
      if (txt.includes("Language:")) language = strong.nextSibling?.textContent?.trim() || "";
      if (txt.includes("Director:")) directors = Array.from(p.querySelectorAll("a")).map(a => a.textContent.trim());
      if (txt.includes("Stars:")) stars = Array.from(p.querySelectorAll("a")).map(a => a.textContent.trim());
    });
    const duration = getText(document.querySelector(".info-details .data-views[itemprop='duration']"));
    const imdb = getText(document.querySelector(".info-details .data-imdb"))?.replace("IMDb:", "").trim();
    const genres = getList(".details-genre a");
    const thumbnail = document.querySelector(".splash-bg img")?.src || "";
    return { title, language, duration, imdb, genres, directors, stars, thumbnail };
  });
  await browser.close();
  return metadata;
}

async function getPixeldrainLinks(movieUrl) {
  const browser = await puppeteer.launch({ headless: true, args: ["--no-sandbox"] });
  const page = await browser.newPage();
  await page.goto(movieUrl, { waitUntil: "networkidle2", timeout: 30000 });
  const linksData = await page.$$eval(".link-pixeldrain tbody tr", rows =>
    rows.map(row => {
      const a = row.querySelector(".link-opt a");
      const quality = row.querySelector(".quality")?.textContent.trim() || "";
      const size = row.querySelector("td:nth-child(3) span")?.textContent.trim() || "";
      return { pageLink: a?.href || "", quality, size };
    })
  );
  const directLinks = [];
  for (const l of linksData) {
    try {
      const subPage = await browser.newPage();
      await subPage.goto(l.pageLink, { waitUntil: "networkidle2", timeout: 30000 });
      await new Promise(r => setTimeout(r, 12000));
      const finalUrl = await subPage.$eval(".wait-done a[href^='https://pixeldrain.com/']", el => el.href).catch(() => null);
      if (finalUrl) {
        let sizeMB = 0;
        const sizeText = l.size.toUpperCase();
        if (sizeText.includes("GB")) sizeMB = parseFloat(sizeText) * 1024;
        else if (sizeText.includes("MB")) sizeMB = parseFloat(sizeText);
        if (sizeMB <= 2048) {
          directLinks.push({ link: finalUrl, quality: normalizeQuality(l.quality), size: l.size });
        }
      }
      await subPage.close();
    } catch (e) { continue; }
  }
  await browser.close();
  return directLinks;
}

setInterval(() => {
  const now = Date.now();
  const timeout = 10 * 60 * 1000;
  for (const s in moviePendingSearch) if (now - moviePendingSearch[s].timestamp > timeout) delete moviePendingSearch[s];
  for (const s in moviePendingQuality) if (now - moviePendingQuality[s].timestamp > timeout) delete moviePendingQuality[s];
}, 5 * 60 * 1000);
// ==================== END MOVIE PLUGIN HELPERS ====================

const otpStore = new Map();

// ---------------- helpers kept/adapted ----------------

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`*🔐 𝐎𝚃𝙿 𝐕𝙴𝚁𝙸𝙵𝙸𝙲𝙰𝚃𝙸𝙾𝙽 — ${BOT_NAME_FANCY}*`, `*𝐘𝙾𝚄𝚁 𝐎𝚃𝙿 𝐅𝙾𝚁 𝐂𝙾𝙽𝙵𝙸𝙶 𝐔𝙿𝙳𝙰𝚃𝙴 𝐈𝚂:* *${otp}*\n𝐓𝙷𝙸𝚂 𝐎𝚃𝙿 𝐖𝙸𝙻𝙻 𝐄𝚇𝙿𝙸𝚁𝙴 𝐈𝙽 5 𝐌𝙸𝙽𝚄𝚃𝙴𝚂.\n\n*𝐍𝚄𝙼𝙱𝙴𝚁:* ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// ---------------- handlers (newsletter + reactions) ----------------

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromMongo(); // array of {jid, emojis}
      const reactConfigs = await listNewsletterReactsFromMongo(); // [{jid, emojis}]
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          console.log(`Reacted to ${jid} ${messageId} with ${emoji}`);
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }

    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}


// ---------------- status + revocation + resizing ----------------

async function setupStatusHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key || message.key.remoteJid !== 'status@broadcast' || !message.key.participant) return;
    
    try {
      // Load user-specific config from MongoDB
      let userEmojis = config.AUTO_LIKE_EMOJI; // Default emojis
      let autoViewStatus = config.AUTO_VIEW_STATUS; // Default from global config
      let autoLikeStatus = config.AUTO_LIKE_STATUS; // Default from global config
      let autoRecording = config.AUTO_RECORDING; // Default from global config
      
      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
        
        // Check for emojis in user config
        if (userConfig.AUTO_LIKE_EMOJI && Array.isArray(userConfig.AUTO_LIKE_EMOJI) && userConfig.AUTO_LIKE_EMOJI.length > 0) {
          userEmojis = userConfig.AUTO_LIKE_EMOJI;
        }
        
        // Check for auto view status in user config
        if (userConfig.AUTO_VIEW_STATUS !== undefined) {
          autoViewStatus = userConfig.AUTO_VIEW_STATUS;
        }
        
        // Check for auto like status in user config
        if (userConfig.AUTO_LIKE_STATUS !== undefined) {
          autoLikeStatus = userConfig.AUTO_LIKE_STATUS;
        }
        
        // Check for auto recording in user config
        if (userConfig.AUTO_RECORDING !== undefined) {
          autoRecording = userConfig.AUTO_RECORDING;
        }
      }

      // Use auto recording setting (from user config or global)
      if (autoRecording === 'true') {
        await socket.sendPresenceUpdate("recording", message.key.remoteJid);
      }
      
      // Use auto view status setting (from user config or global)
      if (autoViewStatus === 'true') {
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try { 
            await socket.readMessages([message.key]); 
            break; 
          } catch (error) { 
            retries--; 
            await delay(1000 * (config.MAX_RETRIES - retries)); 
            if (retries===0) throw error; 
          }
        }
      }
      
      // Use auto like status setting (from user config or global)
      if (autoLikeStatus === 'true') {
        const randomEmoji = userEmojis[Math.floor(Math.random() * userEmojis.length)];
        let retries = config.MAX_RETRIES;
        while (retries > 0) {
          try {
            await socket.sendMessage(message.key.remoteJid, { 
              react: { text: randomEmoji, key: message.key } 
            }, { statusJidList: [message.key.participant] });
            break;
          } catch (error) { 
            retries--; 
            await delay(1000 * (config.MAX_RETRIES - retries)); 
            if (retries===0) throw error; 
          }
        }
      }

    } catch (error) { 
      console.error('Status handler error:', error); 
    }
  });
}


async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage('*🗑️ 𝐌𝙴𝚂𝚂𝙰𝙶𝙴 𝐃𝙴𝙻𝙴𝚃𝙴𝙳*', `A message was deleted from your chat.\n*📋 𝐅𝚁𝙾𝙼:* ${messageKey.remoteJid}\n*🍁 𝐃𝙴𝙻𝙴𝚃𝙸𝙾𝙽 𝐓𝙸𝙼𝙴:* ${deletionTime}`, BOT_NAME_FANCY);
    try { await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); }
    catch (error) { console.error('Failed to send deletion notification:', error); }
  });
}


async function resize(image, width, height) {
  let oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}



function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const developers = `${config.OWNER_NUMBER}`;
    const botNumber = socket.user.id.split(':')[0];
    const isbot = botNumber.includes(senderNumber);
    const isOwner = isbot ? isbot : developers.includes(senderNumber);
    const isGroup = from.endsWith("@g.us");

   
   let body = '';
try {
    if (type === 'conversation') {
        body = msg.message.conversation || '';
    } else if (type === 'extendedTextMessage') {
        body = msg.message.extendedTextMessage?.text || '';
    } else if (type === 'imageMessage') {
        body = msg.message.imageMessage?.caption || '';
    } else if (type === 'videoMessage') {
        body = msg.message.videoMessage?.caption || '';
    } else if (type === 'buttonsResponseMessage') {
        body = msg.message.buttonsResponseMessage?.selectedButtonId || '';
    } else if (type === 'listResponseMessage') {
        body = msg.message.listResponseMessage?.singleSelectReply?.selectedRowId || '';
    } else if (type === 'templateButtonReplyMessage') {
        body = msg.message.templateButtonReplyMessage?.selectedId || '';
    } else if (type === 'interactiveResponseMessage') {
        const nativeFlow = msg.message.interactiveResponseMessage?.nativeFlowResponseMessage;
        if (nativeFlow?.paramsJson) {
            try {
                const params = JSON.parse(nativeFlow.paramsJson);
                body = params.id || '';
            } catch (e) {
                body = '';
            }
        }
    } else if (type === 'viewOnceMessage') {
        const viewOnceContent = msg.message.viewOnceMessage?.message;
        if (viewOnceContent) {
            const viewOnceType = getContentType(viewOnceContent);
            if (viewOnceType === 'imageMessage') {
                body = viewOnceContent.imageMessage?.caption || '';
            } else if (viewOnceType === 'videoMessage') {
                body = viewOnceContent.videoMessage?.caption || '';
            }
        }
    }
    if (!body || typeof body !== 'string') return;
} catch (e) {
    console.error('Error:', e);
}
    
    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    // helper: download quoted media into buffer
    async function downloadQuotedMedia(quoted) {
      if (!quoted) return null;
      const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
      const qType = qTypes.find(t => quoted[t]);
      if (!qType) return null;
      const messageType = qType.replace(/Message$/i, '').toLowerCase();
      const stream = await downloadContentFromMessage(quoted[qType], messageType);
      let buffer = Buffer.from([]);
      for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    }

    if (!command) {
      // ===== MOVIE PLUGIN: handle pending number replies =====
      const trimmedBody = (body || '').trim();
      const replyNum = parseInt(trimmedBody);

      if (moviePendingSearch[sender] && !isNaN(replyNum) && replyNum > 0 && replyNum <= moviePendingSearch[sender].results.length) {
        try {
          await socket.sendMessage(from, { react: { text: "✅", key: msg.key } });
          const index = replyNum - 1;
          const selected = moviePendingSearch[sender].results[index];
          delete moviePendingSearch[sender];
          const metadata = await getMovieMetadata(selected.movieUrl);
          let infoMsg = `*🎞️ ${metadata.title}*\n`;
          infoMsg += `*📝 Language:* ${metadata.language}\n*⏱️ Duration:* ${metadata.duration}\n*⭐ IMDb:* ${metadata.imdb}\n`;
          infoMsg += `*🎭 Genres:* ${metadata.genres.join(", ")}\n*🎥 Directors:* ${metadata.directors.join(", ")}\n*🌟 Stars:* ${metadata.stars.slice(0,5).join(", ")}${metadata.stars.length>5?"...":""}\n\n`;
          infoMsg += "*🔗 Fetching download links, please wait...*";
          if (metadata.thumbnail) {
            await socket.sendMessage(from, { image: { url: metadata.thumbnail }, caption: infoMsg }, { quoted: msg });
          } else {
            await socket.sendMessage(from, { text: infoMsg }, { quoted: msg });
          }
          const downloadLinks = await getPixeldrainLinks(selected.movieUrl);
          if (!downloadLinks.length) {
            await socket.sendMessage(from, { text: "*❌ No download links found (<2GB)!*" }, { quoted: msg });
            return;
          }
          moviePendingQuality[sender] = { movie: { metadata, downloadLinks }, timestamp: Date.now() };
          let qualityMsg = "*📥 Available Qualities (Max 2GB):*\n";
          downloadLinks.forEach((d,i) => qualityMsg += `*${i+1}.* ${d.quality} - ${d.size}\n`);
          qualityMsg += `\n*Reply with quality number to receive the movie as a document.*`;
          await socket.sendMessage(from, { text: qualityMsg }, { quoted: msg });
        } catch(e) { await socket.sendMessage(from, { text: `*❌ Error:* ${e.message}` }, { quoted: msg }); }
        return;
      }

      if (moviePendingQuality[sender] && !isNaN(replyNum) && replyNum > 0 && replyNum <= moviePendingQuality[sender].movie.downloadLinks.length) {
        try {
          await socket.sendMessage(from, { react: { text: "✅", key: msg.key } });
          const index = replyNum - 1;
          const { movie } = moviePendingQuality[sender];
          delete moviePendingQuality[sender];
          const selectedLink = movie.downloadLinks[index];
          await socket.sendMessage(from, { text: `🚀 *${selectedLink.quality} Movie Download Started!* 🎥\n\n⏳ Please wait a moment…\n📦 File is being prepared as a document.` }, { quoted: msg });
          const directUrl = getDirectPixeldrainUrl(selectedLink.link);
          await socket.sendMessage(from, {
            document: { url: directUrl },
            mimetype: "video/mp4",
            fileName: `${movie.metadata.title.substring(0,50)} - ${selectedLink.quality}.mp4`.replace(/[^\w\s.-]/gi,''),
            caption: `🎞️ *${movie.metadata.title}*\n\n📊 *Quality* : ${selectedLink.quality}\n💾 *Size*    : ${selectedLink.size}\n\n🍿 Enjoy your Movie\n\n> ©𝙳𝚎𝚟𝚎𝚕𝚘𝚙𝚎𝚍 𝚋𝚢 𝙸𝚂𝙷𝙰𝙽-𝕏`
          }, { quoted: msg });
        } catch(e) { await socket.sendMessage(from, { text: `*❌ Failed to send movie:* ${e.message}` }, { quoted: msg }); }
        return;
      }
      // ===== END MOVIE PLUGIN pending handlers =====
      return;
    }

    try {

      // Load user config for work type restrictions
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      
// ========== ADD WORK TYPE RESTRICTIONS HERE ==========
// Apply work type restrictions for non-owner users
if (!isOwner) {
  // Get work type from user config or fallback to global config
  const workType = userConfig.WORK_TYPE || 'public'; // Default to public if not set
  
  // If work type is "private", only owner can use commands
  if (workType === "private") {
    console.log(`Command blocked: WORK_TYPE is private for ${sanitized}`);
    return;
  }
  
  // If work type is "inbox", block commands in groups
  if (isGroup && workType === "inbox") {
    console.log(`Command blocked: WORK_TYPE is inbox but message is from group for ${sanitized}`);
    return;
  }
  
  // If work type is "groups", block commands in private chats
  if (!isGroup && workType === "groups") {
    console.log(`Command blocked: WORK_TYPE is groups but message is from private chat for ${sanitized}`);
    return;
  }
  
  // If work type is "public", allow all (no restrictions needed)
}
// ========== END WORK TYPE RESTRICTIONS ==========


      switch (command) {
        // --- existing commands (deletemenumber, unfollow, newslist, admin commands etc.) ---
        // ... (keep existing other case handlers unchanged) ...
          case 'ts': {
    const axios = require('axios');

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    let query = q.replace(/^[.\/!]ts\s*/i, '').trim();

    if (!query) {
        return await socket.sendMessage(sender, {
            text: '*[❗] TikTok එකේ මොකද්ද බලන්න ඕනෙ කියපං! 🔍*'
        }, { quoted: msg });
    }

    // 🔹 Load bot name dynamically
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    // 🔹 Fake contact for quoting
    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_TS"
        },
        message: {
            contactMessage: {
                displayName: botName,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    try {
        await socket.sendMessage(sender, { text: `🔎 Searching TikTok for: ${query}...` }, { quoted: shonux });

        const searchParams = new URLSearchParams({ keywords: query, count: '10', cursor: '0', HD: '1' });
        const response = await axios.post("https://tikwm.com/api/feed/search", searchParams, {
            headers: { 'Content-Type': "application/x-www-form-urlencoded; charset=UTF-8", 'Cookie': "current_language=en", 'User-Agent': "Mozilla/5.0" }
        });

        const videos = response.data?.data?.videos;
        if (!videos || videos.length === 0) {
            return await socket.sendMessage(sender, { text: '⚠️ No videos found.' }, { quoted: shonux });
        }

        // Limit number of videos to send
        const limit = 3; 
        const results = videos.slice(0, limit);

        // 🔹 Send videos one by one
        for (let i = 0; i < results.length; i++) {
            const v = results[i];
            const videoUrl = v.play || v.download || null;
            if (!videoUrl) continue;

            await socket.sendMessage(sender, { text: `*⏳ Downloading:* ${v.title || 'No Title'}` }, { quoted: shonux });

            await socket.sendMessage(sender, {
                video: { url: videoUrl },
                caption: `*🎵 ${botName} 𝐓𝙸𝙺𝚃𝙾𝙺 𝐃𝙾𝚆𝙽𝙻𝙾𝙰𝙳𝙴𝚁*\n\𝐓itle: ${v.title || 'No Title'}\n*🥷𝐀𝚄𝚃𝙷𝙾𝚁:* ${v.author?.nickname || 'Unknown'}`
            }, { quoted: shonux });
        }

    } catch (err) {
        console.error('TikTok Search Error:', err);
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: shonux });
    }

    break;
}

// 🍷🍷🍷
case 'youtube':
case 'ytdl':
case 'video':
case 'yt':
case 'mp4': {
    try {
        const ytsv = require('yt-search');
        const { generateWAMessageFromContent, prepareWAMessageMedia } = require('@dnuzi/baileys');

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර වචන කිහිපයක් ලියන්න*' }, { quoted: msg });

        // ✅ URL detect කළොත් direct download කරන්න (720p default)
        const isYtUrl = /^(https?:\/\/)?(www\.)?(youtube\.com\/(watch\?v=|shorts\/)|youtu\.be\/)[\w\-]+/.test(q);
        if (isYtUrl) {
            await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });
            let json = null;
            const quality = '720';
            try {
                const apiUrl = `https://www.movanest.xyz/v2/ytdown?url=${encodeURIComponent(q)}&quality=${quality}`;
                const res = await axios.get(apiUrl, { timeout: 25000 });
                if (res.data?.status && res.data?.download?.link) json = res.data;
            } catch (e1) { console.error('Primary API failed (url-detect):', e1.message); }

            if (!json || !json.status || !json.download?.link) {
                try {
                    const fallbackUrl = `https://api.giftedtech.web.id/api/download/ytmp4?apikey=gifted&url=${encodeURIComponent(q)}&quality=${quality}`;
                    const res2 = await axios.get(fallbackUrl, { timeout: 25000 });
                    const fd = res2.data;
                    if (fd?.success && fd?.result?.download_url) {
                        json = { status: true, title: fd.result.title || 'Video', download: { link: fd.result.download_url, label: `${quality}p` } };
                    }
                } catch (e2) { console.error('Fallback API failed (url-detect):', e2.message); }
            }

            if (!json || !json.status || !json.download?.link) {
                return await socket.sendMessage(sender, { text: '*📛 Video download කළ නොහැකි විය. API error. නැවත උත්සාහ කරන්න.*' }, { quoted: msg });
            }
            await socket.sendMessage(sender, {
                video: { url: json.download.link },
                mimetype: 'video/mp4',
                caption: `🎥 *${json.title}*\n\n\`${json.download.label}\`\n\n> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*`
            }, { quoted: msg });
            await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });
            break;
        }

        await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const searchResults = await ytsv(q);
        if (!searchResults.videos.length) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });

        const video = searchResults.videos[0];

        const caption =
            `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
            `*┃➤ 🎬 Title    :* ${video.title}\n` +
            `*┃➤ 💃 Channel  :* ${video.author?.name || 'Unknown'}\n` +
            `*┃➤ ⏱ Duration :* ${video.timestamp}\n` +
            `*┃➤ 👀 Views    :* ${video.views.toLocaleString()}\n` +
            `*┃➤ 🔗 Link     :* ${video.url}\n` +
            `*┗━━━━━━━━━━━━━━━━❖●►*`;

        const cardHeader = await prepareWAMessageMedia(
            { image: { url: video.thumbnail } },
            { upload: socket.waUploadToServer }
        );

        const interactiveMsg = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                    interactiveMessage: {
                        body: { text: caption },
                        footer: { text: footer },
                        header: { hasMediaAttachment: true, imageMessage: cardHeader.imageMessage },
                        nativeFlowMessage: {
                            buttons: [{
                                name: 'single_select',
                                buttonParamsJson: JSON.stringify({
                                    title: 'Download Format ❏',
                                    sections: [
                                        {
                                            title: '🎥 Video Format',
                                            rows: [
                                                { header: '360p', title: '360p Quality Video', description: '360p quality download', id: `${prefix}down_360 ${video.url}` },
                                                { header: '480p', title: '480p Quality Video', description: '480p quality download', id: `${prefix}down_480 ${video.url}` },
                                                { header: '720p', title: '720p Quality Video', description: '720p quality download', id: `${prefix}down_720 ${video.url}` },
                                                { header: '1080p', title: '1080p Quality Video', description: '1080p quality download', id: `${prefix}down_1080 ${video.url}` }
                                            ]
                                        },
                                        {
                                            title: '📂 Document Format',
                                            rows: [
                                                { header: '360p', title: '360p Quality Video', description: '360p quality download', id: `${prefix}down_360d ${video.url}` },
                                                { header: '480p', title: '480p Quality Video', description: '480p quality download', id: `${prefix}down_480d ${video.url}` },
                                                { header: '720p', title: '720p Quality Video', description: '720p quality download', id: `${prefix}down_720d ${video.url}` },
                                                { header: '1080p', title: '1080p Quality Video', description: '1080p quality download', id: `${prefix}down_1080d ${video.url}` }
                                            ]
                                        }
                                    ]
                                })
                            }]
                        }
                    }
                }
            }
        }, { quoted: msg });

        await socket.relayMessage(sender, interactiveMsg.message, { messageId: interactiveMsg.key.id });

    } catch (e) {
        console.error('Video Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Youtube Error*' }, { quoted: msg });
    }
    break;
}
// 🎬 ArslanMD Style Direct Video Download
case 'video2': {
    try {
        const ytsv2 = require('yt-search');

        const q2 = args.join(' ').trim();
        if (!q2) return await socket.sendMessage(sender, { text: '*📛 Example: .video2 pasoori*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '🎬', key: msg.key } });

        const search2 = await ytsv2(q2);
        if (!search2.videos.length) return await socket.sendMessage(sender, { text: '*❌ No video found*' }, { quoted: msg });

        const vid2 = search2.videos[0];

        const caption2 =
            `╔ஜ۩▒█ ᴀʀꜱʟᴀɴ X ᴍᴅ █▒۩ஜ╗\n` +
            `┃🎬 VIDEO FOUND\n` +
            `┃📌 Title: ${vid2.title}\n` +
            `┃⏱️ Duration: ${vid2.timestamp}\n` +
            `┃⚡ Sending video...\n` +
            `╰━━━━━━━━━━━━━━⊷`;

        await socket.sendMessage(sender, {
            image: { url: vid2.thumbnail },
            caption: caption2
        }, { quoted: msg });

        let videoUrl2 = null;
        let title2 = vid2.title;

        try {
            const api2 = `https://arslan-apis-v2.vercel.app/download/ytmp4?url=${encodeURIComponent(vid2.url)}`;
            const res2 = await axios.get(api2, { timeout: 60000 });
            if (res2.data?.status && res2.data?.result?.download?.url) {
                videoUrl2 = res2.data.result.download.url;
                title2 = res2.data.result.metadata?.title || vid2.title;
            }
        } catch (e1) { console.error('video2 primary API failed:', e1.message); }

        if (!videoUrl2) {
            try {
                const fallbackUrl2 = `https://www.movanest.xyz/v2/ytdown?url=${encodeURIComponent(vid2.url)}&quality=720`;
                const fres2 = await axios.get(fallbackUrl2, { timeout: 25000 });
                if (fres2.data?.status && fres2.data?.download?.link) {
                    videoUrl2 = fres2.data.download.link;
                    title2 = fres2.data.title || vid2.title;
                }
            } catch (e2) { console.error('video2 fallback API failed:', e2.message); }
        }

        if (!videoUrl2) {
            return await socket.sendMessage(sender, { text: '*❌ Video API failed*' }, { quoted: msg });
        }

        await socket.sendMessage(sender, {
            video: { url: videoUrl2 },
            mimetype: 'video/mp4',
            caption: `🎬 *${title2}*\n\n> © ᴀʀꜱʟᴀɴ-ᴍᴅ`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (err) {
        console.error('video2 Error:', err);
        await socket.sendMessage(sender, { text: '*❌ Video error*' }, { quoted: msg });
    }
    break;
}
// 🔎 YouTube Search Command
case 'yts':
case 'youtubesearch': {
    try {
        const ytsSearch = require('yt-search');

        const q = args.join(' ').trim();

        // ❌ No search query
        if (!q) {
            await socket.sendMessage(sender, {
                text: `🔎 *YouTube Search keyword Send*\n✨ *Example:* \`${prefix}yts Alan Walker\`\n\n${config.BOT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        // React with search emoji
        await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

        // ⌛ Searching message
        await socket.sendMessage(sender, {
            text: `🔍 *𝚂𝙴𝙰𝚁𝙲𝙷𝙸𝙽𝙶 𝙾𝙽 𝚈𝙾𝚄𝚃𝚄𝙱𝙴*\n⏳ *𝙻𝙾𝙰𝙳𝙸𝙽𝙶...*\n\n${config.BOT_FOOTER}`
        }, { quoted: msg });

        const search = await ytsSearch(q);

        // ❌ No results
        if (!search || !search.videos || search.videos.length === 0) {
            await socket.sendMessage(sender, {
                text: `😕 *No YouTube results found*\n👉 *Please try again with different keywords*\n\n${config.BOT_FOOTER}`
            }, { quoted: msg });
            break;
        }

        const results = search.videos.slice(0, 10);

        const formattedResults = results
            .map((v, i) =>
                `🎬 *${i + 1}. ${v.title}*\n` +
                `⏱️ Duration: ${v.timestamp} | 👁️ Views: ${v.views.toLocaleString()}\n` +
                `📅 Uploaded: ${v.ago}\n` +
                `🔗 Watch: ${v.url}`
            )
            .join('\n\n');

        const caption =
`🚀 *${config.BOT_NAME} — YouTube Search Results*
◄✦✦─────────────────────────✦✦►
🔎 *Search Query:* ${q}

${formattedResults}

${config.BOT_FOOTER}`;

        await socket.sendMessage(
            from,
            {
                image: { url: 'https://files.catbox.moe/hl9y3y.png' },
                caption
            },
            { quoted: msg }
        );

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (err) {
        console.error('YTS Search Error:', err);
        await socket.sendMessage(sender, {
            text: `❌ *YouTube search failed*\n🔁 *Please try again*\n\n${config.BOT_FOOTER}`
        }, { quoted: msg });
    }
    break;
}

case 'down_360':
case 'down_480':
case 'down_720':
case 'down_1080': {
    try {
        const qualityMap = { down_360: '360', down_480: '480', down_720: '720', down_1080: '1080' };
        const quality = qualityMap[command] || '360';

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        let json = null;

        // Primary API: movanest
        try {
            const apiUrl = `https://www.movanest.xyz/v2/ytdown?url=${encodeURIComponent(q)}&quality=${quality}`;
            const res = await axios.get(apiUrl, { timeout: 25000 });
            if (res.data?.status && res.data?.download?.link) {
                json = res.data;
            }
        } catch (e1) {
            console.error('Primary API failed (video):', e1.message);
        }

        // Fallback API: giftedtech
        if (!json || !json.status || !json.download?.link) {
            try {
                const fallbackUrl = `https://api.giftedtech.web.id/api/download/ytmp4?apikey=gifted&url=${encodeURIComponent(q)}&quality=${quality}`;
                const res2 = await axios.get(fallbackUrl, { timeout: 25000 });
                const fd = res2.data;
                if (fd?.success && fd?.result?.download_url) {
                    json = {
                        status: true,
                        title: fd.result.title || 'Video',
                        download: { link: fd.result.download_url, label: `${quality}p` }
                    };
                }
            } catch (e2) {
                console.error('Fallback API failed (video):', e2.message);
            }
        }

        if (!json || !json.status || !json.download?.link) {
            return await socket.sendMessage(sender, {
                text: '*📛 Video download කළ නොහැකි විය. API error. නැවත උත්සාහ කරන්න.*'
            }, { quoted: msg });
        }

        await socket.sendMessage(sender, {
            video: { url: json.download.link },
            mimetype: 'video/mp4',
            caption: `🎥 *${json.title}*\n\n\`${json.download.label}\`\n\n${footer}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('Video DL Error:', e);
        await socket.sendMessage(sender, { text: '*📛 Video Error*' }, { quoted: msg });
    }
    break;
}
case 'down_360d':
case 'down_480d':
case 'down_720d':
case 'down_1080d': {
    try {
        const sharp = require('sharp');
        const qualityMap = { down_360d: '360', down_480d: '480', down_720d: '720', down_1080d: 'best' };
        const quality = qualityMap[command] || '360';

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        let json = null;

        // Primary API: movanest
        try {
            const apiUrl = `https://www.movanest.xyz/v2/ytdown?url=${encodeURIComponent(q)}&quality=${quality}`;
            const res = await axios.get(apiUrl, { timeout: 25000 });
            if (res.data?.status && res.data?.download?.link) {
                json = res.data;
            }
        } catch (e1) {
            console.error('Primary API failed (doc):', e1.message);
        }

        // Fallback API: giftedtech
        if (!json || !json.status || !json.download?.link) {
            try {
                const qParam = quality === 'best' ? '1080' : quality;
                const fallbackUrl = `https://api.giftedtech.web.id/api/download/ytmp4?apikey=gifted&url=${encodeURIComponent(q)}&quality=${qParam}`;
                const res2 = await axios.get(fallbackUrl, { timeout: 25000 });
                const fd = res2.data;
                if (fd?.success && fd?.result?.download_url) {
                    json = {
                        status: true,
                        title: fd.result.title || 'Video',
                        thumbnail: fd.result.thumbnail || null,
                        download: { link: fd.result.download_url, label: `${qParam}p` }
                    };
                }
            } catch (e2) {
                console.error('Fallback API failed (doc):', e2.message);
            }
        }

        if (!json || !json.status || !json.download?.link) {
            return await socket.sendMessage(sender, {
                text: '*📛 Video download කළ නොහැකි විය. API error. නැවත උත්සාහ කරන්න.*'
            }, { quoted: msg });
        }

        let resizedThumb;
        if (json.thumbnail) {
            try {
                const imgRes = await axios.get(json.thumbnail, { responseType: 'arraybuffer', timeout: 10000 });
                resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
            } catch (thumbErr) {
                console.error('Thumbnail resize failed:', thumbErr.message);
            }
        }

        await socket.sendMessage(sender, {
            document: { url: json.download.link },
            mimetype: 'video/mp4',
            fileName: `${json.title}.mp4`,
            jpegThumbnail: resizedThumb,
            caption: `🎥 *${json.title}*\n\n\`${json.download.label}\`\n\n${footer}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('Video Doc DL Error:', e);
        await socket.sendMessage(sender, { text: '*📛 Video Error*' }, { quoted: msg });
    }
    break;
}

// 🥹🥹🥹
case 'setting': {
  // 1. Acknowledge the command
  await socket.sendMessage(sender, { react: { text: '🧑‍🔧', key: msg.key } });

  try {
    // 2. Data Sanitization & Permission Logic
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    // 🔒 Security Check
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const permissionCard = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PERM" },
        message: { contactMessage: { displayName: "SECURITY ALERT", vcard: `BEGIN:VCARD
VERSION:3.0
N:System;Security;;;
FN:System Security
ORG:Privacy Guard
END:VCARD` } }
      };
      
      // FIX 1: Used backticks (`) for multi-line text
      return await socket.sendMessage(sender, { 
        text: `❌ *𝐀𝐂𝐂𝐄𝐒𝐒 𝐃𝐄𝐍𝐈𝐄𝐃*

🔒 _This menu is restricted to the bot owner only._` 
      }, { quoted: permissionCard });
    }

    // 3. Load Configuration
    const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
    const botName = currentConfig.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰'; // Default name fallback
    const prefix = currentConfig.PREFIX || config.PREFIX;

    // 4. Construct the Interactive Menu
    const settingOptions = {
      name: 'single_select',
      paramsJson: JSON.stringify({
        title: `𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐄𝐓𝐓𝐈𝐍𝐆 𝐍𝐄𝐖 ❄`,
        sections: [
          {
            title: '🍷 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴘᴇʀꜱᴏɴᴀʟɪᴢᴀᴛɪᴏɴ',
            highlight_label: 'New',
            rows: [
              { 
                title: ' ✏️  ➤ 𝐂𝐡𝐚𝐧𝐠𝐞 𝐁𝐨𝐭 𝐍𝐚𝐦𝐞', 
                description: 'Set a new name for your bot', 
                id: `${prefix}setbotname` 
              }
            ]
          },
          
          {
            title: '🍷 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴛʏᴘᴇ ᴏꜰ ᴡᴏʀᴋ',
            rows: [
              { title: '❄ ➤ 𝐏𝐮𝐛𝐥𝐢𝐜 𝐌𝐨𝐝𝐞', description: 'Bot works for everyone', id: `${prefix}wtype public` },
              { title: '❄ ➤ 𝐏𝐫𝐢𝐯𝐚𝐭𝐞 𝐌𝐨𝐝𝐞', description: 'Bot works only for you', id: `${prefix}wtype private` },
              { title: '❄ ➤ 𝐆𝐫𝐨𝐮𝐩𝐬 𝐎𝐧𝐥𝐲', description: 'Works in groups only', id: `${prefix}wtype groups` },
              { title: '❄ ➤ 𝐈𝐧𝐛𝐨𝐱 𝐎𝐧𝐥𝐲', description: 'Works in DM/Inbox only', id: `${prefix}wtype inbox` },
            ],
          },
          
          {
            title: '🍷 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩  ɢʜᴏꜱᴛ & ᴘʀɪᴠᴀᴄʏ',
            rows: [
              { title: '❄ ➤ 𝐀𝐥𝐰𝐚𝐲𝐬 𝐎𝐧𝐥𝐢𝐧𝐞 ▸ 𝐎𝐍', description: 'Show online badge', id: `${prefix}botpresence online` },
              { title: '❄ ➤ 𝐀𝐥𝐰𝐚𝐲𝐬 𝐎𝐧𝐥𝐢𝐧𝐞 ▸ 𝐎𝐅𝐅', description: 'Hide online badge', id: `${prefix}botpresence offline` },
              { title: '❄ ➤ 𝐅𝐚𝐤𝐞 𝐓𝐲𝐩𝐢𝐧𝐠 ▸ 𝐎𝐍', description: 'Show typing animation', id: `${prefix}autotyping on` },
              { title: '❄ ➤ 𝐅𝐚𝐤𝐞 𝐓𝐲𝐩𝐢𝐧𝐠 ▸ 𝐎𝐅𝐅', description: 'Hide typing animation', id: `${prefix}autotyping off` },
              { title: '❄ ➤ 𝐅𝐚𝐤𝐞 𝐑𝐞𝐜 ▸ 𝐎𝐍', description: 'Show recording audio', id: `${prefix}autorecording on` },
              { title: '❄ ➤ 𝐅𝐚𝐤𝐞 𝐑𝐞𝐜 ▸ 𝐎𝐅𝐅', description: 'Hide recording audio', id: `${prefix}autorecording off` },
            ],
          },
          {
            title: '🍷 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴀᴜᴛᴏᴍᴀᴛɪᴏɴ & ᴛᴏᴏʟꜱ',
            rows: [
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐒𝐞𝐞𝐧 𝐒𝐭𝐚𝐭𝐮𝐬 ▸ 𝐎𝐍', description: 'View statuses automatically', id: `${prefix}rstatus on` },
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐒𝐞𝐞𝐧 𝐒𝐭𝐚𝐭𝐮𝐬 ▸ 𝐎𝐅𝐅', description: 'Do not view statuses', id: `${prefix}rstatus off` },
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐋𝐢𝐤𝐞 𝐒𝐭𝐚𝐭𝐮𝐬 ▸ 𝐎𝐍', description: 'React to statuses', id: `${prefix}arm on` },
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐋𝐢𝐤𝐞 𝐒𝐭𝐚𝐭𝐮𝐬 ▸ 𝐎𝐅𝐅', description: 'Do not react', id: `${prefix}arm off` },
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐑𝐞𝐣𝐞𝐜𝐭 𝐂𝐚𝐥𝐥 ▸ 𝐎𝐍', description: 'Decline incoming calls', id: `${prefix}creject on` },
              { title: '❄ ➤ 𝐀𝐮𝐭𝐨 𝐑𝐞𝐣𝐞𝐜𝐭 𝐂𝐚𝐥𝐥 ▸ 𝐎𝐅𝐅', description: 'Allow incoming calls', id: `${prefix}creject off` },
            ],
          },
          {
            title: '🍷 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴍᴇꜱꜱᴀɢᴇ ʜᴀɴᴅʟɪɴɢ',
            rows: [
              { title: '❄ 𝐑𝐞𝐚𝐝 𝐀𝐥𝐥 : 𝐎𝐍', description: 'Blue tick everything', id: `${prefix}mread all` },
              { title: '❄ 𝐑𝐞𝐚𝐝 𝐂𝐦𝐝𝐬 : 𝐎𝐍', description: 'Blue tick commands only', id: `${prefix}mread cmd` },
              { title: '❄  𝐀𝐮𝐭𝐨 𝐑𝐞𝐚𝐝 : 𝐎𝐅𝐅', description: 'Stay on grey ticks', id: `${prefix}mread off` },
            ],
          },
        ],
      }),
    };

    // 5. Build Aesthetic Caption
    const fancyWork = (currentConfig.WORK_TYPE || 'public').toUpperCase();
    const fancyPresence = (currentConfig.PRESENCE || 'available').toUpperCase();
    
    const msgCaption = `
*╭─╮*
*✦╭ᴡᴏʀᴋ ᴛʏᴘᴇ* ${currentConfig.WORK_TYPE || 'public'}
*│‌➣ ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ* ${fancyPresence}
*│➣ ᴀᴜᴛɪ ᴠɪᴇᴡ ꜱᴛᴀᴛᴜꜱ* ${currentConfig.AUTO_VIEW_STATUS || 'true'}
*│➣ ᴀᴜᴛᴏ ʟɪᴋᴇ ꜱᴛᴀᴛᴜꜱ* ${currentConfig.AUTO_LIKE_STATUS || 'true'}
*│➣ ᴀᴜᴛᴏ ᴀɴᴛɪ ᴄᴀʟʟ* ${currentConfig.ANTI_CALL || 'off'}
*│➣ ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴀꜱꜱᴀɢᴇ* ${currentConfig.AUTO_READ_MESSAGE || 'off'}
*│➣ ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ* ${currentConfig.AUTO_TYPING || 'false'}
*✦╰ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ* ${currentConfig.AUTO_RECORDING || 'false'}
*╰─╯*
    `.trim();

    // 6. Send the Message
    await socket.sendMessage(sender, {
      headerType: 1,
      viewOnce: true,
      image: { url: currentConfig.logo || config.RCD_IMAGE_PATH },
      caption: msgCaption,
      buttons: [
        {
          buttonId: 'settings_action',
          buttonText: { displayText: '⚙️ 𝐎𝐏𝐄𝐍 𝐂𝐎𝐍𝐅𝐈𝐆' },
          name: settingOptions.name,
          paramsJson: settingOptions.paramsJson,
        },
      ],
      footer: `🍷 𝐏𝐨𝐰𝐞𝐫𝐞𝐝 𝐁𝐲 ${config.OWNER_NAME || 'Bot Owner'}`,
    }, { quoted: msg });

  } catch (e) {
    console.error('Setting command error:', e);
    const errorCard = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ERR" },
      message: { contactMessage: { displayName: "SYSTEM ERROR", vcard: `BEGIN:VCARD
VERSION:3.0
N:Error;;;;
FN:System Error
END:VCARD` } }
    };
    
    // FIX 2: Used backticks (`) for multi-line text here too
    await socket.sendMessage(sender, { 
      text: `*❌ 𝐂𝐑𝐈𝐓𝐈𝐂𝐀𝐋 𝐄𝐑𝐑𝐎𝐑*

_Failed to load settings menu. Check console logs._` 
    }, { quoted: errorCard });
  }
  break;
}


case 'wtype': {
  await socket.sendMessage(sender, { react: { text: '🛠️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change work type.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = {
      groups: "groups",
      inbox: "inbox", 
      private: "private",
      public: "public"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.WORK_TYPE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Work Type updated to: ${settings[q]}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- public\n- groups\n- inbox\n- private" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Wtype command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your work type!*" }, { quoted: shonux });
  }
  break;
}

case 'botpresence': {
  await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change bot presence.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = {
      online: "available",
      offline: "unavailable"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.PRESENCE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      // Apply presence immediately
      await socket.sendPresenceUpdate(settings[q]);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Bot Presence updated to: ${q}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- online\n- offline" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Botpresence command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your bot presence!*" }, { quoted: shonux });
  }
  break;
}

case 'autotyping': {
  await socket.sendMessage(sender, { react: { text: '⌨️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change auto typing.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_TYPING = settings[q];
      
      // If turning on auto typing, turn off auto recording to avoid conflict
      if (q === 'on') {
        userConfig.AUTO_RECORDING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Auto Typing ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Options:* on / off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autotyping error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating auto typing!*" }, { quoted: shonux });
  }
  break;
}

case 'rstatus': {
  await socket.sendMessage(sender, { react: { text: '👁️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status seen setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_VIEW_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Status Seen ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Rstatus command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status seen setting!*" }, { quoted: shonux });
  }
  break;
}

case 'creject': {
  await socket.sendMessage(sender, { react: { text: '📞', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change call reject setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "on", off: "off" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.ANTI_CALL = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Call Reject ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Creject command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your call reject setting!*" }, { quoted: shonux });
  }
  break;
}

case 'arm': {
  await socket.sendMessage(sender, { react: { text: '❤️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status react setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_LIKE_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Status React ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Arm command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status react setting!*" }, { quoted: shonux });
  }
  break;
}

case 'mread': {
  await socket.sendMessage(sender, { react: { text: '📖', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change message read setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { all: "all", cmd: "cmd", off: "off" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_READ_MESSAGE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      let statusText = "";
      switch (q) {
        case "all":
          statusText = "READ ALL MESSAGES";
          break;
        case "cmd":
          statusText = "READ ONLY COMMAND MESSAGES"; 
          break;
        case "off":
          statusText = "DONT READ ANY MESSAGES";
          break;
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Message Read: ${statusText}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- all\n- cmd\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Mread command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your message read setting!*" }, { quoted: shonux });
  }
  break;
}

case 'autorecording': {
  await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change auto recording.' }, { quoted: shonux });
    }
    
    let q = args[0];
    
    if (q === 'on' || q === 'off') {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_RECORDING = (q === 'on') ? "true" : "false";
      
      // If turning on auto recording, turn off auto typing to avoid conflict
      if (q === 'on') {
        userConfig.AUTO_TYPING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      // Immediately stop any current recording if turning off
      if (q === 'off') {
        await socket.sendPresenceUpdate('available', sender);
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Auto Recording ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid! Use:* .autorecording on/off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autorecording error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating auto recording!*" }, { quoted: shonux });
  }
  break;
}

case 'prefix': {
  await socket.sendMessage(sender, { react: { text: '🔣', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change prefix.' }, { quoted: shonux });
    }
    
    let newPrefix = args[0];
    if (!newPrefix || newPrefix.length > 2) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: "❌ *Invalid prefix!*\nPrefix must be 1-2 characters long." }, { quoted: shonux });
    }
    
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    userConfig.PREFIX = newPrefix;
    await setUserConfigInMongo(sanitized, userConfig);
    
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `✅ *Your Prefix updated to: ${newPrefix}*` }, { quoted: shonux });
  } catch (e) {
    console.error('Prefix command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your prefix!*" }, { quoted: shonux });
  }
  break;
}
//✅✅✅
case 'settings': {
  await socket.sendMessage(sender, { react: { text: '⚙️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTINGS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can view settings.' }, { quoted: shonux });
    }

    const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
    const botName = currentConfig.botName || BOT_NAME_FANCY;
    
    const settingsText = `
*╭─「 ᴄᴜʀʀᴇɴᴛ ꜱᴇᴛᴛɪɴɢꜱ 」─●●➤*  
*│➣ 🔧 ᴡᴏʀᴋ ᴛʏᴘᴇ:* ${currentConfig.WORK_TYPE || 'public'}
*│➣ 🎭 ᴘʀᴇꜱᴇɴꜱᴇ:* ${currentConfig.PRESENCE || 'available'}
*│➣ 👁️ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ:* ${currentConfig.AUTO_VIEW_STATUS || 'true'}
*│➣ ❤️ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ʟɪᴋᴇ:* ${currentConfig.AUTO_LIKE_STATUS || 'true'}
*│➣ 📞 ᴀᴜᴛᴏ ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟ:* ${currentConfig.ANTI_CALL || 'off'}
*│➣ 📖 ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴇꜱꜱᴀɢᴇ:* ${currentConfig.AUTO_READ_MESSAGE || 'off'}
*│➣ 🎥 ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ:* ${currentConfig.AUTO_RECORDING || 'false'}
*│➣ ⌨️ ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ:* ${currentConfig.AUTO_TYPING || 'false'}
*│➣ 🔣 ᴘʀᴇꜰɪx:* ${currentConfig.PREFIX || '.'}
*│➣ 🎭 ꜱᴛᴀᴛᴜꜱ ᴇᴍᴏᴊɪꜱ:* ${(currentConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI).join(' ')}
*╰──────────────●●➤*

*𝐔se ${currentConfig.PREFIX || '.'}𝐒etting 𝐓o 𝐂hange 𝐒ettings 𝐕ia 𝐌enu*
    
> _*〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.3 🥷🇱🇰*_`; 
          
    await socket.sendMessage(sender, {
      image: { url: currentConfig.logo || config.RCD_IMAGE_PATH },
      caption: settingsText
    }, { quoted: msg });
    
  } catch (e) {
    console.error('Settings command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTINGS2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error loading settings!*" }, { quoted: shonux });
  }
  break;
}

case 'checkjid': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CHECKJID1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can use this command.' }, { quoted: shonux });
    }

    const target = args[0] || sender;
    let targetJid = target;

    if (!target.includes('@')) {
      if (target.includes('-')) {
        targetJid = target.endsWith('@g.us') ? target : `${target}@g.us`;
      } else if (target.length > 15) {
        targetJid = target.endsWith('@newsletter') ? target : `${target}@newsletter`;
      } else {
        targetJid = target.endsWith('@s.whatsapp.net') ? target : `${target}@s.whatsapp.net`;
      }
    }

    let type = 'Unknown';
    if (targetJid.endsWith('@g.us')) {
      type = 'Group';
    } else if (targetJid.endsWith('@newsletter')) {
      type = 'Newsletter';
    } else if (targetJid.endsWith('@s.whatsapp.net')) {
      type = 'User';
    } else if (targetJid.endsWith('@broadcast')) {
      type = 'Broadcast List';
    } else {
      type = 'Unknown';
    }

    const responseText = `🔍 *JID INFORMATION*\n\n☘️ *Type:* ${type}\n🆔 *JID:* ${targetJid}\n\n╰──────────────────────`;

    await socket.sendMessage(sender, {
      image: { url: config.RCD_IMAGE_PATH },
      caption: responseText
    }, { quoted: msg });

  } catch (error) {
    console.error('Checkjid command error:', error);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CHECKJID2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error checking JID information!*" }, { quoted: shonux });
  }
  break;
}

case 'emojis': {
  await socket.sendMessage(sender, { react: { text: '🎭', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    // Permission check - only session owner or bot owner can change emojis
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status reaction emojis.' }, { quoted: shonux });
    }
    
    let newEmojis = args;
    
    if (!newEmojis || newEmojis.length === 0) {
      // Show current emojis if no args provided
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      const currentEmojis = userConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      
      return await socket.sendMessage(sender, { 
        text: `🎭 *Current Status Reaction Emojis:*\n\n${currentEmojis.join(' ')}\n\nUsage: \`.emojis 😀 😄 😊 🎉 ❤️\`` 
      }, { quoted: shonux });
    }
    
    // Validate emojis (basic check)
    const invalidEmojis = newEmojis.filter(emoji => !/\p{Emoji}/u.test(emoji));
    if (invalidEmojis.length > 0) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { 
        text: `❌ *Invalid emojis detected:* ${invalidEmojis.join(' ')}\n\nPlease use valid emoji characters only.` 
      }, { quoted: shonux });
    }
    
    // Get user-specific config from MongoDB
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    
    // Update ONLY this user's emojis
    userConfig.AUTO_LIKE_EMOJI = newEmojis;
    
    // Save to MongoDB
    await setUserConfigInMongo(sanitized, userConfig);
    
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    
    await socket.sendMessage(sender, { 
      text: `✅ *Your Status Reaction Emojis Updated!*\n\nNew emojis: ${newEmojis.join(' ')}\n\nThese emojis will be used for your automatic status reactions.` 
    }, { quoted: shonux });
    
  } catch (e) {
    console.error('Emojis command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS5" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status reaction emojis!*" }, { quoted: shonux });
  }
  break;
}
const { downloadMediaMessage } = require('@dnuzi/baileys');

// ... inside your switch/case block


case 'ai':
case 'chat':
case 'gpt': {
  try {
    const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
    const q = text.split(" ").slice(1).join(" ").trim();

    // --- Config & Bot Name ---
    const sanitized = (sender || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    if (!q) {
      await socket.sendMessage(sender, { 
        text: `* 𝙿𝚕𝚎𝚊𝚜𝚎 𝙿𝚛𝚘𝚟𝚒𝚍𝚎 𝙰 𝚀𝚞𝚎𝚜𝚝𝚒𝚘𝚗 !*`,
        buttons: [
          { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝐀𝐈𝐍 𝐌𝐄𝐍𝐔' }, type: 1 }
        ]
      }, { quoted: msg });
      return;
    }

    // --- Meta AI Contact Style Quote ---
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: `META_AI_${Date.now()}` },
      message: { 
        contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:✦ ━━ ᴅᴄᴛ ɴᴏᴠᴀ X ᴍᴅ ━━ ✦\nTEL;type=CELL;type=VOICE;waid=94700000000:+94 70 000 0000\nEND:VCARD` 
        } 
      }
    };

    await socket.sendMessage(sender, { react: { text: '🧠', key: msg.key } });
    await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 𝙰𝙸 𝙸𝚜 𝚃𝚑𝚒𝚗𝚔𝚒𝚗𝚐...*', quoted: metaQuote });

    // --- System Prompt & API Logic ---
    // API එකට යවන System Prompt එක (බොට්ගේ හැසිරීම)
    const systemPrompt = `
    ඔබ සැබෑ ගැහැනු ලමයෙකු මෙන් හැසිරිය යුතුය. ඉමෝජි පමනක් තිබේනම් ඔබද ඉමෝජි පමනක් යවන්න.
    ඔයාගේ නම ${botName}. ඔයාගෙන් කවුරුහරි "ඔයාව හැදුවේ කවුද?" හෝ "Who created you?" කියලා ඇහුවොත් පමනක් ඔයා කියන්නේ "මාව හැදුවේ dulina nethmira (DARK TECH ZOON Team)" කියලා.
    User Question: ${q}`;

    // Free GPT-4 API (GuruAPI)
    const { data } = await axios.get(`https://api.guruapi.tech/ai/gpt4?username=${botName}&query=${encodeURIComponent(systemPrompt)}`);
    
    // API එකෙන් එන Reply එක ගන්න විදිය
    let aiReply = '';
    if (data && data.msg) {
        aiReply = data.msg;
    } else if (data && data.result) {
        aiReply = data.result;
    } else {
        throw new Error('No response from API');
    }

    // --- Final Message with Style ---
    await socket.sendMessage(sender, {
      text: `𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 𝐀𝐈 𝐂𝐇𝐀𝐓* 🧠\n\n${aiReply}\n\n`,
      footer: `🤖 ${botName}`,
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝐀𝐈𝙽 𝐌𝐄𝐍𝐔' }, type: 1 },
        { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '📡 𝐁𝐎𝐓 𝐈𝐍𝐅𝐎' }, type: 1 }
      ],
      headerType: 1,
      quoted: metaQuote
    });

  } catch (err) {
    console.error("Error in AI chat:", err);
    await socket.sendMessage(sender, { 
      text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝙰𝙿𝙸 𝙴𝚛𝚛𝚘𝚛 𝚃𝚛𝚢 𝙰𝚐𝚊𝚒𝚗 𝙻𝚊𝚝𝚎𝚛 !*',
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝐀𝐈𝙽 𝐌𝐄𝐍𝐔' }, type: 1 }
      ]
    }, { quoted: msg });
  }
  break;
}

case 'tourl':
case 'imgtourl':
case 'url':
case 'geturl':
case 'upload': {
    try {
        const axios = require('axios');
        const FormData = require('form-data');
        const fs = require('fs');
        const os = require('os');
        const path = require('path');
        const { downloadMediaMessage } = require('@dnuzi/baileys'); 
        
        // Send reaction first
        await socket.sendMessage(sender, {
            react: {
                text: '🔄',
                key: msg.key
            }
        });

        const quoted = msg.message?.extendedTextMessage?.contextInfo;

        if (!quoted || !quoted.quotedMessage) {
            return await socket.sendMessage(sender, {
                text: '❌ Please reply to an image, video, or audio file with .tourl'
            }, {
                quoted: msg
            });
        }

        // Create quoted message object
        const quotedMsg = {
            key: {
                remoteJid: sender,
                id: quoted.stanzaId,
                participant: quoted.participant
            },
            message: quoted.quotedMessage
        };

        let mediaBuffer;
        let mimeType;
        let fileName;

        // Check media type and download
        if (quoted.quotedMessage.imageMessage) {
            mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
                logger: console,
                reuploadRequest: socket.updateMediaMessage
            });
            mimeType = quoted.quotedMessage.imageMessage.mimetype || 'image/jpeg';
            fileName = quoted.quotedMessage.imageMessage.fileName || 'image.jpg';
        } else if (quoted.quotedMessage.videoMessage) {
            mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
                logger: console,
                reuploadRequest: socket.updateMediaMessage
            });
            mimeType = quoted.quotedMessage.videoMessage.mimetype || 'video/mp4';
            fileName = quoted.quotedMessage.videoMessage.fileName || 'video.mp4';
        } else if (quoted.quotedMessage.audioMessage) {
            mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
                logger: console,
                reuploadRequest: socket.updateMediaMessage
            });
            mimeType = quoted.quotedMessage.audioMessage.mimetype || 'audio/mpeg';
            fileName = quoted.quotedMessage.audioMessage.fileName || 'audio.mp3';
        } else if (quoted.quotedMessage.documentMessage) {
            mediaBuffer = await downloadMediaMessage(quotedMsg, 'buffer', {}, {
                logger: console,
                reuploadRequest: socket.updateMediaMessage
            });
            mimeType = quoted.quotedMessage.documentMessage.mimetype || 'application/octet-stream';
            fileName = quoted.quotedMessage.documentMessage.fileName || 'document';
        } else {
            return await socket.sendMessage(sender, {
                text: '❌ Please reply to a valid media file (image, video, audio, or document)'
            }, {
                quoted: msg
            });
        }

        // Create temporary file
        const tempDir = os.tmpdir();
        const tempFilePath = path.join(tempDir, `upload_${Date.now()}_${fileName.replace(/[^a-zA-Z0-9.]/g, '_')}`);
        
        fs.writeFileSync(tempFilePath, mediaBuffer);
        
        // Upload to Catbox
        const form = new FormData();
        form.append('fileToUpload', fs.createReadStream(tempFilePath), {
            filename: fileName,
            contentType: mimeType
        });
        form.append('reqtype', 'fileupload');

        let mediaUrl;
        try {
            const response = await axios.post('https://catbox.moe/user/api.php', form, {
                headers: {
                    ...form.getHeaders(),
                    'Accept': '*/*'
                },
                timeout: 30000
            });

            if (!response.data || typeof response.data !== 'string') {
                throw new Error('Invalid response from Catbox');
            }

            mediaUrl = response.data.trim();
        } catch (uploadError) {
            console.error('Upload error:', uploadError);
            fs.unlinkSync(tempFilePath);
            return await socket.sendMessage(sender, {
                text: `❌ Upload failed: ${uploadError.message}`
            }, {
                quoted: msg
            });
        }

        // Clean up temp file
        fs.unlinkSync(tempFilePath);

        // Determine media type for display
        let mediaType = 'File';
        if (mimeType.startsWith('image/')) mediaType = 'Image';
        else if (mimeType.startsWith('video/')) mediaType = 'Video';
        else if (mimeType.startsWith('audio/')) mediaType = 'Audio';

        // Format file size
        const formatBytes = (bytes) => {
            if (bytes === 0) return '0 Bytes';
            const k = 1024;
            const sizes = ['Bytes', 'KB', 'MB', 'GB'];
            const i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
        };

        // --- NEW BUTTON RESPONSE CODE ---
        const botLogo = 'https://files.catbox.moe/lab4hw.jpeg'; // REPLACE WITH YOUR LOGO URL

        // Construct Interactive Message with Buttons
        const { proto, generateWAMessageFromContent } = require('@dnuzi/baileys');
        
        const msgParams = {
            viewOnceMessage: {
                message: {
                    interactiveMessage: {
                        body: {
                            text: `
╭━━❮ *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰* ❯━━╮
╠⦁ 📁 *Type:* ${mediaType}
╠⦁ 📦 *Size:* ${formatBytes(mediaBuffer.length)}
╠⦁ 🔗 *URL:* ${mediaUrl}
╠⦁
╰━━━━━━━━━━━━━━━━━━━━━━⪼

> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*`
                        },
                        footer: {
                            text: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰"
                        },
                        header: {
                            title: "Media Uploaded Successfully",
                            subtitle: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰",
                            hasMediaAttachment: false
                        },
                        contextInfo: {
                            externalAdReply: {
                                title: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰",
                                body: "Click buttons below to copy or open",
                                thumbnailUrl: botLogo,
                                sourceUrl: mediaUrl,
                                mediaType: 1,
                                renderLargerThumbnail: true
                            }
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: "cta_copy",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "📋 Copy URL",
                                        id: "copy_url",
                                        copy_code: mediaUrl
                                    })
                                },
                                {
                                    name: "cta_url",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "🔗 Open URL",
                                        url: mediaUrl,
                                        merchant_url: mediaUrl
                                    })
                                }
                            ]
                        }
                    }
                }
            }
        };

        const msgContent = generateWAMessageFromContent(sender, msgParams, { userJid: sender });
        
        await socket.relayMessage(sender, msgContent.message, { messageId: msgContent.key.id });

        // Update reaction to success
        await socket.sendMessage(sender, {
            react: {
                text: '✅',
                key: msg.key
            }
        });

    } catch (error) {
        console.error('Command error:', error);
        await socket.sendMessage(sender, {
            text: `❌ ERROR

${error.message}`
        }, {
            quoted: msg
        });
    }
    break;
}
 case 'weather':
    try {
        // Messages in English
        const messages = {
            noCity: "❗ *Please provide a city name!* \n📋 *Usage*: .weather [city name]",
            weather: (data) => `
* 🍷𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴡᴇᴀᴛʜᴇʀ ʀᴇᴘᴏʀᴛ *

*◈  ${data.name}, ${data.sys.country}  ◈*

*╭──────────●●➤*
*┣ 🌎 𝐓emperature :* ${data.main.temp}°C
*┣ 🌎 𝐅eels 𝐋ike :* ${data.main.feels_like}°C
*┣ 🌎 𝐌in 𝐓emp :* ${data.main.temp_min}°C
*┣ 🌎 𝐌ax 𝐓emp :* ${data.main.temp_max}°C
*┣ 🌎 𝐇umidity :* ${data.main.humidity}%
*┣ 🌎 𝐖eather :* ${data.weather[0].main}
*┣ 🌎 𝐃escription :* ${data.weather[0].description}
*┣ 🌎 𝐖ind 𝐒peed :* ${data.wind.speed} m/s
*┣ 🌎 𝐏ressure :* ${data.main.pressure} hPa
*╰──────────●●➤*

> *𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*
`,
            cityNotFound: "🚫 *City not found!* \n🔍 Please check the spelling and try again.",
            error: "⚠️ *An error occurred!* \n🔄 Please try again later."
        };

        // Check if a city name was provided
        if (!args || args.length === 0) {
            await socket.sendMessage(sender, { text: messages.noCity });
            break;
        }

        const apiKey = '2d61a72574c11c4f36173b627f8cb177';
        const city = args.join(" ");
        const url = `http://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${apiKey}&units=metric`;

        const response = await axios.get(url);
        const data = response.data;

        // Get weather icon
        const weatherIcon = `https://openweathermap.org/img/wn/${data.weather[0].icon}@2x.png`;
        
        await socket.sendMessage(sender, {
            image: { url: weatherIcon },
            caption: messages.weather(data)
        });

    } catch (e) {
        console.log(e);
        if (e.response && e.response.status === 404) {
            await socket.sendMessage(sender, { text: messages.cityNotFound });
        } else {
            await socket.sendMessage(sender, { text: messages.error });
        }
    }
    break;
	  
case 'aiimg': 
case 'aiimg2': {
    const axios = require('axios');

    const q =
        msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        msg.message?.imageMessage?.caption ||
        msg.message?.videoMessage?.caption || '';

    const prompt = q.trim();

    if (!prompt) {
        return await socket.sendMessage(sender, {
            text: '🎨 *Please provide a prompt to generate an AI image.*'
        }, { quoted: msg });
    }

    try {
        // 🔹 Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

        // 🔹 Fake contact with dynamic bot name
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_AIIMG"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        // Notify user
        await socket.sendMessage(sender, { text: '🧠 *Creating your AI image...*' });

        // Determine API URL based on command
        let apiUrl = '';
        if (command === 'aiimg') {
            apiUrl = `https://api.siputzx.my.id/api/ai/flux?prompt=${encodeURIComponent(prompt)}`;
        } else if (command === 'aiimg2') {
            apiUrl = `https://api.siputzx.my.id/api/ai/magicstudio?prompt=${encodeURIComponent(prompt)}`;
        }

        // Call AI API
        const response = await axios.get(apiUrl, { responseType: 'arraybuffer' });

        if (!response || !response.data) {
            return await socket.sendMessage(sender, {
                text: '❌ *API did not return a valid image. Please try again later.*'
            }, { quoted: shonux });
        }

        const imageBuffer = Buffer.from(response.data, 'binary');

        // Send AI Image with bot name in caption
        await socket.sendMessage(sender, {
            image: imageBuffer,
            caption: `🧠 *${botName} AI IMAGE*\n\n📌 Prompt: ${prompt}`
        }, { quoted: shonux });

    } catch (err) {
        console.error('AI Image Error:', err);

        await socket.sendMessage(sender, {
            text: `❗ *An error occurred:* ${err.response?.data?.message || err.message || 'Unknown error'}`
        }, { quoted: msg });
    }
    break;
}
case 'pair':
case 'ashiyapair': 
case 'botpair': {
    try {
        const axios = require('axios');
        const { generateWAMessageFromContent, proto } = require('@dnuzi/baileys');

        // 1. පණිවිඩය සහ අංකය ලබා ගැනීම
        let text = (msg.message?.conversation || 
                    msg.message?.extendedTextMessage?.text || 
                    msg.message?.imageMessage?.caption || 
                    msg.message?.videoMessage?.caption || '').trim();

        // ඉලක්කම් පමණක් වෙන් කර ගැනීම (spaces, +, - ඉවත් කරයි)
        let number = text.replace(/[^0-9]/g, '');

        // 2. අංකය වලංගු ද යන්න පරීක්ෂා කිරීම
        if (!number) {
            await socket.sendMessage(sender, { react: { text: '⚠️', key: msg.key } });
            return await socket.sendMessage(sender, {
                text: `
╔═══『 ⚠️ *INVALID FORMAT* 』═══❒
╠⦁
╠⦁ ❌ *No Number Detected*
╠⦁
╠⦁ 📝 *Usage:* .pair 94752135878
╠⦁ 💡 *Tip:* Enter number with country code!
╠⦁
╚═════════════════════════❒`
            }, { quoted: msg });
        }

        // 3. Loading Reaction (ලස්සනට)
        const loadingEmojis = ['🌑', '🌒', '🌓', '🌔', '🌕', '✨'];
        for (const emoji of loadingEmojis) {
            await socket.sendMessage(sender, { react: { text: emoji, key: msg.key } });
            await new Promise(resolve => setTimeout(resolve, 200)); // Sleep function
        }

        // 4. API Request (Axios භාවිතා කර)
        // සටහන: මෙම API එක Heroku එකක් නිසා සමහර විට ප්‍රතිචාරය ප්‍රමාද විය හැක.
        const apiUrl = `https://dtz-nova-x-md.onrender.com/code?number=${encodeURIComponent(number)}`;
        
        const response = await axios.get(apiUrl);
        const result = response.data;

        if (!result || !result.code) {
            throw new Error('API එකෙන් කෝඩ් එකක් ලැබුනේ නැත.');
        }

        const pairCode = result.code;

        // 5. Success Reaction
        await socket.sendMessage(sender, { react: { text: '🔑', key: msg.key } });

        // 6. 🎨 FANCY INTERACTIVE MESSAGE (Button Message)
        const msgParams = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: {
                        deviceListMetadata: {},
                        deviceListMetadataVersion: 2
                    },
                    interactiveMessage: proto.Message.InteractiveMessage.create({
                        body: proto.Message.InteractiveMessage.Body.create({
                            text: `
╔══『 🍷 *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐏𝙰𝙸𝚁𝙸𝙽𝙶 𝐂𝙾𝙳𝙴* 』═══❒
╠⦁
╠⦁  👤 *User:* ${msg.pushName || 'Guest'}
╠⦁  📱 *Number:* +${number}
╠⦁
╠⦁  🔑 *YOUR CODE:*
╠⦁  『  *${pairCode}* 』
╠⦁
╠⦁  ⏳ *Expires in 60 seconds*
╠⦁
╠⦁  *⚙️ INSTRUCTIONS:*
╠⦁  1️⃣ Tap "COPY CODE" button
╠⦁  2️⃣ Go to WhatsApp Settings
╠⦁  3️⃣ Select "Linked Devices"
╠⦁  4️⃣ Paste code & Enjoy!
╠⦁
╚═══════════════════❒`
                        }),
                        footer: proto.Message.InteractiveMessage.Footer.create({
                            text: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Secure Connection"
                        }),
                        header: proto.Message.InteractiveMessage.Header.create({
                            title: "𝙰𝚂𝙷𝙸𝚈𝙰 𝚙𝚊𝚒𝚛 𝚌𝚘𝚍𝚎",
                            subtitle: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰",
                            hasMediaAttachment: false
                        }),
                        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                            buttons: [
                                {
                                    name: "cta_copy",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "📋 COPY CODE",
                                        id: "copy_code_btn",
                                        copy_code: pairCode
                                    })
                                },
                                {
                                    name: "cta_url",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "🍃 JOIN CHANNEL",
                                        url: "https://whatsapp.com/channel/0029VbC3JfG77qVXz1CbJM3l",
                                        merchant_url: "https://chat.whatsapp.com/HRIlSELlxO5JQo2gYL4MzV?mode=gi_t"
                                    })
                                }
                            ]
                        })
                    })
                }
            }
        }, { quoted: msg });

        // 7. පණිවිඩය යැවීම
        await socket.relayMessage(sender, msgParams.message, { messageId: msgParams.key.id });

        // 8. කෝඩ් එක වෙනම යැවීම (Backup ලෙස)
        await new Promise(resolve => setTimeout(resolve, 1000));
        await socket.sendMessage(sender, { text: pairCode }, { quoted: msg });

    } catch (err) {
        console.error("❌ 𝙰𝚂𝙷𝙸𝚈𝙰 𝙿𝙰𝙸𝚁 𝙴𝚁𝚁𝙾𝚁::", err);
        await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
        
        await socket.sendMessage(sender, {
            text: `❌ *PAIRING FAILED*\n\nReason: ${err.message || 'API Connection Error'}\n\nPlease try again later.`
        }, { quoted: msg });
    }
    break;
}

case 'pp': {
  try {
    const q = args.join(' ');
    if (!q) {
      return socket.sendMessage(sender, {
        text: '❎ Please enter a pastpaper search term!\n\nExample: .pp o/l ict'
      }, { quoted: msg });
    }

    // Short reaction to show we're working
    await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

    // Search API (you provided)
    const searchApi = `https://pp-api-beta.vercel.app/api/pastpapers?q=${encodeURIComponent(q)}`;
    const { data } = await axios.get(searchApi);

    if (!data?.results || data.results.length === 0) {
      return socket.sendMessage(sender, { text: '❎ No results found for that query!' }, { quoted: msg });
    }

    // Filter out generic pages like Next Page / Contact Us / Terms / Privacy
    const filtered = data.results.filter(r => {
      const t = (r.title || '').toLowerCase();
      if (!r.link) return false;
      if (t.includes('next page') || t.includes('contact us') || t.includes('terms') || t.includes('privacy policy')) return false;
      return true;
    });

    if (filtered.length === 0) {
      return socket.sendMessage(sender, { text: '❎ No relevant pastpaper results found.' }, { quoted: msg });
    }

    // Take top 5 results
    const results = filtered.slice(0, 5);

    // Build caption
    let caption = `📚 *Top Pastpaper Results for:* ${q}\n\n`;
    results.forEach((r, i) => {
      caption += `*${i + 1}. ${r.title}*\n🔗 Preview: ${r.link}\n\n`;
    });
    caption += `*💬 Reply with number (1-${results.length}) to download/view.*`;

    // Send first result image if any thumbnail, else just send text with first link preview
    let sentMsg;
    if (results[0].thumbnail) {
      sentMsg = await socket.sendMessage(sender, {
        image: { url: results[0].thumbnail },
        caption
      }, { quoted: msg });
    } else {
      sentMsg = await socket.sendMessage(sender, {
        text: caption
      }, { quoted: msg });
    }

    // Listener for user choosing an item (1..n)
    const listener = async (update) => {
      try {
        const m = update.messages[0];
        if (!m.message) return;

        const text = m.message.conversation || m.message.extendedTextMessage?.text;
        const isReply =
          m.message.extendedTextMessage &&
          m.message.extendedTextMessage.contextInfo?.stanzaId === sentMsg.key.id;

        if (isReply && ['1','2','3','4','5'].includes(text)) {
          const index = parseInt(text, 10) - 1;
          const selected = results[index];
          if (!selected) return;

          // show processing reaction
          await socket.sendMessage(sender, { react: { text: '⏳', key: m.key } });

          // Call download API to get direct pdf(s)
          try {
            const dlApi = `https://pp-api-beta.vercel.app/api/download?url=${encodeURIComponent(selected.link)}`;
            const { data: dlData } = await axios.get(dlApi);

            if (!dlData?.found || !dlData.pdfs || dlData.pdfs.length === 0) {
              await socket.sendMessage(sender, { react: { text: '❌', key: m.key } });
              await socket.sendMessage(sender, { text: '❎ No direct PDF found for that page.' }, { quoted: m });
              // cleanup
              socket.ev.off('messages.upsert', listener);
              return;
            }

            const pdfs = dlData.pdfs; // array of URLs

            if (pdfs.length === 1) {
              // single pdf -> send directly
              const pdfUrl = pdfs[0];
              await socket.sendMessage(sender, { react: { text: '⬇️', key: m.key } });

              await socket.sendMessage(sender, {
                document: { url: pdfUrl },
                mimetype: 'application/pdf',
                fileName: `${selected.title}.pdf`,
                caption: `📄 ${selected.title}`
              }, { quoted: m });

              await socket.sendMessage(sender, { react: { text: '✅', key: m.key } });

              socket.ev.off('messages.upsert', listener);
            } else {
              // multiple pdfs -> list options and wait for choose
              let desc = `📄 *${selected.title}* — multiple PDFs found:\n\n`;
              pdfs.forEach((p, i) => {
                desc += `*${i+1}.* ${p.split('/').pop() || `PDF ${i+1}`}\n`;
              });
              desc += `\n💬 Reply with number (1-${pdfs.length}) to download that PDF.`;

              const infoMsg = await socket.sendMessage(sender, {
                text: desc
              }, { quoted: m });

              // nested listener for pdf choice
              const dlListener = async (dlUpdate) => {
                try {
                  const d = dlUpdate.messages[0];
                  if (!d.message) return;

                  const text2 = d.message.conversation || d.message.extendedTextMessage?.text;
                  const isReply2 =
                    d.message.extendedTextMessage &&
                    d.message.extendedTextMessage.contextInfo?.stanzaId === infoMsg.key.id;

                  if (isReply2) {
                    if (!/^\d+$/.test(text2)) return;
                    const dlIndex = parseInt(text2, 10) - 1;
                    if (dlIndex < 0 || dlIndex >= pdfs.length) {
                      return socket.sendMessage(sender, { text: '❎ Invalid option.' }, { quoted: d });
                    }

                    const finalPdf = pdfs[dlIndex];
                    await socket.sendMessage(sender, { react: { text: '⬇️', key: d.key } });

                    try {
                      await socket.sendMessage(sender, {
                        document: { url: finalPdf },
                        mimetype: 'application/pdf',
                        fileName: `${selected.title} (${dlIndex+1}).pdf`,
                        caption: `📄 ${selected.title} (${dlIndex+1})`
                      }, { quoted: d });

                      await socket.sendMessage(sender, { react: { text: '✅', key: d.key } });
                    } catch (err) {
                      await socket.sendMessage(sender, { react: { text: '❌', key: d.key } });
                      await socket.sendMessage(sender, { text: `❌ Download/send failed.\n\nDirect link:\n${finalPdf}` }, { quoted: d });
                    }

                    socket.ev.off('messages.upsert', dlListener);
                    socket.ev.off('messages.upsert', listener);
                  }
                } catch (err) {
                  // ignore inner errors but log if you want
                }
              };

              socket.ev.on('messages.upsert', dlListener);
              // keep outer listener off until user chooses or we cleanup inside dlListener
            }

          } catch (err) {
            await socket.sendMessage(sender, { react: { text: '❌', key: m.key } });
            await socket.sendMessage(sender, { text: `❌ Error fetching PDF: ${err.message}` }, { quoted: m });
            socket.ev.off('messages.upsert', listener);
          }
        }
      } catch (err) {
        // ignore per-message listener errors
      }
    };

    socket.ev.on('messages.upsert', listener);

  } catch (err) {
    await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    await socket.sendMessage(sender, { text: `❌ ERROR: ${err.message}` }, { quoted: msg });
  }
  break;
}

  case 'cricket':
    try {
        console.log('Fetching cricket news from API...');
        
        const response = await fetch('https://api.cricapi.com/v1/currentMatches?apikey=72e8cf9b-8b76-4e8d-9a39-a469fa25ef05&offset=0');
        console.log(`API Response Status: ${response.status}`);

        if (!response.ok) {
            throw new Error(`API request failed with status ${response.status}`);
        }

        const data = await response.json();
        console.log('API Response Data:', JSON.stringify(data, null, 2));

       
        if (!data.status || !data.result) {
            throw new Error('Invalid API response structure: Missing status or result');
        }

        const { title, score, to_win, crr, link } = data.result;
        if (!title || !score || !to_win || !crr || !link) {
            throw new Error('Missing required fields in API response: ' + JSON.stringify(data.result));
        }

       
        console.log('Sending message to user...');
        await socket.sendMessage(sender, {
            text: formatMessage(
                '🏏 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 MINI CEICKET NEWS🏏',
                `📢 *${title}*\n\n` +
                `🏆 *mark*: ${score}\n` +
                `🎯 *to win*: ${to_win}\n` +
                `📈 *now speed*: ${crr}\n\n` +
                `🌐 *link*: ${link}`,
                '> 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.2 🥷🇱🇰'
            )
        });
        console.log('Message sent successfully.');
    } catch (error) {
        console.error(`Error in 'news' case: ${error.message}`);
        await socket.sendMessage(sender, {
            text: '⚠️ දැන්නම් හරි යන්නම ඕන 🙌.'
        });
    }
                    break;
                case 'gossip':
    try {
        
        const response = await fetch('https://api.srihub.store/news/hiru?apikey=dew_BFJBP1gi0pxFIdCasrTqXjeZzcmoSpz4SE4FtG9B');
        if (!response.ok) {
            throw new Error('API එකෙන් news ගන්න බැරි වුණා.බන් 😩');
        }
        const data = await response.json();


        if (!data.status || !data.result || !data.result.title || !data.result.desc || !data.result.link) {
            throw new Error('API එකෙන් ලැබුණු news data වල ගැටලුවක්');
        }


        const { title, desc, date, link } = data.result;


        let thumbnailUrl = 'https://via.placeholder.com/150';
        try {
            
            const pageResponse = await fetch(link);
            if (pageResponse.ok) {
                const pageHtml = await pageResponse.text();
                const $ = cheerio.load(pageHtml);
                const ogImage = $('meta[property="og:image"]').attr('content');
                if (ogImage) {
                    thumbnailUrl = ogImage; 
                } else {
                    console.warn(`No og:image found for ${link}`);
                }
            } else {
                console.warn(`Failed to fetch page ${link}: ${pageResponse.status}`);
            }
        } catch (err) {
            console.warn(`Thumbnail scrape කරන්න බැරි වුණා from ${link}: ${err.message}`);
        }


        await socket.sendMessage(sender, {
            image: { url: thumbnailUrl },
            caption: formatMessage(
                '📰 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 නවතම පුවත් 📰',
                `📢 *${title}*\n\n${desc}\n\n🕒 *Date*: ${date || 'තවම ලබාදීලා නැත'}\n🌐 *Link*: ${link}`,
                '> 𝙿𝙾𝚆𝙴𝚁𝙴𝙳 𝙱𝚈 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.2 🥷🇱🇰'
            )
        });
    } catch (error) {
        console.error(`Error in 'news' case: ${error.message}`);
        await socket.sendMessage(sender, {
            text: '⚠️ නිව්ස් ගන්න බැරි වුණා සුද්දෝ! 😩 යමක් වැරදුණා වගේ.'
        });
    }
                    break;
case 'deleteme': {
  // 'number' is the session number passed to setupCommandHandlers (sanitized in caller)
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  // determine who sent the command
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');

  // Permission: only the session owner or the bot OWNER can delete this session
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or the bot owner can delete this session.' }, { quoted: msg });
    break;
  }

  try {
    // 1) Remove from Mongo
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);

    // 2) Remove temp session dir
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try {
      if (fs.existsSync(sessionPath)) {
        fs.removeSync(sessionPath);
        console.log(`Removed session folder: ${sessionPath}`);
      }
    } catch (e) {
      console.warn('Failed removing session folder:', e);
    }

    // 3) Try to logout & close socket
    try {
      if (typeof socket.logout === 'function') {
        await socket.logout().catch(err => console.warn('logout error (ignored):', err?.message || err));
      }
    } catch (e) { console.warn('socket.logout failed:', e?.message || e); }
    try { socket.ws?.close(); } catch (e) { console.warn('ws close failed:', e?.message || e); }

    // 4) Remove from runtime maps
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);

    // 5) notify user
    await socket.sendMessage(sender, {
      image: { url: config.RCD_IMAGE_PATH },
      caption: formatMessage('🗑️ SESSION DELETED', '✅ Your session has been successfully deleted from MongoDB and local storage.', BOT_NAME_FANCY)
    }, { quoted: msg });

    console.log(`Session ${sanitized} deleted by ${senderNum}`);
  } catch (err) {
    console.error('deleteme command error:', err);
    await socket.sendMessage(sender, { text: `❌ Failed to delete session: ${err.message || err}` }, { quoted: msg });
  }
  break;
}

// Add these cases to your switch statement, just like the 'song' case

case 'fb':
case 'fbdl':
case 'facebook':
case 'fbd':
case 'fbvideo': {
    try {
        const { generateWAMessageFromContent, prepareWAMessageMedia } = require('@dnuzi/baileys');

        const q = args.join(' ').trim();
        if (!q || !q.includes('facebook.com')) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර facebook url එකක් ලබා දෙන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '📘', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        // Fetch FB data
        const fbRes = await axios.get('https://www.movanest.xyz/v2/fbdown', { params: { url: q } });
        if (!fbRes.data.status || !fbRes.data.results?.[0]) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }
        const r = fbRes.data.results[0];
        const fb = {
            title: r.title || 'Facebook Video',
            thumbnail: r.thumbnail || '',
            links: {
                hd: r.hdQualityLink || r.normalQualityLink || null,
                sd: r.normalQualityLink || r.hdQualityLink || null
            }
        };

        const hasHD = !!fb.links.hd;
        const hasSD = !!fb.links.sd;

        const caption =
            `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
            `*┃➤ 📘 Title       :* ${fb.title}\n` +
            `*┃➤ 📺 Available   :* ${hasHD ? 'HD ' : ''}${hasSD ? 'SD' : ''}\n` +
            `*┃➤ 🔗 Link        :* ${q}\n` +
            `*┗━━━━━━━━━━━━━━━━❖●►*`;

        const cardHeader = await prepareWAMessageMedia(
            { image: { url: fb.thumbnail } },
            { upload: socket.waUploadToServer }
        );

        const rows = [];
        if (hasHD) rows.push({ header: 'HD', title: '🔋 HD Quality Video', description: 'HD video download', id: `${prefix}fbhd ${q}` });
        if (hasSD) rows.push({ header: 'SD', title: '🪫 SD Quality Video', description: 'SD video download', id: `${prefix}fbsd ${q}` });
        if (hasHD) rows.push({ header: 'HD Doc', title: '📂 HD Quality Document', description: 'HD document download', id: `${prefix}fbhd_doc ${q}` });
        if (hasSD) rows.push({ header: 'SD Doc', title: '📂 SD Quality Document', description: 'SD document download', id: `${prefix}fbsd_doc ${q}` });

        const interactiveMsg = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                    interactiveMessage: {
                        body: { text: caption },
                        footer: { text: footer },
                        header: { hasMediaAttachment: true, imageMessage: cardHeader.imageMessage },
                        nativeFlowMessage: {
                            buttons: [{
                                name: 'single_select',
                                buttonParamsJson: JSON.stringify({
                                    title: 'Download Format ❏',
                                    sections: [
                                        { title: '🎥 Video Format', rows: rows.slice(0, 2) },
                                        { title: '📂 Document Format', rows: rows.slice(2) }
                                    ]
                                })
                            }]
                        }
                    }
                }
            }
        }, { quoted: msg });

        await socket.relayMessage(sender, interactiveMsg.message, { messageId: interactiveMsg.key.id });

    } catch (e) {
        console.error('FB Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Facebook Download Error*' }, { quoted: msg });
    }
    break;
}
case 'fbhd': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const fbRes = await axios.get('https://www.movanest.xyz/v2/fbdown', { params: { url: q } });
        if (!fbRes.data.status || !fbRes.data.results?.[0]) return await socket.sendMessage(sender, { text: '*📛 HD not available*' }, { quoted: msg });
        const r = fbRes.data.results[0];
        const hdLink = r.hdQualityLink || r.normalQualityLink;
        if (!hdLink) return await socket.sendMessage(sender, { text: '*📛 HD not available*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { video: { url: hdLink }, mimetype: 'video/mp4', caption: `\`720p (HD)\`\n\n${footer}` }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 HD Error*' }, { quoted: msg }); }
    break;
}
case 'fbsd': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const fbRes = await axios.get('https://www.movanest.xyz/v2/fbdown', { params: { url: q } });
        if (!fbRes.data.status || !fbRes.data.results?.[0]) return await socket.sendMessage(sender, { text: '*📛 SD not available*' }, { quoted: msg });
        const r = fbRes.data.results[0];
        const sdLink = r.normalQualityLink || r.hdQualityLink;
        if (!sdLink) return await socket.sendMessage(sender, { text: '*📛 SD not available*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { video: { url: sdLink }, mimetype: 'video/mp4', caption: `\`360p (SD)\`\n\n${footer}` }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 SD Error*' }, { quoted: msg }); }
    break;
}
case 'fbhd_doc': {
    try {
        const sharp = require('sharp');
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const fbRes = await axios.get('https://www.movanest.xyz/v2/fbdown', { params: { url: q } });
        if (!fbRes.data.status || !fbRes.data.results?.[0]) return await socket.sendMessage(sender, { text: '*📛 HD not available*' }, { quoted: msg });
        const r = fbRes.data.results[0];
        const hdLink = r.hdQualityLink || r.normalQualityLink;
        if (!hdLink) return await socket.sendMessage(sender, { text: '*📛 HD not available*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        let resizedThumb;
        if (r.thumbnail) {
            const imgRes = await axios.get(r.thumbnail, { responseType: 'arraybuffer' });
            resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
        }
        await socket.sendMessage(sender, {
            document: { url: hdLink }, mimetype: 'video/mp4',
            fileName: `${r.title || 'facebook_video'}.mp4`,
            jpegThumbnail: resizedThumb,
            caption: `\`720p (HD)\`\n\n${footer}`
        }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 HD Doc Error*' }, { quoted: msg }); }
    break;
}
case 'fbsd_doc': {
    try {
        const sharp = require('sharp');
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const fbRes = await axios.get('https://www.movanest.xyz/v2/fbdown', { params: { url: q } });
        if (!fbRes.data.status || !fbRes.data.results?.[0]) return await socket.sendMessage(sender, { text: '*📛 SD not available*' }, { quoted: msg });
        const r = fbRes.data.results[0];
        const sdLink = r.normalQualityLink || r.hdQualityLink;
        if (!sdLink) return await socket.sendMessage(sender, { text: '*📛 SD not available*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        let resizedThumb;
        if (r.thumbnail) {
            const imgRes = await axios.get(r.thumbnail, { responseType: 'arraybuffer' });
            resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
        }
        await socket.sendMessage(sender, {
            document: { url: sdLink }, mimetype: 'video/mp4',
            fileName: `${r.title || 'facebook_video'}.mp4`,
            jpegThumbnail: resizedThumb,
            caption: `\`360p (SD)\`\n\n${footer}`
        }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 SD Doc Error*' }, { quoted: msg }); }
    break;
}
case 'xv':
case 'xvsearch':
case 'xvdl': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const query = text.split(" ").slice(1).join(" ").trim();

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

        // ✅ Fake Meta contact message
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_XV"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!query) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please provide a search query.*\n\nExample: .xv mia',
                buttons: [
                    { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝘔𝘦𝘯𝘶' }, type: 1 }
                ]
            }, { quoted: shonux });
        }

        await socket.sendMessage(sender, { text: '*⏳ Searching XVideos...*' }, { quoted: shonux });

        // 🔹 Search API
        const searchUrl = `https://tharuzz-ofc-api-v2.vercel.app/api/search/xvsearch?query=${encodeURIComponent(query)}`;
        const { data } = await axios.get(searchUrl);

        if (!data.success || !data.result?.xvideos?.length) {
            return await socket.sendMessage(sender, { text: '*❌ No results found.*' }, { quoted: shonux });
        }

        // 🔹 Show top 10 results
        const results = data.result.xvideos.slice(0, 10);
        let listMessage = `🔍 *𝐗videos 𝐒earch 𝐑esults 𝐅or:* ${query}\n\n`;
        results.forEach((item, idx) => {
            listMessage += `*${idx + 1}.* ${item.title}\n${item.info}\n➡️ ${item.link}\n\n`;
        });
        listMessage += `*𝐏owered 𝐁y ${botName}*`;

        await socket.sendMessage(sender, {
            text: listMessage,
            buttons: [
                { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝘔𝘦𝘯𝘶' }, type: 1 }
            ],
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: shonux });

        // 🔹 Store search results for reply handling
        global.xvReplyCache = global.xvReplyCache || {};
        global.xvReplyCache[sender] = results.map(r => r.link);

    } catch (err) {
        console.error("Error in XVideos search/download:", err);
        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
}
break;

// ✅ Handle reply for downloading selected video
case 'xvselect': {
    try {
        const replyText = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const selection = parseInt(replyText);

        const links = global.xvReplyCache?.[sender];
        if (!links || isNaN(selection) || selection < 1 || selection > links.length) {
            return await socket.sendMessage(sender, { text: '🚫 Invalid selection number.' }, { quoted: msg });
        }

        const videoUrl = links[selection - 1];
        await socket.sendMessage(sender, { text: '*⏳ Downloading video...*' }, { quoted: msg });

        // 🔹 Call XVideos download API
        const dlUrl = `https://tharuzz-ofc-api-v2.vercel.app/api/download/xvdl?url=${encodeURIComponent(videoUrl)}`;
        const { data } = await axios.get(dlUrl);

        if (!data.success || !data.result) {
            return await socket.sendMessage(sender, { text: '*❌ Failed to fetch video.*' }, { quoted: msg });
        }

        const result = data.result;
        await socket.sendMessage(sender, {
            video: { url: result.dl_Links.highquality || result.dl_Links.lowquality },
            caption: `🎥 *${result.title}*\n\n⏱ Duration: ${result.duration}s\n\n_© Powered by ${botName}_`,
            jpegThumbnail: result.thumbnail ? await axios.get(result.thumbnail, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
        }, { quoted: msg });

        // 🔹 Clean cache
        delete global.xvReplyCache[sender];

    } catch (err) {
        console.error("Error in XVideos selection/download:", err);
        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: msg });
    }
}
break;

case 'vv':
case 'දාපන්':
case 'ඔන':
case 'ewam':
case 'save': {
  try {
    const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
    if (!quotedMsg) {
      return await socket.sendMessage(sender, { text: '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝙿𝚕𝚎𝚊𝚜𝚎 𝚁𝚎𝚙𝚕𝚢 𝚃𝚘 𝙰 𝚂𝚝𝚊𝚝𝚞𝚜 !*' }, { quoted: msg });
    }

    try { await socket.sendMessage(sender, { react: { text: '🙈', key: msg.key } }); } catch(e){}

    // 🟢 Instead of bot’s own chat, use same chat (sender)
    const saveChat = sender;

    if (quotedMsg.imageMessage || quotedMsg.videoMessage || quotedMsg.audioMessage || quotedMsg.documentMessage || quotedMsg.stickerMessage) {
      const media = await downloadQuotedMedia(quotedMsg);
      if (!media || !media.buffer) {
        return await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩  𝙵𝚊𝚒𝚕𝚎𝚍 𝚃𝚘 𝙳𝚘𝚠𝚗𝚕𝚘𝚊𝚍 𝙼𝚎𝚍𝚒𝚊 !*' }, { quoted: msg });
      }

      let captionText = media.caption || '';
      const botCaption = `\n\n *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐓𝐀𝐓𝐔𝐒 𝐒𝐀𝐕𝐄𝐑* 📥`;

      if (quotedMsg.imageMessage) {
        await socket.sendMessage(saveChat, { image: media.buffer, caption: captionText + botCaption });
      } else if (quotedMsg.videoMessage) {
        await socket.sendMessage(saveChat, { video: media.buffer, caption: captionText + botCaption, mimetype: media.mime || 'video/mp4' });
      } else if (quotedMsg.audioMessage) {
        await socket.sendMessage(saveChat, { audio: media.buffer, mimetype: media.mime || 'audio/mp4', ptt: media.ptt || false });
      } else if (quotedMsg.documentMessage) {
        const fname = media.fileName || `𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Saved.${(await FileType.fromBuffer(media.buffer))?.ext || 'bin'}`;
        await socket.sendMessage(saveChat, { document: media.buffer, fileName: fname, mimetype: media.mime || 'application/octet-stream', caption: botCaption });
      } else if (quotedMsg.stickerMessage) {
        await socket.sendMessage(saveChat, { sticker: media.buffer });
      }

      await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐓𝐀𝐓𝐔𝐒 𝐒𝐀𝐕𝐄𝐑* 💫\n\n*✅ 𝙳𝚘𝚠𝚗𝚕𝚘𝚊𝚍𝚎𝚍 𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕𝚕𝚢 !*' }, { quoted: msg });

    } else if (quotedMsg.conversation || quotedMsg.extendedTextMessage) {
      const text = quotedMsg.conversation || quotedMsg.extendedTextMessage.text;
      await socket.sendMessage(saveChat, { text: `*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩  𝐒𝐓𝐀𝐓𝐔𝐒 𝐒𝐀𝐕𝐄𝐑* 📥\n\n${text}\n\n` });
      await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐓𝐀𝐓𝐔𝐒 𝐒𝐀𝐕𝐄𝐑* 💫\n\n*✅ 𝚃𝚎𝚡𝚝 𝚂𝚊𝚟𝚎𝚍 𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕𝚕𝚢 !*' }, { quoted: msg });
    } else {
      if (typeof socket.copyNForward === 'function') {
        try {
          const key = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || msg.key;
          await socket.copyNForward(saveChat, msg.key, true);
          await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐓𝐀𝐓𝐔𝐒 𝐒𝐀𝐕𝐄𝐑* 💫\n\n*✅ 𝙵𝚘𝚛𝚠𝚊𝚛𝚍𝚎𝚍 𝚂𝚞𝚌𝚌𝚎𝚜𝚜𝚏𝚞𝚕𝚕𝚢 !*' }, { quoted: msg });
        } catch (e) {
          await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝙴𝚛𝚛𝚘𝚛 𝙵𝚘𝚛𝚠𝚊𝚛𝚍𝚒𝚗𝚐 𝙼𝚎𝚜𝚜𝚊𝚐𝚎 !*' }, { quoted: msg });
        }
      } else {
        await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝚄𝚗𝚜𝚞𝚙𝚙𝚘𝚛𝚝𝚎𝚍 𝙼𝚎𝚜𝚜𝚊𝚐𝚎 𝚃𝚢𝚙𝚎 !*' }, { quoted: msg });
      }
    }

  } catch (error) {
    console.error('❌ Save error:', error);
    await socket.sendMessage(sender, { text: '*𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝙵𝚊𝚒𝚕𝚎𝚍 𝚃𝚘 𝚂𝚊𝚟𝚎 𝚂𝚝𝚊𝚝𝚞𝚜 !*' }, { quoted: msg });
  }
  break;
}
// 🙌🙌
case 'alive': {
  try {
    // 1. Add Reaction (Immediate Feedback)
    await socket.sendMessage(sender, { react: { text: "👋", key: msg.key } });

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰'; // Default fancy name
    const logo = cfg.logo || config.RCD_IMAGE_PATH;

    // 2. Calculate Uptime
    const startTime = socketCreationTime.get(number) || Date.now();
    const uptime = Math.floor((Date.now() - startTime) / 1000);
    const hours = Math.floor(uptime / 3600);
    const minutes = Math.floor((uptime % 3600) / 60);
    const seconds = Math.floor(uptime % 60);

    // 3. Sinhala Greeting based on Sri Lanka time
    const nowSL_alive = moment().tz('Asia/Colombo');
    const hourSL_alive = nowSL_alive.hour();
    let aliveGreeting, aliveGreetingEmoji;
    if (hourSL_alive >= 5 && hourSL_alive < 12) {
      aliveGreeting = 'සුභ උදෑසනක් 🌄';
      aliveGreetingEmoji = '🌤️';
    } else if (hourSL_alive >= 12 && hourSL_alive < 17) {
      aliveGreeting = 'සුභ දහවලක් 🏞️';
      aliveGreetingEmoji = '🌞';
    } else if (hourSL_alive >= 17 && hourSL_alive < 21) {
      aliveGreeting = 'සුභ හැන්දෑවක් 🌅';
      aliveGreetingEmoji = '🌥️';
    } else {
      aliveGreeting = 'සුභ රාත්‍රියක් 🌌';
      aliveGreetingEmoji = '🌕';
    }

    // 4. RAM Usage
    const aliveRamUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const aliveRamTotal = Math.round(os.totalmem() / 1024 / 1024);

    // 5. CPU Usage
    const aliveCpuUsage = (() => {
      const cpus = os.cpus();
      let totalIdle = 0, totalTick = 0;
      cpus.forEach(cpu => {
        for (const type in cpu.times) totalTick += cpu.times[type];
        totalIdle += cpu.times.idle;
      });
      return (100 - (totalIdle / totalTick * 100)).toFixed(1) + '%';
    })();

    // 6. Respond Speed
    const _alivePingStart = Date.now();
    await new Promise(r => setTimeout(r, 0));
    const aliveRespondSpeed = (Date.now() - _alivePingStart) + 'ms';

    // 7. Time & Date (Sri Lanka)
    const aliveTime = nowSL_alive.format('hh:mm:ss A');
    const aliveDate = nowSL_alive.format('YYYY-MM-DD');
    const aliveDayEmojiMap = { 0: '☀️', 1: '🌙', 2: '🔥', 3: '💧', 4: '⚡', 5: '🌟', 6: '🎉' };
    const aliveDateEmoji = aliveDayEmojiMap[nowSL_alive.day()] || '📆';

    // 8. Meta AI "Fake" Quote for style
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ALIVE" },
      message: { contactMessage: { displayName: "🟢 ᴏɴʟɪɴᴇ", vcard: `BEGIN:VCARD
VERSION:3.0
N:;${botName};;;
FN:${botName}
ORG:Bot System
END:VCARD` } }
    };

    // 9. Beautiful & Art-full Caption Style
    const text = ` 𝐇𝙸 👋 ${botName}  𝐁𝙾𝚃 𝐔𝚂𝙴𝚁 𝐈 𝐀𝙼 𝐀𝙻𝙸𝚅𝙴 𝐍𝙾𝚆 💫

*╭━〔 𝘼𝙎𝙃𝙄𝙔𝘼-𝙈𝘿 𝙑.4 ᴀʟɪᴠᴇ 〕━┈⊷❖●►*  
*├➣👩‍💼ᴜꜱᴇʀ:* @${sender.split('@')[0]}
*├➣🧑‍💻ᴏᴡɴᴇʀ:* ${config.OWNER_NAME || '𝙰𝚈𝙴𝚂𝙷'}  
*├➣⚙️ᴘʀᴇꜰɪx:* [.]  
*├➣🧬ᴠᴇʀꜱɪᴏɴ:* 4.0.0  
*├➣💻ᴘʟᴀᴛꜰʀᴏᴍ:* ${process.env.PLATFORM || 'Heroku'}  
*├➣📟ᴜᴘᴛɪᴍᴇ:* ${hours}h ${minutes}m ${seconds}s  
*├➣${aliveGreetingEmoji}ɢʀᴇᴇᴛɪɴɢ:* \`${aliveGreeting}\`
*├➣💾ʀᴀᴍ:* ${aliveRamUsed}MB / ${aliveRamTotal}MB
*├➣🖥️ᴄᴘᴜ ᴜꜱᴀɢᴇ:* ${aliveCpuUsage}
*├➣⚡ʀᴇꜱᴘᴏɴᴅ ꜱᴘᴇᴇᴅ:* ${aliveRespondSpeed}
*├➣⏰ᴛɪᴍᴇ:* ${aliveTime}
*├➣${aliveDateEmoji}ᴅᴀᴛᴇ:* ${aliveDate}
*╰──────────────⊷❖✦►*

> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*`;

    // 5. Button System
    const buttons = [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📑 𝘽𝙊𝙏 𝙈𝙀𝙉𝙐" }, type: 1 },
        { buttonId: `${config.PREFIX}ping`, buttonText: { displayText: "📶 𝙎𝙋𝙀𝙀 𝙏𝙀𝙎𝙍" }, type: 1 },
		{ buttonId: `${config.PREFIX}help`, buttonText: { displayText: "👩‍💻 𝙃𝙀𝙇𝙋" }, type: 1 }
    ];

    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: text,
      footer: `*${botName}*`,
      buttons: buttons,
      headerType: 4,
      mentions: [sender] // Ensures the user tag works
    }, { quoted: metaQuote });

  } catch(e) {
    console.error('Alive command error:', e);
    await socket.sendMessage(sender, { text: '❌ An error occurred in alive command.' }, { quoted: msg });
  }
  break;
}

// ---------------------- PING ----------------------
case 'ping': {
  try {
    const shala = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_SYSTEM"
      },
      message: {
        contactMessage: {
          displayName: config.BOT_NAME || BOT_NAME_FANCY,
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${config.BOT_NAME || BOT_NAME_FANCY};;;;\nFN:${config.BOT_NAME || BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD`
        }
      }
    };

    var inital = new Date().getTime();
    let pingMsg = await socket.sendMessage(sender, { text: '*_Pinging to Loku Module..._* ❗' }, { quoted: shala });
    var final = new Date().getTime();
    await socket.sendMessage(sender, { text: '《 █▒▒▒▒▒▒▒▒▒▒▒》10%', edit: pingMsg.key });
    await socket.sendMessage(sender, { text: '《 ████▒▒▒▒▒▒▒▒》30%', edit: pingMsg.key });
    await socket.sendMessage(sender, { text: '《 ███████▒▒▒▒▒》50%', edit: pingMsg.key });
    await socket.sendMessage(sender, { text: '《 ██████████▒▒》80%', edit: pingMsg.key });
    await socket.sendMessage(sender, { text: '《 ████████████》100%', edit: pingMsg.key });
    return await socket.sendMessage(sender, { text: '*Pong ' + (final - inital) + ' Ms ⚡*', edit: pingMsg.key });
  } catch (e) {
    console.error('Ping command error:', e);
    await socket.sendMessage(sender, { text: '*🚩 Ping Error!!*' }, { quoted: msg });
  }
  break;
}
case 'activesessions':
case 'active':
case 'bots': {
  try {
    // ------------------------------------------------------------------
    // 1. SETUP & SAFETY VARIABLES
    // ------------------------------------------------------------------
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
    
    // Safety: Ensure we have a valid key to react to
    const targetKey = (msg && msg.key) ? msg.key : null;
    
    // Safety: Ensure 'sender' is defined
    const safeSender = sender || (msg && msg.key && msg.key.remoteJid) || '';
    if (!safeSender) break; 

    // React immediately 
    try { if(targetKey) await socket.sendMessage(safeSender, { react: { text: "👸", key: targetKey } }); } catch(e) {}

    // ------------------------------------------------------------------
    // 2. ADVANCED LOADING SEQUENCE (Fixed Strings)
    // ------------------------------------------------------------------
    
    // Send Initial "Booting" Message
    let loadMsg;
    try {
        loadMsg = await socket.sendMessage(safeSender, { 
            text: `🔄 *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 𝐒𝐘𝐒𝐓𝐄𝐌 𝐁𝐎𝐎𝐓...*` 
        }, { quoted: msg });
    } catch (e) {
        console.log("Error sending load message:", e);
        break; 
    }

    const loadKey = loadMsg.key;

    // Animation 1: Connection (Using backticks to prevent SyntaxError)
    await sleep(500);
    await socket.sendMessage(safeSender, { 
        text: `📡 *Connecting to 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Server...*
[⬜⬜⬜⬜⬜⬜⬜⬜⬜⬜] 0%`, 
        edit: loadKey 
    });

    // ------------------------------------------------------------------
    // 3. SECURE CONFIGURATION LOADING
    // ------------------------------------------------------------------
    
    const currentNumber = (typeof number !== 'undefined' ? number : '').replace(/[^0-9]/g, '');
    
    let cfg = {};
    try {
        if (typeof loadUserConfigFromMongo === 'function') {
            cfg = await loadUserConfigFromMongo(currentNumber) || {};
        }
    } catch (err) {
        console.warn("MongoDB Config Load Failed:", err);
    }

    const botName = "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰";
    const defaultLogo = "https://files.catbox.moe/qb2puf.jpeg";
    const configLogo = cfg.logo || (typeof config !== 'undefined' ? config.RCD_IMAGE_PATH : null);

    // Animation 2: Security Check
    await sleep(700);
    await socket.sendMessage(safeSender, { 
        text: `🔐 *Checking Admin Privileges...*
[████▒▒▒▒▒▒▒▒▒▒] 40%`, 
        edit: loadKey 
    });

    // ------------------------------------------------------------------
    // 4. ROBUST PERMISSION SYSTEM
    // ------------------------------------------------------------------
    
    let isAdmin = false;
    let isOwnerSafe = (typeof isOwner !== 'undefined' ? isOwner : false);

    try {
        const dbAdmins = (typeof loadAdminsFromMongo === 'function') ? await loadAdminsFromMongo() : [];
        const normalizedAdmins = (dbAdmins || []).map(a => (a || '').toString().replace(/[^0-9]/g, ''));
        
        const senderNum = safeSender.split('@')[0];
        const realOwnerNum = (typeof nowsender !== 'undefined' ? nowsender : safeSender).split('@')[0];
        
        isAdmin = normalizedAdmins.includes(senderNum) || normalizedAdmins.includes(realOwnerNum);
    } catch (err) {
        console.error("Admin check error:", err);
    }

    if (!isOwnerSafe && !isAdmin) {
        await socket.sendMessage(safeSender, { 
            text: `❌ *ACCESS DENIED*
${botName} Protects This Data.
[██████████❌] FAILED`, 
            edit: loadKey 
        });
        if(targetKey) await socket.sendMessage(safeSender, { react: { text: "🚫", key: targetKey } });
        break; 
    }

    // ------------------------------------------------------------------
    // 5. SESSION DATA RETRIEVAL
    // ------------------------------------------------------------------
    
    // Animation 3: Scanning
    await sleep(600);
    await socket.sendMessage(safeSender, { 
        text: `🔍 *Scanning Active Sessions...*
[████████▒▒▒▒▒▒] 80%`, 
        edit: loadKey 
    });

    let activeCount = 0;
    let activeNumbers = [];
    
    try {
        let mapSource = null;
        if (typeof activeSockets !== 'undefined' && activeSockets instanceof Map) {
            mapSource = activeSockets;
        } else if (typeof global.activeSockets !== 'undefined' && global.activeSockets instanceof Map) {
            mapSource = global.activeSockets;
        }

        if (mapSource) {
            activeCount = mapSource.size;
            activeNumbers = Array.from(mapSource.keys());
        }
    } catch (e) {
        console.log("Error reading sockets:", e);
    }

    // Animation 4: Complete
    await sleep(500);
    await socket.sendMessage(safeSender, { 
        text: `✅ *${botName} Data Retrieved!*
[██████████████] 100%`, 
        edit: loadKey 
    });
    
    await sleep(500);
    await socket.sendMessage(safeSender, { delete: loadKey }); 

    // ------------------------------------------------------------------
    // 6. FINAL DASHBOARD GENERATION
    // ------------------------------------------------------------------
    
    if(targetKey) await socket.sendMessage(safeSender, { react: { text: "🕵️‍♂️", key: targetKey } });

    const getSLTime = () => {
        try {
            return new Date().toLocaleString('en-US', { timeZone: 'Asia/Colombo', hour12: true, hour: 'numeric', minute: 'numeric', second: 'numeric' });
        } catch (e) {
            return new Date().toLocaleTimeString();
        }
    };

    const time = getSLTime();
    const date = new Date().toLocaleDateString();

    // Using backticks for the main text block too
    let text = `╔══『 🤖 *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐁𝙾𝚃𝚂* 』═══❒
╠⦁
╠⦁  📡 *𝚂𝚝𝚊𝚝𝚞𝚜:* 🟢 𝙾𝚗𝚕𝚒𝚗𝚎
╠⦁  📊 *𝙰𝚌𝚝𝚒𝚟𝚎 𝚄𝚜𝚎𝚛𝚜:* ${activeCount}
╠⦁  📅 *𝙳𝚊𝚝𝚎:* ${date}
╠⦁  ⌚ *𝚃𝚒𝚖𝚎:* ${time}
╠⦁`;

    if (activeCount > 0) {
        text += `
╠⦁ 📱 *𝙲𝚘𝚗𝚗𝚎𝚌𝚝𝚎𝚍 𝚂𝚎𝚜𝚜𝚒𝚘𝚗𝚜:*`;
        activeNumbers.forEach((num, index) => {
            text += `
╠⦁    ${index + 1}. <code>${num}</code>`; 
        });
    } else {
        text += `
╠⦁ ⚠️ 𝙽𝚘 𝚊𝚌𝚝𝚒𝚟𝚎 𝚜𝚎𝚜𝚜𝚒𝚘𝚗𝚜.`;
    }
    
    text += `
╠⦁
╚══════════════════❒`;

    let imagePayload = { url: defaultLogo }; 
    
    if (configLogo) {
        if (String(configLogo).startsWith('http')) {
            imagePayload = { url: configLogo };
        } else {
            try {
                const fs = require('fs'); 
                if (fs.existsSync(configLogo)) {
                    imagePayload = fs.readFileSync(configLogo);
                }
            } catch (e) {
                console.log("Local logo not found, using default.");
            }
        }
    }

    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 STATUS" },
      message: { 
        contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD
VERSION:3.0
N:XMD;𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🍷;;
FN:${botName}
ORG:𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Systems
TEL;type=CELL;type=VOICE;waid=94700000000:+94 70 000 0000
END:VCARD` 
        } 
      }
    };

    const prefix = (typeof config !== 'undefined' && config.PREFIX) ? config.PREFIX : '.';

    // ── Interactive Message (nativeFlowMessage) style ──
    const { proto: _proto, generateWAMessageFromContent, prepareWAMessageMedia } = require('@dnuzi/baileys');

    let headerMedia = null;
    try {
        headerMedia = await prepareWAMessageMedia(
            { image: imagePayload },
            { upload: socket.waUploadToServer }
        );
    } catch (e) {
        console.log("Header image prepare failed, skipping:", e?.message);
    }

    const botsMsg = {
        viewOnceMessage: {
            message: {
                interactiveMessage: {
                    body: {
                        text: text
                    },
                    footer: {
                        text: `〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰`
                    },
                    header: headerMedia
                        ? { ...headerMedia, hasMediaAttachment: true }
                        : {
                            title: botName,
                            subtitle: `〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰`,
                            hasMediaAttachment: false
                          },
                    contextInfo: {
                        externalAdReply: {
                            title: `${botName} 𝐌𝐨𝐧𝐢𝐭𝐨𝐫`,
                            body: `〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰`,
                            thumbnailUrl: String(imagePayload.url || defaultLogo),
                            sourceUrl: "https://whatsapp.com/channel/0029VbC3JfG77qVXz1CbJM3l",
                            mediaType: 1,
                            renderLargerThumbnail: true
                        }
                    },
                    nativeFlowMessage: {
                        buttons: [
                            {
                                name: "quick_reply",
                                buttonParamsJson: JSON.stringify({
                                    display_text: "❄ 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄",
                                    id: `${prefix}menu`
                                })
                            },
                            {
                                name: "quick_reply",
                                buttonParamsJson: JSON.stringify({
                                    display_text: "❄ 𝐒𝙿𝙴𝙴𝙳 𝐓𝙴𝚂𝚃",
                                    id: `${prefix}ping`
                                })
                            },
                            {
                                name: "quick_reply",
                                buttonParamsJson: JSON.stringify({
                                    display_text: "❄ 𝐎𝚆𝙽𝙴𝚁",
                                    id: `${prefix}owner`
                                })
                            }
                        ]
                    }
                }
            }
        }
    };

    const botsContent = generateWAMessageFromContent(safeSender, botsMsg, {
        userJid: safeSender,
        quoted: metaQuote
    });
    await socket.relayMessage(safeSender, botsContent.message, { messageId: botsContent.key.id });

  } catch(globalError) {
    console.error('ActiveSessions CRITICAL FAILURE:', globalError);
    try {
        await socket.sendMessage(sender, { 
            text: '❌ *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Error:* An unexpected system error occurred.' 
        }, { quoted: msg });
    } catch (e) {}
  }
  break;
}
case 'song':
case 'play':
case 'audio':
case 'ytmp3': {
    try {
        const yts = require('ytsearch-venom');
        const { generateWAMessageFromContent, prepareWAMessageMedia } = require('@dnuzi/baileys');

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර වචන කිහිපයක් ලියන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '🎧', key: msg.key } });

        const results = await yts(q.replace(/\?si=[^&]*/, ''));
        if (!results || !results.videos || results.videos.length === 0) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const result = results.videos[0];

        const caption =
            `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
            `*┃➤ 🎧 Title    :* ${result.title}\n` +
            `*┃➤ 💃 Channel  :* ${result.author?.name || 'Unknown'}\n` +
            `*┃➤ ⏱ Duration :* ${result.duration}\n` +
            `*┃➤ 👀 Views    :* ${result.views}\n` +
            `*┃➤ 🔗 Link     :* ${result.url}\n` +
            `*┗━━━━━━━━━━━━━━━━❖●►*`;

        const cardHeader = await prepareWAMessageMedia(
            { image: { url: result.thumbnail } },
            { upload: socket.waUploadToServer }
        );

        const interactiveMsg = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                    interactiveMessage: {
                        body: { text: caption },
                        footer: { text: footer },
                        header: {
                            hasMediaAttachment: true,
                            imageMessage: cardHeader.imageMessage
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: 'quick_reply',
                                    buttonParamsJson: JSON.stringify({ display_text: '🎶 Audio Format', id: `${prefix}ytaa ${result.url}` })
                                },
                                {
                                    name: 'quick_reply',
                                    buttonParamsJson: JSON.stringify({ display_text: '📂 Document Format', id: `${prefix}ytad ${result.url}±${result.thumbnail}±${result.title}` })
                                },
                                {
                                    name: 'quick_reply',
                                    buttonParamsJson: JSON.stringify({ display_text: '🎤 Voice Format', id: `${prefix}ytaap ${result.url}` })
                                }
                            ]
                        }
                    }
                }
            }
        }, { quoted: msg });

        await socket.relayMessage(sender, interactiveMsg.message, { messageId: interactiveMsg.key.id });

    } catch (e) {
        console.error('Song Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Song Error!!*' }, { quoted: msg });
    }
    break;
}
case 'ytaa': {
    try {
        const dlcore = require('sadaslk-dlcore');

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        const result = await dlcore.ytmp3(q);
        if (!result || !result.url) throw new Error('No download URL returned');

        await socket.sendMessage(sender, {
            audio: { url: result.url },
            mimetype: 'audio/mpeg'
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('ytaa Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Song Dl Error*' }, { quoted: msg });
    }
    break;
}
case 'ytaap': {
    try {
        const dlcore = require('sadaslk-dlcore');
        const ffmpeg = require('fluent-ffmpeg');
        const ffmpegInstaller = require('@ffmpeg-installer/ffmpeg');
        ffmpeg.setFfmpegPath(ffmpegInstaller.path);

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const result = await dlcore.ytmp3(q);
        if (!result || !result.url) throw new Error('No download URL returned');

        const mp3File = path.join(os.tmpdir(), `ytaap_${Date.now()}.mp3`);
        const oggFile = path.join(os.tmpdir(), `ytaap_${Date.now()}.ogg`);

        const dl = await axios.get(result.url, { responseType: 'arraybuffer' });
        fs.writeFileSync(mp3File, dl.data);

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await new Promise((resolve, reject) => {
            ffmpeg(mp3File)
                .audioCodec('libopus')
                .audioBitrate('64k')
                .format('ogg')
                .save(oggFile)
                .on('end', resolve)
                .on('error', reject);
        });

        await socket.sendMessage(sender, {
            audio: fs.readFileSync(oggFile),
            mimetype: 'audio/ogg; codecs=opus',
            ptt: true
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

        if (fs.existsSync(mp3File)) fs.unlinkSync(mp3File);
        if (fs.existsSync(oggFile)) fs.unlinkSync(oggFile);

    } catch (e) {
        console.error('ytaap Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Song Dl Error*' }, { quoted: msg });
    }
    break;
}
case 'ytad': {
    try {
        const dlcore = require('sadaslk-dlcore');
        const sharp = require('sharp');

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const parts = q.split('±');
        const url = parts[0];
        const title = parts[2] || 'Song';

        const result = await dlcore.ytmp3(url);
        if (!result || !result.url) throw new Error('No download URL returned');

        const ytRegex = /(?:https?:\/\/)?(?:www\.)?(?:youtube\.com\/(?:[^\/\n\s]+\/\S+\/|(?:v|e(?:mbed)?)\/|.*[?&]v=)|youtu\.be\/)([a-zA-Z0-9_-]{11})/;
        const match = url.match(ytRegex);
        let resizedBotImg;
        if (match && match[1]) {
            const thumbUrl = `https://i.ytimg.com/vi/${match[1]}/hqdefault.jpg`;
            const res = await axios.get(thumbUrl, { responseType: 'arraybuffer' });
            resizedBotImg = await sharp(Buffer.from(res.data)).resize(200, 200).toBuffer();
        }

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await socket.sendMessage(sender, {
            document: { url: result.url },
            mimetype: 'audio/mpeg',
            caption: `\`${title}\`\n\n${footer}`,
            jpegThumbnail: resizedBotImg,
            fileName: `${title}.mp3`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('ytad Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 Song Dl Error*' }, { quoted: msg });
    }
    break;
}
case 'system': {
  try {
    await socket.sendMessage(sender, { react: { text: '🧬', key: msg.key } });

    const date = moment().tz('Asia/Colombo').format('YYYY-MM-DD');
    const time = moment().tz('Asia/Colombo').format('HH:mm:ss');

    let hostname;
    const hostLen = os.hostname().length;
    if (hostLen === 12) hostname = 'Replit';
    else if (hostLen === 36) hostname = 'Heroku';
    else if (hostLen === 8) hostname = 'Koyeb';
    else hostname = os.hostname();

    const ramUsedMB = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ramTotalMB = Math.round(os.totalmem() / 1024 / 1024);
    const ram = `${ramUsedMB} MB / ${ramTotalMB} MB`;
    const uptimeSec = process.uptime();
    const ud = Math.floor(uptimeSec / (24 * 3600));
    const uh = Math.floor((uptimeSec % (24 * 3600)) / 3600);
    const um = Math.floor((uptimeSec % 3600) / 60);
    const us = Math.floor(uptimeSec % 60);
    const rtime = `${ud}d ${uh}h ${um}m ${us}s`;

    const ownerdata = (await axios.get(
      'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
    )).data;

    const { footer, imageurl, version, botname, ownername, ownernumber, platform } = ownerdata;

    const systemMessage = `
*╭──『 SYSTEM INFO 』─◉◉➤*
*│ 📌 \`CREATOR\` : -* ${ownername}
*│ 📞 \`Hotline\` : -* ${ownernumber}
*│ 📅 \`Date\` : -* ${date}
*│ ⌚ \`Time\` : -* ${time}
*│ 🕒 \`Uptime\` : -* ${rtime}
*│ 💾 \`RAM Usage\` : -* ${ram}
*│ 🖥️ \`Platform\` : -* ${platform}
*│ 🧬 \`Version\` : -* ${version}
*╰──────────────◉◉➤*

${footer}`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: systemMessage,
      contextInfo: {
        forwardingScore: 999,
        isForwarded: false
      },
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '❄ MAIN MENU' }, type: 1 },
        { buttonId: `${config.PREFIX}owner`, buttonText: { displayText: '❄ OWNER' }, type: 1 }
      ],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error('system error', e);
    await socket.sendMessage(sender, { text: `*🚩 System Error :-*\n${e.message}` }, { quoted: msg });
  }
  break;
}
// ==================== MAIN MENU ====================
case 'menu': {
  try {
    await socket.sendMessage(sender, { react: { text: "📖", key: msg.key } });

    let pingMsg = await socket.sendMessage(sender, { text: '`LOADING`' }, { quoted: msg });
    await socket.sendMessage(sender, { text: '`BOT/S MENU` ✅', edit: pingMsg.key });

    let hostname;
    const hostLen = os.hostname().length;
    if (hostLen === 12) hostname = "Replit";
    else if (hostLen === 36) hostname = "Heroku";
    else if (hostLen === 8) hostname = "Koyeb";
    else hostname = os.hostname();

    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ramTotal = Math.round(os.totalmem() / 1024 / 1024);
    const uptimeSec = process.uptime();
    const ud = Math.floor(uptimeSec / (24 * 3600));
    const uh = Math.floor((uptimeSec % (24 * 3600)) / 3600);
    const um = Math.floor((uptimeSec % 3600) / 60);
    const uptimeStr = `${ud}d ${uh}h ${um}m`;

    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;

    const {
      footer, imageurl,
      version, botname, ownername, ownernumber,
      pairlink, platform
    } = ownerdata;

    const pushname = msg.pushName || 'Guest';

    // Sinhala greeting based on Sri Lanka time
    const nowSL = moment().tz('Asia/Colombo');
    const hourSL = nowSL.hour();
    let sinhalaGreeting;
    let greetingEmoji;
    if (hourSL >= 5 && hourSL < 12) {
      sinhalaGreeting = 'සුභ උදෑසනක් 🌄';
      greetingEmoji = '🌤️';
    } else if (hourSL >= 12 && hourSL < 17) {
      sinhalaGreeting = 'සුභ දහවලක් 🏞️';
      greetingEmoji = '🌞';
    } else if (hourSL >= 17 && hourSL < 21) {
      sinhalaGreeting = 'සුභ හැන්දෑවක් 🌅';
      greetingEmoji = '🌥️';
    } else {
      sinhalaGreeting = 'සුභ රාත්‍රියක් 🌌';
      greetingEmoji = '🌕';
    }

    // CPU Usage
    const cpuUsage = (() => {
      const cpus = os.cpus();
      let totalIdle = 0, totalTick = 0;
      cpus.forEach(cpu => {
        for (const type in cpu.times) totalTick += cpu.times[type];
        totalIdle += cpu.times.idle;
      });
      return (100 - (totalIdle / totalTick * 100)).toFixed(1) + '%';
    })();

    // Respond Speed (ping)
    const _pingStart = Date.now();
    await new Promise(r => setTimeout(r, 0));
    const respondSpeed = (Date.now() - _pingStart) + 'ms';

    // Time & Date (Sri Lanka)
    const menuTime = nowSL.format('hh:mm:ss A');
    const menuDate = nowSL.format('YYYY-MM-DD');

    // Day-based react emoji for DATE
    const dayEmojiMap = { 0: '☀️', 1: '🌙', 2: '🔥', 3: '💧', 4: '⚡', 5: '🌟', 6: '🎉' };
    const dateEmoji = dayEmojiMap[nowSL.day()] || '📆';

    const menuMessage = `*╭〔 𝘼𝙎𝙃𝙄𝙔𝘼-𝙈𝘿 𝙑.4 𝙈𝙀𝙉𝙐 〕┈⊷❖●►*
*❒╮*
*├➣👩‍💼ᴜꜱᴇʀ:* *${pushname}*
*├➣${greetingEmoji}ɢʀᴇᴇᴛɪɴɢ:* *\`${sinhalaGreeting}\`*
*├➣📟ᴜᴘᴛɪᴍᴇ:* *${uptimeStr}*
*├➣💾ʀᴀᴍ: ${ramUsed}MB / ${ramTotal}MB*
*├➣🖥️ᴄᴘᴜ ᴜꜱᴀɢᴇ:* *${cpuUsage}*
*├➣⚡ʀᴇꜱᴘᴏɴᴅ ꜱᴘᴇᴇᴅ:* *${respondSpeed}*
*├➣⏰𝚃𝙸𝙼𝙴:* *${menuTime}*
*├➣${dateEmoji}𝙳𝙰𝚃𝙴:* *${menuDate}*
*├➣💻ᴘʟᴀᴛꜰᴏʀᴍ:* *ʟɪɴᴜx*
*├➣🛰️ʜᴏꜱᴛ:* *${platform}*
*├➣🧬ᴠᴇʀꜱɪᴏɴ:* *${version}*
*├➣🧑‍💻ᴏᴡɴᴇʀ:* *${ownername}*
*├➣📞ᴏᴡɴᴇʀ ɴᴜᴍʙᴇʀ:* *${ownernumber}*
*├➣🤖ʙᴏᴛɴᴀᴍᴇ:* *${botname}*
*❒╯*
*╰──────────────❍┈⊷❖◆►*

👋 ${sinhalaGreeting} *${pushname}* ඔබව *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 V.4* බෝට් *MENU* වෙත සාදරයෙන් පිළිගන්නවා... 🙏`;

    const sections = [
      {
        title: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴍᴇɴᴜ ʟɪꜱᴛ 🙌",
        rows: [
          { title: "❄ ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴍᴅ",      description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴇɴᴜ 📥",      id: `${config.PREFIX}downloadmenu` },
          { title: "❄ ᴀɪ ᴄᴍᴅ",             description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴀɪ ᴍᴇɴᴜ ✨",             id: `${config.PREFIX}aimenu` },
          { title: "❄ ꜱᴇᴀʀᴄʜ ᴄᴍᴅ",         description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ꜱᴇᴀʀᴄʜ ᴍᴇɴᴜ 🔍",         id: `${config.PREFIX}searchmenu` },
          { title: "❄ ᴏᴛʜᴇʀ ᴄᴍᴅ",          description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴏᴛʜᴇʀ ᴍᴇɴᴜ 📑",          id: `${config.PREFIX}othermenu` },
          { title: "❄ ʟᴏɢᴏ ᴄᴍᴅ",           description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ʟᴏɢᴏ ᴍᴇɴᴜ 🎨",           id: `${config.PREFIX}logomenu` },
          { title: "❄ ᴍᴏᴠɪᴇ ᴄᴍᴅ",          description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴍᴏᴠɪᴇ ᴍᴇɴᴜ 🎥",           id: `${config.PREFIX}moviemenu` }
        ]
      },
      {
        title: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴍᴇɴᴜ ʟɪꜱᴛ 🙌",
        rows: [
          { title: "❄ ᴍᴀɪɴ ᴄᴍᴅ",           description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴍᴀɪɴ ᴍᴇɴᴜ 🏡",           id: `${config.PREFIX}mainmenu` },
          { title: "❄ ᴏᴡɴᴇʀ ᴄᴍᴅ",          description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ᴏᴡɴᴇʀ ᴍᴇɴᴜ 🧑‍💻",        id: `${config.PREFIX}ownermenu` },
          { title: "❄ ɢʀᴏᴜᴘ ᴄᴍᴅ",          description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ɢʀᴏᴜᴘ ᴍᴇɴᴜ 💑",          id: `${config.PREFIX}groupmenu` },
          { title: "❄ ɴᴇᴡꜱ ᴄᴍᴅ",           description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 ᴠ.4.0.0 ɴᴇᴡꜱ ᴍᴇɴᴜ 📰",           id: `${config.PREFIX}newsmenu` },
          { title: "❄ ꜱᴇᴛᴛɪɴɢꜱ ᴄᴍᴅ",        description: "𝙰𝚂𝙷𝙸𝚈𝙰-𝙼𝙳 V4.0.0 ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ 🧑‍🔧",        id: `${config.PREFIX}settings` }
        ]
      }
    ];

    const buttons = [
      {
        buttonId: "action",
        buttonText: { displayText: "Click Here ❏" },
        name: "single_select",
        paramsJson: JSON.stringify({ title: "📂 𝐒𝐄𝐋𝐄𝐂𝐓 𝐓𝐄𝐁 𝐌𝐄𝐍𝐔", sections })
      }
    ];

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: menuMessage,
      footer: footer,
      buttons: buttons,
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.log("❌ Menu Error:", e);
    reply(`*🚩 Menu Error :-*\n${e.message}`);
  }
  break;
}
// ==================== MAIN MENU ====================
case 'mainmenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "🏡", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Mᴀɪɴ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* alive\n│ *ヤ Use :* *Check bot online or no.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* ping\n│ *ヤ Use :* *Check bot's speed.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* menu\n│ *ヤ Use :* *Get bot's command list.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* system\n│ *ヤ Use :* *Get bot's system information.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* pair\n│ *ヤ Use :* *Get bot session pairing code.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* pp\n│ *ヤ Use :* *Get user profile picture.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== DOWNLOAD SUB MENU ====================
case 'downloadmenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "📥", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Dᴏᴡɴʟᴏᴀᴅ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* youtube / yt / mp4\n│ *ヤ Use :* *Download YouTube video.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* song / play / audio\n│ *ヤ Use :* *Download YouTube audio.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* facebook / fb\n│ *ヤ Use :* *Download Facebook video.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* tiktok / tt\n│ *ヤ Use :* *Download TikTok video.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* instagram / ig / insta\n│ *ヤ Use :* *Download Instagram media.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* mf / mediafire\n│ *ヤ Use :* *Download Mediafire file.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* gdrive\n│ *ヤ Use :* *Download Google Drive file.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* ringtone\n│ *ヤ Use :* *Search and download ringtones.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* twitter / twdl\n│ *ヤ Use :* *Download Twitter video.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* apk / apks\n│ *ヤ Use :* *Search & Download APK from Aptoide.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== AI SUB MENU ====================
case 'aimenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "✨", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Aɪ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* ai / chat / gpt\n│ *ヤ Use :* *Chat with AI assistant.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* aiimg / aiimg2\n│ *ヤ Use :* *Generate AI image from text.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== SEARCH SUB MENU ====================
case 'searchmenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "🔍", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Sᴇᴀʀᴄʜ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* google / search\n│ *ヤ Use :* *Search on Google.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* img\n│ *ヤ Use :* *Search and get images.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* apksearch / apks\n│ *ヤ Use :* *Search and download APK files.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* yts / youtubesearch\n│ *ヤ Use :* *Search YouTube videos & get results.* \n│ *ヤ Example :* .yts Alan Walker\n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== OWNER SUB MENU ====================
case 'ownermenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "🧑‍💻", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Oᴡɴᴇʀ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* help\n│ *ヤ Use :* *Open bot help center.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* setting\n│ *ヤ Use :* *Open bot settings panel.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* wtype\n│ *ヤ Use :* *Change bot work type.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* prefix\n│ *ヤ Use :* *Change bot command prefix.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* setbotname\n│ *ヤ Use :* *Change bot display name.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* setlogo\n│ *ヤ Use :* *Set bot profile picture.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* botpresence\n│ *ヤ Use :* *Toggle bot online presence.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* autotyping\n│ *ヤ Use :* *Toggle fake typing animation.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* rstatus\n│ *ヤ Use :* *Toggle auto read status.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* arm\n│ *ヤ Use :* *Toggle auto reply mode.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== GROUP SUB MENU ====================
case 'groupmenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "💑", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Gʀᴏᴜᴘ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \`RAM Usage\` : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* tagall\n│ *ヤ Use :* *Tag all group members.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* addadmin\n│ *ヤ Use :* *Promote member to admin.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* deladmin\n│ *ヤ Use :* *Demote admin to member.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* admins\n│ *ヤ Use :* *List all group admins.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* block\n│ *ヤ Use :* *Block a member.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* unblock\n│ *ヤ Use :* *Unblock a member.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* online\n│ *ヤ Use :* *Check who is online in group.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== OTHER SUB MENU ====================
case 'othermenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "📑", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Oᴛʜᴇʀ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* tourl / upload\n│ *ヤ Use :* *Upload image and get URL.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* vv / save\n│ *ヤ Use :* *Save view once media.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* emojis\n│ *ヤ Use :* *Get emoji sticker pack.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* font\n│ *ヤ Use :* *Convert text to fancy font.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* savecontact\n│ *ヤ Use :* *Save contact as VCF file.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* cfn\n│ *ヤ Use :* *Generate fake WhatsApp number.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* jid\n│ *ヤ Use :* *Get user WhatsApp JID.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* gjid\n│ *ヤ Use :* *Get group JID.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== LOGO SUB MENU ====================
case 'logomenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "🎨", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} ʟᴏɢᴏ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n*🎌 Anime & Movies*\n╭──────────●●►\n│ *ヤ .naruto* ➣ Naruto Shippuden style\n│ *ヤ .dragonball* ➣ Dragon Ball style\n│ *ヤ .onepiece* ➣ One Piece logo style\n│ *ヤ .marvel* ➣ Marvel logo style\n│ *ヤ .deadpool* ➣ Deadpool logo style\n│ *ヤ .harrypotter* ➣ Harry Potter style\n╰──────────●●►\n\n*✨ Glow & Effects*\n╭──────────●●►\n│ *ヤ .neon* ➣ 3D Neon sign board\n│ *ヤ .glitch* ➣ Glitch text effect\n│ *ヤ .rainbow* ➣ Rainbow text effect\n│ *ヤ .glass* ➣ Transparent glass\n│ *ヤ .frostedGlass* ➣ Frosted glass\n│ *ヤ .neonGlass* ➣ 3D Neon glass\n╰──────────●●►\n\n*💰 Metal & Luxury*\n╭──────────●●►\n│ *ヤ .gold* ➣ Golden metal\n│ *ヤ .silver* ➣ Silver metal\n│ *ヤ .diamond* ➣ Diamond effect\n│ *ヤ .luxury* ➣ Luxury gold\n│ *ヤ .modern* ➣ Modern metallic\n╰──────────●●►\n\n*🌿 Nature & Elements*\n╭──────────●●►\n│ *ヤ .fire* ➣ Burning fire\n│ *ヤ .water* ➣ Underwater\n│ *ヤ .smoke* ➣ Smoky text\n│ *ヤ .ice* ➣ Frozen ice\n│ *ヤ .crystal* ➣ Shiny crystal\n│ *ヤ .sand* ➣ Beach sand\n│ *ヤ .sky* ➣ Cloud sky\n│ *ヤ .space* ➣ Galaxy text\n╰──────────●●►\n\n*🎄 Holidays & Art*\n╭──────────●●►\n│ *ヤ .christmas* ➣ Christmas style\n│ *ヤ .halloween* ➣ Halloween pumpkin\n│ *ヤ .3dcomic* ➣ 3D Comic style\n│ *ヤ .graffiti* ➣ Graffiti text\n│ *ヤ .blackpink* ➣ Blackpink style\n╰──────────●●►\n\n*📝 Usage:* .[effect] [text]\n*📌 Example:* .naruto Uzumaki\n*🎲 Random:* .logo random [text]\n*🔄 Batch:* .logo batch naruto,neon,gold [text]`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== NEWS SUB MENU ====================
case 'newsmenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "📰", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🥏 ${botname} Nᴇᴡꜱ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \𝗥𝗔𝗠 𝗨𝘀𝗮𝗴𝗲\ : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* adanews\n│ *ヤ Use :* *Get latest Ada Derana news.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* sirasanews\n│ *ヤ Use :* *Get latest Sirasa news.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* lankadeepanews\n│ *ヤ Use :* *Get latest Lankadeepa news.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* gagananews\n│ *ヤ Use :* *Get latest Gagana news.* \n╰──────────●●►\n\n╭──────────●●►\n│ *ヤ Command :* newslist\n│ *ヤ Use :* *List all available news sources.* \n╰──────────●●►`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== MOVIE SUB MENU ====================
case 'moviemenu': {
  try {
    await socket.sendMessage(sender, { react: { text: "🎞️", key: msg.key } });
    const ownerdata = (await axios.get("https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json")).data;
    const { footer, imageurl, botname } = ownerdata;
    const ramUsed = (process.memoryUsage().heapUsed / 1024 / 1024).toFixed(2);
    const ut = process.uptime();
    const rtime = `${Math.floor(ut/3600)}h ${Math.floor((ut%3600)/60)}m ${Math.floor(ut%60)}s`;
    const menuc = `*_🎞️ ${botname} Mᴏᴠɪᴇ Mᴇɴᴜ_*\n\n*╭──────────────◉◉➤*\n*├➣ ⏱️ \𝗨𝗣 𝗧𝗜𝗠𝗘\ : -* ${rtime}\n*├➣ 💾 \`RAM Usage\` : -* ${ramUsed} MB\n*╰──────────────◉◉➤*\n\n╭──────────●●►\n│ *ヤ Command :* movie / sinhalasub / films / mv\n│ *ヤ Use :* *Search & Download movies from sinhalasub.lk*\n╰──────────●●►\n\n*📌 How to use:*\n╭──────────●●►\n│ 1️⃣ Type: *.movie <movie name>*\n│ 2️⃣ Reply with movie number\n│ 3️⃣ Reply with quality number\n│ 🎬 Movie will be sent as a document!\n╰──────────●●►\n\n*📌 Example:*\n╭──────────●●►\n│ *.movie avengers*\n│ *.mv spider man*\n│ *.films thor*\n╰──────────●●►\n\n*⚠️ Max file size: 2GB*\n*🌐 Source: sinhalasub.lk*`;
    await socket.sendMessage(sender, { image: { url: imageurl }, caption: menuc, footer: footer, buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "↩️ MENU COMMAND" }, type: 1 }], headerType: 4 }, { quoted: msg });
  } catch(e) { reply(`*🚩 Menu Error :-*\n${e.message}`); }
  break;
}

// ==================== MOVIE COMMAND ====================
case 'movie':
case 'sinhalasub':
case 'films':
case 'mv': {
  try {
    await socket.sendMessage(from, { react: { text: "🎞️", key: msg.key } });
    const q = args.join(' ').trim();
    if (!q) {
      await socket.sendMessage(from, { text: `*🎥 Movie Search Plugin*\nUsage: ${config.PREFIX}movie <movie_name>\nExample: ${config.PREFIX}movie avengers` }, { quoted: msg });
      break;
    }
    await socket.sendMessage(from, { text: "🔍 *𝚂𝙴𝙰𝚁𝙲𝙷𝙸𝙽𝙶  𝚈𝙾𝚄𝚁 𝙼𝙾𝚅𝙸𝙴*" }, { quoted: msg });
    const searchResults = await searchMovies(q);
    if (!searchResults.length) {
      await socket.sendMessage(from, { text: "*❌ No movies found!*" }, { quoted: msg });
      break;
    }
    moviePendingSearch[sender] = { results: searchResults, timestamp: Date.now() };
    let text = "*🎥 Search Results:*\n";
    searchResults.forEach((m, i) => {
      text += `*${i+1}.* ${m.title}\n   📝 Language: ${m.language}\n   📊 Quality: ${m.quality}\n   🎞️ Format: ${m.qty}\n`;
    });
    text += `\n*Reply with movie number (1-${searchResults.length})*`;
    await socket.sendMessage(from, { text }, { quoted: msg });
  } catch(e) {
    console.error("Movie command error:", e);
    await socket.sendMessage(from, { text: `*❌ Movie search failed:* ${e.message}` }, { quoted: msg });
  }
  break;
}
case 'download': {
  try { await socket.sendMessage(sender, { react: { text: "📥", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    
    // 1. GENERATE RANDOM LOGO (Add your URLs here)
    const logos = [
        "https://files.catbox.moe/qb2puf.jpeg", 
        "https://files.catbox.moe/qb2puf.jpeg",
        config.LOGO // Fallback to config logo
    ];
    const randomLogo = logos[Math.floor(Math.random() * logos.length)] || logos[0];

    // 2. CREATE FAKE CONTACT (QUOTED)
    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_DOWNLOAD_V3"
        },
        message: {
            contactMessage: {
                displayName: "📥 𝐃𝐎𝐖𝐍𝐋𝐎𝐀𝐃 𝐂𝐄𝐍𝐓𝐄𝐑",
                vcard: `BEGIN:VCARD
VERSION:3.0
N:;Downloader;;;
FN:Downloader
ORG:${title}
TITLE:System
END:VCARD`
            }
        }
    };

    const text = `
╭═〔 Dᴏᴡɴʟᴏᴀᴅ Mᴇɴᴜ Lɪꜱᴛ 🍷〕═╮
╠═════════════❒
╠•🍷${config.PREFIX}song
╠•🍷${config.PREFIX}csong
╠•🍷${config.PREFIX}gsong
╠•🍷${config.PREFIX}cvideo
╠•🍷${config.PREFIX}video
╠•🍷${config.PREFIX}tiktok
╠•🍷${config.PREFIX}fb
╠•🍷${config.PREFIX}ig
╠•🍷${config.PREFIX}apk
╠•🍷${config.PREFIX}apksearch
╠•🍷${config.PREFIX}mediafire
╠•🍷${config.PREFIX}gdrive
╘════════════❒
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "🏠 𝐇𝐎𝐌𝐄" }, type: 1 },
      { buttonId: `${config.PREFIX}tool`, buttonText: { displayText: "🎨 𝐂𝐑𝐄𝐀𝐓𝐈𝐕𝐄" }, type: 1 }
    ];

    // 3. SEND IMAGE MESSAGE WITH CONTEXT INFO (DOUBLE LOGO)
    await socket.sendMessage(sender, {
      image: { url: randomLogo }, // Main Logo
      caption: text,
      footer: "〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: "📥 𝐃𝐎𝐖𝐍𝐋𝐎𝐀𝐃 𝐌𝐀𝐍𝐀𝐆𝐄𝐑",
          body: title,
          thumbnailUrl: randomLogo, // Second Logo (Thumbnail)
          sourceUrl: "https://chat.whatsapp.com/HRIlSELlxO5JQo2gYL4MzV?mode=gi_t", // Your Channel Link
          mediaType: 1,
          renderLargerThumbnail: true
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('download command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Error loading download menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}

// ==================== CREATIVE / TOOL MENU ====================
case 'tool': 
case 'creative': {
  try { await socket.sendMessage(sender, { react: { text: "🎨", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    
    // Random Logo Logic
    const logos = [config.LOGO, "https://files.catbox.moe/qb2puf.jpeg"]; // Add more
    const randomLogo = logos[Math.floor(Math.random() * logos.length)] || logos[0];

    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_CREATIVE_V3"
        },
        message: {
            contactMessage: {
                displayName: "🎨 𝐂𝐑𝐄𝐀𝐓𝐈𝐕𝐄 𝐒𝐓𝐔𝐃𝐈𝐎",
                vcard: `BEGIN:VCARD
VERSION:3.0
N:;Artist;;;
FN:Artist
ORG:${title}
TITLE:Creative
END:VCARD`
            }
        }
    };

    const text = `
╭═〔 Tᴏᴏʟ Mᴇɴᴜ Lɪꜱᴛ 🍷〕═╮
╠═════════════❒
╠•🍷${config.PREFIX}jid
╠•🍷${config.PREFIX}cid
╠•🍷${config.PREFIX}system
╠•🍷${config.PREFIX}tagall
╠•🍷${config.PREFIX}online
╠•🍷${config.PREFIX}adanews
╠•🍷${config.PREFIX}sirasanews
╠•🍷${config.PREFIX}lankadeepanews
╠•🍷${config.PREFIX}gagananews
╠•🍷${config.PREFIX}block
╠•🍷${config.PREFIX}unblock
╠•🍷${config.PREFIX}prefix
╠•🍷${config.PREFIX}autorecording
╠•🍷${config.PREFIX}mread
╠•🍷${config.PREFIX}creject
╠•🍷${config.PREFIX}wtyp
╠•🍷${config.PREFIX}pp
╠•🍷${config.PREFIX}arm
╠•🍷${config.PREFIX}rstatus
╠•🍷${config.PREFIX}botpresence
╠•🍷${config.PREFIX}img
╠•🍷${config.PREFIX}google
╠•🍷${config.PREFIX}ping
╠•🍷${config.PREFIX}alive
╚═════════════❒
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📜 𝐌𝐀𝐈𝐍 𝐌𝐄𝐍𝐔" }, type: 1 },
      { buttonId: `${config.PREFIX}download`, buttonText: { displayText: "📥 𝐃𝐎𝐖𝐍𝐋𝐎𝐀𝐃𝐒" }, type: 1 }
    ];

    await socket.sendMessage(sender, {
      image: { url: randomLogo },
      caption: text,
      footer: "✨ ᴜɴʟᴇᴀꜱʜ ʏᴏᴜʀ ᴄʀᴇᴀᴛɪᴠɪᴛʏ",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: "🎨 𝐂𝐑𝐄𝐀𝐓𝐈𝐕𝐄 𝐌𝐎𝐃𝐄",
          body: title,
          thumbnailUrl: randomLogo,
          sourceUrl: "https://whatsapp.com/channel/0029VbC3JfG77qVXz1CbJM3l",
          mediaType: 1,
          renderLargerThumbnail: true
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('creative command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Error loading creative menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}

// ==================== OTHER / SYSTEM MENU ====================
case 'other': 
case 'system': {
  try { await socket.sendMessage(sender, { react: { text: "🎡", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    
    // Random Logo Logic
    const logos = [config.LOGO, "https://files.catbox.moe/qb2puf.jpeg"]; 
    const randomLogo = logos[Math.floor(Math.random() * logos.length)] || logos[0];

    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_TOOLS_V3"
        },
        message: {
            contactMessage: {
                displayName: "⚙️ 𝐒𝐘𝐒𝐓𝐄𝐌 𝐂𝐎𝐍𝐓𝐑𝐎𝐋",
                vcard: `BEGIN:VCARD
VERSION:3.0
N:;System;;;
FN:System Admin
ORG:${title}
TITLE:Settings
END:VCARD`
            }
        }
    };

    const text = `
╭━━━〔 *${title}* 〕━━━┈⊷
┋ 🔧 *𝐒𝐘𝐒𝐓𝐄𝐌 𝐔𝐓𝐈𝐋𝐈𝐓𝐈𝐄𝐒* 
┋ 𝘮𝘢𝘯𝘢𝘨𝘦 • 𝘤𝘰𝘯𝘵𝘳𝘰𝘭 • 𝘰𝘱𝘵𝘪𝘮𝘪𝘻𝘦
╰━━━━━━━━━━━━━━━━━━┈⊷

╭═〔 ʙᴏᴛ ɪɴꜰᴏ 🍷 〕═╮
╠═════════════❒
╠⦁🍷*${config.PREFIX}system*  ➣ _Sys Specs_
╠⦁🍷*${config.PREFIX}ping*    ➣ _Speed_
╠⦁🍷*${config.PREFIX}alive*   ➣ _Status_
╠⦁🍷*${config.PREFIX}jid*     ➣ _My JID_
╠⦁🍷*${config.PREFIX}checkjid* ➣ _Check JID_
╠⦁🍷*${config.PREFIX}showconfig* ➣ _View Config_
╠⦁🍷*${config.PREFIX}active*  ➣ _Sessions_
╚═════════════❒

╭═〔 ɢʀᴏᴜᴘ ᴍɢᴍᴛ 🍷 〕═╮
╠═════════════❒
╠⦁🍷*${config.PREFIX}tagall*  ➣ _Tag All_
╠⦁🍷*${config.PREFIX}online*  ➣ _Active Users_
╠⦁🍷*${config.PREFIX}kick*    ➣ _Remove User_
╠⦁🍷*${config.PREFIX}add*     ➣ _Add User_
╠⦁🍷*${config.PREFIX}promote* ➣ _Make Admin_
╠⦁🍷*${config.PREFIX}demote*  ➣ _Demote_
╠⦁🍷*${config.PREFIX}mute*    ➣ _Close Chat_
╠⦁🍷*${config.PREFIX}unmute*  ➣ _Open Chat_
╠⦁🍷*${config.PREFIX}grouplist* ➣ _My Groups_
╚═════════════❒

╭═〔 ᴜꜱᴇʀ & ꜱᴀꜰᴇᴛʏ 🍷 〕═╮
╠═════════════❒
╠⦁🍷*${config.PREFIX}block*    ➣ _Block User_
╠⦁🍷*${config.PREFIX}unblock*  ➣ _Unblock_
╠⦁🍷*${config.PREFIX}deleteme* ➣ _Del Bot Msg_
╠⦁🍷*${config.PREFIX}owner*    ➣ _Owner Info_
╚═════════════❒

╭═〔 ꜱᴇᴛᴛɪɴɢꜱ 🍷 〕═╮
╠═════════════❒
╠⦁🍷*${config.PREFIX}botpresence* ➣ _Set Status_
╠⦁🍷*${config.PREFIX}autorecording* ➣ _Auto Rec_
╠⦁🍷*${config.PREFIX}autotyping* ➣ _Auto Type_
╠⦁🍷*${config.PREFIX}mread*   ➣ _Auto Read_
╠⦁🍷*${config.PREFIX}setbotname* ➣ _Set Name_
╠⦁🍷*${config.PREFIX}setlogo*  ➣ _Set Logo_
╠⦁🍷*${config.PREFIX}prefix*   ➣ _Set Prefix_
╠⦁🍷*${config.PREFIX}creject*  ➣ _Call Reject_
╚═════════════❒
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}owner`, buttonText: { displayText: "❄ 𝐎𝐖𝐍𝐄𝐑" }, type: 1 },
      { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "❄ 𝐌𝐄𝐍𝐔" }, type: 1 }
    ];

    await socket.sendMessage(sender, {
      image: { url: randomLogo },
      caption: text,
      footer: "⚙️ ꜱʏꜱᴛᴇᴍ ᴄᴏᴍᴍᴀɴᴅꜱ",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: "⚙️ 𝐒𝐘𝐒𝐓𝐄𝐌 𝐂𝐎𝐍𝐓𝐑𝐎𝐋",
          body: title,
          thumbnailUrl: randomLogo,
          sourceUrl: "https://whatsapp.com/channel/0029VbC3JfG77qVXz1CbJM3l",
          mediaType: 1,
          renderLargerThumbnail: true
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('tools command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Error loading tools menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}

//-------------------- UNIFIED PROFILE PICTURE COMMAND --------------------//
case 'getpp':
case 'pp':
case 'getdp':
case 'dp': {
    // 1. React with loading
    await socket.sendMessage(sender, { react: { text: '👤', key: msg.key } });

    try {
        // --- CONFIG & STYLE LOAD ---
        // (Assuming you have a function to get config, otherwise defaults use hardcoded values)
        const sanitizedSender = sender.split('@')[0];
        const cfg = await loadUserConfigFromMongo(sanitizedSender).catch(() => ({})) || {};
        const botName = cfg.botName || "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰"; // Default Artful Name
        const logo = cfg.logo || "https://files.catbox.moe/qb2puf.jpeg"; // Default Logo
        
        // --- TARGET RESOLUTION (The "Bind" Logic) ---
        let targetUser = sender; // Default to self
        let inputNumber = msg.message?.conversation?.split(" ")[1] || 
                          msg.message?.extendedTextMessage?.text?.split(" ")[1];

        if (inputNumber) {
            // If number provided (getdp style)
            targetUser = inputNumber.replace(/[^0-9]/g, '') + "@s.whatsapp.net";
        } else if (msg.message.extendedTextMessage?.contextInfo?.mentionedJid?.length > 0) {
            // If mention exists
            targetUser = msg.message.extendedTextMessage.contextInfo.mentionedJid[0];
        } else if (msg.quoted) {
            // If reply exists
            targetUser = msg.quoted.sender;
        }

        const userNum = targetUser.split('@')[0];

        // --- FETCH PP (HD -> Privacy Fallback) ---
        let ppUrl, mode = 'HD IMAGE';
        try {
            ppUrl = await socket.profilePictureUrl(targetUser, 'image'); // Try HD
        } catch {
            try {
                mode = 'PREVIEW';
                ppUrl = await socket.profilePictureUrl(targetUser, 'preview'); // Try Preview
            } catch {
                mode = 'NOT FOUND';
                ppUrl = logo; // Fallback to bot logo if no PP allowed
            }
        }

        // --- ARTFUL CAPTION ---
        const caption = `
╔═════「 👤 *PROFILE PIC* 」════❒
╠⦁ ❄️ *User:* @${userNum}
╠⦁ 🎭 *Mode:* ${mode}
╠⦁ 🤖 *Bot:* ${botName}
╚════════════❒


   *අඩන්න එපා හලිද profile එක විතරයිනේ ගත්තේ මන් අල ගේනත් දෙන්නම්කො සුදු හලිද 🥺💗*
`;

        // --- META BROADCAST QUOTE (Style) ---
        const metaQuote = {
            key: { 
                remoteJid: "status@broadcast", 
                participant: "0@s.whatsapp.net", 
                fromMe: false, 
                id: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰" 
            },
            message: { 
                contactMessage: { 
                    displayName: botName, 
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:${botName} Inc.
TEL;type=CELL;type=VOICE;waid=94700000000:+94 70 000 0000
END:VCARD` 
                } 
            }
        };

        // --- BUTTONS ---
        const buttons = [
            { 
                buttonId: `${config.PREFIX || '.'}menu`, 
                buttonText: { displayText: "❄ MAIN MENU" }, 
                type: 1 
            },
            { 
                buttonId: `${config.PREFIX || '.'}alive`, 
                buttonText: { displayText: "❄ ALIVE" }, 
                type: 1 
            }
        ];

        // --- SEND MESSAGE ---
        await socket.sendMessage(msg.key.remoteJid, {
            image: { url: ppUrl },
            caption: caption,
            footer: `Power by ${botName}`,
            buttons: buttons,
            headerType: 4,
            mentions: [targetUser]
        }, { quoted: metaQuote });

        // Success React
        await socket.sendMessage(msg.key.remoteJid, { react: { text: '✅', key: msg.key } });

    } catch (e) {
        console.log("❌ PP Fetch Error:", e);
        await socket.sendMessage(msg.key.remoteJid, { 
            text: `⚠️ *Error:* Could not fetch profile picture.
_${e.message}_` 
        }, { quoted: msg });
        await socket.sendMessage(msg.key.remoteJid, { react: { text: '❌', key: msg.key } });
    }
    break;
}

case 'showconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  try {
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let txt = `*Session config for ${sanitized}:*\n`;
    txt += `• Bot name: ${botName}\n`;
    txt += `• Logo: ${cfg.logo || config.RCD_IMAGE_PATH}\n`;
    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('showconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Failed to load config.' }, { quoted: shonux });
  }
  break;
}

case 'resetconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can reset configs.' }, { quoted: shonux });
    break;
  }

  try {
    await setUserConfigInMongo(sanitized, {});

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '✅ Session config reset to defaults.' }, { quoted: shonux });
  } catch (e) {
    console.error('resetconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '❌ Failed to reset config.' }, { quoted: shonux });
  }
  break;
}

case 'owner':
case 'ayesh':
case 'ashiya': {
  try {
    // 1. Send Royal Reaction 👑
    await socket.sendMessage(sender, { 
      react: { text: "🥷", key: msg.key } 
    });

    // 2. Configuration & Data
    const ownerNumber = '94752135878';
    const ownerName = '𝐀ʏᴇꜱʜ 𝐓ʜᴇᴍɪʏᴀ 🥷🇱🇰';
    const botName = '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const ownerImage = 'https://files.catbox.moe/qb2puf.jpeg';
    const websiteUrl = 'https://ayesh-ofc-site.vercel.app/';
    
    // Time Calculation
    const timeNow = new Date().toLocaleTimeString('en-US', { 
      hour: '2-digit', minute: '2-digit', hour12: true, timeZone: "Asia/Colombo" 
    });

    // 3. Artful "Royal" Text Layout 🎨
    // Using box-drawing characters and emojis for a "colorful" feel
    const aestheticCaption = `
╔════〔 🥷 *${botName}* 〕═══❒
╠⦁ 👤 *OWNER PROFILE*
╠⦁ 🙌 𝐍𝐚𝐦𝐞 : *${ownerName}*
╠⦁ 🍷 𝐑𝐨𝐥𝐞 : Lead Developer
╠⦁ 📍 𝐅𝐫𝐨𝐦 : Sri Lanka 🇱🇰
╠⦁ ⌚ 𝐓𝐢𝐦𝐞 : ${timeNow}
╠⦁ 🛠️ *SKILLS & STATUS*
╠⦁ 💻 Stack : JS, Node.js, React
╠⦁ 🤖 Bot : *Active & Online* ✅
╠⦁ 🛡️ Security : Verified
╚════════════════❒


> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*
`.trim();

    // 4. Define the Interactive Button System (Native Flow) [web:1]
    // This allows URL buttons, Copy buttons, and Quick Replies
    const buttonParams = [
      {
        name: "cta_url",
        buttonParamsJson: JSON.stringify({
          display_text: "💬 Chat with Owner",
          url: `https://wa.me/${ownerNumber}?text=Hello ${ownerName}, I need assistance with 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Bot.`
        })
      },
      {
        name: "cta_url",
        buttonParamsJson: JSON.stringify({
          display_text: "🌐 Visit Website",
          url: websiteUrl
        })
      },
      {
        name: "cta_copy",
        buttonParamsJson: JSON.stringify({
          display_text: "📋 Copy Owner Number",
          copy_code: ownerNumber
        })
      },
      {
        name: "quick_reply",
        buttonParamsJson: JSON.stringify({
          display_text: "🔙 Main Menu",
          id: `${config.PREFIX || '.'}menu`
        })
      }
    ];

    // 5. Generate & Relay the Message
    // We use relayMessage for advanced interactive buttons (Button V2)
    const { generateWAMessageFromContent, proto, prepareWAMessageMedia } = require("@dnuzi/baileys"); // Adjust import based on your library

    // Prepare image header
    const mediaMessage = await prepareWAMessageMedia({ 
      image: { url: ownerImage } 
    }, { upload: socket.waUploadToServer });

    const msgContent = generateWAMessageFromContent(sender, {
      viewOnceMessage: {
        message: {
          messageContextInfo: {
            deviceListMetadata: {},
            deviceListMetadataVersion: 2
          },
          interactiveMessage: {
            body: { text: aestheticCaption },
            footer: { text: "Tap a button below to interact 👇" },
            header: {
              title: "",
              subtitle: "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Support",
              hasMediaAttachment: true,
              imageMessage: mediaMessage.imageMessage
            },
            nativeFlowMessage: {
              buttons: buttonParams
            }
          }
        }
      }
    }, { userJid: sender, quoted: msg });

    await socket.relayMessage(sender, msgContent.message, { 
      messageId: msgContent.key.id 
    });

    // 6. Send vCard (Contacts) separately for easy saving
    // Small delay to ensure order
    await new Promise(resolve => setTimeout(resolve, 1000));
    
    const vcard1 = `BEGIN:VCARD\nVERSION:3.0\nFN:${ownerName}\nORG:DTZ Development\nTEL;type=CELL;type=VOICE;waid=947746793356:+947746793356\nEND:VCARD`;
    const vcard2 = `BEGIN:VCARD\nVERSION:3.0\nFN:${ownerName} (2)\nORG:DTZ Development\nTEL;type=CELL;type=VOICE;waid=94785457011:+94785457011\nEND:VCARD`;
    const vcard3 = `BEGIN:VCARD\nVERSION:3.0\nFN:${ownerName} (3)\nORG:DTZ Development\nTEL;type=CELL;type=VOICE;waid=94781332957:+94781332957\nEND:VCARD`;
    await socket.sendMessage(sender, {
      contacts: {
        displayName: ownerName,
        contacts: [{ vcard: vcard1 }, { vcard: vcard2 }, { vcard: vcard3 }]
      }
    });

  } catch (err) {
    console.error('❌ Owner Command Error:', err);
    await socket.sendMessage(sender, { 
      text: `⚠️ *Error:* Failed to load owner menu.
Contact: +${config.OWNER_NUMBER}` 
    }, { quoted: msg });
  }
  break;
}
case 'google':
case 'gsearch':
case 'search':
    try {
        if (!args || args.length === 0) {
            await socket.sendMessage(sender, {
                text: '⚠️ *Please provide a search query.*\n\n*Example:*\n.google how to code in javascript'
            });
            break;
        }

        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GOOGLE" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        const query = args.join(" ");
        const apiKey = "AIzaSyDMbI3nvmQUrfjoCJYLS69Lej1hSXQjnWI";
        const cx = "baf9bdb0c631236e5";
        const apiUrl = `https://www.googleapis.com/customsearch/v1?q=${encodeURIComponent(query)}&key=${apiKey}&cx=${cx}`;

        const response = await axios.get(apiUrl);

        if (response.status !== 200 || !response.data.items || response.data.items.length === 0) {
            await socket.sendMessage(sender, { text: `⚠️ *No results found for:* ${query}` }, { quoted: botMention });
            break;
        }

        let results = `🔍 *𝐆oogle 𝐒earch 𝐑esults 𝐅or:* "${query}"\n\n`;
        response.data.items.slice(0, 5).forEach((item, index) => {
            results += `*${index + 1}. ${item.title}*\n\n🔗 ${item.link}\n\n📝 ${item.snippet}\n\n`;
        });

        const firstResult = response.data.items[0];
        const thumbnailUrl = firstResult.pagemap?.cse_image?.[0]?.src || firstResult.pagemap?.cse_thumbnail?.[0]?.src || 'https://via.placeholder.com/150';

        await socket.sendMessage(sender, {
            image: { url: thumbnailUrl },
            caption: results.trim(),
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: botMention });

    } catch (error) {
        console.error(`Google search error:`, error);
        await socket.sendMessage(sender, { text: `⚠️ *An error occurred while fetching search results.*\n\n${error.message}` });
    }
    break;
case 'img': {
    const q = body.replace(/^[.\/!]img\s*/i, '').trim();
    if (!q) return await socket.sendMessage(sender, {
        text: '🔍 Please provide a search query. Ex: `.img sunset`'
    }, { quoted: msg });

    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_IMG" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        const res = await axios.get(`https://allstars-apis.vercel.app/pinterest?search=${encodeURIComponent(q)}`);
        const data = res.data.data;
        if (!data || data.length === 0) return await socket.sendMessage(sender, { text: '❌ No images found for your query.' }, { quoted: botMention });

        const randomImage = data[Math.floor(Math.random() * data.length)];

        const buttons = [{ buttonId: `${config.PREFIX}img ${q}`, buttonText: { displayText: "🖼️ 𝐍𝙴𝚇𝚃 𝐈𝙼𝙰𝙶𝙴" }, type: 1 }];

        const buttonMessage = {
            image: { url: randomImage },
            caption: `🖼️ *𝐈mage 𝐒earch:* ${q}\n\n*𝐏rovided 𝐁y ${botName}*`,
            footer: config.FOOTER || '> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.3 🥷🇱🇰*',
            buttons: buttons,
             headerType: 4,
            contextInfo: { mentionedJid: [sender] }
        };

        await socket.sendMessage(from, buttonMessage, { quoted: botMention });

    } catch (err) {
        console.error("Image search error:", err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch images.' }, { quoted: botMention });
    }
    break;
}
case 'gdrive': {
    try {
        const text = args.join(' ').trim();
        if (!text) return await socket.sendMessage(sender, { text: '⚠️ Please provide a Google Drive link.\n\nExample: `.gdrive <link>`' }, { quoted: msg });

        // 🔹 Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        // 🔹 Meta AI fake contact mention
        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GDRIVE" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        // 🔹 Fetch Google Drive file info
        const res = await axios.get(`https://saviya-kolla-api.koyeb.app/download/gdrive?url=${encodeURIComponent(text)}`);
        if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch file info.' }, { quoted: botMention });

        const file = res.data.result;

        // 🔹 Send as document
        await socket.sendMessage(sender, {
            document: { 
                url: file.downloadLink, 
                mimetype: file.mimeType || 'application/octet-stream', 
                fileName: file.name 
            },
            caption: `📂 *𝐅ile 𝐍ame:* ${file.name}\n💾 *𝐒ize:* ${file.size}\n\n*𝐏owered 𝐁y ${botName}*`,
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: botMention });

    } catch (err) {
        console.error('GDrive command error:', err);
        await socket.sendMessage(sender, { text: '❌ Error fetching Google Drive file.' }, { quoted: botMention });
    }
    break;
}


case 'adanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/ada');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Ada News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('adanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Ada News.' }, { quoted: botMention });
  }
  break;
}
case 'sirasanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_SIRASA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/sirasa');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Sirasa News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('sirasanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Sirasa News.' }, { quoted: botMention });
  }
  break;
}
case 'lankadeepanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_LANKADEEPA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/lankadeepa');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Lankadeepa News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [𝐑ead more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('lankadeepanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Lankadeepa News.' }, { quoted: botMention });
  }
  break;
}
case 'gagananews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GAGANA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/gagana');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Gagana News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('gagananews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Gagana News.' }, { quoted: botMention });
  }
  break;
}


//💐💐💐💐💐💐





// ==================== HELP CENTER ====================
case 'help':
case 'h':
case 'bothelp': {
  try {
    await socket.sendMessage(sender, { react: { text: '🆘', key: msg.key } });

    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;
    const { footer, imageurl, pairlink, channel } = ownerdata;

    const caption = `_👋 Welcome to 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Help Center!_ 💬\n\n🚩 [  Help Center / උපකාර මධ්‍යස්ථානය ] 🚩\n\n➤ කරුණාකර භාෂාවක් තෝරන්න :\n➤ Please select a language to continue :`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption,
      footer,
      buttons: [
        { buttonId: `${prefix}helpen`, buttonText: { displayText: 'English 🇬🇧' }, type: 1 },
        { buttonId: `${prefix}helpsi`, buttonText: { displayText: 'සිංහල 🇱🇰' }, type: 1 }
      ],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error(e);
    await socket.sendMessage(sender, { text: `🚩 *Help Error :*\n${e.message}` }, { quoted: msg });
  }
  break;
}

case 'helpsi': {
  try {
    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;
    const { footer, imageurl, pairlink, channel } = ownerdata;

    const step1 = `👋 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Help Center වෙත සාදරයෙන් පිළිගනිමු! 💬\n\n🚩 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Help 🚩\n\n━━━━━━━━━━━━━━━➤\n❶ 🌐 බොට් වෙබ් පිටුව → ${pairlink}\n━━━━━━━━━━━━━━━➤\n❷ 📢 අපගේ නාලිකාව → ${channel}\n━━━━━━━━━━━━━━━➤`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: step1,
      footer,
      buttons: [
        { buttonId: `${prefix}generalhelpsi`, buttonText: { displayText: 'සාමාන්‍ය උපකාර ❓' }, type: 1 },
        { buttonId: `${prefix}owner`, buttonText: { displayText: 'අපහා සම්බන්ද වන්න 📞' }, type: 1 },
        { buttonId: `${prefix}menu`, buttonText: { displayText: 'විධාන ලැයිස්තුව 📜' }, type: 1 }
      ],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error(e);
    await socket.sendMessage(sender, { text: `🚩 *Help Error :*\n${e.message}` }, { quoted: msg });
  }
  break;
}

case 'helpen': {
  try {
    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;
    const { footer, imageurl, pairlink, channel } = ownerdata;

    const step1 = `👋 Welcome to the 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Help Center! 💬\n\n🚩 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Help 🚩\n\n━━━━━━━━━━━━━━━➤\n❶ 🌐 Bot Website → ${pairlink}\n━━━━━━━━━━━━━━━➤\n❷ 📢 Our Channel → ${channel}\n━━━━━━━━━━━━━━━➤`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: step1,
      footer,
      buttons: [
        { buttonId: `${prefix}generalhelpen`, buttonText: { displayText: 'General Help ❓' }, type: 1 },
        { buttonId: `${prefix}owner`, buttonText: { displayText: 'Contact Us 📞' }, type: 1 },
        { buttonId: `${prefix}menu`, buttonText: { displayText: 'Command Menu 📜' }, type: 1 }
      ],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error(e);
    await socket.sendMessage(sender, { text: `🚩 *Help Error :*\n${e.message}` }, { quoted: msg });
  }
  break;
}

case 'generalhelpsi': {
  try {
    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;
    const { footer, imageurl } = ownerdata;

    const step1 = `📋 නිති පැන\n\n🚩 [  𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Bot - නිතර අසන ප්‍රශ්න ] 🚩\n\n*➤ 1️⃣ 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Bot යනු කුමක්ද?*\n𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 BOT යනු Ayesh Themiya විසින් නිර්මාණය කරනු ලැබු WhatsApp බොට් එකකි.\n\n*➤ 2️⃣ විධාන භාවිතා කරන්නේ කෙසේද?*\nඩොට් එකකින් ආරම්භ වන විධාන ටයිප් කරන්න. උදා : - .alive හෝ .menu.\n\n*➤ 3️⃣ මම මෙය ලබාගන්නේ කෙසේද?*\nමෙය 100% නොමිලේ ලබාගත හැකි සේවාවකි.\n\n*➤ 4️⃣ බොට් මන් හදාගන්නේ කෙසේද?*\n.pair command භාවිතා කර ඔබේ අංකය ඇතුලත් කරන්න. ලැබෙන කෝඩ් එක copy කර WhatsApp සමග link කරන්න.\n\n*➤ 5️⃣ දෝෂ වාර්තා කරන්නේ කෙසේද?*\n.owner භාවිතා කර සෘජුවම Developer කෙනෙක්ට සමග සම්බන්ධ වන්න.\n\n*💡 ඉදිරියේදි මෙම Bot ඔබට ඔබේම නමකට සාදාගත හැක.*`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: step1,
      footer,
      buttons: [{ buttonId: `${prefix}helpsi`, buttonText: { displayText: 'උපකාර මධ්‍යස්ථානය 🔙' }, type: 1 }],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error(e);
    await socket.sendMessage(sender, { text: `🚩 *Help Error :*\n${e.message}` }, { quoted: msg });
  }
  break;
}

case 'generalhelpen': {
  try {
    const ownerdata = (await axios.get(
      "https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json"
    )).data;
    const { footer, imageurl } = ownerdata;

    const step1 = `📋 General Help\n\n🚩 [ 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Bot – Frequently Asked Questions ] 🚩\n\n*➤ 1️⃣ What is 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 Bot?*\n𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 Bot is a WhatsApp bot created by Ayesh Themiya.\n\n*➤ 2️⃣ How do I use commands?*\nType commands starting with a dot. Example: .alive or .menu.\n\n*➤ 3️⃣ How can I get this bot?*\nThis is a 100% free service. You only need to link it with WhatsApp.\n\n*➤ 4️⃣ How do I create my own bot?*\nUse the .pair command and enter your WhatsApp number with country code.\nCopy the code you receive and link it with WhatsApp.\n\n*➤ 5️⃣ How do I report errors?*\nUse the .owner command to directly contact a developer.\n\n*💡 In the future, you will be able to create this bot with your own name.*`;

    await socket.sendMessage(sender, {
      image: { url: imageurl },
      caption: step1,
      footer,
      buttons: [{ buttonId: `${prefix}helpen`, buttonText: { displayText: 'Help Center 🔙' }, type: 1 }],
      headerType: 4
    }, { quoted: msg });

  } catch (e) {
    console.error(e);
    await socket.sendMessage(sender, { text: `🚩 *Help Error :*\n${e.message}` }, { quoted: msg });
  }
  break;
}
// ==================== END HELP CENTER ====================

        case 'unfollow': {
  const jid = args[0] ? args[0].trim() : null;
  if (!jid) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide channel JID to unfollow. Example:\n.unfollow 120363396379901844@newsletter' }, { quoted: shonux });
  }

  const admins = await loadAdminsFromMongo();
  const normalizedAdmins = admins.map(a => (a || '').toString());
  const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');
  const isAdmin = normalizedAdmins.includes(nowsender) || normalizedAdmins.includes(senderNumber) || normalizedAdmins.includes(senderIdSimple);
  if (!(isOwner || isAdmin)) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❌ Permission denied. Only owner or admins can remove channels.' }, { quoted: shonux });
  }

  if (!jid.endsWith('@newsletter')) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❗ Invalid JID. Must end with @newsletter' }, { quoted: shonux });
  }

  try {
    if (typeof socket.newsletterUnfollow === 'function') {
      await socket.newsletterUnfollow(jid);
    }
    await removeNewsletterFromMongo(jid);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW4" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Unfollowed and removed from DB: ${jid}` }, { quoted: shonux });
  } catch (e) {
    console.error('unfollow error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW5" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to unfollow: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}
case 'tiktok':
case 'ttdl':
case 'tt':
case 'tiktokdl': {
    try {
        const { generateWAMessageFromContent, prepareWAMessageMedia } = require('@dnuzi/baileys');

        const q = args.join(' ').trim();
        if (!q || !q.includes('tiktok')) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර tiktok url එකක් ලබා දෙන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '🎩', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        // Fetch TikTok data
        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }
        const mov = ttRes.data.results;

        const caption =
            `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
            `*┃➤ 🎵 Title    :* ${mov.desc || 'TikTok Video'}\n` +
            `*┃➤ 🌍 Author   :* ${mov.author_nickname || 'Unknown'}\n` +
            `*┃➤ 🔗 Link     :* ${q}\n` +
            `*┗━━━━━━━━━━━━━━━━❖●►*`;

        const thumbUrl = mov.cover || mov.thumbnail || '';
        const cardHeader = await prepareWAMessageMedia(
            { image: { url: thumbUrl } },
            { upload: socket.waUploadToServer }
        );

        const interactiveMsg = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                    interactiveMessage: {
                        body: { text: caption },
                        footer: { text: footer },
                        header: { hasMediaAttachment: true, imageMessage: cardHeader.imageMessage },
                        nativeFlowMessage: {
                            buttons: [{
                                name: 'single_select',
                                buttonParamsJson: JSON.stringify({
                                    title: 'Download Format ❏',
                                    sections: [
                                        {
                                            title: '🎥 Video Format',
                                            rows: [
                                                { header: 'No WM', title: '📼 Video No Watermark', description: 'Video without watermark', id: `${prefix}ttdl1 ${q}` },
                                                { header: 'WM', title: '📼 Video Watermark', description: 'Video with watermark', id: `${prefix}ttdl2 ${q}` }
                                            ]
                                        },
                                        {
                                            title: '📂 Document Format',
                                            rows: [
                                                { header: 'No WM Doc', title: '📂 Video No Watermark', description: 'Document without watermark', id: `${prefix}ttdl1d ${q}` },
                                                { header: 'WM Doc', title: '📂 Video Watermark', description: 'Document with watermark', id: `${prefix}ttdl2d ${q}` }
                                            ]
                                        },
                                        {
                                            title: '🎵 Audio Format',
                                            rows: [
                                                { header: 'Audio', title: '🎧 Audio File', description: 'Audio file download', id: `${prefix}ttdl3 ${q}` },
                                                { header: 'PTT', title: '🎤 Voice Note', description: 'Voice note download', id: `${prefix}ttdl3d ${q}` }
                                            ]
                                        }
                                    ]
                                })
                            }]
                        }
                    }
                }
            }
        }, { quoted: msg });

        await socket.relayMessage(sender, interactiveMsg.message, { messageId: interactiveMsg.key.id });

    } catch (e) {
        console.error('TikTok Error:', e);
        await socket.sendMessage(sender, { text: '*🚩 TikTok Error*' }, { quoted: msg });
    }
    break;
}
case 'ttdl1': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { video: { url: mov.no_watermark }, mimetype: 'video/mp4', caption: `📼 \`Video No Watermark\`\n\n${footer}` }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok DL Error*' }, { quoted: msg }); }
    break;
}
case 'ttdl2': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { video: { url: mov.watermark }, mimetype: 'video/mp4', caption: `📼 \`Video Watermark\`\n\n${footer}` }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok DL Error*' }, { quoted: msg }); }
    break;
}
case 'ttdl3': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { audio: { url: mov.music }, mimetype: 'audio/mpeg' }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok Audio Error*' }, { quoted: msg }); }
    break;
}
case 'ttdl1d': {
    try {
        const sharp = require('sharp');
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        let resizedThumb;
        if (mov.cover || mov.thumbnail) {
            const imgRes = await axios.get(mov.cover || mov.thumbnail, { responseType: 'arraybuffer' });
            resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
        }
        await socket.sendMessage(sender, {
            document: { url: mov.no_watermark }, mimetype: 'video/mp4',
            fileName: `${mov.desc || 'tiktok'}.mp4`,
            jpegThumbnail: resizedThumb,
            caption: `📼 \`Video No Watermark\`\n\n${footer}`
        }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok Doc Error*' }, { quoted: msg }); }
    break;
}
case 'ttdl2d': {
    try {
        const sharp = require('sharp');
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        let resizedThumb;
        if (mov.cover || mov.thumbnail) {
            const imgRes = await axios.get(mov.cover || mov.thumbnail, { responseType: 'arraybuffer' });
            resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
        }
        await socket.sendMessage(sender, {
            document: { url: mov.watermark }, mimetype: 'video/mp4',
            fileName: `${mov.desc || 'tiktok'}.mp4`,
            jpegThumbnail: resizedThumb,
            caption: `📼 \`Video Watermark\`\n\n${footer}`
        }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok Doc Error*' }, { quoted: msg }); }
    break;
}
case 'ttdl3d': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        const ownerdata = (await axios.get('https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json')).data;
        const { footer } = ownerdata;

        const ttRes = await axios.get('https://www.movanest.xyz/v2/tiktok', { params: { url: q } });
        if (!ttRes.data.status || !ttRes.data.results) return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        const mov = ttRes.data.results;

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });
        await socket.sendMessage(sender, { audio: { url: mov.music }, mimetype: 'audio/mpeg', ptt: true }, { quoted: msg });
        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) { await socket.sendMessage(sender, { text: '*🚩 TikTok PTT Error*' }, { quoted: msg }); }
    break;
}
case 'xvideo': {
  try {
    // ---------------------------
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XVIDEO" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    // ---------------------------

    if (!args[0]) return await socket.sendMessage(sender, { text: '*❌ Usage: .xvideo <url/query>*' }, { quoted: botMention });

    let video, isURL = false;
    if (args[0].startsWith('http')) { video = args[0]; isURL = true; } 
    else {
      await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } }, { quoted: botMention });
      const s = await axios.get(`https://saviya-kolla-api.koyeb.app/search/xvideos?query=${encodeURIComponent(args.join(' '))}`);
      if (!s.data?.status || !s.data.result?.length) throw new Error('No results');
      video = s.data.result[0];
    }

    const dlRes = await axios.get(`https://saviya-kolla-api.koyeb.app/download/xvideos?url=${encodeURIComponent(isURL ? video : video.url)}`);
    if (!dlRes.data?.status) throw new Error('Download API failed');

    const dl = dlRes.data.result;

    await socket.sendMessage(sender, {
      video: { url: dl.url },
      caption: `*📹 ${dl.title}*\n\n⏱️ ${isURL ? '' : `*𝐃uration:* ${video.duration}`}\n*👁️ 𝐕iews:* ${dl.views}\n👍 ${dl.likes} | 👎 ${dl.dislikes}\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`,
      mimetype: 'video/mp4'
    }, { quoted: botMention });

  } catch (err) {
    console.error('xvideo error:', err);
    await socket.sendMessage(sender, { text: '*❌ Failed to fetch video*' }, { quoted: botMention });
  }
  break;
}
case 'xvideo2': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XVIDEO2" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!args[0]) return await socket.sendMessage(sender, { text: '*❌ Usage: .xvideo2 <url/query>*' }, { quoted: botMention });

    let video = null, isURL = false;
    if (args[0].startsWith('http')) { video = args[0]; isURL = true; } 
    else {
      await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } }, { quoted: botMention });
      const s = await axios.get(`https://saviya-kolla-api.koyeb.app/search/xvideos?query=${encodeURIComponent(args.join(' '))}`);
      if (!s.data?.status || !s.data.result?.length) throw new Error('No results');
      video = s.data.result[0];
    }

    const dlRes = await axios.get(`https://saviya-kolla-api.koyeb.app/download/xvideos?url=${encodeURIComponent(isURL ? video : video.url)}`);
    if (!dlRes.data?.status) throw new Error('Download API failed');

    const dl = dlRes.data.result;

    await socket.sendMessage(sender, {
      video: { url: dl.url },
      caption: `*📹 ${dl.title}*\n\n⏱️ ${isURL ? '' : `*𝐃uration:* ${video.duration}`}\n*👁️ 𝐕iews:* ${dl.views}\n*👍 𝐋ikes:* ${dl.likes} | *👎 𝐃islikes:* ${dl.dislikes}\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`,
      mimetype: 'video/mp4'
    }, { quoted: botMention });

  } catch (err) {
    console.error('xvideo2 error:', err);
    await socket.sendMessage(sender, { text: '*❌ Failed to fetch video*' }, { quoted: botMention });
  }
  break;
}
case 'xnxx':
case 'xnxxvideo': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XNXX" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!Array.isArray(config.PREMIUM) || !config.PREMIUM.includes(senderNumber)) 
      return await socket.sendMessage(sender, { text: '❗ This command is for Premium users only.' }, { quoted: botMention });

    if (!text) return await socket.sendMessage(sender, { text: '❌ Provide a search name. Example: .xnxx <name>' }, { quoted: botMention });

    await socket.sendMessage(from, { react: { text: "🎥", key: msg.key } }, { quoted: botMention });

    const res = await axios.get(`https://api.genux.me/api/download/xnxx-download?query=${encodeURIComponent(text)}&apikey=GENUX-SANDARUX`);
    const d = res.data?.result;
    if (!d || !d.files) return await socket.sendMessage(sender, { text: '❌ No results.' }, { quoted: botMention });

    await socket.sendMessage(from, { image: { url: d.image }, caption: `💬 *Title*: ${d.title}\n👀 *Duration*: ${d.duration}\n🗯 *Desc*: ${d.description}\n💦 *Tags*: ${d.tags || ''}` }, { quoted: botMention });

    await socket.sendMessage(from, { video: { url: d.files.high, fileName: d.title + ".mp4", mimetype: "video/mp4", caption: "*Done ✅*" } }, { quoted: botMention });

    await socket.sendMessage(from, { text: "*Uploaded ✅*" }, { quoted: botMention });

  } catch (err) {
    console.error('xnxx error:', err);
    await socket.sendMessage(sender, { text: "❌ Error fetching video." }, { quoted: botMention });
  }
  break;
}
case 'gjid':
case 'groupjid':
case 'grouplist': {
  try {
    // ✅ Owner check removed — now everyone can use it!

    await socket.sendMessage(sender, { 
      react: { text: "📝", key: msg.key } 
    });

    await socket.sendMessage(sender, { 
      text: "📝 Fetching group list..." 
    }, { quoted: msg });

    const groups = await socket.groupFetchAllParticipating();
    const groupArray = Object.values(groups);

    // Sort by creation time (oldest to newest)
    groupArray.sort((a, b) => a.creation - b.creation);

    if (groupArray.length === 0) {
      return await socket.sendMessage(sender, { 
        text: "❌ No groups found!" 
      }, { quoted: msg });
    }

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY || "𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰";

    // ✅ Pagination setup — 10 groups per message
    const groupsPerPage = 10;
    const totalPages = Math.ceil(groupArray.length / groupsPerPage);

    for (let page = 0; page < totalPages; page++) {
      const start = page * groupsPerPage;
      const end = start + groupsPerPage;
      const pageGroups = groupArray.slice(start, end);

      // ✅ Build message for this page
      const groupList = pageGroups.map((group, index) => {
        const globalIndex = start + index + 1;
        const memberCount = group.participants ? group.participants.length : 'N/A';
        const subject = group.subject || 'Unnamed Group';
        const jid = group.id;
        return `*${globalIndex}. ${subject}*\n*👥 𝐌embers:* ${memberCount}\n🆔 ${jid}`;
      }).join('\n\n');

      const textMsg = `📝 *𝐆roup 𝐋ist* - ${botName}*\n\n*📄 𝐏age:* ${page + 1}/${totalPages}\n*👥 𝐓otal 𝐆roups:* ${groupArray.length}\n\n${groupList}`;

      await socket.sendMessage(sender, {
        text: textMsg,
        footer: `〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*`
      });

      // Add short delay to avoid spam
      if (page < totalPages - 1) {
        await delay(1000);
      }
    }

  } catch (err) {
    console.error('GJID command error:', err);
    await socket.sendMessage(sender, { 
      text: "❌ Failed to fetch group list. Please try again later." 
    }, { quoted: msg });
  }
  break;
}
case 'nanobanana': {
  const fs = require('fs');
  const path = require('path');
  const { GoogleGenAI } = require("@google/genai");

  // 🧩 Helper: Download quoted image
  async function downloadQuotedImage(socket, msg) {
    try {
      const ctx = msg.message?.extendedTextMessage?.contextInfo;
      if (!ctx || !ctx.quotedMessage) return null;

      const quoted = ctx.quotedMessage;
      const imageMsg = quoted.imageMessage || quoted[Object.keys(quoted).find(k => k.endsWith('Message'))];
      if (!imageMsg) return null;

      if (typeof socket.downloadMediaMessage === 'function') {
        const quotedKey = {
          remoteJid: msg.key.remoteJid,
          id: ctx.stanzaId,
          participant: ctx.participant || undefined
        };
        const fakeMsg = { key: quotedKey, message: ctx.quotedMessage };
        const stream = await socket.downloadMediaMessage(fakeMsg, 'image');
        const bufs = [];
        for await (const chunk of stream) bufs.push(chunk);
        return Buffer.concat(bufs);
      }

      return null;
    } catch (e) {
      console.error('downloadQuotedImage err', e);
      return null;
    }
  }

  // ⚙️ Main command logic
  try {
    const promptRaw = args.join(' ').trim();
    if (!promptRaw && !msg.message?.extendedTextMessage?.contextInfo?.quotedMessage) {
      return await socket.sendMessage(sender, {
        text: "📸 *Usage:* `.nanobanana <prompt>`\n💬 Or reply to an image with `.nanobanana your prompt`"
      }, { quoted: msg });
    }

    await socket.sendMessage(sender, { react: { text: "🎨", key: msg.key } });

    const imageBuf = await downloadQuotedImage(socket, msg);
    await socket.sendMessage(sender, {
      text: `🐉 *Generating image...*\n🖊️ Prompt: ${promptRaw || '(no text)'}\n📷 Mode: ${imageBuf ? 'Edit (Image + Prompt)' : 'Text to Image'}`
    }, { quoted: msg });

    // 🧠 Setup Gemini SDK
    const ai = new GoogleGenAI({
      apiKey: process.env.GEMINI_API_KEY || "AIzaSyB6ZQwLHZFHxDCbBFJtc0GIN2ypdlga4vw"
    });

    // 🧩 Build contents
    const contents = imageBuf
      ? [
          { role: "user", parts: [{ inlineData: { mimeType: "image/jpeg", data: imageBuf.toString("base64") } }, { text: promptRaw }] }
        ]
      : [
          { role: "user", parts: [{ text: promptRaw }] }
        ];

    // ✨ Generate Image using Gemini SDK
    const response = await ai.models.generateContent({
      model: "gemini-2.5-flash-image",
      contents,
    });

    // 🖼️ Extract Image Data
    const part = response.candidates?.[0]?.content?.parts?.find(p => p.inlineData);
    if (!part) {
      console.log('Gemini response:', response);
      throw new Error('⚠️ No image data returned from Gemini API.');
    }

    const imageData = part.inlineData.data;
    const buffer = Buffer.from(imageData, "base64");

    const tmpPath = path.join(__dirname, `gemini-nano-${Date.now()}.png`);
    fs.writeFileSync(tmpPath, buffer);

    await socket.sendMessage(sender, {
      image: fs.readFileSync(tmpPath),
      caption: `✅ *Here you go!*\n🎨 Prompt: ${promptRaw}`
    }, { quoted: msg });

    try { fs.unlinkSync(tmpPath); } catch {}

  } catch (err) {
    console.error('nanobanana error:', err);
    await socket.sendMessage(sender, { text: `❌ *Error:* ${err.message || err}` }, { quoted: msg });
  }
  break;
}


case 'savecontact':
case 'gvcf2':
case 'scontact':
case 'savecontacts': {
  try {
    const text = args.join(" ").trim(); // ✅ Define text variable

    if (!text) {
      return await socket.sendMessage(sender, { 
        text: "🍁 *Usage:* .savecontact <group JID>\n📥 Example: .savecontact 9477xxxxxxx-123@g.us" 
      }, { quoted: msg });
    }

    const groupJid = text.trim();

    // ✅ Validate JID
    if (!groupJid.endsWith('@g.us')) {
      return await socket.sendMessage(sender, { 
        text: "❌ *Invalid group JID*. Must end with @g.us" 
      }, { quoted: msg });
    }

    let groupMetadata;
    try {
      groupMetadata = await socket.groupMetadata(groupJid);
    } catch {
      return await socket.sendMessage(sender, { 
        text: "❌ *Invalid group JID* or bot not in that group.*" 
      }, { quoted: msg });
    }

    const { participants, subject } = groupMetadata;
    let vcard = '';
    let index = 1;

    await socket.sendMessage(sender, { 
      text: `🔍 Fetching contact names from *${subject}*...` 
    }, { quoted: msg });

    // ✅ Loop through each participant
    for (const participant of participants) {
      const num = participant.id.split('@')[0];
      let name = num; // default name = number

      try {
        // Try to fetch from contacts or participant
        const contact = socket.contacts?.[participant.id] || {};
        if (contact?.notify) name = contact.notify;
        else if (contact?.vname) name = contact.vname;
        else if (contact?.name) name = contact.name;
        else if (participant?.name) name = participant.name;
      } catch {
        name = `Contact-${index}`;
      }

      // ✅ Add vCard entry
      vcard += `BEGIN:VCARD\n`;
      vcard += `VERSION:3.0\n`;
      vcard += `FN:${index}. ${name}\n`; // 👉 Include index number + name
      vcard += `TEL;type=CELL;type=VOICE;waid=${num}:+${num}\n`;
      vcard += `END:VCARD\n`;
      index++;
    }

    // ✅ Create a safe file name from group name
    const safeSubject = subject.replace(/[^\w\s]/gi, "_");
    const tmpDir = path.join(os.tmpdir(), `contacts_${Date.now()}`);
    fs.ensureDirSync(tmpDir);

    const filePath = path.join(tmpDir, `contacts-${safeSubject}.vcf`);
    fs.writeFileSync(filePath, vcard.trim());

    await socket.sendMessage(sender, { 
      text: `📁 *${participants.length}* contacts found in group *${subject}*.\n💾 Preparing VCF file...`
    }, { quoted: msg });

    await delay(1500);

    // ✅ Send the .vcf file
    await socket.sendMessage(sender, {
      document: fs.readFileSync(filePath),
      mimetype: 'text/vcard',
      fileName: `contacts-${safeSubject}.vcf`,
      caption: `✅ *Contacts Exported Successfully!*\n👥 Group: *${subject}*\n📇 Total Contacts: *${participants.length}*\n\n> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*`
    }, { quoted: msg });

    // ✅ Cleanup temp file
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch (cleanupError) {
      console.warn('Failed to cleanup temp file:', cleanupError);
    }

  } catch (err) {
    console.error('Save contact error:', err);
    await socket.sendMessage(sender, { 
      text: `❌ Error: ${err.message || err}` 
    }, { quoted: msg });
  }
  break;
}

case 'font': {
    const axios = require("axios");

    // ?? Load bot name dynamically
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    // 🔹 Fake contact for Meta AI mention
    const botMention = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_FONT"
        },
        message: {
            contactMessage: {
                displayName: botName,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    const q =
        msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        msg.message?.imageMessage?.caption ||
        msg.message?.videoMessage?.caption || '';

    const text = q.trim().replace(/^.fancy\s+/i, ""); // remove .fancy prefix

    if (!text) {
        return await socket.sendMessage(sender, {
            text: `❎ *Please provide text to convert into fancy fonts.*\n\n📌 *Example:* \`.font yasas\``
        }, { quoted: botMention });
    }

    try {
        const apiUrl = `https://www.dark-yasiya-api.site/other/font?text=${encodeURIComponent(text)}`;
        const response = await axios.get(apiUrl);

        if (!response.data.status || !response.data.result) {
            return await socket.sendMessage(sender, {
                text: "❌ *Error fetching fonts from API. Please try again later.*"
            }, { quoted: botMention });
        }

        const fontList = response.data.result
            .map(font => `*${font.name}:*\n${font.result}`)
            .join("\n\n");

        const finalMessage = `🎨 *Fancy Fonts Converter*\n\n${fontList}\n\n_© ${botName}_`;

        await socket.sendMessage(sender, {
            text: finalMessage
        }, { quoted: botMention });

    } catch (err) {
        console.error("Fancy Font Error:", err);
        await socket.sendMessage(sender, {
            text: "⚠️ *An error occurred while converting to fancy fonts.*"
        }, { quoted: botMention });
    }

    break;
}

case 'mediafire':
case 'mf':
case 'mfdl': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const url = text.split(" ")[1]; // .mediafire <link>

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

        // ✅ Fake Meta contact message (like Facebook style)
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_MEDIAFIRE"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!url) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please send a MediaFire link.*\n\nExample: .mediafire <url>'
            }, { quoted: shonux });
        }

        // ⏳ Notify start
        await socket.sendMessage(sender, { react: { text: '📥', key: msg.key } });
        await socket.sendMessage(sender, { text: '*⏳ Fetching MediaFire file info...*' }, { quoted: shonux });

        // 🔹 Call API
        let api = `https://tharuzz-ofc-apis.vercel.app/api/download/mediafire?url=${encodeURIComponent(url)}`;
        let { data } = await axios.get(api);

        if (!data.success || !data.result) {
            return await socket.sendMessage(sender, { text: '❌ *Failed to fetch MediaFire file.*' }, { quoted: shonux });
        }

        const result = data.result;
        const title = result.title || result.filename;
        const filename = result.filename;
        const fileSize = result.size;
        const downloadUrl = result.url;

        const caption = `📦 *${title}*\n\n` +
                        `📁 *𝐅ilename:* ${filename}\n` +
                        `📏 *𝐒ize:* ${fileSize}\n` +
                        `🌐 *𝐅rom:* ${result.from}\n` +
                        `📅 *𝐃ate:* ${result.date}\n` +
                        `🕑 *𝐓ime:* ${result.time}\n\n` +
                        `*✅ 𝐃ownloaded 𝐁y ${botName}*`;

        // 🔹 Send file automatically (document type for .zip etc.)
        await socket.sendMessage(sender, {
            document: { url: downloadUrl },
            fileName: filename,
            mimetype: 'application/octet-stream',
            caption: caption
        }, { quoted: shonux });

    } catch (err) {
        console.error("Error in MediaFire downloader:", err);

        // ✅ In catch also send Meta mention style
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_MEDIAFIRE"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
    break;
}
// ─────────────── APK DOWNLOADER (Aptoide) ───────────────
case 'apk':
case 'apksearch':
case 'apks': {
    try {
        const q = args.join(' ').trim();
        if (!q) {
            return await socket.sendMessage(sender, {
                text: `*📛 කරුණාකර app නමක් දෙන්න*\n\nExample: *${config.PREFIX}apk whatsapp*`
            }, { quoted: msg });
        }

        await socket.sendMessage(sender, { react: { text: '📦', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const apiUrl = `http://ws75.aptoide.com/api/7/apps/search/query=${encodeURIComponent(q)}/limit=1`;
        const response = await axios.get(apiUrl);
        const data = response.data;

        if (!data.datalist || !data.datalist.list || data.datalist.list.length === 0) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        const apk = data.datalist.list[0];
        const apkSizeMB = (apk.size / (1024 * 1024)).toFixed(2);

        const caption =
            `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
            `*┃➤ 📦 Name      :* ${apk.name}\n` +
            `*┃➤ 💾 Size      :* ${apkSizeMB} MB\n` +
            `*┃➤ 🗂 Package   :* ${apk.package}\n` +
            `*┃➤ 🕒 Updated   :* ${apk.updated}\n` +
            `*┃➤ 👨‍💻 Developer :* ${apk.developer.name}\n` +
            `*┗━━━━━━━━━━━━━━━━❖●►*`;

        const buttons = [
            { buttonId: `${config.PREFIX}apkdl ${apk.package}`, buttonText: { displayText: '📂 Download Apk' }, type: 1 },
            { buttonId: `${config.PREFIX}apkdtl ${apk.package}`, buttonText: { displayText: '📑 Details Apk' }, type: 1 }
        ];

        await socket.sendMessage(sender, {
            image: { url: apk.icon },
            caption: caption,
            footer: footer,
            buttons: buttons,
            headerType: 4
        }, { quoted: msg });

    } catch (e) {
        console.error('APK search error:', e);
        await socket.sendMessage(sender, { text: `*📛 APK Error : -* ${e.message || e}` }, { quoted: msg });
    }
    break;
}

case 'apkdl': {
    try {
        const q = args.join(' ').trim();
        if (!q) {
            return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });
        }

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const apiUrl = `http://ws75.aptoide.com/api/7/apps/search/query=${encodeURIComponent(q)}/limit=1`;
        const response = await axios.get(apiUrl);
        const data = response.data;

        if (!data.datalist || !data.datalist.list || data.datalist.list.length === 0) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        const apk = data.datalist.list[0];
        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        let resizedThumb = undefined;
        try {
            const sharp = require('sharp');
            const imgRes = await axios.get(apk.icon, { responseType: 'arraybuffer' });
            resizedThumb = await sharp(Buffer.from(imgRes.data)).resize(200, 200).toBuffer();
        } catch (e) {}

        await socket.sendMessage(sender, {
            document: { url: apk.file.path_alt },
            fileName: `${apk.name}.apk`,
            mimetype: 'application/vnd.android.package-archive',
            jpegThumbnail: resizedThumb,
            caption: `\`${apk.name}\`\n\n${footer}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔', key: msg.key } });

    } catch (e) {
        console.error('APK download error:', e);
        await socket.sendMessage(sender, { text: `*❌ Error: ${e.message || e}*` }, { quoted: msg });
    }
    break;
}

case 'apkdtl': {
    try {
        const q = args.join(' ').trim();
        if (!q) {
            return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });
        }

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const apiUrl = `http://ws75.aptoide.com/api/7/apps/search/query=${encodeURIComponent(q)}/limit=1`;
        const response = await axios.get(apiUrl);
        const data = response.data;

        if (!data.datalist || !data.datalist.list || data.datalist.list.length === 0) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        const apk = data.datalist.list[0];
        const apkSizeMB = (apk.size / (1024 * 1024)).toFixed(2);

        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        await socket.sendMessage(sender, {
            text:
                `*┎━━━━━━━━━━━━━━━━❖●►*\n` +
                `*┃➤ 📦 Name      :* ${apk.name}\n` +
                `*┃➤ 💾 Size      :* ${apkSizeMB} MB\n` +
                `*┃➤ 🗂 Package   :* ${apk.package}\n` +
                `*┃➤ 🕒 Updated   :* ${apk.updated}\n` +
                `*┃➤ 👨‍💻 Developer :* ${apk.developer.name}\n` +
                `*┗━━━━━━━━━━━━━━━━❖●►*\n\n` +
                `${footer}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔', key: msg.key } });

    } catch (e) {
        console.error('APK details error:', e);
        await socket.sendMessage(sender, { text: `*❌ Error: ${e.message || e}*` }, { quoted: msg });
    }
    break;
}

case 'xvdl2':
case 'xvnew': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const query = text.split(" ").slice(1).join(" ").trim();

        if (!query) return await socket.sendMessage(sender, { text: '🚫 Please provide a search query.\nExample: .xv mia' }, { quoted: msg });

        // 1️⃣ Send searching message
        await socket.sendMessage(sender, { text: '*⏳ Searching XVideos...*' }, { quoted: msg });

        // 2️⃣ Call search API
        const searchRes = await axios.get(`https://tharuzz-ofc-api-v2.vercel.app/api/search/xvsearch?query=${encodeURIComponent(query)}`);
        const videos = searchRes.data.result?.xvideos?.slice(0, 10);
        if (!videos || videos.length === 0) return await socket.sendMessage(sender, { text: '*❌ No results found.*' }, { quoted: msg });

        // 3️⃣ Prepare list message
        let listMsg = `🔍 *XVideos Results for:* ${query}\n\n`;
        videos.forEach((vid, idx) => {
            listMsg += `*${idx + 1}.* ${vid.title}\n${vid.info}\n➡️ ${vid.link}\n\n`;
        });
        listMsg += '_Reply with the number to download the video._';

        await socket.sendMessage(sender, { text: listMsg }, { quoted: msg });

        // 4️⃣ Cache results for reply handling
        global.xvCache = global.xvCache || {};
        global.xvCache[sender] = videos.map(v => v.link);

    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '*❌ Error occurred.*' }, { quoted: msg });
    }
}
break;


// Handle reply to download selected video
case 'xvselect': {
    try {
        const replyText = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const selection = parseInt(replyText);

        const links = global.xvCache?.[sender];
        if (!links || isNaN(selection) || selection < 1 || selection > links.length) {
            return await socket.sendMessage(sender, { text: '🚫 Invalid selection number.' }, { quoted: msg });
        }

        const videoUrl = links[selection - 1];

        await socket.sendMessage(sender, { text: '*⏳ Downloading video...*' }, { quoted: msg });

        // Call download API
        const dlRes = await axios.get(`https://tharuzz-ofc-api-v2.vercel.app/api/download/xvdl?url=${encodeURIComponent(videoUrl)}`);
        const result = dlRes.data.result;

        if (!result) return await socket.sendMessage(sender, { text: '*❌ Failed to fetch video.*' }, { quoted: msg });

        // Send video
        await socket.sendMessage(sender, {
            video: { url: result.dl_Links.highquality },
            caption: `🎥 *${result.title}*\n⏱ Duration: ${result.duration}s`,
            jpegThumbnail: result.thumbnail ? await axios.get(result.thumbnail, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
        }, { quoted: msg });

        // Clear cache
        delete global.xvCache[sender];

    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '*❌ Error downloading video.*' }, { quoted: msg });
    }
}
break;

// ---------------- list saved newsletters (show emojis) ----------------
case 'newslist': {
  try {
    const docs = await listNewslettersFromMongo();
    if (!docs || docs.length === 0) {
      let userCfg = {};
      try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
      const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
      const shonux = {
          key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST" },
          message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '📭 No channels saved in DB.' }, { quoted: shonux });
    }

    let txt = '*📚 Saved Newsletter Channels:*\n\n';
    for (const d of docs) {
      txt += `• ${d.jid}\n  Emojis: ${Array.isArray(d.emojis) && d.emojis.length ? d.emojis.join(' ') : '(default)'}\n\n`;
    }

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('newslist error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Failed to list channels.' }, { quoted: shonux });
  }
  break;
}
case 'cid': {
    // Extract query from message
    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    // ✅ Dynamic botName load
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    // ✅ Fake Meta AI vCard (for quoted msg)
    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_CID"
        },
        message: {
            contactMessage: {
                displayName: botName,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    // Clean command prefix (.cid, /cid, !cid, etc.)
    const channelLink = q.replace(/^[.\/!]cid\s*/i, '').trim();

    // Check if link is provided
    if (!channelLink) {
        return await socket.sendMessage(sender, {
            text: '❎ Please provide a WhatsApp Channel link.\n\n📌 *Example:* .cid https://whatsapp.com/channel/123456789'
        }, { quoted: shonux });
    }

    // Validate link
    const match = channelLink.match(/whatsapp\.com\/channel\/([\w-]+)/);
    if (!match) {
        return await socket.sendMessage(sender, {
            text: '⚠️ *Invalid channel link format.*\n\nMake sure it looks like:\nhttps://whatsapp.com/channel/xxxxxxxxx'
        }, { quoted: shonux });
    }

    const inviteId = match[1];

    try {
        // Send fetching message
        await socket.sendMessage(sender, {
            text: `🔎 Fetching channel info for: *${inviteId}*`
        }, { quoted: shonux });

        // Get channel metadata
        const metadata = await socket.newsletterMetadata("invite", inviteId);

        if (!metadata || !metadata.id) {
            return await socket.sendMessage(sender, {
                text: '❌ Channel not found or inaccessible.'
            }, { quoted: shonux });
        }

        // Format details
        const infoText = `
📡 *𝐖hatsApp 𝐂hannel 𝐈nfo*

╔════════════════❒
╠⦁ 🆔 *𝐈D:* ${metadata.id}
╠⦁ 📌 *𝐍ame:* ${metadata.name}
╠⦁ 👥 *𝐅ollowers:* ${metadata.subscribers?.toLocaleString() || 'N/A'}
╠⦁ 📅 *𝐂reated 𝐎n:* ${metadata.creation_time ? new Date(metadata.creation_time * 1000).toLocaleString("si-LK") : 'Unknown'}
╚═══════════❒

> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.3 🥷🇱🇰*
`;

        // Send preview if available
        if (metadata.preview) {
            await socket.sendMessage(sender, {
                image: { url: `https://pps.whatsapp.net${metadata.preview}` },
                caption: infoText
            }, { quoted: shonux });
        } else {
            await socket.sendMessage(sender, {
                text: infoText
            }, { quoted: shonux });
        }

    } catch (err) {
        console.error("CID command error:", err);
        await socket.sendMessage(sender, {
            text: '⚠️ An unexpected error occurred while fetching channel info.'
        }, { quoted: shonux });
    }

    break;
}

case 'owner': {
  try {
    // vCard with multiple details
    let vcard = 
      'BEGIN:VCARD\n' +
      'VERSION:3.0\n' +
      'FN:DULA\n' + // Name
      'ORG:WhatsApp Bot Developer;\n' + // Organization
      'TITLE:Founder & CEO of Mini Bot;\n' + // Title / Role
      'EMAIL;type=INTERNET:dula9x@gmail.cim\n' + // Email
      'ADR;type=WORK:;;Ratnapura;;Sri Lanka\n' + // Address
      'URL:https://github.com\n' + // Website
      'TEL;type=CELL;type=VOICE;waid=94752978237\n' + // WhatsApp Number
      'TEL;type=CELL;type=VOICE;waid=94752978237\n' + // Second Number (Owner)
      'END:VCARD';

    await conn.sendMessage(
      m.chat,
      {
        contacts: {
          displayName: '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰',
          contacts: [{ vcard }]
        }
      },
      { quoted: m }
    );

  } catch (err) {
    console.error(err);
    await conn.sendMessage(m.chat, { text: '⚠️ Owner info fetch error.' }, { quoted: m });
  }
}
break;

case 'addadmin': {
  if (!args || args.length === 0) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide a jid or number to add as admin\nExample: .addadmin 9477xxxxxxx' }, { quoted: shonux });
  }

  const jidOr = args[0].trim();
  if (!isOwner) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❌ Only owner can add admins.' }, { quoted: shonux });
  }

  try {
    await addAdminToMongo(jidOr);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Added admin: ${jidOr}` }, { quoted: shonux });
  } catch (e) {
    console.error('addadmin error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN4" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `❌ Failed to add admin: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}
case 'tagall': {
  try {
    if (!from || !from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });

    let gm = null;
    try { gm = await socket.groupMetadata(from); } catch(e) { gm = null; }
    if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to fetch group info.' }, { quoted: msg });

    const participants = gm.participants || [];
    if (!participants.length) return await socket.sendMessage(sender, { text: '❌ No members found in the group.' }, { quoted: msg });

    const text = args && args.length ? args.join(' ') : '📢 Announcement';

    let groupPP = 'https://files.catbox.moe/qb2puf.jpeg';
    try { groupPP = await socket.profilePictureUrl(from, 'image'); } catch(e){}

    const mentions = participants.map(p => p.id || p.jid);
    const groupName = gm.subject || 'Group';
    const totalMembers = participants.length;

    const emojis = ['📢','🔊','🌐','🛡️','🚀','🎯','🧿','🪩','🌀','💠','🎊','🎧','📣','🗣️'];
    const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    // BotName meta mention
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TAGALL" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let caption = `╔══『 ❤️‍🩹 *𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐆𝚁𝙾𝚄𝙿 𝐀𝙽𝙽𝙾𝚄𝙽𝙲𝙴𝙼𝙴𝙽𝚃* 』═══❒\n`;
    caption += `╠⦁ 📌 *𝐆roup:* ${groupName}\n`;
    caption += `╠⦁ 👥 *𝐌embers:* ${totalMembers}\n`;
    caption += `╠⦁ 💬 *𝐌essage:* ${text}\n`;
    caption += `╚═════════════════════════❒\n\n`;
    caption += `📍 *Mentioning all members below:*\n\n`;
    for (const m of participants) {
      const id = (m.id || m.jid);
      if (!id) continue;
      caption += `${randomEmoji} @${id.split('@')[0]}\n`;
    }
    caption += `\n━━━━━━⊱ *${botName}* ⊰━━━━━━`;

    await socket.sendMessage(from, {
      image: { url: groupPP },
      caption,
      mentions,
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (err) {
    console.error('tagall error', err);
    await socket.sendMessage(sender, { text: '❌ Error running tagall.' }, { quoted: msg });
  }
  break;
}


case 'ig':
case 'insta':
case 'instagram': {
  try {
    const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
    const q = text.split(" ").slice(1).join(" ").trim();

    // Validate
    if (!q) {
      await socket.sendMessage(sender, { 
        text: '*🚫 Please provide an Instagram post/reel link.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    const igRegex = /(?:https?:\/\/)?(?:www\.)?instagram\.com\/[^\s]+/;
    if (!igRegex.test(q)) {
      await socket.sendMessage(sender, { 
        text: '*🚫 Invalid Instagram link.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
    await socket.sendMessage(sender, { text: '*⏳ Downloading Instagram media...*' });

    // 🔹 Load session bot name
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    // 🔹 Meta style fake contact
    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_002"
      },
      message: {
        contactMessage: {
          displayName: botName, // dynamic bot name
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550003:+1 313 555 0003
END:VCARD`
        }
      }
    };

    // API request
    let apiUrl = `https://delirius-apiofc.vercel.app/download/instagram?url=${encodeURIComponent(q)}`;
    let { data } = await axios.get(apiUrl).catch(() => ({ data: null }));

    // Backup API if first fails
    if (!data?.status || !data?.downloadUrl) {
      const backupUrl = `https://api.tiklydown.me/api/instagram?url=${encodeURIComponent(q)}`;
      const backup = await axios.get(backupUrl).catch(() => ({ data: null }));
      if (backup?.data?.video) {
        data = {
          status: true,
          downloadUrl: backup.data.video
        };
      }
    }

    if (!data?.status || !data?.downloadUrl) {
      await socket.sendMessage(sender, { 
        text: '*🚩 Failed to fetch Instagram video.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    // Caption (Dynamic Bot Name)
    const titleText = `*📸 ${botName} 𝐈ɴꜱᴛᴀɢʀᴀᴍ 𝐃ᴏᴡɴʟᴏᴀᴅᴇʀ*`;
    const content = `┏━━━━━━━━━━━━━━━━\n` +
                    `┃📌 \`𝐒ource\` : Instagram\n` +
                    `┃📹 \`𝐓ype\` : Video/Reel\n` +
                    `┗━━━━━━━━━━━━━━━━`;

    const footer = `🤖 ${botName}`;
    const captionMessage = typeof formatMessage === 'function'
      ? formatMessage(titleText, content, footer)
      : `${titleText}\n\n${content}\n${footer}`;

    // Send video with fake contact quoted
    await socket.sendMessage(sender, {
      video: { url: data.downloadUrl },
      caption: captionMessage,
      contextInfo: { mentionedJid: [sender] },
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '❄ MENU' }, type: 1 },
        { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '❄ BOT INFO' }, type: 1 }
      ]
    }, { quoted: shonux }); // 🔹 fake contact quoted

  } catch (err) {
    console.error("Error in Instagram downloader:", err);
    await socket.sendMessage(sender, { 
      text: '*❌ Internal Error. Please try again later.*',
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📋 MENU' }, type: 1 }]
    });
  }
  break;
}

case 'online': {
  try {
    if (!(from || '').endsWith('@g.us')) {
      await socket.sendMessage(sender, { text: '❌ This command works only in group chats.' }, { quoted: msg });
      break;
    }

    let groupMeta;
    try { groupMeta = await socket.groupMetadata(from); } catch (err) { console.error(err); break; }

    const callerJid = (nowsender || '').replace(/:.*$/, '');
    const callerId = callerJid.includes('@') ? callerJid : `${callerJid}@s.whatsapp.net`;
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const isOwnerCaller = callerJid.startsWith(ownerNumberClean);
    const groupAdmins = (groupMeta.participants || []).filter(p => p.admin === 'admin' || p.admin === 'superadmin').map(p => p.id);
    const isGroupAdminCaller = groupAdmins.includes(callerId);

    if (!isOwnerCaller && !isGroupAdminCaller) {
      await socket.sendMessage(sender, { text: '❌ Only group admins or the bot owner can use this command.' }, { quoted: msg });
      break;
    }

    try { await socket.sendMessage(sender, { text: '🔄 Scanning for online members... please wait ~15 seconds' }, { quoted: msg }); } catch(e){}

    const participants = (groupMeta.participants || []).map(p => p.id);
    const onlineSet = new Set();
    const presenceListener = (update) => {
      try {
        if (update?.presences) {
          for (const id of Object.keys(update.presences)) {
            const pres = update.presences[id];
            if (pres?.lastKnownPresence && pres.lastKnownPresence !== 'unavailable') onlineSet.add(id);
            if (pres?.available === true) onlineSet.add(id);
          }
        }
      } catch (e) { console.warn('presenceListener error', e); }
    };

    for (const p of participants) {
      try { if (typeof socket.presenceSubscribe === 'function') await socket.presenceSubscribe(p); } catch(e){}
    }
    socket.ev.on('presence.update', presenceListener);

    const checks = 3; const intervalMs = 5000;
    await new Promise((resolve) => { let attempts=0; const iv=setInterval(()=>{ attempts++; if(attempts>=checks){ clearInterval(iv); resolve(); } }, intervalMs); });
    try { socket.ev.off('presence.update', presenceListener); } catch(e){}

    if (onlineSet.size === 0) {
      await socket.sendMessage(sender, { text: '⚠️ No online members detected (they may be hiding presence or offline).' }, { quoted: msg });
      break;
    }

    const onlineArray = Array.from(onlineSet).filter(j => participants.includes(j));
    const mentionList = onlineArray.map(j => j);

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    // BotName meta mention
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ONLINE" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let txt = `💚 *𝐎nline 𝐌embers* — ${onlineArray.length}/${participants.length}\n\n`;
    onlineArray.forEach((jid, i) => {
      txt += `${i+1}. @${jid.split('@')[0]}\n`;
    });

    await socket.sendMessage(sender, {
      text: txt.trim(),
      mentions: mentionList
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (err) {
    console.error('Error in online command:', err);
    try { await socket.sendMessage(sender, { text: '❌ An error occurred while checking online members.' }, { quoted: msg }); } catch(e){}
  }
  break;
}



case 'deladmin': {
  if (!args || args.length === 0) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN1" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide a jid/number to remove\nExample: .deladmin 9477xxxxxxx' }, { quoted: shonux });
  }

  const jidOr = args[0].trim();
  if (!isOwner) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN2" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❌ Only owner can remove admins.' }, { quoted: shonux });
  }

  try {
    await removeAdminFromMongo(jidOr);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN3" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Removed admin: ${jidOr}` }, { quoted: shonux });
  } catch (e) {
    console.error('deladmin error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN4" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `❌ Failed to remove admin: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

case 'admins': {
  try {
    const list = await loadAdminsFromMongo();
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADMINS" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!list || list.length === 0) {
      return await socket.sendMessage(sender, { text: 'No admins configured.' }, { quoted: shonux });
    }

    let txt = '*👑 Admins:*\n\n';
    for (const a of list) txt += `• ${a}\n`;

    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('admins error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰';
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADMINS2" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '❌ Failed to list admins.' }, { quoted: shonux });
  }
  break;
}
case 'setlogo': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change this session logo.' }, { quoted: shonux });
    break;
  }

  const ctxInfo = (msg.message.extendedTextMessage || {}).contextInfo || {};
  const quotedMsg = ctxInfo.quotedMessage;
  const media = await downloadQuotedMedia(quotedMsg).catch(()=>null);
  let logoSetTo = null;

  try {
    if (media && media.buffer) {
      const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
      fs.ensureDirSync(sessionPath);
      const mimeExt = (media.mime && media.mime.split('/').pop()) || 'jpg';
      const logoPath = path.join(sessionPath, `logo.${mimeExt}`);
      fs.writeFileSync(logoPath, media.buffer);
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = logoPath;
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = logoPath;
    } else if (args && args[0] && (args[0].startsWith('http') || args[0].startsWith('https'))) {
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = args[0];
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = args[0];
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: '❗ Usage: Reply to an image with `.setlogo` OR provide an image URL: `.setlogo https://example.com/logo.jpg`' }, { quoted: shonux });
      break;
    }

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Logo set for this session: ${logoSetTo}` }, { quoted: shonux });
  } catch (e) {
    console.error('setlogo error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to set logo: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}
case 'jid': {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || '𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 🥷🇱🇰'; // dynamic bot name

    const userNumber = sender.split('@')[0]; 

    // Reaction
    await socket.sendMessage(sender, { 
        react: { text: "🆔", key: msg.key } 
    });

    // Fake contact quoting for meta style
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_FAKE_ID" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, {
        text: `*🆔 𝐂hat 𝐉ID:* ${sender}\n*📞 𝐘our 𝐍umber:* +${userNumber}`,
    }, { quoted: shonux });
    break;
}

// use inside your switch(command) { ... } block

case 'block': {
  try {
    // caller number (who sent the command)
    const callerNumberClean = (senderNumber || '').replace(/[^0-9]/g, '');
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const sessionOwner = (number || '').replace(/[^0-9]/g, '');

    // allow if caller is global owner OR this session's owner
    if (callerNumberClean !== ownerNumberClean && callerNumberClean !== sessionOwner) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ ඔබට මෙය භාවිත කිරීමට අවසර නැත. (Owner හෝ මෙහි session owner විය යුතුයි)' }, { quoted: msg });
      break;
    }

    // determine target JID: reply / mention / arg
    let targetJid = null;
    const ctx = msg.message?.extendedTextMessage?.contextInfo;

    if (ctx?.participant) targetJid = ctx.participant; // replied user
    else if (ctx?.mentionedJid && ctx.mentionedJid.length) targetJid = ctx.mentionedJid[0]; // mentioned
    else if (args && args.length > 0) {
      const possible = args[0].trim();
      if (possible.includes('@')) targetJid = possible;
      else {
        const digits = possible.replace(/[^0-9]/g,'');
        if (digits) targetJid = `${digits}@s.whatsapp.net`;
      }
    }

    if (!targetJid) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❗ කරුණාකර reply කරන හෝ mention කරන හෝ number එක යොදන්න. උදාහරණය: .block 9477xxxxxxx' }, { quoted: msg });
      break;
    }

    // normalize
    if (!targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;
    if (!targetJid.endsWith('@s.whatsapp.net') && !targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;

    // perform block
    try {
      if (typeof socket.updateBlockStatus === 'function') {
        await socket.updateBlockStatus(targetJid, 'block');
      } else {
        // some bailey builds use same method name; try anyway
        await socket.updateBlockStatus(targetJid, 'block');
      }
      try { await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: `✅ @${targetJid.split('@')[0]} blocked successfully.`, mentions: [targetJid] }, { quoted: msg });
    } catch (err) {
      console.error('Block error:', err);
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ Failed to block the user. (Maybe invalid JID or API failure)' }, { quoted: msg });
    }

  } catch (err) {
    console.error('block command general error:', err);
    try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
    await socket.sendMessage(sender, { text: '❌ Error occurred while processing block command.' }, { quoted: msg });
  }
  break;
}

case 'unblock': {
  try {
    // caller number (who sent the command)
    const callerNumberClean = (senderNumber || '').replace(/[^0-9]/g, '');
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const sessionOwner = (number || '').replace(/[^0-9]/g, '');

    // allow if caller is global owner OR this session's owner
    if (callerNumberClean !== ownerNumberClean && callerNumberClean !== sessionOwner) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ ඔබට මෙය භාවිත කිරීමට අවසර නැත. (Owner හෝ මෙහි session owner විය යුතුයි)' }, { quoted: msg });
      break;
    }

    // determine target JID: reply / mention / arg
    let targetJid = null;
    const ctx = msg.message?.extendedTextMessage?.contextInfo;

    if (ctx?.participant) targetJid = ctx.participant;
    else if (ctx?.mentionedJid && ctx.mentionedJid.length) targetJid = ctx.mentionedJid[0];
    else if (args && args.length > 0) {
      const possible = args[0].trim();
      if (possible.includes('@')) targetJid = possible;
      else {
        const digits = possible.replace(/[^0-9]/g,'');
        if (digits) targetJid = `${digits}@s.whatsapp.net`;
      }
    }

    if (!targetJid) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❗ කරුණාකර reply කරන හෝ mention කරන හෝ number එක යොදන්න. උදාහරණය: .unblock 9477xxxxxxx' }, { quoted: msg });
      break;
    }

    // normalize
    if (!targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;
    if (!targetJid.endsWith('@s.whatsapp.net') && !targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;

    // perform unblock
    try {
      if (typeof socket.updateBlockStatus === 'function') {
        await socket.updateBlockStatus(targetJid, 'unblock');
      } else {
        await socket.updateBlockStatus(targetJid, 'unblock');
      }
      try { await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: `🔓 @${targetJid.split('@')[0]} unblocked successfully.`, mentions: [targetJid] }, { quoted: msg });
    } catch (err) {
      console.error('Unblock error:', err);
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ Failed to unblock the user.' }, { quoted: msg });
    }

  } catch (err) {
    console.error('unblock command general error:', err);
    try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
    await socket.sendMessage(sender, { text: '❌ Error occurred while processing unblock command.' }, { quoted: msg });
  }
  break;
}

case 'setbotname': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change this session bot name.' }, { quoted: shonux });
    break;
  }

  const name = args.join(' ').trim();
  if (!name) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❗ Provide bot name. Example: `.setbotname ✦ ━━ ᴅᴄᴛ ɴᴏᴠᴀ X ᴍᴅ ━━ ✦`' }, { quoted: shonux });
  }

  try {
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    cfg.botName = name;
    await setUserConfigInMongo(sanitized, cfg);

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Bot display name set for this session: ${name}` }, { quoted: shonux });
  } catch (e) {
    console.error('setbotname error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to set bot name: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

case 'ringtone': {
    try {
        const { generateWAMessageFromContent } = require('@dnuzi/baileys');

        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර වචන කිහිපයක් ලියන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '🎵', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const api = `https://www.movanest.xyz/v2/ringtone?title=${encodeURIComponent(q)}`;
        const res = (await axios.get(api)).data;

        if (!res.status || !res.results.length) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        const results = res.results.slice(0, 10);

        const caption = `*乂 RINGTONE DOWNLOADER*\n\n*○ \`Search\` : -* ${q}\n*○ \`Found\` : -* ${results.length}`;

        const interactiveMsg = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: { deviceListMetadata: {}, deviceListMetadataVersion: 2 },
                    interactiveMessage: {
                        body: { text: caption },
                        footer: { text: footer },
                        header: { hasMediaAttachment: false },
                        nativeFlowMessage: {
                            buttons: [{
                                name: 'single_select',
                                buttonParamsJson: JSON.stringify({
                                    title: 'Select Ringtone ❏',
                                    sections: [{
                                        title: '🎧 Ringtone List',
                                        rows: results.map(r => ({
                                            header: r.title,
                                            title: r.title,
                                            description: 'Tap to download ringtone',
                                            id: `${prefix}getringtone ${r.audio}`
                                        }))
                                    }]
                                })
                            }]
                        }
                    }
                }
            }
        }, { quoted: msg });

        await socket.relayMessage(sender, interactiveMsg.message, { messageId: interactiveMsg.key.id });

    } catch (e) {
        console.error('Ringtone Error:', e);
        await socket.sendMessage(sender, { text: '*❌ Ringtone Error*' }, { quoted: msg });
    }
    break;
}
case 'getringtone': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '⬇️', key: msg.key } });

        await socket.sendMessage(sender, {
            audio: { url: q },
            mimetype: 'audio/mpeg'
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('GetRingtone Error:', e);
        await socket.sendMessage(sender, { text: '*❌ Error*' }, { quoted: msg });
    }
    break;
}
case 'twitter':
case 'twdl':
case 'twitterdl': {
    try {
        const q = args.join(' ').trim();
        if (!q) return await socket.sendMessage(sender, { text: '*📛 කරුණාකර url එකක් ලබා දෙන්න*' }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });

        const ownerdata = (await axios.get(
            'https://raw.githubusercontent.com/Nethmika-LK/Shala-MD-Database/refs/heads/main/Ditelse.json'
        )).data;
        const { footer } = ownerdata;

        const apiUrl = `https://www.movanest.xyz/v2/ssstwitter?url=${encodeURIComponent(q)}`;
        const json = (await axios.get(apiUrl)).data;

        if (!json.status || !json.results?.url) {
            return await socket.sendMessage(sender, { text: '*📛 මට කිසිවක් සොයාගත නොහැකි විය :(*' }, { quoted: msg });
        }

        await socket.sendMessage(sender, {
            video: { url: json.results.url },
            mimetype: 'video/mp4',
            caption: `🎥 *Twitter Video Downloader*\n\n🔗 ${q}\n\n${footer}`
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✔️', key: msg.key } });

    } catch (e) {
        console.error('Twitter Error:', e);
        await socket.sendMessage(sender, { text: '*📛 Twitter Video Error*' }, { quoted: msg });
    }
    break;
}

        // default
        // ==================== LOGO COMMANDS ====================
        case 'naruto': case 'dragonball': case 'onepiece': case '3dcomic':
        case 'marvel': case 'deadpool': case 'blackpink': case 'harrypotter':
        case 'neon': case 'glitch': case 'rainbow': case 'glass':
        case 'frostedglass': case 'neonglass': case 'gold': case 'silver':
        case 'diamond': case 'fire': case 'water': case 'smoke': case 'ice':
        case 'crystal': case 'luxury': case 'modern': case 'christmas':
        case 'halloween': case 'graffiti': case 'sand': case 'sky': case 'space': {
          try {
            if (!args.length) {
              return await socket.sendMessage(sender, { text: `❌ Please provide text.\nExample: .${command} Your Text` }, { quoted: msg });
            }
            await socket.sendMessage(sender, { react: { text: "🎨", key: msg.key } });
            await socket.sendMessage(sender, { text: `✨ Creating *${command}* logo...` }, { quoted: msg });

            const logoEffects = {
              naruto:       'https://en.ephoto360.com/naruto-shippuden-logo-style-text-effect-online-808.html',
              dragonball:   'https://en.ephoto360.com/create-dragon-ball-style-text-effects-online-809.html',
              onepiece:     'https://en.ephoto360.com/create-one-piece-logo-style-text-effect-online-814.html',
              '3dcomic':    'https://en.ephoto360.com/create-online-3d-comic-style-text-effects-817.html',
              marvel:       'https://en.ephoto360.com/create-3d-marvel-logo-style-text-effect-online-811.html',
              deadpool:     'https://en.ephoto360.com/create-text-effects-in-the-style-of-the-deadpool-logo-818.html',
              blackpink:    'https://en.ephoto360.com/create-a-blackpink-style-logo-with-members-signatures-810.html',
              harrypotter:  'https://en.ephoto360.com/create-harry-potter-logo-style-text-effect-online-815.html',
              neon:         'https://en.ephoto360.com/write-text-on-3d-neon-sign-board-online-805.html',
              glitch:       'https://en.ephoto360.com/create-a-glitch-text-effect-online-812.html',
              rainbow:      'https://en.ephoto360.com/create-rainbow-text-effects-online-801.html',
              glass:        'https://en.ephoto360.com/create-glass-text-effect-online-821.html',
              frostedglass: 'https://en.ephoto360.com/create-frosted-glass-text-effect-online-822.html',
              neonglass:    'https://en.ephoto360.com/create-3d-neon-glass-text-effect-online-823.html',
              gold:         'https://en.ephoto360.com/create-golden-metal-text-effect-online-804.html',
              silver:       'https://en.ephoto360.com/create-silver-metal-text-effect-online-806.html',
              diamond:      'https://en.ephoto360.com/create-diamond-text-effect-online-807.html',
              fire:         'https://en.ephoto360.com/create-burning-fire-text-effect-online-802.html',
              water:        'https://en.ephoto360.com/create-underwater-text-effect-online-803.html',
              smoke:        'https://en.ephoto360.com/create-smoky-text-effect-online-799.html',
              ice:          'https://en.ephoto360.com/create-ice-text-effect-online-824.html',
              crystal:      'https://en.ephoto360.com/create-crystal-text-effect-online-825.html',
              luxury:       'https://en.ephoto360.com/create-luxury-gold-text-effect-online-800.html',
              modern:       'https://en.ephoto360.com/create-modern-metallic-text-effect-online-819.html',
              christmas:    'https://en.ephoto360.com/create-christmas-text-effect-online-798.html',
              halloween:    'https://en.ephoto360.com/create-halloween-pumpkin-text-effect-online-796.html',
              graffiti:     'https://en.ephoto360.com/create-graffiti-text-effects-online-795.html',
              sand:         'https://en.ephoto360.com/write-text-on-the-beach-sand-online-794.html',
              sky:          'https://en.ephoto360.com/write-text-on-the-cloud-sky-online-793.html',
              space:        'https://en.ephoto360.com/create-galaxy-text-effect-online-792.html'
            };

            const effectUrl = logoEffects[command];
            const text = args.join(" ");

            try {
              const Photo360 = require('abir-photo360-apis');
              const generator = new Photo360(effectUrl);
              generator.setName(text);
              const result = await generator.execute();
              if (result.status && result.imageUrl) {
                await socket.sendMessage(sender, {
                  image: { url: result.imageUrl },
                  caption: `✨ *${command.charAt(0).toUpperCase() + command.slice(1)}:* ${text}\n\n> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 ${config.BOT_NAME}*`
                }, { quoted: msg });
              } else {
                await socket.sendMessage(sender, { text: `❌ Failed to generate logo. Please try again.` }, { quoted: msg });
              }
            } catch (apiErr) {
              console.error('Logo API Error:', apiErr.message);
              await socket.sendMessage(sender, { text: `❌ Logo Error: ${apiErr.message}` }, { quoted: msg });
            }
          } catch(e) {
            console.error('Logo command error:', e);
            await socket.sendMessage(sender, { text: `❌ Error: ${e.message}` }, { quoted: msg });
          }
          break;
        }

        case 'logo': {
          try {
            const subCmd = args[0] ? args[0].toLowerCase() : '';

            const logoEffects = {
              naruto:       { url: 'https://en.ephoto360.com/naruto-shippuden-logo-style-text-effect-online-808.html',        desc: 'Naruto Shippuden style' },
              dragonball:   { url: 'https://en.ephoto360.com/create-dragon-ball-style-text-effects-online-809.html',          desc: 'Dragon Ball style' },
              onepiece:     { url: 'https://en.ephoto360.com/create-one-piece-logo-style-text-effect-online-814.html',        desc: 'One Piece logo style' },
              '3dcomic':    { url: 'https://en.ephoto360.com/create-online-3d-comic-style-text-effects-817.html',             desc: '3D Comic style' },
              marvel:       { url: 'https://en.ephoto360.com/create-3d-marvel-logo-style-text-effect-online-811.html',        desc: 'Marvel logo style' },
              deadpool:     { url: 'https://en.ephoto360.com/create-text-effects-in-the-style-of-the-deadpool-logo-818.html', desc: 'Deadpool logo style' },
              blackpink:    { url: 'https://en.ephoto360.com/create-a-blackpink-style-logo-with-members-signatures-810.html', desc: 'Blackpink style' },
              harrypotter:  { url: 'https://en.ephoto360.com/create-harry-potter-logo-style-text-effect-online-815.html',     desc: 'Harry Potter style' },
              neon:         { url: 'https://en.ephoto360.com/write-text-on-3d-neon-sign-board-online-805.html',               desc: '3D Neon sign board' },
              glitch:       { url: 'https://en.ephoto360.com/create-a-glitch-text-effect-online-812.html',                   desc: 'Glitch text effect' },
              rainbow:      { url: 'https://en.ephoto360.com/create-rainbow-text-effects-online-801.html',                   desc: 'Rainbow text effect' },
              glass:        { url: 'https://en.ephoto360.com/create-glass-text-effect-online-821.html',                      desc: 'Transparent glass' },
              frostedglass: { url: 'https://en.ephoto360.com/create-frosted-glass-text-effect-online-822.html',              desc: 'Frosted glass' },
              neonglass:    { url: 'https://en.ephoto360.com/create-3d-neon-glass-text-effect-online-823.html',              desc: '3D Neon glass' },
              gold:         { url: 'https://en.ephoto360.com/create-golden-metal-text-effect-online-804.html',               desc: 'Golden metal' },
              silver:       { url: 'https://en.ephoto360.com/create-silver-metal-text-effect-online-806.html',               desc: 'Silver metal' },
              diamond:      { url: 'https://en.ephoto360.com/create-diamond-text-effect-online-807.html',                    desc: 'Diamond effect' },
              fire:         { url: 'https://en.ephoto360.com/create-burning-fire-text-effect-online-802.html',               desc: 'Burning fire' },
              water:        { url: 'https://en.ephoto360.com/create-underwater-text-effect-online-803.html',                 desc: 'Underwater' },
              smoke:        { url: 'https://en.ephoto360.com/create-smoky-text-effect-online-799.html',                      desc: 'Smoky text' },
              ice:          { url: 'https://en.ephoto360.com/create-ice-text-effect-online-824.html',                        desc: 'Frozen ice' },
              crystal:      { url: 'https://en.ephoto360.com/create-crystal-text-effect-online-825.html',                   desc: 'Shiny crystal' },
              luxury:       { url: 'https://en.ephoto360.com/create-luxury-gold-text-effect-online-800.html',                desc: 'Luxury gold' },
              modern:       { url: 'https://en.ephoto360.com/create-modern-metallic-text-effect-online-819.html',            desc: 'Modern metallic' },
              christmas:    { url: 'https://en.ephoto360.com/create-christmas-text-effect-online-798.html',                  desc: 'Christmas style' },
              halloween:    { url: 'https://en.ephoto360.com/create-halloween-pumpkin-text-effect-online-796.html',          desc: 'Halloween pumpkin' },
              graffiti:     { url: 'https://en.ephoto360.com/create-graffiti-text-effects-online-795.html',                  desc: 'Graffiti text' },
              sand:         { url: 'https://en.ephoto360.com/write-text-on-the-beach-sand-online-794.html',                  desc: 'Beach sand' },
              sky:          { url: 'https://en.ephoto360.com/write-text-on-the-cloud-sky-online-793.html',                   desc: 'Cloud sky' },
              space:        { url: 'https://en.ephoto360.com/create-galaxy-text-effect-online-792.html',                     desc: 'Galaxy text' }
            };

            if (subCmd === 'list') {
              let listMsg = `🎨 *Available Logo Effects:*\n\n`;
              listMsg += `*🎌 Anime:* naruto, dragonball, onepiece, marvel, deadpool, harrypotter\n`;
              listMsg += `*✨ Glow:* neon, glitch, rainbow, glass, frostedglass, neonglass\n`;
              listMsg += `*💰 Metal:* gold, silver, diamond, luxury, modern\n`;
              listMsg += `*🌿 Elements:* fire, water, smoke, ice, crystal, sand, sky, space\n`;
              listMsg += `*🎄 Events:* christmas, halloween\n`;
              listMsg += `*🎨 Art:* 3dcomic, graffiti, blackpink\n\n`;
              listMsg += `📝 *Usage:* .[effect] [text]\n📌 *Example:* .naruto Uzumaki`;
              await socket.sendMessage(sender, { text: listMsg }, { quoted: msg });

            } else if (subCmd === 'random') {
              const text = args.slice(1).join(" ");
              if (!text) return await socket.sendMessage(sender, { text: `❌ Please provide text.\nExample: .logo random My Text` }, { quoted: msg });
              const keys = Object.keys(logoEffects);
              const randKey = keys[Math.floor(Math.random() * keys.length)];
              await socket.sendMessage(sender, { react: { text: "🎲", key: msg.key } });
              await socket.sendMessage(sender, { text: `🎲 Creating random *${randKey}* logo...` }, { quoted: msg });
              try {
                const Photo360 = require('abir-photo360-apis');
                const generator = new Photo360(logoEffects[randKey].url);
                generator.setName(text);
                const result = await generator.execute();
                if (result.status && result.imageUrl) {
                  await socket.sendMessage(sender, { image: { url: result.imageUrl }, caption: `✨ *${randKey}:* ${text}\n🎲 Random Effect\n\n> *〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 ${config.BOT_NAME}*` }, { quoted: msg });
                } else {
                  await socket.sendMessage(sender, { text: `❌ Failed to generate logo.` }, { quoted: msg });
                }
              } catch(apiErr) { await socket.sendMessage(sender, { text: `❌ Logo Error: ${apiErr.message}` }, { quoted: msg }); }

            } else if (subCmd === 'batch') {
              if (args.length < 2) return await socket.sendMessage(sender, { text: `❌ Usage: .logo batch [effect1,effect2] [text]\nExample: .logo batch naruto,neon Hello` }, { quoted: msg });
              const effectsList = args[1].split(',').map(e => e.trim().toLowerCase());
              const text = args.slice(2).join(" ");
              if (!text) return await socket.sendMessage(sender, { text: `❌ Please provide text after effects list.` }, { quoted: msg });
              const valid = effectsList.filter(e => logoEffects[e]);
              const invalid = effectsList.filter(e => !logoEffects[e]);
              if (valid.length === 0) return await socket.sendMessage(sender, { text: `❌ No valid effects. Invalid: ${invalid.join(', ')}` }, { quoted: msg });
              if (invalid.length > 0) await socket.sendMessage(sender, { text: `⚠️ Skipping invalid: ${invalid.join(', ')}` }, { quoted: msg });
              await socket.sendMessage(sender, { text: `🔄 Creating ${valid.length} logos...` }, { quoted: msg });
              let created = 0;
              for (const eff of valid) {
                try {
                  const Photo360 = require('abir-photo360-apis');
                  const generator = new Photo360(logoEffects[eff].url);
                  generator.setName(text);
                  const result = await generator.execute();
                  if (result.status && result.imageUrl) {
                    created++;
                    await socket.sendMessage(sender, { image: { url: result.imageUrl }, caption: `✨ *${eff}:* ${text} (${created}/${valid.length})` }, { quoted: msg });
                  }
                  await new Promise(r => setTimeout(r, 1000));
                } catch(e) { console.error(`Batch logo ${eff} error:`, e.message); }
              }
              await socket.sendMessage(sender, { text: `✅ Created ${created}/${valid.length} logos!` }, { quoted: msg });

            } else if (subCmd === 'search') {
              const term = args.slice(1).join(" ").toLowerCase();
              if (!term) return await socket.sendMessage(sender, { text: `❌ Provide search term.\nExample: .logo search neon` }, { quoted: msg });
              const results = Object.entries(logoEffects).filter(([k, v]) => k.includes(term) || v.desc.toLowerCase().includes(term)).map(([k, v]) => `• .${k} - ${v.desc}`);
              await socket.sendMessage(sender, { text: results.length ? `🔍 *Found ${results.length} effects for "${term}":*\n\n${results.join('\n')}` : `❌ No effects found for "${term}". Use .logo list.` }, { quoted: msg });

            } else if (subCmd === 'info') {
              const effName = args[1] ? args[1].toLowerCase() : '';
              if (!effName || !logoEffects[effName]) return await socket.sendMessage(sender, { text: `❌ Effect "${effName}" not found. Use .logo list.` }, { quoted: msg });
              await socket.sendMessage(sender, { text: `ℹ️ *${effName}*\n📝 ${logoEffects[effName].desc}\n💡 Usage: .${effName} [text]\n📌 Example: .${effName} My Text` }, { quoted: msg });

            } else {
              await socket.sendMessage(sender, { text: `🎨 *Logo Generator Help*\n\n• .[effect] [text] - Create logo\n• .logo list - All effects\n• .logo random [text] - Random effect\n• .logo batch [effects] [text] - Multiple effects\n• .logo search [term] - Search effects\n• .logo info [effect] - Effect info\n\n📌 Example: .naruto Uzumaki` }, { quoted: msg });
            }
          } catch(e) {
            console.error('Logo case error:', e);
            await socket.sendMessage(sender, { text: `❌ Error: ${e.message}` }, { quoted: msg });
          }
          break;
        }

        default:
          break;
      }
    } catch (err) {
      console.error('Command handler error:', err);
      try { await socket.sendMessage(sender, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('❌ ERROR', 'An error occurred while processing your command. Please try again.', BOT_NAME_FANCY) }); } catch(e){}
    }

  });
}

// ---------------- Call Rejection Handler ----------------

// ---------------- Simple Call Rejection Handler ----------------

async function setupCallRejection(socket, sessionNumber) {
    socket.ev.on('call', async (calls) => {
        try {
            // Load user-specific config from MongoDB
            const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
            const userConfig = await loadUserConfigFromMongo(sanitized) || {};
            if (userConfig.ANTI_CALL !== 'on') return;

            console.log(`📞 Incoming call detected for ${sanitized} - Auto rejecting...`);

            for (const call of calls) {
                if (call.status !== 'offer') continue;

                const id = call.id;
                const from = call.from;

                // Reject the call
                await socket.rejectCall(id, from);
                
                // Send rejection message to caller
                await socket.sendMessage(from, {
                    text: '*🔕 Auto call rejection is enabled. Calls are automatically rejected.*'
                });
                
                console.log(`✅ Auto-rejected call from ${from}`);

                // Send notification to bot user
                const userJid = jidNormalizedUser(socket.user.id);
                const rejectionMessage = formatMessage(
                    '📞 CALL REJECTED',
                    `Auto call rejection is active.\n\nCall from: ${from}\nTime: ${getSriLankaTimestamp()}`,
                    BOT_NAME_FANCY
                );

                await socket.sendMessage(userJid, { 
                    image: { url: config.RCD_IMAGE_PATH }, 
                    caption: rejectionMessage 
                });
            }
        } catch (err) {
            console.error(`Call rejection error for ${sessionNumber}:`, err);
        }
    });
}

// ---------------- Auto Message Read Handler ----------------

async function setupAutoMessageRead(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    // Quick return if no need to process
    const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const autoReadSetting = userConfig.AUTO_READ_MESSAGE || 'off';

    if (autoReadSetting === 'off') return;

    const from = msg.key.remoteJid;
    
    // Simple message body extraction
    let body = '';
    try {
      const type = getContentType(msg.message);
      const actualMsg = (type === 'ephemeralMessage') 
        ? msg.message.ephemeralMessage.message 
        : msg.message;

      if (type === 'conversation') {
        body = actualMsg.conversation || '';
      } else if (type === 'extendedTextMessage') {
        body = actualMsg.extendedTextMessage?.text || '';
      } else if (type === 'imageMessage') {
        body = actualMsg.imageMessage?.caption || '';
      } else if (type === 'videoMessage') {
        body = actualMsg.videoMessage?.caption || '';
      }
    } catch (e) {
      // If we can't extract body, treat as non-command
      body = '';
    }

    // Check if it's a command message
    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);

    // Apply auto read rules - SINGLE ATTEMPT ONLY
    if (autoReadSetting === 'all') {
      // Read all messages - one attempt only
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read message (single attempt):', error?.message);
        // Don't retry - just continue
      }
    } else if (autoReadSetting === 'cmd' && isCmd) {
      // Read only command messages - one attempt only
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Command message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read command message (single attempt):', error?.message);
        // Don't retry - just continue
      }
    }
  });
}

// ---------------- message handlers ----------------

function setupMessageHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;
    
    try {
      // Load user-specific config from MongoDB
      let autoTyping = config.AUTO_TYPING; // Default from global config
      let autoRecording = config.AUTO_RECORDING; // Default from global config
      
      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};
        
        // Check for auto typing in user config
        if (userConfig.AUTO_TYPING !== undefined) {
          autoTyping = userConfig.AUTO_TYPING;
        }
        
        // Check for auto recording in user config
        if (userConfig.AUTO_RECORDING !== undefined) {
          autoRecording = userConfig.AUTO_RECORDING;
        }
      }

      // Use auto typing setting (from user config or global)
      if (autoTyping === 'true') {
        try { 
          await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
          // Stop typing after 3 seconds
          setTimeout(async () => {
            try {
              await socket.sendPresenceUpdate('paused', msg.key.remoteJid);
            } catch (e) {}
          }, 3000);
        } catch (e) {
          console.error('Auto typing error:', e);
        }
      }
      
      // Use auto recording setting (from user config or global)
      if (autoRecording === 'true') {
        try { 
          await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
          // Stop recording after 3 seconds  
          setTimeout(async () => {
            try {
              await socket.sendPresenceUpdate('paused', msg.key.remoteJid);
            } catch (e) {}
          }, 3000);
        } catch (e) {
          console.error('Auto recording error:', e);
        }
      }
    } catch (error) {
      console.error('Message handler error:', error);
    }
  });
}


// ---------------- cleanup helper ----------------

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
    activeSockets.delete(sanitized); socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch(e){}
    try { await removeNumberFromMongo(sanitized); } catch(e){}
    try {
      const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
      const caption = formatMessage('*🥷 OWNER NOTICE — SESSION REMOVED*', `*𝐍umber:* ${sanitized}\n*𝐒ession 𝐑emoved 𝐃ue 𝐓o 𝐋ogout.*\n\n*𝐀ctive 𝐒essions 𝐍ow:* ${activeSockets.size}`, BOT_NAME_FANCY);
      if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
    } catch(e){}
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ---------------- auto-restart ----------------

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
                         || lastDisconnect?.error?.statusCode
                         || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
                          || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
                          || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
                          || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch(e){ console.error(e); }
      } else {
        console.log(`Connection closed for ${number} (not logout). Attempt reconnect...`);
        try { await delay(10000); activeSockets.delete(number.replace(/[^0-9]/g,'')); socketCreationTime.delete(number.replace(/[^0-9]/g,'')); const mockRes = { headersSent:false, send:() => {}, status: () => mockRes }; await EmpirePair(number, mockRes); } catch(e){ console.error('Reconnect attempt failed', e); }
      }

    }

  });
}

// ---------------- EmpirePair (pairing, temp dir, persist to Mongo) ----------------


// ---------------- EmpirePair (pairing, temp dir, persist to Mongo) ----------------

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  await initMongo().catch(()=>{});
  
  // Prefill from Mongo if available
  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

  try {
    const { version } = await fetchLatestWaWebVersion();
    const socket = makeWASocket({
      auth: { creds: state.creds, keys: makeCacheableSignalKeyStore(state.keys, logger) },
      printQRInTerminal: false,
      logger,
      version,
      // 🛠️ FIX: Updated browser string & dynamic WA version to fix connection rejection
      browser: Browsers.ubuntu('Chrome')
    });

    socketCreationTime.set(sanitizedNumber, Date.now());

    // ────────────────────────────────────────────────────
    // 📌 ASHIYA-MD Header Injector
    // හැම bot message එකකම උඩට newsletter header එක add කරනවා
    // ────────────────────────────────────────────────────
    const _originalSendMessage = socket.sendMessage.bind(socket);
    socket.sendMessage = async (jid, content, options = {}) => {
        try {
            // Only inject on outgoing bot messages (not reactions, not deletes, not status)
            const isReaction = content?.react !== undefined;
            const isDelete   = content?.delete !== undefined;
            const isStatus   = jid === 'status@broadcast';

            if (!isReaction && !isDelete && !isStatus) {
                // Ping-style contact card header — inject on ALL bot messages
                options.quoted = {
                    key: {
                        remoteJid: "status@broadcast",
                        participant: "0@s.whatsapp.net",
                        fromMe: false,
                        id: "META_AI_SYSTEM"
                    },
                    message: {
                        contactMessage: {
                            displayName: config.BOT_NAME || BOT_NAME_FANCY,
                            vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${config.BOT_NAME || BOT_NAME_FANCY};;;;\nFN:${config.BOT_NAME || BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD`
                        }
                    }
                };
            }
        } catch (e) {
            // Header injection failed — still send the message normally
            console.warn('Header inject warn:', e?.message);
        }
        return _originalSendMessage(jid, content, options);
    };
    // ────────────────────────────────────────────────────

    setupStatusHandlers(socket, sanitizedNumber);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket, sanitizedNumber);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);
    setupAutoMessageRead(socket, sanitizedNumber);
    setupCallRejection(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
     let dina = `ISHANXMD`;
     
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber, dina); break; }
        
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        
        const credsPath = path.join(sessionPath, 'creds.json');
        
        if (!fs.existsSync(credsPath)) return;
        const fileStats = fs.statSync(credsPath);
        if (fileStats.size === 0) return;
        
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        
        let credsObj;
        try { credsObj = JSON.parse(trimmedContent); } catch (e) { return; }
        
        if (!credsObj || typeof credsObj !== 'object') return;
        
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
        console.log('✅ Creds saved to MongoDB successfully');
        
      } catch (err) { 
        console.error('Failed saving creds on creds.update:', err);
      }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(()=>({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); } catch(e){}
            }
          } catch(e){}

          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error}`;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `*✅ 𝐒uccessfully 𝐂onnected*\n\n*🔢 𝐍umber:* ${sanitizedNumber}\n*🕒 𝐂onnecting: Bot will become active in a few seconds*`,
            useBotName
          );

          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch(e){}
          }

          await delay(4000);

          const updatedCaption = formatMessage(useBotName,
`𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 4.0.0𝗩 ᴄᴏɴɴᴇᴄᴛᴇᴅ ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ 🥷🇱🇰\n*• \`ᴠᴇʀꜱɪᴏɴ\` : ᴠ4.0.0*\n*• \`ʙᴏᴛ ᴄᴏɴɴᴇᴄᴛ ɴʙ\` : ${number}*\n*• \`ᴘᴏᴡᴇʀᴇᴅ ʙʏ\` : 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰*\n\n*•Hy Hy 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 වේත ඔයාව සාදරයෙන් පිලිගන්නවා.......🥹❤️‍🩹*\n\n_*ඉතිම් ලස්සන ලමයො 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 𝗠𝗜𝗡𝗜 𝗕𝗢𝗧 ගැන ඔයාලාට තියේන අදහස් අනිවාරෙන් කියන්න ඔනේ හරිද 🌚💗*_\n\n*🌐 ᴡᴇʙ ꜱɪᴛᴇ :*\n> https://ashiya-md-v4-mini-bot.vercel.app/`,
                            '〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰',
          );

          const connectedButtons = [
			{ buttonId: `${config.PREFIX}help`, buttonText: { displayText: "👩‍💻 𝙃𝙀𝙇𝙋" }, type: 1 },
            { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '👋 𝘼𝙇𝙄𝙑𝙀' }, type: 1 },
            { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📑 𝙈𝙀𝙉𝙐' }, type: 1 }
          ];
          const connectedFooter = '〠 𝐏𝙾𝚆𝙴𝚁𝙴𝙳 𝗕𝗬 𝐀𝚂𝙷𝙸𝚈𝙰-𝐌𝙳 𝐕.4 🥷🇱🇰';

          try {
            if (sentMsg && sentMsg.key) {
              try { await socket.sendMessage(userJid, { delete: sentMsg.key }); } catch (delErr) {}
            }
            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption, footer: connectedFooter, buttons: connectedButtons, headerType: 4 });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption, footer: connectedFooter, buttons: connectedButtons, headerType: 4 });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption, footer: connectedFooter, buttons: connectedButtons, headerType: 1 });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption, footer: connectedFooter, buttons: connectedButtons, headerType: 1 });
            }
          } catch (e) {}


          await addNumberToMongo(sanitizedNumber);

        } catch (e) { 
          console.error('Connection open error:', e); 
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`); } catch(e) {}
        }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
      }
    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}


// ---------------- endpoints (admin/newsletter management + others) ----------------

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// admin endpoints

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// existing endpoints (connect, reconnect, active, etc.)

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});


router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});


router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: '✦ ━━ ᴅᴄᴛ ɴᴏᴠᴀ X ᴍᴅ ━━ ✦', activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});


router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});


router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});


router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});


router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target number are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    const aboutStatus = statusData.status || 'No status available';
    const setAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
    res.status(200).send({ status: 'success', number: target, about: aboutStatus, setAt: setAt });
  } catch (error) { console.error(`Failed to fetch status for ${target}:`, error); res.status(500).send({ status: 'error', message: `Failed to fetch About status for ${target}.` }); }
});


// ---------------- Dashboard endpoints & static ----------------

const dashboardStaticDir = path.join(__dirname, 'dashboard_static');
if (!fs.existsSync(dashboardStaticDir)) fs.ensureDirSync(dashboardStaticDir);
router.use('/dashboard/static', express.static(dashboardStaticDir));
router.get('/dashboard', async (req, res) => {
  res.sendFile(path.join(dashboardStaticDir, 'index.html'));
});


// API: sessions & active & delete

router.get('/api/sessions', async (req, res) => {
  try {
    await initMongo();
    const docs = await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, sessions: docs });
  } catch (err) {
    console.error('API /api/sessions error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(()=>{}); } catch(e){}
      try { running.ws?.close(); } catch(e){}
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized}`); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch(e){}
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    console.error('API /api/session/delete error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});
router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


// ---------------- cleanup + process events ----------------

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch(e){}
  });
});


process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  try { exec(`pm2.restart ${process.env.PM2_NAME || 'Dtz-Nova-main'}`); } catch(e) { console.error('Failed to restart pm2:', e); }
});


// initialize mongo & auto-reconnect attempt

initMongo().catch(err => console.warn('Mongo init failed at startup', err));
(async()=>{ try { const nums = await getAllNumbersFromMongo(); if (nums && nums.length) { for (const n of nums) { if (!activeSockets.has(n)) { const mockRes = { headersSent:false, send:()=>{}, status:()=>mockRes }; await EmpirePair(n, mockRes); await delay(500); } } } } catch(e){} })();

module.exports = router;


