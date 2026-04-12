const Database = require("better-sqlite3");
const Path = require("path");
const config = require("./config");
const Log = require("./log");

// Configuration
const MAX_ALERTS = config.WAZE_MAX_ALERTS;
const AREA_TOP = config.WAZE_AREA_TOP;
const AREA_BOTTOM = config.WAZE_AREA_BOTTOM;
const AREA_LEFT = config.WAZE_AREA_LEFT;
const AREA_RIGHT = config.WAZE_AREA_RIGHT;
const QUERY_DELAY_MS = config.WAZE_QUERY_DELAY_MS;
const CACHE_DIR_PATH = config.HEATMAP_CACHE_DIR_PATH;
const DB_FILENAME = config.DB_FILENAME;

// Database
const dbPath = Path.join(CACHE_DIR_PATH, DB_FILENAME);
const db = new Database(dbPath);
db.pragma("journal_mode = WAL");
db.exec(`CREATE TABLE IF NOT EXISTS alerts (uuid TEXT PRIMARY KEY, pubMillis INTEGER, latitude REAL, longitude REAL, confidence INTEGER, reliability INTEGER)`);
db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_pubMillis ON alerts (pubMillis)`);

// Helpers
function Area(top, bottom, left, right) {
  this.top = top;
  this.bottom = bottom;
  this.left = left;
  this.right = right;
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function getData(top, bottom, left, right) {
  const retryDelays = [5000, 10000, 30000];
  let attempts = 0;

  const url = `https://www.waze.com/live-map/api/georss?top=${top}&bottom=${bottom}&left=${left}&right=${right}&env=row&types=alerts`;

  while (attempts <= retryDelays.length) {
    try {
      const response = await fetch(url, {
        method: "GET",
        // High-fidelity browser headers
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
          Accept: "application/json, text/plain, */*",
          "Accept-Language": "en-US,en;q=0.9",
          Referer: "https://www.waze.com/live-map/",
          "Sec-Fetch-Dest": "empty",
          "Sec-Fetch-Mode": "cors",
          "Sec-Fetch-Site": "same-origin",
          "sec-ch-ua": '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
          "sec-ch-ua-mobile": "?0",
          "sec-ch-ua-platform": '"Windows"',
        },
        // undici specific: signal to keep the connection alive
        dispatcher: undefined,
      });

      if (response.ok) {
        return await response.json();
      }

      if (response.status === 403) {
        if (attempts < retryDelays.length) {
          const delay = retryDelays[attempts];
          Log.warn(`Waze API 403 Forbidden. Retrying in ${delay / 1000}s... (Attempt ${attempts + 1}/${retryDelays.length})`);
          await sleep(delay);
          attempts++;
          continue;
        } else {
          Log.error(`API Request Failed: Max retries reached (403 Forbidden) for area T:${top},B:${bottom}.`);
          return null;
        }
      }

      // Handle other HTTP errors (404, 500, etc)
      Log.error(`API Request Failed: Status ${response.status}`);
      return null;
    } catch (error) {
      Log.error(`Network Error: ${error.message}. Skipping chunk.`);
      return null;
    }
  }
  return null;
}

function splitData(top, bottom, left, right) {
  const midVertical = left + (right - left) / 2;
  const midHorizontal = bottom + (top - bottom) / 2;
  return [new Area(top, midHorizontal, left, midVertical), new Area(top, midHorizontal, midVertical, right), new Area(midHorizontal, bottom, left, midVertical), new Area(midHorizontal, bottom, midVertical, right)];
}

const insertAlertStmt = db.prepare(`INSERT OR IGNORE INTO alerts (uuid, pubMillis, latitude, longitude, confidence, reliability) VALUES (?, ?, ?, ?, ?, ?)`);

function usePoliceData(data) {
  if (!data || !data.alerts || !Array.isArray(data.alerts)) return;
  let policeCount = 0;
  for (const alert of data.alerts) {
    if (alert.type === "POLICE") {
      insertAlertStmt.run(alert.uuid, alert.pubMillis, alert.location.y, alert.location.x, alert.confidence, alert.reliability);
      policeCount++;
    }
  }
  if (policeCount > 0) {
    Log.info(`Stored ${policeCount} police alerts`);
  }
}

async function fetchWazeAlerts() {
  Log.info(`Starting Waze alerts fetch for area: T:${AREA_TOP}, B:${AREA_BOTTOM}, L:${AREA_LEFT}, R:${AREA_RIGHT}`);
  const queue = [];
  queue.push(new Area(AREA_TOP, AREA_BOTTOM, AREA_LEFT, AREA_RIGHT));
  let areasProcessed = 0;
  let areasSplit = 0;

  while (queue.length > 0) {
    const currentArea = queue.pop();
    const data = await getData(currentArea.top, currentArea.bottom, currentArea.left, currentArea.right);

    if (!data) continue; // getData now returns null only after exhaustion or non-403 errors

    if (data.error) {
      Log.error(`Waze API Error structure: ${JSON.stringify(data.error)}`);
      continue;
    }
    if (!data.alerts || !Array.isArray(data.alerts)) continue;

    if (data.alerts.length >= MAX_ALERTS) {
      queue.push(...splitData(currentArea.top, currentArea.bottom, currentArea.left, currentArea.right));
      areasSplit++;
      Log.info(`Area split due to ${data.alerts.length} alerts (>= ${MAX_ALERTS}). Queue size: ${queue.length}`);
    } else {
      usePoliceData(data);
      areasProcessed++;
    }

    if (QUERY_DELAY_MS > 0 && queue.length > 0) {
      await sleep(QUERY_DELAY_MS);
    }
  }

  Log.info(`Waze fetch completed. Areas processed: ${areasProcessed}, Areas split: ${areasSplit}`);
}

module.exports = { fetchWazeAlerts };
