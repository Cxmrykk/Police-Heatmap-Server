const Database = require("better-sqlite3");
const Path = require("path");
const Axios = require("axios");
const config = require("./config");
const Log = require("./log");

// Configuration (now from config module)
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

async function getData(top, bottom, left, right) {
  try {
    const response = await Axios.get(`https://www.waze.com/live-map/api/georss?top=${top}&bottom=${bottom}&left=${left}&right=${right}&env=row&types=alerts`);
    return response.data;
  } catch (error) {
    Log.error(`API Request Error: ${error.message} for area T:${top},B:${bottom},L:${left},R:${right}`);
    return null;
  }
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
    if (!data) continue;
    if (data.error) {
      Log.error(`Waze API Error: ${data.error}`);
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
      await new Promise((resolve) => setTimeout(resolve, QUERY_DELAY_MS));
    }
  }

  Log.info(`Waze fetch completed. Areas processed: ${areasProcessed}, Areas split: ${areasSplit}`);
}

module.exports = { fetchWazeAlerts };
