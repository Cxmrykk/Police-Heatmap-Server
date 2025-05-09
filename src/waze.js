const Database = require("better-sqlite3");
const Path = require("path");
const Axios = require("axios");

// Configuration
const MAX_ALERTS = parseInt(process.env.WAZE_MAX_ALERTS);
const AREA_TOP = parseFloat(process.env.WAZE_AREA_TOP);
const AREA_BOTTOM = parseFloat(process.env.WAZE_AREA_BOTTOM);
const AREA_LEFT = parseFloat(process.env.WAZE_AREA_LEFT);
const AREA_RIGHT = parseFloat(process.env.WAZE_AREA_RIGHT);
const QUERY_DELAY_MS = parseInt(process.env.WAZE_QUERY_DELAY_MS);
const CACHE_DIR_PATH = process.env.HEATMAP_CACHE_DIR_PATH;

if (!CACHE_DIR_PATH || [AREA_TOP, AREA_BOTTOM, AREA_LEFT, AREA_RIGHT, MAX_ALERTS, QUERY_DELAY_MS].some(isNaN)) {
  console.error("FATAL: Missing or invalid required environment variables. Ensure WAZE_MAX_ALERTS, WAZE_AREA_TOP, WAZE_AREA_BOTTOM, WAZE_AREA_LEFT, WAZE_AREA_RIGHT, WAZE_QUERY_DELAY_MS, HEATMAP_CACHE_DIR_PATH are set correctly.");
  process.exit(1);
}

// Database
const db = new Database(Path.join(CACHE_DIR_PATH, "alerts.db"));
db.pragma("journal_mode = WAL");
db.exec(`CREATE TABLE IF NOT EXISTS alerts (uuid TEXT PRIMARY KEY, pubMillis INTEGER, latitude REAL, longitude REAL, confidence INTEGER, reliability INTEGER)`);

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
    console.error(`API Request Error: ${error.message} for area T:${top},B:${bottom},L:${left},R:${right}`);
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
  for (const alert of data.alerts) {
    if (alert.type === "POLICE") {
      insertAlertStmt.run(alert.uuid, alert.pubMillis, alert.location.y, alert.location.x, alert.confidence, alert.reliability);
    }
  }
}

// Fetches the Waze alerts and adds them to the database
async function fetchWazeAlerts() {
  const queue = [];
  queue.push(new Area(AREA_TOP, AREA_BOTTOM, AREA_LEFT, AREA_RIGHT));

  while (queue.length > 0) {
    const currentArea = queue.pop();
    const data = await getData(currentArea.top, currentArea.bottom, currentArea.left, currentArea.right);
    if (!data) continue;
    if (data.error) {
      console.error(`Waze API Error: ${data.error}`);
      continue;
    }
    if (!data.alerts || !Array.isArray(data.alerts)) continue;

    if (data.alerts.length >= MAX_ALERTS) {
      queue.push(...splitData(currentArea.top, currentArea.bottom, currentArea.left, currentArea.right));
    } else {
      usePoliceData(data);
    }

    if (QUERY_DELAY_MS > 0 && queue.length > 0) {
      await new Promise((resolve) => setTimeout(resolve, QUERY_DELAY_MS));
    }
  }
}

module.exports = { fetchWazeAlerts };
