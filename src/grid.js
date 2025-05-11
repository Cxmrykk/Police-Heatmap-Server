const Database = require("better-sqlite3");
const Path = require("path");
const config = require("./config");

// Core configuration
const PRECISION = { MAX: 5, MIN: 0 };
const DAY_MS = 86400000; // 24h in milliseconds
const CACHE_DIR = config.HEATMAP_CACHE_DIR_PATH;
const DB_FILE = config.DB_FILENAME;

// Time window definitions
const TIME_WINDOWS = [
  { id: 0, name: "last 7 days", filter: (now) => `pubMillis >= ${now - 7 * DAY_MS}` },
  { id: 1, name: "7-14 days ago", filter: (now) => `pubMillis < ${now - 7 * DAY_MS} AND pubMillis >= ${now - 14 * DAY_MS}` },
  { id: 2, name: "14-30 days ago", filter: (now) => `pubMillis < ${now - 14 * DAY_MS} AND pubMillis >= ${now - 30 * DAY_MS}` },
  { id: 3, name: "30-90 days ago", filter: (now) => `pubMillis < ${now - 30 * DAY_MS} AND pubMillis >= ${now - 90 * DAY_MS}` },
];

// Coordinate processing functions
function scaleCoordinate(coord, precision) {
  if (!coord || typeof coord !== "number" || isNaN(coord)) return null;
  return Math.trunc(coord * 10 ** precision);
}

function normalize(value, min, max, scale = 255) {
  if (max === min) return min > 0 ? scale : 0;
  if (value === null || isNaN(value)) return 0;
  return Math.round(((value - min) / (max - min)) * scale);
}

function getMinMax(countsMap) {
  let min = Infinity,
    max = -Infinity;
  let hasValues = false;

  for (const val of countsMap.values()) {
    if (val === null || isNaN(val)) continue;
    hasValues = true;
    min = Math.min(min, val);
    max = Math.max(max, val);
  }

  return hasValues ? { min, max } : { min: 0, max: 0 };
}

function initializeDatabase(db) {
  // Create alerts table if it doesn't exist
  db.exec(`
    CREATE TABLE IF NOT EXISTS alerts (
      uuid TEXT PRIMARY KEY, 
      pubMillis INTEGER, 
      latitude REAL, 
      longitude REAL, 
      confidence INTEGER, 
      reliability INTEGER
    )
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_pubMillis ON alerts (pubMillis)`);

  // Recreate density_grids table
  db.exec(`DROP TABLE IF EXISTS density_grids`);
  db.exec(`
    CREATE TABLE density_grids (
      time_window_id INTEGER NOT NULL, 
      level INTEGER NOT NULL, 
      lon_scaled INTEGER NOT NULL, 
      lat_scaled INTEGER NOT NULL, 
      density INTEGER NOT NULL,
      PRIMARY KEY (time_window_id, level, lon_scaled, lat_scaled)
    )
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_density_grids_coords ON density_grids (time_window_id, level, lon_scaled, lat_scaled)`);
}

function processTimeWindow(db, window, now) {
  console.log(`Processing time window: ${window.name} (ID: ${window.id})`);

  const reports = db.prepare(`SELECT longitude, latitude FROM alerts WHERE ${window.filter(now)}`).all();

  if (reports.length === 0) {
    console.log(`  No alert data for window ${window.id}. Skipping.`);
    return;
  }

  console.log(`  Found ${reports.length} reports for window ${window.id}.`);
  const validReports = reports.filter((r) => r.longitude != null && r.latitude != null && !isNaN(r.longitude) && !isNaN(r.latitude));

  return validReports;
}

function processPrecisionLevel(reports, level, windowId) {
  console.log(`  Processing precision level ${level} for window ${windowId}...`);
  const cellCounts = new Map();

  // Aggregate alerts into tiles
  for (const report of reports) {
    const lonScaled = scaleCoordinate(report.longitude, level);
    const latScaled = scaleCoordinate(report.latitude, level);

    if (lonScaled === null || latScaled === null) continue;

    const key = `${lonScaled}_${latScaled}`;
    cellCounts.set(key, (cellCounts.get(key) || 0) + 1);
  }

  if (cellCounts.size === 0) {
    console.log(`    No cells with data for level ${level}, window ${windowId}. Skipping.`);
    return null;
  }

  console.log(`    Level ${level} (window ${windowId}): Found ${cellCounts.size} unique cells with alert data.`);
  return cellCounts;
}

function prepareInserts(cellCounts, level, windowId) {
  const { min, max } = getMinMax(cellCounts);
  const inserts = [];

  cellCounts.forEach((count, key) => {
    const [lonStr, latStr] = key.split("_");
    inserts.push({
      time_window_id: windowId,
      level,
      lon_scaled: Number(lonStr),
      lat_scaled: Number(latStr),
      density: normalize(count, min, max),
    });
  });

  return inserts;
}

async function generateHeatmapData() {
  const db = new Database(Path.join(CACHE_DIR, DB_FILE));
  initializeDatabase(db);

  const insertStmt = db.prepare(`
    INSERT INTO density_grids (time_window_id, level, lon_scaled, lat_scaled, density)
    VALUES (?, ?, ?, ?, ?)
  `);

  const bulkInsert = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(item.time_window_id, item.level, item.lon_scaled, item.lat_scaled, item.density);
    }
  });

  const now = Date.now();

  for (const window of TIME_WINDOWS) {
    const validReports = processTimeWindow(db, window, now);
    if (!validReports) continue;

    for (let level = PRECISION.MAX; level >= PRECISION.MIN; level--) {
      const cellCounts = processPrecisionLevel(validReports, level, window.id);
      if (!cellCounts) continue;

      const inserts = prepareInserts(cellCounts, level, window.id);

      try {
        bulkInsert(inserts);
        console.log(`    Successfully inserted ${inserts.length} density records for level ${level}, window ${window.id}.`);
      } catch (error) {
        console.error(`    Error inserting data for window ${window.id}, level ${level}:`, error);
      }
    }
  }

  db.close();
  console.log("Heatmap data generation complete.");
}

module.exports = { updateGrids: generateHeatmapData };
