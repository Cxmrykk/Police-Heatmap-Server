const Database = require("better-sqlite3");
const Path = require("path");

// Core configuration
const PRECISION = { MAX: 5, MIN: 0 };
const DAY_MS = 86400000; // 24h in milliseconds
const CACHE_DIR = process.env.HEATMAP_CACHE_DIR_PATH;
const DB_FILE = "alerts.sqlite";

// Time window definitions
const TIME_WINDOWS = [
  { id: 0, name: "last 7 days", filter: (now) => `pubMillis >= ${now - 7 * DAY_MS}` },
  { id: 1, name: "7-14 days ago", filter: (now) => `pubMillis < ${now - 7 * DAY_MS} AND pubMillis >= ${now - 14 * DAY_MS}` },
  { id: 2, name: "14-30 days ago", filter: (now) => `pubMillis < ${now - 14 * DAY_MS} AND pubMillis >= ${now - 30 * DAY_MS}` },
  { id: 3, name: "older than 30 days", filter: (now) => `pubMillis < ${now - 30 * DAY_MS}` },
];

// Coordinate processing functions
function scaleCoordinate(coord, precision) {
  if (coord === null || isNaN(coord)) return null;
  return Math.trunc(coord * Math.pow(10, precision));
}

function normalize(value, min, max, scale = 255) {
  if (max === min) return min > 0 ? scale : 0;
  return Math.round(((value - min) / (max - min)) * scale);
}

function getMinMax(values) {
  let min = Infinity,
    max = -Infinity;
  let hasValues = false;

  for (const val of values.values()) {
    if (val === null || isNaN(val)) continue;
    hasValues = true;
    min = Math.min(min, val);
    max = Math.max(max, val);
  }

  return hasValues ? { min, max } : { min: 0, max: 0 };
}

async function generateHeatmapData() {
  if (!CACHE_DIR) {
    console.error("FATAL: HEATMAP_CACHE_DIR_PATH environment variable is not set.");
    process.exit(1);
  }

  const db = new Database(Path.join(CACHE_DIR, DB_FILE));
  initializeDatabase(db);

  const insert = db.prepare(`
    INSERT INTO density_grids (time_window_id, level, lon_scaled, lat_scaled, density)
    VALUES (?, ?, ?, ?, ?)
  `);

  const bulkInsert = db.transaction((items) => {
    for (const item of items) {
      insert.run(item.time_window_id, item.level, item.lon_scaled, item.lat_scaled, item.density);
    }
  });

  const now = Date.now();

  for (const window of TIME_WINDOWS) {
    console.log(`Processing: ${window.name} (ID: ${window.id})`);

    const reports = db.prepare(`SELECT longitude, latitude FROM alerts WHERE ${window.filter(now)}`).all();

    if (reports.length === 0) {
      console.log(`No data for window ${window.id}. Skipping.`);
      continue;
    }

    console.log(`Found ${reports.length} reports for window ${window.id}.`);

    // Process highest precision level first
    console.log(`Processing precision level ${PRECISION.MAX}...`);
    const highestPrecisionCounts = processMaxPrecisionLevel(reports, window.id, bulkInsert);

    if (!highestPrecisionCounts || highestPrecisionCounts.size === 0) {
      console.log(`No data generated for level ${PRECISION.MAX}.`);
      continue;
    }

    console.log(`Level ${PRECISION.MAX} complete: ${highestPrecisionCounts.size} cells.`);

    // Process lower precision levels
    let previousLevelData = highestPrecisionCounts;

    for (let level = PRECISION.MAX - 1; level >= PRECISION.MIN; level--) {
      console.log(`Processing precision level ${level}...`);

      previousLevelData = aggregateToLowerPrecision(previousLevelData, level, window.id, bulkInsert);

      if (!previousLevelData || previousLevelData.size === 0) {
        console.log(`No data for level ${level}. Stopping for this window.`);
        break;
      }

      console.log(`Level ${level} complete: ${previousLevelData.size} cells.`);
    }
  }

  db.close();
}

function initializeDatabase(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS alerts (
    uuid TEXT PRIMARY KEY, 
    pubMillis INTEGER, 
    latitude REAL, 
    longitude REAL, 
    confidence INTEGER, 
    reliability INTEGER
  )`);

  db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_pubMillis ON alerts (pubMillis)`);
  db.exec(`DROP TABLE IF EXISTS density_grids;`);

  db.exec(`
    CREATE TABLE density_grids (
      time_window_id INTEGER NOT NULL, 
      level INTEGER NOT NULL, 
      lon_scaled INTEGER NOT NULL, 
      lat_scaled INTEGER NOT NULL, 
      density INTEGER NOT NULL,
      PRIMARY KEY (time_window_id, level, lon_scaled, lat_scaled)
    );
  `);

  db.exec(`CREATE INDEX IF NOT EXISTS idx_density_grids_coords ON density_grids (time_window_id, level, lon_scaled, lat_scaled);`);
}

function processMaxPrecisionLevel(reports, windowId, bulkInsert) {
  const cellCounts = new Map();

  // Count reports per cell
  for (const report of reports) {
    if (!report.longitude || !report.latitude || isNaN(report.longitude) || isNaN(report.latitude)) continue;

    const lon = scaleCoordinate(report.longitude, PRECISION.MAX);
    const lat = scaleCoordinate(report.latitude, PRECISION.MAX);

    if (lon === null || lat === null) continue;

    const key = `${lon}_${lat}`;
    cellCounts.set(key, (cellCounts.get(key) || 0) + 1);
  }

  if (cellCounts.size === 0) return null;

  // Normalize counts for storage
  const { max } = getMinMax(cellCounts);
  const inserts = [];

  cellCounts.forEach((count, key) => {
    const [lon, lat] = key.split("_").map(Number);
    const density = normalize(count, 0, max);

    inserts.push({
      time_window_id: windowId,
      level: PRECISION.MAX,
      lon_scaled: lon,
      lat_scaled: lat,
      density,
    });
  });

  try {
    if (inserts.length > 0) bulkInsert(inserts);
  } catch (error) {
    console.error(`Error inserting data for window ${windowId}:`, error);
    throw error;
  }

  return cellCounts;
}

function aggregateToLowerPrecision(higherLevelData, currentLevel, windowId, bulkInsert) {
  // Aggregate counts from higher precision to current level
  const aggregatedCounts = new Map();

  higherLevelData.forEach((count, key) => {
    const [lon, lat] = key.split("_").map(Number);

    const parentLon = Math.trunc(lon / 10);
    const parentLat = Math.trunc(lat / 10);
    const parentKey = `${parentLon}_${parentLat}`;

    aggregatedCounts.set(parentKey, (aggregatedCounts.get(parentKey) || 0) + count);
  });

  if (aggregatedCounts.size === 0) return null;

  // Get non-zero counts for normalization
  const nonZeroCounts = new Map();
  for (const [key, count] of aggregatedCounts) {
    if (count > 0) nonZeroCounts.set(key, count);
  }

  // Normalize and prepare for storage
  const { min, max } = getMinMax(nonZeroCounts);
  const inserts = [];

  aggregatedCounts.forEach((count, key) => {
    const [lon, lat] = key.split("_").map(Number);
    const density = count > 0 ? normalize(count, min, max) : 0;

    inserts.push({
      time_window_id: windowId,
      level: currentLevel,
      lon_scaled: lon,
      lat_scaled: lat,
      density,
    });
  });

  try {
    if (inserts.length > 0) bulkInsert(inserts);
  } catch (error) {
    console.error(`Error inserting data for window ${windowId}, level ${currentLevel}:`, error);
    throw error;
  }

  return aggregatedCounts;
}

module.exports = { updateGrids: generateHeatmapData };
