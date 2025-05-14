const Database = require("better-sqlite3");
const Path = require("path");
const config = require("./config");

// Constants
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

// Helper functions
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
  let min = Infinity, max = -Infinity;
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
  db.exec(`DROP TABLE IF EXISTS density_grids`);
  db.exec(`
    CREATE TABLE density_grids (
      time_window_id INTEGER NOT NULL, 
      level INTEGER NOT NULL, 
      lon_scaled INTEGER NOT NULL, 
      lat_scaled INTEGER NOT NULL, 
      density INTEGER NOT NULL,
      density_scaled INTEGER NOT NULL,
      PRIMARY KEY (time_window_id, level, lon_scaled, lat_scaled)
    )
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_density_grids_coords ON density_grids (time_window_id, level, lon_scaled, lat_scaled)`);
}

function processTimeWindow(db, window, now) {
  console.log(`Processing time window: ${window.name} (ID: ${window.id})`);
  const reports = db.prepare(`SELECT longitude, latitude FROM alerts WHERE ${window.filter(now)}`).all();

  if (reports.length === 0) {
    console.log(`No alert data for window ${window.id}. Skipping density generation for this window.`);
    return null;
  }

  console.log(`Found ${reports.length} reports for window ${window.id}.`);
  const validReports = reports.filter(r => 
    r.longitude != null && r.latitude != null && !isNaN(r.longitude) && !isNaN(r.latitude)
  );

  if (validReports.length === 0) {
    console.log(`No valid (lon/lat) alert data for window ${window.id} after filtering. Skipping density generation.`);
    return null;
  }
  
  return validReports;
}

function getReportCountForWindow(db, window, now) {
  const result = db.prepare(`SELECT COUNT(*) as count FROM alerts WHERE ${window.filter(now)}`).get();
  return result ? result.count : 0;
}

function processPrecisionLevel(reports, level, windowId) {
  console.log(`Processing precision level ${level} for window ${windowId}...`);
  const cellCounts = new Map();
  
  for (const report of reports) {
    const lonScaled = scaleCoordinate(report.longitude, level);
    const latScaled = scaleCoordinate(report.latitude, level);
    
    if (lonScaled === null || latScaled === null) continue;
    
    const key = `${lonScaled}_${latScaled}`;
    cellCounts.set(key, (cellCounts.get(key) || 0) + 1);
  }

  if (cellCounts.size === 0) {
    console.log(`No cells with data for level ${level}, window ${windowId}. Skipping.`);
    return null;
  }
  
  console.log(`Level ${level} (window ${windowId}): Found ${cellCounts.size} unique cells with alert data.`);
  return cellCounts;
}

function prepareInserts(cellCounts, level, windowId, multiplier) {
  const logScaledCounts = new Map();
  
  cellCounts.forEach((count, key) => {
    logScaledCounts.set(key, Math.log1p(count));
  });

  const { min, max } = getMinMax(logScaledCounts);
  const inserts = [];

  cellCounts.forEach((originalCount, key) => {
    const [lonStr, latStr] = key.split("_");
    const logScaledValue = logScaledCounts.get(key);
    const density = normalize(logScaledValue, min, max);
    
    let densityScaled = Math.round(density * multiplier);
    densityScaled = Math.max(0, Math.min(255, densityScaled));

    if (density > 0) {
      inserts.push({
        time_window_id: windowId,
        level,
        lon_scaled: Number(lonStr),
        lat_scaled: Number(latStr),
        density,
        density_scaled: densityScaled
      });
    }
  });
  
  return inserts;
}

async function generateHeatmapData() {
  const db = new Database(Path.join(CACHE_DIR, DB_FILE));
  initializeDatabase(db);

  const insertStmt = db.prepare(`
    INSERT INTO density_grids (time_window_id, level, lon_scaled, lat_scaled, density, density_scaled)
    VALUES (?, ?, ?, ?, ?, ?)
  `);
  
  const bulkInsert = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(
        item.time_window_id, 
        item.level, 
        item.lon_scaled, 
        item.lat_scaled, 
        item.density, 
        item.density_scaled
      );
    }
  });

  const now = Date.now();

  // Calculate report counts and multipliers
  console.log("Calculating report counts per time window for global scaling...");
  const windowReportCounts = new Map();
  let totalAlerts = 0;
  
  for (const window of TIME_WINDOWS) {
    const count = getReportCountForWindow(db, window, now);
    windowReportCounts.set(window.id, count);
    totalAlerts += count;
    console.log(`Reports for window ${window.id} (${window.name}): ${count}`);
  }
  console.log(`Total alerts across all windows: ${totalAlerts}`);

  // Calculate multipliers
  const windowMultipliers = new Map();
  for (const window of TIME_WINDOWS) {
    const count = windowReportCounts.get(window.id) || 0;
    let multiplier = 1.0;
    
    if (totalAlerts > 0 && count > 0) {
      const activeWindows = TIME_WINDOWS.filter(tw => 
        (windowReportCounts.get(tw.id) || 0) > 0
      ).length;
      
      if (count !== totalAlerts || activeWindows > 1) {
        multiplier = 1 - count / totalAlerts;
        multiplier = Math.max(0.001, multiplier);
      }
    }
    
    windowMultipliers.set(window.id, multiplier);
    console.log(`Window ${window.id} multiplier: ${multiplier.toFixed(4)}`);
  }

  // Process each time window
  for (const window of TIME_WINDOWS) {
    const validReports = processTimeWindow(db, window, now);
    if (!validReports) continue;

    const multiplier = windowMultipliers.get(window.id) ?? 1.0;

    // Process each precision level
    for (let level = PRECISION.MAX; level >= PRECISION.MIN; level--) {
      const cellCounts = processPrecisionLevel(validReports, level, window.id);
      if (!cellCounts) continue;

      const inserts = prepareInserts(cellCounts, level, window.id, multiplier);

      if (inserts.length > 0) {
        try {
          bulkInsert(inserts);
          console.log(`Inserted ${inserts.length} density records for level ${level}, window ${window.id}.`);
        } catch (error) {
          console.error(`Error inserting data for window ${window.id}, level ${level}:`, error);
        }
      } else {
        console.log(`No density records to insert for level ${level}, window ${window.id}.`);
      }
    }
  }

  db.close();
  console.log("Heatmap data generation complete.");
}

module.exports = { updateGrids: generateHeatmapData };