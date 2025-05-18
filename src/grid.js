const Database = require("better-sqlite3");
const config = require("./config");

// Constants
const PRECISION = { MAX: 5, MIN: 0 };
const DAY_MS = 86400000; // 24h in milliseconds
const CACHE_DIR = config.HEATMAP_CACHE_DIR_PATH;
const DB_FILE = config.DB_FILENAME;

// New: Hardcoded diversity radii
const DIVERSITY_RADII = [0.00001, 0.000025, 0.00005, 0.0001];

// Time window definitions
const TIME_WINDOWS = [
  { id: 0, name: "last 7 days", daysAgoStart: 0, daysAgoEnd: 7 },
  { id: 1, name: "7-14 days ago", daysAgoStart: 7, daysAgoEnd: 14 },
  { id: 2, name: "14-30 days ago", daysAgoStart: 14, daysAgoEnd: 30 },
  { id: 3, name: "30-90 days ago", daysAgoStart: 30, daysAgoEnd: 90 },
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
  let min = Infinity,
    max = -Infinity,
    hasValues = false;
  for (const val of countsMap.values()) {
    if (val === null || isNaN(val)) continue;
    hasValues = true;
    min = Math.min(min, val);
    max = Math.max(max, val);
  }
  return hasValues ? { min, max } : { min: 0, max: 0 };
}

function initializeDatabase(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS alerts (uuid TEXT PRIMARY KEY, pubMillis INTEGER, latitude REAL, longitude REAL, confidence INTEGER, reliability INTEGER)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_pubMillis ON alerts (pubMillis)`);

  db.exec(`DROP TABLE IF EXISTS density_grids`);
  db.exec(`CREATE TABLE density_grids (time_window_id INTEGER NOT NULL, level INTEGER NOT NULL, lon_scaled INTEGER NOT NULL, lat_scaled INTEGER NOT NULL, density INTEGER NOT NULL, PRIMARY KEY (time_window_id, level, lon_scaled, lat_scaled))`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_density_grids_coords ON density_grids (time_window_id, level, lon_scaled, lat_scaled)`);

  db.exec(`DROP TABLE IF EXISTS temporal_diversity_grids`);
  db.exec(`
    CREATE TABLE temporal_diversity_grids (
      radius_group_id INTEGER NOT NULL, -- New column
      level INTEGER NOT NULL,
      lon_scaled INTEGER NOT NULL,
      lat_scaled INTEGER NOT NULL,
      diversity_score INTEGER NOT NULL,
      PRIMARY KEY (radius_group_id, level, lon_scaled, lat_scaled) -- Updated PK
    )
  `);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_temporal_diversity_grids_coords ON temporal_diversity_grids (radius_group_id, level, lon_scaled, lat_scaled)`);

  // Metadata table
  db.exec(`CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT)`);
}

// --- Density Grid Generation Logic ---
function processTimeWindowForDensity(db, windowDef, referenceTimestamp) {
  const startMillis = referenceTimestamp - windowDef.daysAgoEnd * DAY_MS;
  const endMillisStrict = referenceTimestamp - windowDef.daysAgoStart * DAY_MS;
  // console.log(`Density: Processing time window: ${windowDef.name} (ID: ${windowDef.id})`);
  const reports = db
    .prepare(
      `SELECT longitude, latitude FROM alerts 
         WHERE pubMillis >= ? AND pubMillis < ? 
         AND longitude IS NOT NULL AND latitude IS NOT NULL`
    )
    .all(startMillis, endMillisStrict);
  if (reports.length === 0) {
    // console.log(`Density: No alert data for window ${windowDef.id}. Skipping.`);
    return null;
  }
  // console.log(`Density: Found ${reports.length} valid reports for window ${windowDef.id}.`);
  return reports;
}
function processPrecisionLevelForDensity(reports, level, windowId) {
  const cellCounts = new Map();
  for (const report of reports) {
    const lonScaled = scaleCoordinate(report.longitude, level);
    const latScaled = scaleCoordinate(report.latitude, level);
    if (lonScaled === null || latScaled === null) continue;
    const key = `${lonScaled}_${latScaled}`;
    cellCounts.set(key, (cellCounts.get(key) || 0) + 1);
  }
  if (cellCounts.size === 0) return null;
  return cellCounts;
}
function prepareDensityInserts(cellCounts, level, windowId) {
  const logScaledCounts = new Map();
  cellCounts.forEach((count, key) => {
    logScaledCounts.set(key, Math.log1p(count));
  });
  const { min, max } = getMinMax(logScaledCounts);
  const inserts = [];
  cellCounts.forEach((_, key) => {
    const [lonStr, latStr] = key.split("_");
    const logScaledValue = logScaledCounts.get(key);
    const density = normalize(logScaledValue, min, max);
    if (density > 0) {
      inserts.push({ time_window_id: windowId, level, lon_scaled: Number(lonStr), lat_scaled: Number(latStr), density });
    }
  });
  return inserts;
}
async function generateDensityGridData(db, referenceTimestamp) {
  console.log("Starting density grid generation...");
  const insertStmt = db.prepare(`INSERT INTO density_grids (time_window_id, level, lon_scaled, lat_scaled, density) VALUES (?, ?, ?, ?, ?)`);
  const bulkInsert = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(item.time_window_id, item.level, item.lon_scaled, item.lat_scaled, item.density);
    }
  });
  for (const windowDef of TIME_WINDOWS) {
    const validReports = processTimeWindowForDensity(db, windowDef, referenceTimestamp);
    if (!validReports) continue;
    console.log(`Density: Processing window ${windowDef.id}, level ${PRECISION.MAX} down to ${PRECISION.MIN}. Reports: ${validReports.length}`);
    for (let level = PRECISION.MAX; level >= PRECISION.MIN; level--) {
      const cellCounts = processPrecisionLevelForDensity(validReports, level, windowDef.id);
      if (!cellCounts) continue;
      const inserts = prepareDensityInserts(cellCounts, level, windowDef.id);
      if (inserts.length > 0) {
        try {
          bulkInsert(inserts); /* console.log(`Density: Inserted ${inserts.length} for L${level}, W${windowDef.id}.`); */
        } catch (error) {
          console.error(`Density: Error L${level}, W${windowDef.id}:`, error);
        }
      }
    }
  }
  console.log("Density grid generation complete.");
}

// --- Temporal Diversity Grid Generation Logic ---
function getTimeWindowIdSqlCase(referenceTimestamp) {
  let caseStatement = "CASE\n";
  for (const tw of TIME_WINDOWS) {
    const boundaryMillisForWindowStart = referenceTimestamp - tw.daysAgoEnd * DAY_MS;
    caseStatement += `  WHEN pubMillis >= ${boundaryMillisForWindowStart} THEN ${tw.id}\n`;
  }
  caseStatement += "  ELSE NULL\nEND";
  return caseStatement;
}

async function generateTemporalDiversityGridData(db, referenceTimestamp) {
  console.log("Starting temporal diversity grid generation for multiple radii...");

  const timeWindowIdCaseSql = getTimeWindowIdSqlCase(referenceTimestamp);
  const oldestTimeWindow = TIME_WINDOWS[TIME_WINDOWS.length - 1];
  const oldestRelevantPubMillis = referenceTimestamp - oldestTimeWindow.daysAgoEnd * DAY_MS;

  console.log("Temporal Diversity: Fetching alerts with time window IDs from DB...");
  const alertsWithSqlTimeWindow = db
    .prepare(
      `
    SELECT uuid, pubMillis, latitude, longitude, (${timeWindowIdCaseSql}) AS timeWindowId 
    FROM alerts 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL AND pubMillis >= ? 
  `
    )
    .all(oldestRelevantPubMillis);

  const validAlertsForDiversity = alertsWithSqlTimeWindow.filter((a) => a.timeWindowId !== null);
  console.log(`Temporal Diversity: ${validAlertsForDiversity.length} alerts successfully assigned to a time window.`);

  if (validAlertsForDiversity.length === 0) {
    console.log("Temporal Diversity: No alerts with time window data to process. Skipping.");
    return;
  }

  console.log("Temporal Diversity: Building map of most recent time window IDs per cell...");
  const cellMostRecentTimeWindowIdMap = new Map();
  for (const alert of validAlertsForDiversity) {
    const lonScaled = scaleCoordinate(alert.longitude, PRECISION.MAX);
    const latScaled = scaleCoordinate(alert.latitude, PRECISION.MAX);
    if (lonScaled === null || latScaled === null) continue;
    const cellKey = `${lonScaled}_${latScaled}`;
    const currentTWIDInMap = cellMostRecentTimeWindowIdMap.get(cellKey);
    if (currentTWIDInMap === undefined || alert.timeWindowId < currentTWIDInMap) {
      cellMostRecentTimeWindowIdMap.set(cellKey, alert.timeWindowId);
    }
  }
  console.log(`Temporal Diversity: Built map with ${cellMostRecentTimeWindowIdMap.size} cells at PRECISION.MAX.`);

  const insertStmt = db.prepare(`
    INSERT INTO temporal_diversity_grids (radius_group_id, level, lon_scaled, lat_scaled, diversity_score)
    VALUES (?, ?, ?, ?, ?)
  `);
  const bulkInsertDiversity = db.transaction((items) => {
    for (const item of items) {
      insertStmt.run(item.radius_group_id, item.level, item.lon_scaled, item.lat_scaled, item.diversity_score);
    }
  });

  const cellResolution = Math.pow(10, -PRECISION.MAX);

  for (let radiusGroupId = 0; radiusGroupId < DIVERSITY_RADII.length; radiusGroupId++) {
    const currentRadius = DIVERSITY_RADII[radiusGroupId];
    console.log(`\nTemporal Diversity: Processing for radius group ${radiusGroupId} (radius: ${currentRadius})...`);

    const levelMaxCellDiversity = new Map();
    const neighborhoodHalfWidthInCells = Math.floor(currentRadius / cellResolution);

    let processedCount = 0;
    const totalToProcessForScores = validAlertsForDiversity.length;
    if (totalToProcessForScores === 0) continue;

    process.stdout.write(`Temporal Diversity (Radius Group ${radiusGroupId}): Calculating scores... 0% (0/${totalToProcessForScores})\r`);

    for (const anchorAlert of validAlertsForDiversity) {
      const anchorLonScaled = scaleCoordinate(anchorAlert.longitude, PRECISION.MAX);
      const anchorLatScaled = scaleCoordinate(anchorAlert.latitude, PRECISION.MAX);

      if (anchorLonScaled === null || anchorLatScaled === null) {
        processedCount++;
        if (processedCount % 100 === 0 || processedCount === totalToProcessForScores) {
          const percentage = Math.floor((processedCount / totalToProcessForScores) * 100);
          process.stdout.write(`Temporal Diversity (Radius Group ${radiusGroupId}): Calculating scores... ${percentage}% (${processedCount}/${totalToProcessForScores})\r`);
        }
        continue;
      }

      const uniqueTimeWindowsInProximity = new Set();
      for (let dy = -neighborhoodHalfWidthInCells; dy <= neighborhoodHalfWidthInCells; dy++) {
        for (let dx = -neighborhoodHalfWidthInCells; dx <= neighborhoodHalfWidthInCells; dx++) {
          const targetLonScaled = anchorLonScaled + dx;
          const targetLatScaled = anchorLatScaled + dy;
          const neighborCellKey = `${targetLonScaled}_${targetLatScaled}`;
          if (cellMostRecentTimeWindowIdMap.has(neighborCellKey)) {
            uniqueTimeWindowsInProximity.add(cellMostRecentTimeWindowIdMap.get(neighborCellKey));
          }
        }
      }

      const diversityScoreForAnchor = uniqueTimeWindowsInProximity.size;
      const anchorCellKey = `${anchorLonScaled}_${anchorLatScaled}`;
      levelMaxCellDiversity.set(anchorCellKey, Math.max(levelMaxCellDiversity.get(anchorCellKey) || 0, diversityScoreForAnchor));

      processedCount++;
      if (processedCount % 100 === 0 || processedCount === totalToProcessForScores) {
        const percentage = Math.floor((processedCount / totalToProcessForScores) * 100);
        process.stdout.write(`Temporal Diversity (Radius Group ${radiusGroupId}): Calculating scores... ${percentage}% (${processedCount}/${totalToProcessForScores})\r`);
      }
    }
    process.stdout.write("\n");
    console.log(`Temporal Diversity (Radius Group ${radiusGroupId}): Finished. Found ${levelMaxCellDiversity.size} cells with scores at level ${PRECISION.MAX}.`);

    const levelMaxInserts = [];
    levelMaxCellDiversity.forEach((score, key) => {
      const [lonStr, latStr] = key.split("_");
      if (score > 0) {
        levelMaxInserts.push({ radius_group_id: radiusGroupId, level: PRECISION.MAX, lon_scaled: Number(lonStr), lat_scaled: Number(latStr), diversity_score: score });
      }
    });

    if (levelMaxInserts.length > 0) {
      try {
        bulkInsertDiversity(levelMaxInserts);
        console.log(`Temporal Diversity (Radius Group ${radiusGroupId}): Inserted ${levelMaxInserts.length} records for level ${PRECISION.MAX}.`);
      } catch (error) {
        console.error(`Temporal Diversity (Radius Group ${radiusGroupId}): Error L${PRECISION.MAX}:`, error);
      }
    } else {
      console.log(`Temporal Diversity (Radius Group ${radiusGroupId}): No records to insert for level ${PRECISION.MAX}.`);
    }

    for (let level = PRECISION.MAX - 1; level >= PRECISION.MIN; level--) {
      const lowerLevelCellDiversity = new Map();
      const higherLevelCells = db.prepare(`SELECT lon_scaled, lat_scaled, diversity_score FROM temporal_diversity_grids WHERE radius_group_id = ? AND level = ?`).all(radiusGroupId, level + 1);

      if (higherLevelCells.length === 0) {
        continue;
      }

      for (const higherCell of higherLevelCells) {
        const parentLonScaled = Math.trunc(higherCell.lon_scaled / 10);
        const parentLatScaled = Math.trunc(higherCell.lat_scaled / 10);
        const parentCellKey = `${parentLonScaled}_${parentLatScaled}`;
        lowerLevelCellDiversity.set(parentCellKey, Math.max(lowerLevelCellDiversity.get(parentCellKey) || 0, higherCell.diversity_score));
      }

      const currentLevelInserts = [];
      lowerLevelCellDiversity.forEach((score, key) => {
        const [lonStr, latStr] = key.split("_");
        if (score > 0) {
          currentLevelInserts.push({ radius_group_id: radiusGroupId, level: level, lon_scaled: Number(lonStr), lat_scaled: Number(latStr), diversity_score: score });
        }
      });

      if (currentLevelInserts.length > 0) {
        try {
          bulkInsertDiversity(currentLevelInserts);
        } catch (error) {
          console.error(`Error L${level}, RG${radiusGroupId}:`, error);
        }
      }
    }
    console.log(`Temporal Diversity (Radius Group ${radiusGroupId}): Aggregation complete.`);
  }
  console.log("All temporal diversity grid generation complete.");
}

async function updateMetadata(db, referenceTimestamp) {
  console.log("Updating metadata...");
  const insertMetadataStmt = db.prepare(`INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)`);

  // Last grid update timestamp
  insertMetadataStmt.run("last_grid_update_timestamp", referenceTimestamp.toString());

  // Center coordinates
  const centerLon = (config.WAZE_AREA_LEFT + config.WAZE_AREA_RIGHT) / 2;
  const centerLat = (config.WAZE_AREA_BOTTOM + config.WAZE_AREA_TOP) / 2;
  insertMetadataStmt.run("center_longitude", centerLon.toString());
  insertMetadataStmt.run("center_latitude", centerLat.toString());

  // Total alerts in time windows
  let totalAlertsInWindows = 0;
  if (TIME_WINDOWS.length > 0) {
    const maxDaysAgoEnd = Math.max(...TIME_WINDOWS.map((tw) => tw.daysAgoEnd));
    const minDaysAgoStart = Math.min(...TIME_WINDOWS.map((tw) => tw.daysAgoStart)); // Should be 0 for "last X days"

    const overallStartMillis = referenceTimestamp - maxDaysAgoEnd * DAY_MS;
    const overallEndMillisStrict = referenceTimestamp - minDaysAgoStart * DAY_MS; // This is effectively referenceTimestamp

    const result = db
      .prepare(
        `SELECT COUNT(uuid) as total_alerts FROM alerts 
       WHERE pubMillis >= ? AND pubMillis < ? 
       AND longitude IS NOT NULL AND latitude IS NOT NULL`
      )
      .get(overallStartMillis, overallEndMillisStrict);
    totalAlertsInWindows = result ? result.total_alerts : 0;
  }
  insertMetadataStmt.run("total_alerts_in_time_windows", totalAlertsInWindows.toString());
  console.log(`Metadata updated: Last Update: ${new Date(referenceTimestamp).toISOString()}, Center: ${centerLat.toFixed(4)},${centerLon.toFixed(4)}, Total Alerts: ${totalAlertsInWindows}`);
}

// --- Main Update Function ---
async function updateGrids() {
  const dbPath = require("path").join(CACHE_DIR, DB_FILE);
  console.log(`Using database at: ${dbPath}`);
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");

  initializeDatabase(db);

  let referenceTimestamp;
  try {
    const tableCheck = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='alerts'").get();
    if (!tableCheck) {
      console.warn("Alerts table does not exist. Grid generation will use current time as reference.");
      referenceTimestamp = Date.now();
    } else {
      const maxPubMillisRow = db.prepare("SELECT MAX(pubMillis) as max_millis FROM alerts").get();
      if (maxPubMillisRow && maxPubMillisRow.max_millis != null) {
        referenceTimestamp = maxPubMillisRow.max_millis;
        console.log(`Using latest alert pubMillis as reference timestamp: ${new Date(referenceTimestamp).toISOString()} (${referenceTimestamp})`);
      } else {
        referenceTimestamp = Date.now();
        console.warn("No alerts found or max pubMillis is NULL. Falling back to current time as reference for grid generation: " + new Date(referenceTimestamp).toISOString());
      }
    }
  } catch (error) {
    console.error("Error determining reference timestamp from database. Falling back to current time.", error);
    referenceTimestamp = Date.now();
  }

  await generateDensityGridData(db, referenceTimestamp);
  await generateTemporalDiversityGridData(db, referenceTimestamp);
  await updateMetadata(db, referenceTimestamp); // Add this call

  db.close();
  console.log("All grid data generation and database updates are complete.");
}

module.exports = { updateGrids };
