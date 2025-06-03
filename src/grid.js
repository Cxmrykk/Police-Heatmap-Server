const Database = require("better-sqlite3");
const config = require("./config");
const Log = require("./log")

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

function initializeDatabase(db) {
  db.exec(`CREATE TABLE IF NOT EXISTS alerts (uuid TEXT PRIMARY KEY, pubMillis INTEGER, latitude REAL, longitude REAL, confidence INTEGER, reliability INTEGER)`);
  db.exec(`CREATE INDEX IF NOT EXISTS idx_alerts_pubMillis ON alerts (pubMillis)`);

  // Density grids table removed
  // db.exec(`DROP TABLE IF EXISTS density_grids`);
  // db.exec(`CREATE TABLE density_grids (time_window_id INTEGER NOT NULL, level INTEGER NOT NULL, lon_scaled INTEGER NOT NULL, lat_scaled INTEGER NOT NULL, density INTEGER NOT NULL, PRIMARY KEY (time_window_id, level, lon_scaled, lat_scaled))`);
  // db.exec(`CREATE INDEX IF NOT EXISTS idx_density_grids_coords ON density_grids (time_window_id, level, lon_scaled, lat_scaled)`);

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

// --- Density Grid Generation Logic (REMOVED) ---

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
  Log.info("Starting temporal diversity grid generation for multiple radii...");

  const timeWindowIdCaseSql = getTimeWindowIdSqlCase(referenceTimestamp);
  const oldestTimeWindow = TIME_WINDOWS[TIME_WINDOWS.length - 1];
  const oldestRelevantPubMillis = referenceTimestamp - oldestTimeWindow.daysAgoEnd * DAY_MS;

  Log.info("Temporal Diversity: Fetching alerts with time window IDs from DB...");
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
  Log.info(`Temporal Diversity: ${validAlertsForDiversity.length} alerts successfully assigned to a time window.`);

  if (validAlertsForDiversity.length === 0) {
    Log.info("Temporal Diversity: No alerts with time window data to process. Skipping.");
    return;
  }

  Log.info("Temporal Diversity: Building map of most recent time window IDs per cell...");
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
  Log.info(`Temporal Diversity: Built map with ${cellMostRecentTimeWindowIdMap.size} cells at PRECISION.MAX.`);

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
    Log.info(`\nTemporal Diversity: Processing for radius group ${radiusGroupId} (radius: ${currentRadius})...`);

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
    Log.info(`Temporal Diversity (Radius Group ${radiusGroupId}): Finished. Found ${levelMaxCellDiversity.size} cells with scores at level ${PRECISION.MAX}.`);

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
        Log.info(`Temporal Diversity (Radius Group ${radiusGroupId}): Inserted ${levelMaxInserts.length} records for level ${PRECISION.MAX}.`);
      } catch (error) {
        Log.error(`Temporal Diversity (Radius Group ${radiusGroupId}): Error L${PRECISION.MAX}:`, error);
      }
    } else {
      Log.info(`Temporal Diversity (Radius Group ${radiusGroupId}): No records to insert for level ${PRECISION.MAX}.`);
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
          Log.error(`Error L${level}, RG${radiusGroupId}:`, error);
        }
      }
    }
    Log.info(`Temporal Diversity (Radius Group ${radiusGroupId}): Aggregation complete.`);
  }
  Log.info("All temporal diversity grid generation complete.");
}

async function updateMetadata(db, referenceTimestamp) {
  Log.info("Updating metadata...");
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
  Log.info(`Metadata updated: Last Update: ${new Date(referenceTimestamp).toISOString()}, Center: ${centerLat.toFixed(4)},${centerLon.toFixed(4)}, Total Alerts: ${totalAlertsInWindows}`);
}

// --- Main Update Function ---
async function updateGrids() {
  const dbPath = require("path").join(CACHE_DIR, DB_FILE);
  Log.info(`Using database at: ${dbPath}`);
  const db = new Database(dbPath);
  db.pragma("journal_mode = WAL");

  initializeDatabase(db);

  let referenceTimestamp;
  try {
    const tableCheck = db.prepare("SELECT name FROM sqlite_master WHERE type='table' AND name='alerts'").get();
    if (!tableCheck) {
      Log.warn("Alerts table does not exist. Grid generation will use current time as reference.");
      referenceTimestamp = Date.now();
    } else {
      const maxPubMillisRow = db.prepare("SELECT MAX(pubMillis) as max_millis FROM alerts").get();
      if (maxPubMillisRow && maxPubMillisRow.max_millis != null) {
        referenceTimestamp = maxPubMillisRow.max_millis;
        Log.info(`Using latest alert pubMillis as reference timestamp: ${new Date(referenceTimestamp).toISOString()} (${referenceTimestamp})`);
      } else {
        referenceTimestamp = Date.now();
        Log.warn("No alerts found or max pubMillis is NULL. Falling back to current time as reference for grid generation: " + new Date(referenceTimestamp).toISOString());
      }
    }
  } catch (error) {
    Log.error("Error determining reference timestamp from database. Falling back to current time.", error);
    referenceTimestamp = Date.now();
  }

  // await generateDensityGridData(db, referenceTimestamp); // Removed
  await generateTemporalDiversityGridData(db, referenceTimestamp);
  await updateMetadata(db, referenceTimestamp); // Add this call

  db.close();
  Log.info("All grid data generation and database updates are complete.");
}

module.exports = { updateGrids };
