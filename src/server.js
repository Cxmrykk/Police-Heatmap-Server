const express = require("express");
const Database = require("better-sqlite3");
const Path = require("path");
const config = require("./config");

const CACHE_DIR_PATH = config.HEATMAP_CACHE_DIR_PATH;
const DB_FILENAME = config.DB_FILENAME;
const API_PORT = config.API_PORT;
const MAX_PRECISION_LEVEL = 5;

let db;

function getScaledIntCoordinate(coord, precision) {
  const multiplier = Math.pow(10, precision);
  return Math.trunc(coord * multiplier);
}

function getFloatCoordinateFromScaled(scaledCoord, precision) {
  if (precision === 0) return scaledCoord;
  const divisor = Math.pow(10, precision);
  return scaledCoord / divisor;
}

function startServer() {
  const dbPath = Path.join(CACHE_DIR_PATH, DB_FILENAME);
  // Open in readonly mode, ensure file exists (grid.js or waze.js should have created it)
  try {
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
  } catch (error) {
    console.error(`FATAL: Could not open database at ${dbPath}. Ensure it's created by the data processing scripts. Error: ${error.message}`);
    process.exit(1);
  }

  const app = express();
  app.use(express.json());

  app.get("/api/density", (req, res) => {
    const { time_window_id, level, min_lon, min_lat, max_lon, max_lat } = req.query;

    const queryTimeWindowId = parseInt(time_window_id, 10);
    const queryLevel = parseInt(level, 10);
    const queryMinLon = parseFloat(min_lon);
    const queryMinLat = parseFloat(min_lat);
    const queryMaxLon = parseFloat(max_lon);
    const queryMaxLat = parseFloat(max_lat);

    if (isNaN(queryTimeWindowId) || queryTimeWindowId < 0 || queryTimeWindowId > 3 || isNaN(queryLevel) || queryLevel < 0 || queryLevel > MAX_PRECISION_LEVEL || isNaN(queryMinLon) || isNaN(queryMinLat) || isNaN(queryMaxLon) || isNaN(queryMaxLat)) {
      return res.status(400).json({ error: "Invalid query parameters" });
    }

    const lonScaledMin = getScaledIntCoordinate(queryMinLon, queryLevel);
    const latScaledMin = getScaledIntCoordinate(queryMinLat, queryLevel);
    const lonScaledMax = getScaledIntCoordinate(queryMaxLon, queryLevel);
    const latScaledMax = getScaledIntCoordinate(queryMaxLat, queryLevel);

    try {
      const stmt = db.prepare(`
                SELECT lon_scaled, lat_scaled, density
                FROM density_grids
                WHERE time_window_id = @time_window_id
                  AND level = @level
                  AND lon_scaled >= @lonScaledMin AND lon_scaled <= @lonScaledMax
                  AND lat_scaled >= @latScaledMin AND lat_scaled <= @latScaledMax
            `);

      const results = stmt.all({
        time_window_id: queryTimeWindowId,
        level: queryLevel,
        lonScaledMin: lonScaledMin,
        latScaledMin: latScaledMin,
        lonScaledMax: lonScaledMax,
        latScaledMax: latScaledMax,
      });

      const formattedResults = results.map((row) => ({
        lon: getFloatCoordinateFromScaled(row.lon_scaled, queryLevel),
        lat: getFloatCoordinateFromScaled(row.lat_scaled, queryLevel),
        density: row.density,
      }));
      res.json(formattedResults);
    } catch (error) {
      console.error("Error retrieving density data:", error);
      res.status(500).json({ error: "Failed to retrieve density data" });
    }
  });

  app.listen(API_PORT, () => {
    console.log(`API Server listening on port ${API_PORT}`);
  });
}

module.exports = { startServer };
