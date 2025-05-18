const express = require("express");
const Database = require("better-sqlite3");
const Path = require("path");
const config = require("./config");

const CACHE_DIR_PATH = config.HEATMAP_CACHE_DIR_PATH;
const DB_FILENAME = config.DB_FILENAME;
const API_PORT = config.API_PORT;
const MAX_PRECISION_LEVEL = 5;
const NUM_DIVERSITY_RADIUS_GROUPS = 4; // Matches DIVERSITY_RADII.length in grid.js

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
  try {
    db = new Database(dbPath, { readonly: true, fileMustExist: true });
  } catch (error) {
    console.error(`FATAL: Could not open database at ${dbPath}. Error: ${error.message}`);
    process.exit(1);
  }

  const app = express();
  app.use(express.json());

  const handleDensityRequest = (req, res) => {
    const { time_window_id, level, min_lon, min_lat, max_lon, max_lat } = req.query;
    const queryTimeWindowId = parseInt(time_window_id, 10);
    const queryLevel = parseInt(level, 10);
    const queryMinLon = parseFloat(min_lon);
    const queryMinLat = parseFloat(min_lat);
    const queryMaxLon = parseFloat(max_lon);
    const queryMaxLat = parseFloat(max_lat);
    const numberOfTimeWindows = config.TIME_WINDOWS ? config.TIME_WINDOWS.length : 4;

    if (isNaN(queryTimeWindowId) || queryTimeWindowId < 0 || queryTimeWindowId >= numberOfTimeWindows || isNaN(queryLevel) || queryLevel < 0 || queryLevel > MAX_PRECISION_LEVEL || isNaN(queryMinLon) || isNaN(queryMinLat) || isNaN(queryMaxLon) || isNaN(queryMaxLat)) {
      return res.status(400).json({ error: "Invalid query parameters for density" });
    }
    const lonScaledMin = getScaledIntCoordinate(queryMinLon, queryLevel);
    const latScaledMin = getScaledIntCoordinate(queryMinLat, queryLevel);
    const lonScaledMax = getScaledIntCoordinate(queryMaxLon, queryLevel);
    const latScaledMax = getScaledIntCoordinate(queryMaxLat, queryLevel);
    try {
      const stmt = db.prepare(`
        SELECT lon_scaled, lat_scaled, density as density_value
        FROM density_grids
        WHERE time_window_id = @time_window_id AND level = @level
          AND lon_scaled >= @lonScaledMin AND lon_scaled <= @lonScaledMax
          AND lat_scaled >= @latScaledMin AND lat_scaled <= @latScaledMax
          AND density > 0
      `);
      const results = stmt.all({ time_window_id: queryTimeWindowId, level: queryLevel, lonScaledMin, latScaledMin, lonScaledMax, latScaledMax });
      const formattedResults = results.map((row) => ({
        lon: getFloatCoordinateFromScaled(row.lon_scaled, queryLevel),
        lat: getFloatCoordinateFromScaled(row.lat_scaled, queryLevel),
        density: row.density_value,
      }));
      res.json(formattedResults);
    } catch (error) {
      console.error(`Error retrieving density data:`, error);
      res.status(500).json({ error: `Failed to retrieve density data` });
    }
  };

  const handleTemporalDiversityRequest = (req, res) => {
    const { radius_group_id, level, min_lon, min_lat, max_lon, max_lat } = req.query;

    const queryRadiusGroupId = parseInt(radius_group_id, 10);
    const queryLevel = parseInt(level, 10);
    const queryMinLon = parseFloat(min_lon);
    const queryMinLat = parseFloat(min_lat);
    const queryMaxLon = parseFloat(max_lon);
    const queryMaxLat = parseFloat(max_lat);

    // Validate radius_group_id
    if (isNaN(queryRadiusGroupId) || queryRadiusGroupId < 0 || queryRadiusGroupId >= NUM_DIVERSITY_RADIUS_GROUPS) {
      return res.status(400).json({ error: `Invalid radius_group_id. Must be between 0 and ${NUM_DIVERSITY_RADIUS_GROUPS - 1}.` });
    }
    if (isNaN(queryLevel) || queryLevel < 0 || queryLevel > MAX_PRECISION_LEVEL || isNaN(queryMinLon) || isNaN(queryMinLat) || isNaN(queryMaxLon) || isNaN(queryMaxLat)) {
      return res.status(400).json({ error: "Invalid query parameters for temporal diversity" });
    }

    const lonScaledMin = getScaledIntCoordinate(queryMinLon, queryLevel);
    const latScaledMin = getScaledIntCoordinate(queryMinLat, queryLevel);
    const lonScaledMax = getScaledIntCoordinate(queryMaxLon, queryLevel);
    const latScaledMax = getScaledIntCoordinate(queryMaxLat, queryLevel);

    try {
      const stmt = db.prepare(`
        SELECT lon_scaled, lat_scaled, diversity_score
        FROM temporal_diversity_grids
        WHERE radius_group_id = @radius_group_id -- Filter by radius_group_id
          AND level = @level
          AND lon_scaled >= @lonScaledMin AND lon_scaled <= @lonScaledMax
          AND lat_scaled >= @latScaledMin AND lat_scaled <= @latScaledMax
          AND diversity_score > 0 
      `);
      const results = stmt.all({
        radius_group_id: queryRadiusGroupId,
        level: queryLevel,
        lonScaledMin,
        latScaledMin,
        lonScaledMax,
        latScaledMax,
      });
      const formattedResults = results.map((row) => ({
        lon: getFloatCoordinateFromScaled(row.lon_scaled, queryLevel),
        lat: getFloatCoordinateFromScaled(row.lat_scaled, queryLevel),
        score: row.diversity_score,
      }));
      res.json(formattedResults);
    } catch (error) {
      console.error(`Error retrieving temporal diversity data:`, error);
      res.status(500).json({ error: `Failed to retrieve temporal diversity data` });
    }
  };

  app.get("/api/density", handleDensityRequest);
  app.get("/api/diversity", handleTemporalDiversityRequest);

  app.listen(API_PORT, () => {
    console.log(`API Server listening on port ${API_PORT}`);
  });
}

if (!config.TIME_WINDOWS) {
  config.TIME_WINDOWS = [{}, {}, {}, {}];
}

module.exports = { startServer };
