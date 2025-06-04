/*
  -- This script is intended to be run as a child process --
*/
const Waze = require("./waze");
const Log = require("./log");

async function runWazeTask() {
  try {
    Log.info("[Waze Runner] Starting Waze alerts fetch task.");
    await Waze.fetchWazeAlerts();
    Log.info("[Waze Runner] Waze alerts fetch task completed successfully.");
    process.exit(0); // Success
  } catch (error) {
    Log.error("[Waze Runner] Error during Waze alerts fetch task:", error);
    process.exit(1); // Failure
  }
}

runWazeTask();
