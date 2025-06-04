/*
  -- This script is intended to be run as a child process --
*/
const Grid = require("./grid");
const Log = require("./log");

async function runGridTask() {
  try {
    Log.info("[Grid Runner] Starting grid update task.");
    await Grid.updateGrids();
    Log.info("[Grid Runner] Grid update task completed successfully.");
    process.exit(0); // Success
  } catch (error) {
    Log.error("[Grid Runner] Error during grid update task:", error);
    process.exit(1); // Failure
  }
}

runGridTask();
