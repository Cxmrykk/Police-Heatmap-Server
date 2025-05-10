require("dotenv").config();

const Waze = require("./waze");
// const SpeedCameras = require("./speed-cameras");
const Grid = require("./grid");
const Server = require("./server");

const WAZE_UPDATE_INTERVAL_MS = 1000 * 60 * 10;
const GRID_UPDATE_INTERVAL_MS = 1000 * 60 * 60 * 24;
let lastGridUpdateTime = 0;

async function main() {
  let lastWazeFetchMs = new Date().getTime();

  while (true) {
    const currentMs = new Date().getTime();

    // Fetch Waze Alerts (every 10 minutes)
    await Waze.fetchWazeAlerts();
    lastWazeFetchMs = currentMs;

    // Update Grid storage (every 24 hours)
    if (currentMs - lastGridUpdateTime >= GRID_UPDATE_INTERVAL_MS) {
      console.log("Updating density grids...");
      try {
        await Grid.updateGrids();
        lastGridUpdateTime = currentMs;
        console.log("Density grids updated.");
      } catch (err) {
        console.error("Error updating density grids:", err);
      }
    }

    const timeElapsedForWaze = currentMs - lastWazeFetchMs;
    const delayForWaze = WAZE_UPDATE_INTERVAL_MS - timeElapsedForWaze;

    if (delayForWaze > 0) {
      await new Promise((resolve) => setTimeout(resolve, delayForWaze));
    }
  }
}

// Run the main loop
main();

// Start the API server
Server.startServer();
