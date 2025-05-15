require("dotenv").config();
require("./config");

const Waze = require("./waze");
// const SpeedCameras = require("./speed-cameras");
const Grid = require("./grid");
const Server = require("./server");

const WAZE_UPDATE_INTERVAL_MS = 1000 * 60 * 10;
const GRID_UPDATE_INTERVAL_MS = 1000 * 60 * 60 * 24;

// Set update intervals
setInterval(Waze.fetchWazeAlerts, WAZE_UPDATE_INTERVAL_MS);
setInterval(Grid.updateGrids, GRID_UPDATE_INTERVAL_MS);

async function setup() {
  //await Waze.fetchWazeAlerts();
  await Grid.updateGrids();
}

// Start the API server
setup();
Server.startServer();
