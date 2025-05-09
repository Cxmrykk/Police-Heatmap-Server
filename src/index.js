require("dotenv").config();

const Waze = require("./waze");
const SpeedCameras = require("./speed-cameras");
const Grid = require("./grid");

const CHECK_DELAY_MS = 1000 * 60 * 10; // 10 minutes

async function main() {
  let lastMs = new Date().getTime();

  while (true) {
    await Waze.fetchWazeAlerts();
    // Fetch Speed Camera information every month
    // Update Grid storage every 24 hours

    await new Promise((resolve) => {
      const currentMs = new Date().getTime();
      const timeElapsed = currentMs - lastMs;

      if (timeElapsed >= CHECK_DELAY_MS) {
        lastMs = currentMs;
        resolve();
      } else {
        setTimeout(() => {
          lastMs = new Date().getTime();
          resolve();
        }, CHECK_DELAY_MS - timeElapsed);
      }
    });
  }
}

main();
