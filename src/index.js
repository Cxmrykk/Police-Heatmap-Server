require("dotenv").config();
const config = require("./config");

const fs = require("fs");
const Path = require("path");
const { fork } = require("child_process");

const Server = require("./server");
const Log = require("./log");

const WAZE_UPDATE_INTERVAL_MS = config.WAZE_UPDATE_INTERVAL_MS;
const GRID_UPDATE_INTERVAL_MS = config.GRID_UPDATE_INTERVAL_MS;

const MINIMUM_POST_COMPLETION_INTERVAL_MS = 1 * 60 * 1000;

const TIMESTAMP_FILE_PATH = Path.join(config.HEATMAP_CACHE_DIR_PATH, "last_execution_times.json");

const WAZE_TASK_NAME = "waze_alerts_fetch";
const GRID_TASK_NAME = "grid_data_update";

// Paths to runner scripts
const WAZE_RUNNER_PATH = Path.resolve(__dirname, "waze-runner.js");
const GRID_RUNNER_PATH = Path.resolve(__dirname, "grid-runner.js");

const taskRunningFlags = {
  [WAZE_TASK_NAME]: false,
  [GRID_TASK_NAME]: false,
};

try {
  if (!fs.existsSync(config.HEATMAP_CACHE_DIR_PATH)) {
    fs.mkdirSync(config.HEATMAP_CACHE_DIR_PATH, { recursive: true });
    Log.info(`Created cache directory: ${config.HEATMAP_CACHE_DIR_PATH}`);
  }
} catch (error) {
  Log.error(`FATAL: Error creating cache directory ${config.HEATMAP_CACHE_DIR_PATH}:`, error);
  process.exit(1);
}

function readTimestamps() {
  try {
    if (fs.existsSync(TIMESTAMP_FILE_PATH)) {
      const data = fs.readFileSync(TIMESTAMP_FILE_PATH, "utf8");
      const parsedData = JSON.parse(data);
      for (const taskKey of [WAZE_TASK_NAME, GRID_TASK_NAME]) {
        if (parsedData[taskKey] && (typeof parsedData[taskKey].lastAttemptedStart !== "number" || (parsedData[taskKey].lastCompletion !== undefined && typeof parsedData[taskKey].lastCompletion !== "number"))) {
          Log.warn(`Timestamp data for ${taskKey} has unexpected structure. Resetting for this task.`);
          delete parsedData[taskKey];
        }
      }
      return parsedData;
    }
  } catch (error) {
    Log.warn(`Warning: Could not read or parse timestamp file (${TIMESTAMP_FILE_PATH}): ${error.message}. Assuming no prior executions.`);
  }
  return {};
}

function writeTimestamps(timestamps) {
  try {
    fs.writeFileSync(TIMESTAMP_FILE_PATH, JSON.stringify(timestamps, null, 2), "utf8");
  } catch (error) {
    Log.error(`Error writing timestamp file (${TIMESTAMP_FILE_PATH}):`, error);
  }
}

async function runTaskIfDue(taskName, taskRunnerPath, intervalMs) {
  if (taskRunningFlags[taskName]) {
    Log.info(`${taskName} is already running. Skipping this interval check.`);
    return;
  }

  const allTimestamps = readTimestamps();
  const taskTimestamps = allTimestamps[taskName] || {};
  const lastAttemptedStart = taskTimestamps.lastAttemptedStart || 0;
  const lastCompletion = taskTimestamps.lastCompletion || 0;
  const now = Date.now();

  const timeSinceLastAttempt = now - lastAttemptedStart;
  const timeSinceLastCompletion = lastCompletion ? now - lastCompletion : Infinity;

  const isDueByMainInterval = timeSinceLastAttempt >= intervalMs;
  const canRunAfterMinCompletionDelay = timeSinceLastCompletion >= MINIMUM_POST_COMPLETION_INTERVAL_MS;

  if (isDueByMainInterval) {
    if (!canRunAfterMinCompletionDelay) {
      const timeToWaitMs = MINIMUM_POST_COMPLETION_INTERVAL_MS - timeSinceLastCompletion;
      const secondsToWait = Math.ceil(timeToWaitMs / 1000);
      Log.info(`${taskName}: Main interval passed, but waiting for minimum post-completion delay of ${MINIMUM_POST_COMPLETION_INTERVAL_MS / 1000}s. Last completion: ${new Date(lastCompletion).toISOString()}. Need to wait approx. ${secondsToWait} more sec(s).`);
      return;
    }

    taskRunningFlags[taskName] = true;
    const lastAttemptStr = lastAttemptedStart === 0 ? "Never" : new Date(lastAttemptedStart).toISOString();
    Log.info(`Executing ${taskName}: Conditions met. Last attempt: ${lastAttemptStr}. Current time: ${new Date(now).toISOString()}`);

    const updatedTaskTimestamps = {
      lastAttemptedStart: now,
      lastCompletion: lastCompletion, // Preserve old completion until new one is confirmed
    };
    allTimestamps[taskName] = updatedTaskTimestamps;
    writeTimestamps(allTimestamps); // Write attempt time

    Log.info(`Forking child process for ${taskName} from ${taskRunnerPath}...`);
    const child = fork(taskRunnerPath, [], { stdio: "inherit" }); // 'inherit' to see child logs

    child.on("exit", (code) => {
      const executionCompletionTime = Date.now();
      if (code === 0) {
        Log.info(`${taskName} child process exited successfully (code 0).`);
        // Read timestamps again to ensure we have the latest, then update completion
        const currentTimestamps = readTimestamps();
        const taskSpecificTimestamps = currentTimestamps[taskName] || { lastAttemptedStart: now };
        taskSpecificTimestamps.lastCompletion = executionCompletionTime;
        currentTimestamps[taskName] = taskSpecificTimestamps;
        writeTimestamps(currentTimestamps);
        Log.info(`${taskName} executed successfully. Completion timestamp updated to ${new Date(executionCompletionTime).toISOString()}.`);
      } else {
        Log.error(`${taskName} child process exited with error code ${code}. Last completion time not updated.`);
      }
      taskRunningFlags[taskName] = false;
    });

    child.on("error", (error) => {
      Log.error(`Failed to start or error in child process for ${taskName}:`, error);
      // lastCompletion is not updated on error
      taskRunningFlags[taskName] = false;
      // Timestamps with lastAttemptedStart are already written
    });
  } else {
    const timeToWaitMs = intervalMs - timeSinceLastAttempt;
    const secondsToWait = Math.ceil(timeToWaitMs / 1000);
    const lastAttemptStr = lastAttemptedStart === 0 ? "Never (or no timestamp)" : new Date(lastAttemptedStart).toISOString();
    Log.info(`${taskName} not due yet (main interval). Last attempt: ${lastAttemptStr}. Will be due in approximately ${secondsToWait} sec(s).`);
  }
}

setInterval(() => {
  runTaskIfDue(WAZE_TASK_NAME, WAZE_RUNNER_PATH, WAZE_UPDATE_INTERVAL_MS).catch((err) => Log.error(`Error in scheduled execution wrapper for ${WAZE_TASK_NAME}:`, err));
}, WAZE_UPDATE_INTERVAL_MS);

setInterval(() => {
  runTaskIfDue(GRID_TASK_NAME, GRID_RUNNER_PATH, GRID_UPDATE_INTERVAL_MS).catch((err) => Log.error(`Error in scheduled execution wrapper for ${GRID_TASK_NAME}:`, err));
}, GRID_UPDATE_INTERVAL_MS);

async function setup() {
  Log.info("Running initial setup checks for tasks...");
  await runTaskIfDue(WAZE_TASK_NAME, WAZE_RUNNER_PATH, WAZE_UPDATE_INTERVAL_MS);
  await runTaskIfDue(GRID_TASK_NAME, GRID_RUNNER_PATH, GRID_UPDATE_INTERVAL_MS);
  Log.info("Initial setup checks complete.");
}

async function main() {
  await setup();
  Server.startServer(); // Server runs in the main process
}

main().catch((error) => {
  Log.error("FATAL: Unhandled error in main application execution:", error);
  process.exit(1);
});
