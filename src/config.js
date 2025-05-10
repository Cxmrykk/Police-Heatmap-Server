const envSettings = [
  { key: "HEATMAP_CACHE_DIR_PATH", type: "string", required: true, validate: (val) => val && val.length > 0, errorMsg: "must be a non-empty string" },
  { key: "WAZE_MAX_ALERTS", type: "integer", required: true },
  { key: "WAZE_AREA_TOP", type: "float", required: true },
  { key: "WAZE_AREA_BOTTOM", type: "float", required: true },
  { key: "WAZE_AREA_LEFT", type: "float", required: true },
  { key: "WAZE_AREA_RIGHT", type: "float", required: true },
  { key: "WAZE_QUERY_DELAY_MS", type: "integer", required: true },
  { key: "API_PORT", type: "integer", required: false, default: 3000 },
];

const config = {};
const errors = [];

envSettings.forEach((setting) => {
  let value = process.env[setting.key];

  if (value === undefined) {
    if (setting.required) {
      errors.push(`Missing required environment variable: ${setting.key}`);
    } else if (setting.default !== undefined) {
      config[setting.key] = setting.default;
    }
    return; // Continue to next setting
  }

  let parsedValue;
  switch (setting.type) {
    case "integer":
      parsedValue = parseInt(value, 10);
      if (isNaN(parsedValue)) {
        errors.push(`Invalid value for ${setting.key}: expected integer, got "${value}"`);
      }
      break;
    case "float":
      parsedValue = parseFloat(value);
      if (isNaN(parsedValue)) {
        errors.push(`Invalid value for ${setting.key}: expected float, got "${value}"`);
      }
      break;
    case "string":
      parsedValue = value; // Already a string
      break;
    default:
      // This case should ideally not be reached if envSettings is correct
      errors.push(`Internal error: Unknown type definition for ${setting.key}: ${setting.type}`);
      return; // Don't assign if type is unknown
  }

  // Check if parsing was successful (e.g., not NaN for numbers)
  // For strings, parsedValue will be the original string value.
  if (parsedValue !== undefined && !errors.some((err) => err.startsWith(`Invalid value for ${setting.key}`))) {
    if (setting.validate && !setting.validate(parsedValue)) {
      errors.push(`Invalid value for ${setting.key}: ${setting.errorMsg || `failed validation`}. Got "${value}"`);
    } else {
      config[setting.key] = parsedValue;
    }
  }
});

if (errors.length > 0) {
  console.error("FATAL: Environment variable configuration errors:");
  errors.forEach((err) => console.error(`- ${err}`));
  process.exit(1);
}

// Add other shared, static configurations here if desired
config.DB_FILENAME = "alerts.sqlite";

module.exports = config;
