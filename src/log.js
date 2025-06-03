function getTimestamp() {
  const now = new Date();

  const timestamp = [now.getDate().toString().padStart(2, "0"), "/", (now.getMonth() + 1).toString().padStart(2, "0"), "/", now.getFullYear(), " - ", now.getHours().toString().padStart(2, "0"), ":", now.getMinutes().toString().padStart(2, "0"), ":", now.getSeconds().toString().padStart(2, "0")].join("");

  return timestamp;
}

function getFormattedText(text, prefix = undefined) {
  return prefix === undefined ? `[${getTimestamp()}] ${text}` : `${prefix} [${getTimestamp()}] ${text}`;
}

module.exports = {
  debug: (text) => console.debug(getFormattedText(text, "[DEBUG]")),
  info: (text) => console.info(getFormattedText(text, "[INFO]")),
  warn: (text) => console.warn(getFormattedText(text, "[WARNING]")),
  error: (text) => console.error(getFormattedText(text, "[ERROR]")),
};
