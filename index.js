const fs = require("fs");
const csv = require("csv-parser");
const path = require("path");

async function init() {
  try {
    // Tracker object to see at which line we are at in a given file.
    const trackers = {};
    const currentRows = [];

    fs.readdirSync("./temp").forEach((file, index) => {
      // Add each file to the tracker
      const filePath = path.join(__dirname, "temp", file);
      trackers[filePath] = {
        filePath,
        currentLine: 1
      };
    });

    // Get the first line of reach file
    const filePaths = Object.keys(trackers);
    for (let i = 0; i < filePaths.length; i++) {
      const filePath = filePaths[i];
      const row = await getNextRow(filePath, 1);
      currentRows.push(row);
    }

    // Start processing the logs.
    while (true) {
      // Sort the values by timestamp in ascending order
      currentRows.sort((a, b) => a.timestamp - b.timestamp);

      // Check if there are no more lines to print.
      if (!currentRows.length) {
        break;
      }

      // Get the latest log.
      const nextRow = currentRows.shift();

      // Print it in the console
      console.log(`${nextRow.timestamp.toISOString()},${nextRow.event}`);

      // Grab the next row of the printed filed
      const tracker = trackers[nextRow.filePath];
      tracker.currentLine++;

      const newRow = await getNextRow(tracker.filePath, tracker.currentLine);
      if (newRow) {
        currentRows.push(newRow);
      }
    }
  } catch (error) {
    console.error("There was a problem processing the logs.", error.stack);
  }
}

// Gets a specific row from a CSV file
function getNextRow(filePath, currentLine = 1) {
  let r = 0;
  return new Promise((resolve, reject) => {
    const readStream = fs
      .createReadStream(filePath)
      .pipe(csv({ headers: ["timestamp", "event"] }));
    readStream
      .on("data", row => {
        r++;
        if (r === currentLine) {
          readStream.destroy();
          row.timestamp = new Date(row.timestamp);
          resolve({ ...row, filePath });
        }
      })
      .on("end", error => {
        resolve(null);
      });
  });
}

init();
