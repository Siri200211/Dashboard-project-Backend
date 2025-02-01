const cron = require("node-cron");
const moment = require("moment");
const { sendEmail } = require("./emailService"); // Correct import
const Disconnection = require("./models/Disconnection"); // Ensure the correct path
const Order = require("./models/Order"); // Ensure the correct path

// CRON job to check for updates at 9 AM every day
cron.schedule("0 9 * * *", async () => {
  console.log("Running CSV update check...");

  try {
    // Check last updated dates for disconnections and new connections
    const disconnectionUpdate = await Disconnection.findOne().sort({ churn_date: -1 }).select("churn_date");
    const connectionUpdate = await Order.findOne().sort({ date: -1 }).select("date");

    const now = moment(); // Current date
    const disconnectionLastUpdate = disconnectionUpdate ? moment(disconnectionUpdate.churn_date) : null;
    const connectionLastUpdate = connectionUpdate ? moment(connectionUpdate.date) : null;

    const disconnectionDiff = disconnectionLastUpdate ? now.diff(disconnectionLastUpdate, "days") : null;
    const connectionDiff = connectionLastUpdate ? now.diff(connectionLastUpdate, "days") : null;

    let emailSubject = "Reminder: CSV File Update Required";
    let emailMessage = "";

    // Check disconnections
    if (disconnectionDiff !== null && disconnectionDiff >= 3) {
      emailMessage += `The disconnection CSV file hasn't been updated for ${disconnectionDiff} days.<br>`;
    }

    // Check new connections
    if (connectionDiff !== null && connectionDiff >= 3) {
      emailMessage += `The new connections CSV file hasn't been updated for ${connectionDiff} days.<br>`;
    }

    // If either of the files is outdated, send an email
    if (emailMessage) {
      await sendEmail({
        service: "outlook", // Or "gmail", depending on your preference
        to: process.env.ADMIN_EMAIL, // Admin email from .env
        subject: emailSubject,
        html: `<p>${emailMessage}</p>`,
      });
      console.log("Reminder email sent successfully.");
    } else {
      console.log("All CSV files are up-to-date.");
    }
  } catch (error) {
    console.error("Error in CSV update check:", error);
  }
});