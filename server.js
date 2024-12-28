const express = require("express");
const mongoose = require("mongoose");
const csvParser = require("csv-parser");
const cors = require("cors");
const bodyParser = require("body-parser");
const moment = require("moment"); // Import moment.js for date parsing
const multer = require("multer"); // Import multer for handling file uploads
require("dotenv").config(); // Load environment variables from .env file
//sirimanne
const app = express();
app.use(cors());

// Increase JSON body parsing limit to handle large payloads
app.use(bodyParser.json({ limit: "50mb" }));
app.use(bodyParser.urlencoded({ limit: "50mb", extended: true }));

// Setup multer to handle file uploads
const storage = multer.memoryStorage(); // Store the file in memory
const upload = multer({
  storage: storage,
  limits: { fileSize: 50 * 1024 * 1024 }, // 50MB max file size
}); // Create the multer upload instance

// MongoDB connection
mongoose
  .connect(process.env.MONGODB_URI, {
    useNewUrlParser: true,
    useUnifiedTopology: true,
  })
  .then(() => {
    console.log("MongoDB connected successfully");
  })
  .catch((err) => {
    console.error("MongoDB connection error:", err);
  });

// Order schema
const orderSchema = new mongoose.Schema({
  rto_split: String,
  category: String,
  date: Date,
  month: Number, // Add a field to store the month
  oss_service_order_type: String,
  order_type: String,
  order_sub_type: String,
  order_line_oss_service_type: String,
});

const Order = mongoose.model("Order", orderSchema);

const disconnectionSchema = new mongoose.Schema({
  order_line_rto_area: String,
  churn_date: Date, // Date of Churn
  account_num: String,
  activated_date: Date, // Date of Activation
  order_line_oss_service_type: String,
  bss_tariff_name: String,
  deleted_method: String,
  customer_type: String,
});

const Disconnection = mongoose.model("Disconnection", disconnectionSchema);

// Endpoint to upload CSV

app.post(
  "/upload-csv",
  upload.single("sales_details_2024"),
  async (req, res) => {
    const replaceData = req.query.replaceData === "true"; // Check if full replace is requested

    if (!req.file) {
      return res.status(400).send("No file uploaded");
    }

    const results = [];

    // Parse CSV from memory (Buffer)
    const csvData = req.file.buffer.toString(); // Convert buffer to string
    const readableStream = require("stream").Readable.from(csvData);

    readableStream
      .pipe(csvParser())
      .on("data", (data) => {
        // Use moment.js to parse the date string correctly and extract the month
        const parsedDate = moment(data.date, "MMMM D, YYYY").utc().toDate(); // Parse the date
        const month = parsedDate.getMonth(); // Extract the month (0-based index)

        results.push({
          rto_split: data.Rto_Split, // Mapping 'Rto Split' column from CSV
          category: data.Category,
          date: parsedDate, // Store the full Date object
          month: month, // Store the month as a number (0-11)
          oss_service_order_type: data["Oss Service Order Type"],
          order_type: data["Order Type"],
          order_sub_type: data["Order Sub Type"],
          order_line_oss_service_type: data["Order Line Oss Service Type"],
        });
      })
      .on("end", async () => {
        try {
          if (replaceData) {
            // Delete all existing records before inserting new ones (Full Replace)
            await Order.deleteMany({});
          } else {
            // Only insert data after the last saved date
            const lastSavedDate = await Order.findOne()
              .sort({ date: -1 })
              .select("date"); // Get the most recent date
            const lastSaved = lastSavedDate ? lastSavedDate.date : new Date(0); // Default to Unix epoch if no data

            // Filter new data to insert only records after the last saved date
            const newData = results.filter((item) =>
              moment(item.date).isAfter(lastSaved)
            );

            if (newData.length > 0) {
              await Order.insertMany(newData);
            } else {
              res.send("No new data to insert.");
              return;
            }
          }

          res.send("CSV data uploaded successfully");
        } catch (err) {
          console.error("Error uploading CSV data:", err);
          res.status(500).send("Error uploading CSV data");
        }
      });
  }
);

app.post(
  "/upload-disconnection-csv",
  upload.single("disconnection_file"),
  async (req, res) => {
    const replaceData = req.query.replaceData === "true"; // Check if full replace is requested

    if (!req.file) {
      return res.status(400).send("No file uploaded.");
    }

    const results = [];

    try {
      const csvData = req.file.buffer.toString(); // Convert buffer to string
      const readableStream = require("stream").Readable.from(csvData);

      readableStream
        .pipe(
          csvParser({
            mapHeaders: ({ header }) => header.trim(), // Trim spaces around headers
          })
        )
        .on("data", (data) => {
          try {
            let churnDate;
            let activatedDate;

            // First, try parsing with 4-digit year format
            let momentChurnDate = moment.utc(
              data["Churn_Date"],
              "MMMM D, YYYY",
              true
            );
            let momentActivatedDate = moment.utc(
              data["Activated_Date"],
              "MMMM D, YYYY",
              true
            );

            // If 4-digit parsing fails, try parsing with 2-digit year format
            if (!momentChurnDate.isValid()) {
              momentChurnDate = moment.utc(
                data["Churn_Date"],
                "MMMM D, YY",
                true
              );
            }
            if (!momentActivatedDate.isValid()) {
              momentActivatedDate = moment.utc(
                data["Activated_Date"],
                "MMMM D, YY",
                true
              );
            }

            // Skip rows with invalid dates
            if (!momentChurnDate.isValid()) {
              console.warn(
                `Invalid Churn_Date: '${data["Churn_Date"]}' - Skipping row.`
              );
              return;
            }
            if (!momentActivatedDate.isValid()) {
              console.warn(
                `Invalid Activated_Date: '${data["Activated_Date"]}' - Skipping row.`
              );
              return;
            }

            churnDate = momentChurnDate.toDate();
            activatedDate = momentActivatedDate.toDate();

            // Push parsed and valid data to results array
            results.push({
              order_line_rto_area: data["ORDER_LINE_RTO_AREA"],
              churn_date: churnDate,
              account_num: data["ACCOUNT_NUM"],
              activated_date: activatedDate,
              order_line_oss_service_type: data["ORDER_LINE_OSS_SERVICE_TYPE"],
              bss_tariff_name: data["BSS_TARIFF_NAME"],
              deleted_method: data["Deleted_method"],
              customer_type: data["CUSTOMER_TYPE"],
            });
          } catch (parseError) {
            console.error(
              "Error parsing row:",
              parseError.message,
              "Row:",
              data
            );
          }
        })
        .on("end", async () => {
          try {
            console.log(`Total valid rows processed: ${results.length}`);

            if (replaceData) {
              console.log("Replacing all existing disconnection records...");
              await Disconnection.deleteMany({});
              await Disconnection.insertMany(results);
              return res.send("Disconnection data replaced successfully.");
            } else {
              console.log("Performing incremental insertion...");
              try {
                // Fetch the most recent churn_date from the database
                const lastSaved = await Disconnection.findOne()
                  .sort({ churn_date: -1 }) // Sort in descending order to get the latest record
                  .select("churn_date"); // Retrieve only the churn_date field

                const lastSavedDate = lastSaved
                  ? lastSaved.churn_date
                  : new Date(0); // Default to epoch if no records

                console.log("Last saved churn date in the database:", lastSavedDate);

                // Filter results: only include rows with churn_date after lastSavedDate
                const newData = results.filter((item) =>
                  moment(item.churn_date).isAfter(lastSavedDate)
                );

                if (newData.length > 0) {
                  console.log(`Inserting ${newData.length} new records...`);
                  await Disconnection.insertMany(newData); // Insert filtered data
                  return res.send(
                    "New disconnection records inserted successfully."
                  );
                } else {
                  console.log("No new records to insert. All data is up-to-date.");
                  return res.send("No new disconnection data to insert.");
                }
              } catch (dbError) {
                console.error(
                  "Database error during incremental insertion:",
                  dbError.message
                );
                return res
                  .status(500)
                  .send(
                    "Error checking or saving disconnection data to the database."
                  );
              }
            }
          } catch (dbError) {
            console.error("Database error:", dbError.message);
            res
              .status(500)
              .send("Error saving disconnection data to the database.");
          }
        });
    } catch (error) {
      console.error("Error processing CSV file:", error.message);
      res.status(500).send("Error processing the uploaded file.");
    }
  }
);

// Aggregation endpoint for counts grouped by category, oss_service_order_type, and month
app.get("/get-counts", async (req, res) => {
  const { year, month, day } = req.query; // Extract filters from query params

  const matchStage = {};

  // Apply year filter
  if (year) {
    matchStage["$expr"] = { $eq: [{ $year: "$date" }, Number(year)] };
  }

  // Apply month filter (if provided)
  if (month && month !== "all") {
    matchStage["$expr"] = {
      ...matchStage["$expr"],
      $and: [
        ...(matchStage["$expr"]?.$and || []),
        { $eq: [{ $month: "$date" }, Number(month)] },
      ],
    };
  }

  // Apply day filter (if provided)
  if (day && day !== "all") {
    matchStage["$expr"] = {
      ...matchStage["$expr"],
      $and: [
        ...(matchStage["$expr"]?.$and || []),
        { $eq: [{ $dayOfMonth: "$date" }, Number(day)] },
      ],
    };
  }

  try {
    const counts = await Order.aggregate([
      { $match: matchStage }, // Apply date filters
      {
        $project: {
          category: 1,
          oss_service_order_type: 1,
          year: { $year: "$date" },
          month: { $month: "$date" },
          day: { $dayOfMonth: "$date" },
          order_line_oss_service_type: 1,
        },
      },
      {
        $group: {
          _id: {
            category: "$category",
            oss_service_order_type: "$oss_service_order_type",
            year: "$year",
            month: "$month",
            day: "$day",
            order_line_oss_service_type: "$order_line_oss_service_type",
          },
          count: { $sum: 1 },
        },
      },
      { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } }, // Sort by year, month, day
    ]);

    res.json(counts); // Return the aggregated counts
  } catch (err) {
    console.error("Error fetching counts:", err);
    res.status(500).send("Error fetching counts");
  }
});

app.get("/get-peo-tv-counts", async (req, res) => {
  const { year, month, day, order_line_rto_area, deleted_method, duration, DGM, GM } = req.query;

  // Mapping for RTO areas to DGM and GM
  const rtoMapping = {
  "RTO - AD": { DGM: "NP", GM: "REGION 3" },
  "RTO - AG": { DGM: "WPS", GM: "REGION 2" },
  "RTO - AP": { DGM: "EP", GM: "REGION 3" },
  "RTO - BC": { DGM: "EP", GM: "REGION 3" },
  "RTO - BD": { DGM: "SAB & UVA", GM: "REGION 2" },
  "RTO - BW": { DGM: "SAB & UVA", GM: "REGION 2" },
  "RTO - CW": { DGM: "NWP", GM: "REGION 1" },
  "RTO - GL": { DGM: "SP", GM: "REGION 2" },
  "RTO - GP": { DGM: "CP", GM: "REGION 1" },
  "RTO - GQ": { DGM: "WPN", GM: "REGION 1" },
  "RTO - HB": { DGM: "SP", GM: "REGION 2" },
  "RTO - HK": { DGM: "METRO 1", GM: "METRO" },
  "RTO - HO": { DGM: "METRO 2", GM: "METRO" },
  "RTO - HR": { DGM: "WPS", GM: "REGION 2" },
  "RTO - HT": { DGM: "CP", GM: "REGION 1" },
  "RTO - JA": { DGM: "NP", GM: "REGION 3" },
  "RTO - KE": { DGM: "SAB & UVA", GM: "REGION 2" },
  "RTO - KG": { DGM: "NWP", GM: "REGION 1" },
  "RTO - KI": { DGM: "WPN", GM: "REGION 1" },
  "RTO - KL": { DGM: "EP", GM: "REGION 3" },
  "RTO - KLY": { DGM: "NWP", GM: "REGION 1" },
  "RTO - KO": { DGM: "NP", GM: "REGION 3" },
  "RTO - KON": { DGM: "METRO 1", GM: "METRO" },
  "RTO - KT": { DGM: "WPS", GM: "REGION 2" },
  "RTO - KX": { DGM: "METRO 1", GM: "METRO" },
  "RTO - KY": { DGM: "CP", GM: "REGION 1" },
  "RTO - MB": { DGM: "NP", GM: "REGION 3" },
  "RTO - MD": { DGM: "METRO 1", GM: "METRO" },
  "RTO - MH": { DGM: "SP", GM: "REGION 2" },
  "RTO - MLT": { DGM: "NP", GM: "REGION 2" },
  "RTO - MRG": { DGM: "SAB & UVA", GM: "REGION 2" },
  "RTO - MT": { DGM: "CP", GM: "REGION 1" },
  "RTO - ND": { DGM: "METRO 2", GM: "METRO" },
  "RTO - NG": { DGM: "WPN", GM: "REGION 1" },
  "RTO - NTB": { DGM: "WPN", GM: "REGION 1" },
  "RTO - NW": { DGM: "CP", GM: "REGION 1" },
  "RTO - PH": { DGM: "WPS", GM: "REGION 2" },
  "RTO - PR": { DGM: "EP", GM: "REGION 3" },
  "RTO - RM": { DGM: "METRO 2", GM: "METRO" },
  "RTO - RN": { DGM: "SAB & UVA", GM: "REGION 2" },
  "RTO - TC": { DGM: "EP", GM: "REGION 3" },
  "RTO - VA": { DGM: "NP", GM: "REGION 3" },
  "RTO - WT": { DGM: "WPN", GM: "REGION 1" },
};


  const matchStage = { $and: [] };

  // Filters based on Churn Date
  if (year)
    matchStage.$and.push({
      $expr: { $eq: [{ $year: "$churn_date" }, Number(year)] },
    });
  if (month)
    matchStage.$and.push({
      $expr: { $eq: [{ $month: "$churn_date" }, Number(month)] },
    });
  if (day)
    matchStage.$and.push({
      $expr: { $eq: [{ $dayOfMonth: "$churn_date" }, Number(day)] },
    });

  // Filters for order_line_rto_area and deleted_method
  if (order_line_rto_area) matchStage.$and.push({ order_line_rto_area });
  if (deleted_method) {
    if (deleted_method === "Customer Requested") {
      matchStage.$and.push({
        deleted_method: {
          $in: [
            "Customer Requested",
            "Promotion Downgrade",
            "Promotion Upgrade",
          ],
        },
      });
    } else if (deleted_method === "Non Payment") {
      matchStage.$and.push({ deleted_method: "Non Payment" });
    }
  }

  // DGM and GM Filters
  if (DGM || GM) {
    const filteredRTOAreas = Object.entries(rtoMapping)
      .filter(([area, mapping]) => (!DGM || mapping.DGM === DGM) && (!GM || mapping.GM === GM))
      .map(([area]) => area);

    if (filteredRTOAreas.length > 0) {
      matchStage.$and.push({ order_line_rto_area: { $in: filteredRTOAreas } });
    } else {
      return res.json({
        peo_tv_disconnections: 0,
        peo_tv_copper: 0,
        peo_tv_fiber: 0,
      });
    }
  }

  // Duration filter
  const durationFilters = {
    "below 1 year": { $lt: ["$durationInYears", 1] },
    "1 year to 2 years": {
      $and: [{ $gte: ["$durationInYears", 1] }, { $lt: ["$durationInYears", 2] }],
    },
    "2 years to 3 years": {
      $and: [{ $gte: ["$durationInYears", 2] }, { $lt: ["$durationInYears", 3] }],
    },
    "3 years to 4 years": {
      $and: [{ $gte: ["$durationInYears", 3] }, { $lt: ["$durationInYears", 4] }],
    },
    "4 years to 5 years": {
      $and: [{ $gte: ["$durationInYears", 4] }, { $lt: ["$durationInYears", 5] }],
    },
    "more than 5 years": { $gte: ["$durationInYears", 5] },
  };

  try {
    const counts = await Disconnection.aggregate([
      { $match: matchStage.$and.length > 0 ? matchStage : {} },

      // Group by account_num and collect oss_service_types
      {
        $group: {
          _id: "$account_num",
          oss_types: { $addToSet: "$order_line_oss_service_type" },
          total_occurrences: { $sum: 1 },
          churn_date: { $first: "$churn_date" },
          activated_date: { $first: "$activated_date" },
        },
      },

      // Add duration in years
      {
        $addFields: {
          durationInYears: {
            $divide: [
              { $subtract: ["$churn_date", "$activated_date"] },
              1000 * 60 * 60 * 24 * 365, // Convert milliseconds to years
            ],
          },
        },
      },

      // Apply duration filter if provided in query
      ...(duration && durationFilters[duration]
        ? [{ $match: { $expr: durationFilters[duration] } }]
        : []),

      // Classify by PEO categories
      {
        $addFields: {
          serviceType: {
            $switch: {
              branches: [
                {
                  case: {
                    $and: [
                      {
                        $setEquals: [
                          {
                            $setDifference: [
                              "$oss_types",
                              ["E-IPTV FTTH", "E-IPTV COPPER"],
                            ],
                          },
                          [],
                        ],
                      },
                      { $lte: ["$total_occurrences", 3] },
                    ],
                  },
                  then: "PEO Only",
                },
                {
                  case: {
                    $and: [
                      { $gt: ["$total_occurrences", 1] },
                      { $in: ["AB-CAB", "$oss_types"] },
                      { $in: ["E-IPTV COPPER", "$oss_types"] },
                    ],
                  },
                  then: "PEO with Copper",
                },
                {
                  case: {
                    $and: [
                      { $gt: ["$total_occurrences", 1] },
                      { $in: ["AB-FTTH", "$oss_types"] },
                      { $in: ["E-IPTV FTTH", "$oss_types"] },
                    ],
                  },
                  then: "PEO with Fibre",
                },
              ],
              default: null, // Exclude unknown service types
            },
          },
        },
      },

      // Filter out null or unknown service types
      {
        $match: {
          serviceType: { $ne: null },
        },
      },

      // Group by service type
      {
        $group: {
          _id: "$serviceType",
          count: { $sum: 1 },
        },
      },

      // Transform to final response format
      {
        $project: {
          peo_tv_disconnections: {
            $cond: [{ $eq: ["$_id", "PEO Only"] }, "$count", 0],
          },
          peo_tv_copper: {
            $cond: [{ $eq: ["$_id", "PEO with Copper"] }, "$count", 0],
          },
          peo_tv_fiber: {
            $cond: [{ $eq: ["$_id", "PEO with Fibre"] }, "$count", 0],
          },
        },
      },

      // Combine counts into a single document
      {
        $group: {
          _id: null,
          peo_tv_disconnections: { $sum: "$peo_tv_disconnections" },
          peo_tv_copper: { $sum: "$peo_tv_copper" },
          peo_tv_fiber: { $sum: "$peo_tv_fiber" },
        },
      },
      {
        $project: {
          _id: 0, // Remove _id from the output
          peo_tv_disconnections: 1,
          peo_tv_copper: 1,
          peo_tv_fiber: 1,
        },
      },
    ]);

    // Return the final result
    res.json(
      counts[0] || {
        peo_tv_disconnections: 0,
        peo_tv_copper: 0,
        peo_tv_fiber: 0,
      }
    );
  } catch (err) {
    console.error("Error fetching PEO TV counts:", err.message);
    res.status(500).send("Error fetching PEO TV counts");
  }
});

app.get("/get-last-update", async (req, res) => {
  try {
    // Get the most recent churn_date from the database
    const lastUpdate = await Disconnection.findOne()
      .sort({ churn_date: -1 })  // Sort by churn_date in descending order
      .select("churn_date")  // Only select the churn_date field
      .exec();

    // If no data exists, return a default message
    if (!lastUpdate) {
      return res.json({ lastUpdated: "No data available" });
    }

    // Convert to a readable format (e.g., using Moment.js or native JS)
    const lastUpdatedDate = moment(lastUpdate.churn_date).format("MMMM D, YYYY");

    res.json({ lastUpdated: lastUpdatedDate });
  } catch (error) {
    console.error("Error fetching last updated date:", error);
    res.status(500).json({ error: "Failed to fetch last updated date" });
  }
});
// Endpoint to get the last updated date from the database
app.get("/get-last-updated", async (req, res) => {
  try {
    // Find the most recent order based on the date
    const lastUpdated = await Order.findOne()
      .sort({ date: -1 }) // Sort by date in descending order
      .select("date"); // Only select the date field

    if (lastUpdated) {
      // Send the most recent date as a response
      res.json({ lastUpdated: lastUpdated.date });
    } else {
      res.status(404).send("No data available.");
    }
  } catch (err) {
    console.error("Error fetching last updated date:", err);
    res.status(500).send("Error fetching last updated date.");
  }
});


// Start the server
const PORT = process.env.PORT || 8070;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});