const express = require("express");
const mongoose = require("mongoose");
const csvParser = require("csv-parser");
const cors = require("cors");
const path = require('path');
const bodyParser = require("body-parser");
const moment = require("moment"); // Import moment.js for date parsing
const multer = require("multer"); // Import multer for handling file uploads
require("dotenv").config(); // Load environment variables from .env file
//sirimanne
const cron = require("node-cron");
const nodemailer = require("nodemailer");
const { Readable } = require("stream");
const app = express();

// Middleware for CORS and JSON body parsing
app.use(cors());
app.use(bodyParser.json()); // JSON parser (added body-parser before routes)
app.use(bodyParser.urlencoded({ extended: true }));
const authRoutes = require("./routes/authRoutes"); // Adjust if path differs
// Routes
app.use("/api/auth", authRoutes); // Corrected route import
// Increase JSON body parsing limit to handle large payloads
app.use(express.json()); // Parses incoming JSON requests (IMPORTANT)
app.use(bodyParser.json({ limit: "350mb" }));
app.use(bodyParser.urlencoded({ limit: "350mb", extended: true }));

// Setup multer to handle file uploads
const storage = multer.memoryStorage(); // Store the file in memory
const upload = multer({
  storage: storage,
  limits: { fileSize: 350 * 1024 * 1024 }, // 350MB max file size
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
    const requiredHeaders = [
      "Rto_Split",
      "Category",
      "Month, Day, Year of Service Order Status Updated Dtm",
      "Oss Service Order Type",
      "Order Type",
      "Order Sub Type",
      "Order Line Oss Service Type",
    ];

    const replaceData = req.query.replaceData === "true"; // Flag to choose mode

    if (!req.file) {
      return res.status(400).send("No file uploaded");
    }

    const results = [];
    const headersValidated = new Set();

    // Parse CSV from memory (Buffer)
    const csvData = req.file.buffer.toString(); // Convert buffer to string
    const readableStream = require("stream").Readable.from(csvData);

    readableStream
  .pipe(csvParser())
  .on("headers", (headers) => {
    // Normalize headers: Remove spaces and make lowercase
    const normalizedHeaders = headers.map((header) =>
      header.trim().replace(/\s+/g, "_").toLowerCase()
    );
    const normalizedRequiredHeaders = requiredHeaders.map((header) =>
      header.trim().replace(/\s+/g, "_").toLowerCase()
    );

    console.log("Headers from CSV:", headers); // Debug: Actual headers from the CSV file
    console.log("Normalized Headers:", normalizedHeaders); // Debug: Normalized headers
    console.log("Normalized Required Headers:", normalizedRequiredHeaders); // Debug: Expected normalized headers

    // Validate if all required headers are present
    const missingHeaders = normalizedRequiredHeaders.filter(
      (header) => !normalizedHeaders.includes(header)
    );

    if (missingHeaders.length > 0) {
      res
        .status(400)
        .send(`Missing required headers: ${missingHeaders.join(", ")}`);
      readableStream.destroy(); // Stop processing further
    } else {
      headersValidated.add(true);
    }
  })
  .on("data", (data) => {
    if (!headersValidated.size) return; // Skip processing if headers are invalid
  
    // Use moment.js to parse the date string correctly and extract the month
    const parsedDate = moment(
      data["Month, Day, Year of Service Order Status Updated Dtm"], // Updated field name
      "MMMM D, YYYY"
    )
      .utc()
      .toDate();
  
    const month = parsedDate.getMonth(); // Extract the month (0-based index)
  
    // Push only relevant fields
    results.push({
      rto_split: data["Rto_Split"] || data["Rto Split"], // Handle variations
      category: data.Category,
      date: parsedDate, // Save parsed date
      month: month, // Store the month as a number (0-11)
      oss_service_order_type: data["Oss Service Order Type"],
      order_type: data["Order Type"],
      order_sub_type: data["Order Sub Type"],
      order_line_oss_service_type: data["Order Line Oss Service Type"],
    });
  })
      .on("end", async () => {
        try {
          if (!headersValidated.size) return; // Do nothing if headers are invalid

          if (replaceData) {
            // Replace mode: Clear the database and insert new data
            await Order.deleteMany({});
            await Order.insertMany(results);
            res.send("CSV data replaced successfully.");
          } else {
            // Incremental mode: Insert only new records
            const existingDates = (
              await Order.find({}, { date: 1 }).exec()
            ).map((order) => order.date.getTime()); // Get all existing dates as timestamps

            const newRecords = results.filter(
              (record) => !existingDates.includes(record.date.getTime())
            );

            if (newRecords.length > 0) {
              await Order.insertMany(newRecords);
              res.send(
                `CSV data uploaded successfully. Added ${newRecords.length} new records.`
              );
            } else {
              res.send("No new data to insert.");
            }
          }
        } catch (err) {
          console.error("Error uploading CSV data:", err);
          res.status(500).send("Error uploading CSV data");
        }
      })
      .on("error", (err) => {
        console.error("CSV parsing error:", err);
        res.status(500).send("Error parsing CSV");
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
    const requiredHeaders = [
      "ORDER_LINE_RTO_AREA",
      "Month, Day, Year of Churn Date (SOSUD)", // Updated
      "ACCOUNT_NUM",
      "Month, Day, Year of DSP", // Updated
      "ORDER_LINE_OSS_SERVICE_TYPE",
      "BSS_TARIFF_NAME",
      "Deleted_method",
      "CUSTOMER_TYPE",
    ];

    try {
      const csvData = req.file.buffer.toString();
      const readableStream = Readable.from(csvData); // Use Readable.from

      readableStream
        .pipe(
          csvParser({
            mapHeaders: ({ header }) => header.trim(),
          })
        )
        .on("data", (data) => {
          try {
            // Validate required headers
            const record = {};
            requiredHeaders.forEach((header) => {
              if (!data[header]) {
                console.warn(`Missing field: ${header} in row, skipping row.`);
                return;
              }
              record[header] = data[header];
            });

            // Parse dates
          // Parse dates
const momentChurnDate = moment.utc(
  record["Month, Day, Year of Churn Date (SOSUD)"], // Updated
  ["MMMM D, YYYY", "MMMM D, YY"], // Supported formats
  true
);

const momentActivatedDate = moment.utc(
  record["Month, Day, Year of DSP"], // Updated
  ["MMMM D, YYYY", "MMMM D, YY"], // Supported formats
  true
);

            if (!momentChurnDate.isValid() || !momentActivatedDate.isValid()) {
              console.warn("Invalid date format, skipping row:", record);
              return;
            }

            // Standardize deleted_method
            let standardizedDeletedMethod = record["Deleted_method"];
            if (["Promotion Upgrade", "Promotion Downgrade"].includes(standardizedDeletedMethod)) {
              standardizedDeletedMethod = "Customer Requested";
            }

            // Add to results
            results.push({
              order_line_rto_area: record["ORDER_LINE_RTO_AREA"],
              churn_date: momentChurnDate.toDate(),
              account_num: record["ACCOUNT_NUM"],
              activated_date: momentActivatedDate.toDate(),
              order_line_oss_service_type: record["ORDER_LINE_OSS_SERVICE_TYPE"],
              bss_tariff_name: record["BSS_TARIFF_NAME"],
              deleted_method: standardizedDeletedMethod,
              customer_type: record["CUSTOMER_TYPE"],
            });
          } catch (parseError) {
            console.error("Error processing row:", parseError.message);
          }
        })
        .on("end", async () => {
          try {
            console.log(`Total valid rows processed: ${results.length}`);
            if (results.length === 0) {
              return res.status(400).send("No valid records found in the uploaded file.");
            }

            if (replaceData) {
              console.log("Replacing all existing disconnection records...");
              await Disconnection.deleteMany({});
              await Disconnection.insertMany(results);
              res.send("Disconnection data replaced successfully.");
            } else {
              console.log("Performing incremental insertion...");
              const lastSaved = await Disconnection.findOne()
                .sort({ churn_date: -1 })
                .select("churn_date");

              const lastSavedDate = lastSaved ? lastSaved.churn_date : new Date(0);

              const newData = results.filter((item) =>
                moment(item.churn_date).isAfter(lastSavedDate)
              );

              if (newData.length > 0) {
                console.log(`Inserting ${newData.length} new records...`);
                await Disconnection.insertMany(newData);
                res.send("New disconnection records inserted successfully.");
              } else {
                res.send("No new disconnection data to insert.");
              }
            }
          } catch (dbError) {
            console.error("Database error:", dbError.message);
            res.status(500).send("Error saving disconnection data to the database.");
          }
        })
        .on("error", (error) => {
          console.error("Error reading CSV:", error.message);
          res.status(500).send("Error reading the uploaded file.");
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
// ==========================================
//   BACKEND CODE WITH DURATION FILTER ADDED
// ==========================================

const getCategorizedCounts = async (filters = {}) => {
  let {
    year,
    month,
    day,
    order_line_rto_area,
    deleted_method,
    dgm,
    gm,
    duration,
  } = filters;

  // Convert comma-separated strings into arrays
  year = typeof year === "string" ? year.split(",").map(Number) : year;
  month = typeof month === "string" ? month.split(",").map(Number) : month;
  day = typeof day === "string" ? day.split(",").map(Number) : day;
  order_line_rto_area = typeof order_line_rto_area === "string" ? order_line_rto_area.split(",") : order_line_rto_area;
  dgm = typeof dgm === "string" ? dgm.split(",") : dgm;
  gm = typeof gm === "string" ? gm.split(",") : gm;
  duration = typeof duration === "string" ? duration.split(",") : duration;

  // Static mapping for ORDER_LINE_RTO_AREA to DGM and GM
  const mapping = {
    "RTO - AD": { DGM: "NP", GM: "REGION 3" },
    "RTO - AG": { DGM: "WPS", GM: "REGION 2" },
    "RTO - AP": { DGM: "EP", GM: "REGION 3" },
    "RTO - BC": { DGM: "EP", GM: "REGION 3" },
    "RTO - BD": { DGM: "SAB-UVA", GM: "REGION 2" },
    "RTO - BW": { DGM: "SAB-UVA", GM: "REGION 2" },
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
    "RTO - KE": { DGM: "SAB-UVA", GM: "REGION 2" },
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
    "RTO - MRG": { DGM: "SAB-UVA", GM: "REGION 2" },
    "RTO - MT": { DGM: "CP", GM: "REGION 1" },
    "RTO - ND": { DGM: "METRO 2", GM: "METRO" },
    "RTO - NG": { DGM: "WPN", GM: "REGION 1" },
    "RTO - NTB": { DGM: "WPN", GM: "REGION 1" },
    "RTO - NW": { DGM: "CP", GM: "REGION 1" },
    "RTO - PH": { DGM: "WPS", GM: "REGION 2" },
    "RTO - PR": { DGM: "EP", GM: "REGION 3" },
    "RTO - RM": { DGM: "METRO 2", GM: "METRO" },
    "RTO - RN": { DGM: "SAB-UVA", GM: "REGION 2" },
    "RTO - TC": { DGM: "EP", GM: "REGION 3" },
    "RTO - VA": { DGM: "NP", GM: "REGION 3" },
    "RTO - WT": { DGM: "WPN", GM: "REGION 1" },
    "RTO - YK": { DGM: "METRO 2", GM: "METRO" },
  };

  // Define duration filter conditions
  const durationFilters = {
    "below 1 year": { $lt: ["$durationInYears", 1] },
    "1 year to 2 years": { $and: [{ $gte: ["$durationInYears", 1] }, { $lt: ["$durationInYears", 2] }] },
    "2 years to 3 years": { $and: [{ $gte: ["$durationInYears", 2] }, { $lt: ["$durationInYears", 3] }] },
    "3 years to 4 years": { $and: [{ $gte: ["$durationInYears", 3] }, { $lt: ["$durationInYears", 4] }] },
    "4 years to 5 years": { $and: [{ $gte: ["$durationInYears", 4] }, { $lt: ["$durationInYears", 5] }] },
    "more than 5 years": { $gte: ["$durationInYears", 5] },
  };

  const matchStage = { $and: [] };

  if (year?.length) {
    matchStage.$and.push({
      $expr: { $in: [{ $year: "$churn_date" }, year] },
    });
  }

  // Add global filters for order_line_rto_area
  if (order_line_rto_area?.length) {
    matchStage.$and.push({ order_line_rto_area: { $in: order_line_rto_area } });
  }

  // Handle deleted_method filtering dynamically
  // Apply filters for order_line_rto_area
if (order_line_rto_area?.length) {
  matchStage.$and.push({ order_line_rto_area: { $in: order_line_rto_area } });
}

// Apply simplified filter for deleted_method
if (deleted_method) {
  matchStage.$and.push({ deleted_method });
}

  // Execute aggregation pipeline
  const results = await Disconnection.aggregate([
    // Apply general filters (order_line_rto_area, etc.)
    { $match: matchStage.$and.length > 0 ? matchStage : {} },
  
    // Add fields to categorize deleted_method
    {
      $addFields: {
        isCustomerRequested: {
          $in: ["$deleted_method", ["Customer Requested", "Promotion Upgrade", "Promotion Downgrade"]],
        },
        isNonPayment: { $eq: ["$deleted_method", "Non Payment"] },
      },
    },
  
    // Add DGM and GM fields using the mapping
    {
      $addFields: {
        DGM: {
          $let: {
            vars: {
              map: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: { $objectToArray: mapping },
                      as: "m",
                      cond: { $eq: ["$order_line_rto_area", "$$m.k"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$map.v.DGM",
          },
        },
        GM: {
          $let: {
            vars: {
              map: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: { $objectToArray: mapping },
                      as: "m",
                      cond: { $eq: ["$order_line_rto_area", "$$m.k"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$map.v.GM",
          },
        },
        GM: {
          $let: {
            vars: {
              map: {
                $arrayElemAt: [
                  {
                    $filter: {
                      input: { $objectToArray: mapping },
                      as: "m",
                      cond: { $eq: ["$order_line_rto_area", "$$m.k"] },
                    },
                  },
                  0,
                ],
              },
            },
            in: "$$map.v.GM",
          },
        },
      },
    },

    // Apply DGM and GM filters if provided (moved here after DGM and GM are added)
    ...(dgm?.length || gm?.length
      ? [
          {
            $match: {
              $or: [
                ...(dgm?.length ? [{ DGM: { $in: dgm } }] : []),
                ...(gm?.length ? [{ GM: { $in: gm } }] : []),
              ],
            },
          },
        ]
      : []),

    // Add durationInYears for each record
    {
      $addFields: {
        durationInYears: {
          $divide: [{ $subtract: ["$churn_date", "$activated_date"] }, 1000 * 60 * 60 * 24 * 365],
        },
      },
    },

    // Apply duration filter globally at this stage
    ...(duration?.length
      ? [
          {
            $match: {
              $expr: {
                $or: duration.map((dur) => durationFilters[dur]),
              },
            },
          },
        ]
      : []),

    // Group records by ACCOUNT_NUM
    {
      $group: {
        _id: "$account_num",
        records: { $push: "$$ROOT" },
      },
    },

    // Add fields for disconnection conditions
    {
      $addFields: {
        hasCopperDisconnection: {
          $and: [
            { $in: ["AB-CAB", "$records.order_line_oss_service_type"] },
            { $in: ["E-IPTV COPPER", "$records.order_line_oss_service_type"] },
          ],
        },
        hasFiberDisconnection: {
          $and: [
            { $in: ["AB-FTTH", "$records.order_line_oss_service_type"] },
            { $in: ["E-IPTV FTTH", "$records.order_line_oss_service_type"] },
          ],
        },
        filteredRecords: {
          $filter: {
            input: "$records",
            as: "record",
            cond: {
              $and: [
                year?.length ? { $in: [{ $year: "$$record.churn_date" }, year] } : {},
                month?.length ? { $in: [{ $month: "$$record.churn_date" }, month] } : {},
                day?.length ? { $in: [{ $dayOfMonth: "$$record.churn_date" }, day] } : {},
              ],
            },
          },
        },
        eIptvRecords: {
          $filter: {
            input: "$records",
            as: "record",
            cond: {
              $regexMatch: {
                input: "$$record.order_line_oss_service_type",
                regex: /^E-IPTV/,
                options: "i",
              },
            },
          },
        },
      },
    },

    // Count disconnections based on conditions
    {
      $addFields: {
        categorizedCopper: {
          $cond: [
            "$hasCopperDisconnection",
            {
              $size: {
                $filter: {
                  input: "$filteredRecords",
                  as: "record",
                  cond: {
                    $eq: ["$$record.order_line_oss_service_type", "E-IPTV COPPER"],
                  },
                },
              },
            },
            0,
          ],
        },
        categorizedFiber: {
          $cond: [
            "$hasFiberDisconnection",
            {
              $size: {
                $filter: {
                  input: "$filteredRecords",
                  as: "record",
                  cond: {
                    $eq: ["$$record.order_line_oss_service_type", "E-IPTV FTTH"],
                  },
                },
              },
            },
            0,
          ],
        },
        categorizedOnlyPeotv: {
          $cond: [
            {
              $and: [
                { $not: "$hasCopperDisconnection" },
                { $not: "$hasFiberDisconnection" },
              ],
            },
            {
              $size: {
                $filter: {
                  input: "$filteredRecords",
                  as: "record",
                  cond: {
                    $or: [
                      { $eq: ["$$record.order_line_oss_service_type", "E-IPTV COPPER"] },
                      { $eq: ["$$record.order_line_oss_service_type", "E-IPTV FTTH"] },
                    ],
                  },
                },
              },
            },
            0,
          ],
        },
      },
    },

    // Summarize the counts
    {
      $group: {
        _id: null,
        total_peotv_with_copper: { $sum: "$categorizedCopper" },
        total_peotv_with_fiber: { $sum: "$categorizedFiber" },
        total_only_peotv: { $sum: "$categorizedOnlyPeotv" },
      },
    },

    // Calculate the total
    {
      $project: {
        _id: 0,
        total_peotv_with_copper: 1,
        total_peotv_with_fiber: 1,
        total_only_peotv: 1,
        total: {
          $add: [
            "$total_peotv_with_copper",
            "$total_peotv_with_fiber",
            "$total_only_peotv",
          ],
        },
      },
    },
  ]);

  return results[0] || {};
};
app.get("/get-counts-by-category-and-rto", async (req, res) => {
  const { year, month, day } = req.query;

  // Initial match stage with fixed category filter
  const matchStage = {
    category: "PEO DP BB Up_30K", // Fixed category filter
  };

  // Add filters based on provided query parameters
  if (year) {
    matchStage["$expr"] = { $eq: [{ $year: "$date" }, Number(year)] };
  }

  const pipeline = [
    { $match: matchStage }, // Apply category and date filters
    {
      $project: {
        rto_split: 1,
        category: 1,
        year: { $year: "$date" },
        month: { $month: "$date" },
        day: { $dayOfMonth: "$date" },
      },
    },
  ];

  // Apply the month filter
  if (month && month !== "all") {
    pipeline.push({
      $match: {
        month: Number(month),
      },
    });
  }

  // Apply the day filter
  if (day && day !== "all") {
    pipeline.push({
      $match: {
        day: Number(day),
      },
    });
  }

  // Group by rto_split, year, month, day
  pipeline.push(
    {
      $group: {
        _id: {
          rto_split: "$rto_split",
          year: "$year",
          month: "$month",
          day: "$day",
        },
        count: { $sum: 1 },
      },
    },
    { $sort: { "_id.year": 1, "_id.month": 1, "_id.day": 1 } } // Sort by year, month, day
  );

  try {
    const counts = await Order.aggregate(pipeline); // Run aggregation pipeline
    res.json(counts); // Return the aggregated counts
  } catch (err) {
    console.error("Error fetching counts:", err);
    res.status(500).send("Error fetching counts");
  }
});
// =============================================================
//  ROUTE TO FETCH THE CATEGORIZED COUNTS WITH ALL FILTERS
// =============================================================
app.get("/disconnection-counts", async (req, res) => {
  try {
    // 5) Include the duration in query destructuring:
    const {
      year,
      month,
      day,
      order_line_rto_area,
      deleted_method,
      dgm,
      gm,
      duration, // NEW: add duration here
    } = req.query;

    const filters = {
      year,
      month,
      day,
      order_line_rto_area,
      deleted_method,
      dgm,
      gm,
      duration, // NEW: pass duration to filters
    };

    const counts = await getCategorizedCounts(filters);
    res.status(200).json(counts);
  } catch (error) {
    console.error("Error fetching categorized counts:", error.message);
    res.status(500).send("Error fetching disconnection counts.");
  }
});

// ==========================================
// New Route: /monthlyCounts for all 12 months
// ==========================================
app.get("/monthlyCounts", async (req, res) => {
  try {
    const { year, ...filters } = req.query;

    // Parse the year parameter into an array of numbers
    const parsedYears = typeof year === "string" ? year.split(",").map(Number) : [];

    const monthlyCounts = [];

    // Loop through each year and month to get the counts
    for (const currentYear of parsedYears) {
      for (let month = 1; month <= 12; month++) {
        const counts = await getCategorizedCounts({
          ...filters,
          year: [currentYear], // Pass the current year
          month: [month], // Pass the current month
        });

        monthlyCounts.push({
          year: currentYear, // Include the year in the response
          month,
          total_peotv_with_copper: counts.total_peotv_with_copper || 0,
          total_peotv_with_fiber: counts.total_peotv_with_fiber || 0,
          total_only_peotv: counts.total_only_peotv || 0,
        });
      }
    }

    res.status(200).json({ monthlyCounts });
  } catch (error) {
    console.error("Error fetching monthly disconnection counts:", error.message);
    res.status(500).json({ error: "Internal Server Error" });
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

// CRON job to check for updates every minute (for testing)
cron.schedule("0 11 * * *", async () => {
  console.log("Running CSV update check...");

  try {
    const disconnectionUpdate = await Disconnection.findOne().sort({ churn_date: -1 }).select("churn_date");
    const connectionUpdate = await Order.findOne().sort({ date: -1 }).select("date");

    const now = moment(); // Current date
    const disconnectionLastUpdate = disconnectionUpdate ? moment(disconnectionUpdate.churn_date) : null;
    const connectionLastUpdate = connectionUpdate ? moment(connectionUpdate.date) : null;

    const disconnectionDiff = disconnectionLastUpdate ? now.diff(disconnectionLastUpdate, "days") : null;
    const connectionDiff = connectionLastUpdate ? now.diff(connectionLastUpdate, "days") : null;

    const emailSubject = "Reminder: CSV File Update Required";
    let emailMessage = "";

    if (disconnectionDiff !== null && disconnectionDiff >= 3) {
      emailMessage += `<li>The <strong>Disconnection CSV file</strong> hasn't been updated for <strong>${disconnectionDiff} days</strong>.</li>`;
    }

    if (connectionDiff !== null && connectionDiff >= 3) {
      emailMessage += `<li>The <strong>New Connections CSV file</strong> hasn't been updated for <strong>${connectionDiff} days</strong>.</li>`;
    }

    if (emailMessage) {
      const styledEmail = `
        <div style="font-family: Arial, sans-serif; color: #333; padding: 20px;">
          <div style="border-bottom: 2px solid #0073e6; padding-bottom: 10px;">
            <h2 style="color: #0073e6;">PEO TV - Operations Team</h2>
            <p style="font-size: 14px;">Sri Lanka Telecom PLC</p>
          </div>
          <p style="margin-top: 20px;">Dear Team,</p>
          <p>Please be informed of the following:</p>
          <ul style="background-color: #f9f9f9; padding: 15px; border-radius: 5px;">
            ${emailMessage}
          </ul>
          <p>Please update the necessary CSV files promptly to avoid any service interruptions.</p>
          <p>For assistance, please contact the IT Support Desk.</p>
          <p style="margin-top: 20px;">Kind Regards,</p>
          <p><strong>PEO TV Monitoring Team</strong></p>
          <div style="border-top: 2px solid #0073e6; margin-top: 20px; padding-top: 10px;">
            <p style="font-size: 12px; color: #666;">This is an automated email. Please do not reply to this message.</p>
          </div>
        </div>
      `;

      await sendEmail({
        service: "gmail", // Use "gmail" if you're testing Gmail
        to: process.env.ADMIN_EMAIL, // Use the admin email for testing
        subject: emailSubject,
        html: styledEmail,
      });
      console.log("Reminder email sent successfully.");
    } else {
      console.log("All CSV files are up-to-date.");
    }
  } catch (error) {
    console.error("Error in CSV update check:", error);
  }
});
// Email transporter for Gmail
const getEmailTransporter = (service) => {
  let transporterConfig;

  if (service === "gmail") {
    transporterConfig = {
      host: "smtp.gmail.com",
      port: 587,
      secure: false, // Use TLS
      auth: {
        user: process.env.GMAIL_USER,
        pass: process.env.GMAIL_PASS,
      },
      family: 4, // Force IPv4
    };
  } else {
    throw new Error("Invalid email service selected");
  }

  return nodemailer.createTransport(transporterConfig);
};


// Function to send email
// Function to send email
const sendEmail = async ({ service, to, subject, text, html }) => {
  try {
    const transporter = getEmailTransporter(service);
    const mailOptions = {
      from: process.env.GMAIL_USER, // Ensure this is correct for Gmail
      to,
      subject,
      text,
      html,
    };

    const info = await transporter.sendMail(mailOptions);
    console.log(`Email sent: ${info.messageId}`);
    return info;
  } catch (error) {
    console.error("Error sending email:", error.message);
    throw new Error("Failed to send email");
  }
};

// Test route for manual testing
app.get("/test-email", async (req, res) => {
  try {
    await sendEmail({
      service: "gmail",
      to: "tashiperera00@gmail.com", // Your email to receive the test
      subject: "Test Email Notification",
      html: `
        <h3>Hello from the CSV Reminder System!</h3>
        <p>This is a test email to check how the notification will appear.</p>
        <p>Here's how your actual reminder might look:</p>
        <ul>
          <li>The <strong>disconnection</strong> CSV hasn't been updated for 3+ days.</li>
          <li>The <strong>new connection</strong> CSV hasn't been updated for 3+ days.</li>
        </ul>
        <p>Please ensure to upload the missing files to avoid service issues.</p>
        <p>Kind regards,<br> Your Report System</p>
      `,
    });
    res.send("Test email sent successfully!");
  } catch (error) {
    console.error("Error sending test email:", error);
    res.status(500).send("Failed to send test email.");
  }
});
module.exports = { sendEmail };
// Serve static files from the React app
app.use(express.static(path.join(__dirname, 'dashboard-app/build')));

// Handle API routes
app.get('/api/data', (req, res) => {
  res.json({ message: 'Hello from the backend!' });
});

// Catch all other routes and return the React app
app.get('*', (req, res) => {
  res.sendFile(path.join(__dirname, 'dashboard-app/build', 'index.html'));
});
// Start the server
const PORT = process.env.PORT || 8070;
app.listen(PORT, () => {
  console.log(`Server running on http://localhost:${PORT}`);
});