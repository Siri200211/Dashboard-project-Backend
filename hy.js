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

      // Categorize and Calculate Counts
      {
        $addFields: {
          isCopperDisconnection: {
            $and: [
              { $in: ["AB-CAB", "$records.order_line_oss_service_type"] },
              { $in: ["E-IPTV COPPER", "$records.order_line_oss_service_type"] },
            ],
          },
          isFiberDisconnection: {
            $and: [
              { $in: ["AB-FTTH", "$records.order_line_oss_service_type"] },
              { $in: ["E-IPTV FTTH", "$records.order_line_oss_service_type"] },
            ],
          },
          copperCount: {
            $cond: [
              {
                $and: [
                  { $in: ["AB-CAB", "$records.order_line_oss_service_type"] },
                  { $in: ["E-IPTV COPPER", "$records.order_line_oss_service_type"] },
                ],
              },
              {
                $size: {
                  $filter: {
                    input: "$records",
                    as: "record",
                    cond: { $eq: ["$$record.order_line_oss_service_type", "E-IPTV COPPER"] },
                  },
                },
              },
              0,
            ],
          },
          fiberCount: {
            $cond: [
              {
                $and: [
                  { $in: ["AB-FTTH", "$records.order_line_oss_service_type"] },
                  { $in: ["E-IPTV FTTH", "$records.order_line_oss_service_type"] },
                ],
              },
              {
                $size: {
                  $filter: {
                    input: "$records",
                    as: "record",
                    cond: { $eq: ["$$record.order_line_oss_service_type", "E-IPTV FTTH"] },
                  },
                },
              },
              0,
            ],
          },
          onlyPeotvCount: {
            $cond: [
              {
                $and: [
                  {
                    $or: [
                      { $in: ["E-IPTV COPPER", "$records.order_line_oss_service_type"] },
                      { $in: ["E-IPTV FTTH", "$records.order_line_oss_service_type"] },
                    ],
                  },
                  {
                    $not: {
                      $or: [
                        { $in: ["AB-CAB", "$records.order_line_oss_service_type"] },
                        { $in: ["AB-FTTH", "$records.order_line_oss_service_type"] },
                      ],
                    },
                  },
                ],
              },
              {
                $size: {
                  $filter: {
                    input: "$records",
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

      // Summarize Counts
      {
        $group: {
          _id: null,
          total_peotv_with_copper: { $sum: "$copperCount" },
          total_peotv_with_fiber: { $sum: "$fiberCount" },
          total_only_peotv: { $sum: "$onlyPeotvCount" },
        },
      },
      {
        $project: {
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







here is the database structure 

ORDER_LINE_RTO_AREA	Churn_Date	ACCOUNT_NUM	Activated_Date	ORDER_LINE_OSS_SERVICE_TYPE	BSS_TARIFF_NAME	Deleted_method	CUSTOMER_TYPE
RTO - GL	April 1, 2024	0040122054	August 1, 2018	E-IPTV COPPER	Cu_PEO  RELIGIOUS  PACKAGE	Non Payment	Religious Institute
RTO - GP	April 1, 2024	0007347218	December 23, 2020	AB-FTTH	Access_Charge	Non Payment	Individual-Residential
RTO - HB	April 1, 2024	0041592070	October 10, 2018	AB-CAB	AB_Copper Access Bearer	Non Payment	Individual-Residential
RTO - HO	April 1, 2024	0051246432	September 7, 2023	E-IPTV FTTH	FTTH_PEO Bronze	Non Payment	Individual-Residential
RTO - KE	April 1, 2024	0040366895	August 1, 2018	AB-CAB	AB_Copper Access Bearer	Non Payment	Individual-Residential
RTO - KE	April 1, 2024	0009360011	August 1, 2018	AB-CAB	AB_Copper Access Bearer	Non Payment	Individual-Residential
RTO - KG	April 1, 2024	0042668316	June 6, 2019	E-IPTV COPPER	Cu_Peo Silver	Non Payment	Individual-Residential
RTO - KLY	April 1, 2024	0048658239	December 17, 2021	AB-CAB	AB_Copper Access Bearer	Non Payment	Individual-Residential
RTO - MD	April 1, 2024	0045153275	July 8, 2020	AB-CAB	AB_Copper Access Bearer	Non Payment	Individual-Residential
RTO - ND	April 1, 2024	0028491870	March 1, 2019	E-IPTV COPPER	Cu_Peo Silver	Non Payment	Individual-Residential


here is the post method for saving database

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
                Invalid Churn_Date: '${data["Churn_Date"]}' - Skipping row.
              );
              return;
            }
            if (!momentActivatedDate.isValid()) {
              console.warn(
                Invalid Activated_Date: '${data["Activated_Date"]}' - Skipping row.
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
            console.log(Total valid rows processed: ${results.length});

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
                  console.log(Inserting ${newData.length} new records...);
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

now I need to categorise and filter those data get counts to a dashboard 

this is the criteria please understand this very carefully 

I need to check the datasets ACCOUNT_NUM first 

check is there any records which are with same account number in whole dataset 

and then with those datas we need the output as this 
I need to get the count as 
only peotv disconnections , peotv with copper disconnections , peotv with fiber disconnections.

so to get count as peotv with copper disconnections 

the same ACCOUNT_NUM will surely appear more than once and maximum up to 4 times in the whole dataset. and one of those same ACCOUNT_NUM records order_line_oss_service_type should equal to "AB-CAB" and other remain ACCOUNT_NUM records records order_line_oss_service_type should equal to "E-IPTV COPPER"

so in the database there have like 2 records with same ACCOUNT_NUM and one of those records order_line_oss_service_type equal to "AB-CAB" and other order_line_oss_service_type equal to "E-IPTV COPPER" and then we can take it as a peotv with copper disconnections. and remember in peotv with copper disconnections if there 2 records with same ACCOUNT_NUM and satisfied above conditions and we are counting as 1 
and if there 3 records with same ACCOUNT_NUM and satisfied above conditions and we count as 2 and if there 4 records with same ACCOUNT_NUM and satisfied above conditions and we count as 3 
Actually we only need to take count that E-IPTV COPPER counts 
How about if we first check these conditions and create a array and save counts of E-IPTV COPPER counts satisfied these condition.

so to get count as peotv with fiber disconnections 

the same ACCOUNT_NUM will surely appear more than once and maximum up to 4 times in the whole dataset. and one of those same ACCOUNT_NUM records order_line_oss_service_type should equal to "AB-FTTH" and other remain ACCOUNT_NUM records records order_line_oss_service_type should equal to "E-IPTV FTTH"

so in the database there have like 2 records with same ACCOUNT_NUM and one of those records order_line_oss_service_type equal to "AB-FTTH" and other order_line_oss_service_type equal to "E-IPTV FTTH" and then we can take it as a peotv with fiber disconnections. and remember in peotv with fiber disconnections if there 2 records with same ACCOUNT_NUM and satisfied above conditions and we are counting as 1 
and if there 3 records with same ACCOUNT_NUM and satisfied above conditions and we count as 2 and if there 4 records with same ACCOUNT_NUM and satisfied above conditions and we count as 3 
Actually we only need to take count that E-IPTV FTTH counts 
How about if we first check these conditions and create a array and save counts of E-IPTV FTTH counts satisfied these condition.

so to get count as only peotv disconnections 

so 2 of above un satisfied all records that records order_line_oss_service_type equal to E-IPTV FTTH or E-IPTV COPPER should take count as only peo tv disconnections
like there will be same ACCOUNT_NUM records and all of those records order_line_oss_service_type should equal to E-IPTV FTTH or E-IPTV COPPER and there should not be any records with order_line_oss_service_type equal to AB-FTTH or AB-CAB 
and sometime the ACCOUNT_NUM only appear once in the whole dataset and that record order_line_oss_service_type equal to E-IPTV FTTH or E-IPTV COPPER. these also should take count as only peo tv disconnections
so for this ACCOUNT_NUM will once or maximum up to 3 times and that all records order_line_oss_service_type should equal to E-IPTV FTTH or E-IPTV COPPER and there should not be any records with order_line_oss_service_type equal to AB-FTTH or AB-CAB for that ACCOUNT_NUM
so how ever I think you understood for total count of these 3 should equal to total count of E-PTV FTTH + E-IPTV COPPER total count 



const mongoose = require("mongoose");

const UserSchema = new mongoose.Schema({
  username: { type: String, required: true, unique: true },
  password: { type: String, required: true },
  role: { type: String, enum: ["admin", "user"], required: true },
});

module.exports = mongoose.model("User", UserSchema);


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

            // Standardize `deleted_method` field
            let standardizedDeletedMethod = data["Deleted_method"];
            if (
              ["Promotion Upgrade", "Promotion Downgrade"].includes(
                data["Deleted_method"]
              )
            ) {
              standardizedDeletedMethod = "Customer Requested";
            }

            // Push parsed and valid data to results array
            results.push({
              order_line_rto_area: data["ORDER_LINE_RTO_AREA"],
              churn_date: churnDate,
              account_num: data["ACCOUNT_NUM"],
              activated_date: activatedDate,
              order_line_oss_service_type: data["ORDER_LINE_OSS_SERVICE_TYPE"],
              bss_tariff_name: data["BSS_TARIFF_NAME"],
              deleted_method: standardizedDeletedMethod, // Save standardized value
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