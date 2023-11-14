const express = require("express");
const app = express();
const { open } = require("sqlite");
const sqlite3 = require("sqlite3");
const axios = require("axios");
const cors = require("cors");
const path = require("path");

app.use(express.json());
app.use(cors());

const port = process.env.PORT || 4130;

const dbPath = path.join(__dirname, "transactionsData.db");

let database = null;

const initializeDBAndServer = async () => {
  try {
    database = await open({
      filename: dbPath,
      driver: sqlite3.Database,
    });
    app.listen(port, () => {
      console.log(`Server running at http://localhost:${port}/`);
    });
    createTable();
  } catch (e) {
    console.log(`DB Error ${e.message}`);
    process.exit(1);
  }
};

initializeDBAndServer();

const createTable = async () => {
  const createTableQuery = `
    CREATE TABLE IF NOT EXISTS transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        price REAL,
        description TEXT,
        category TEXT,
        image TEXT,
        sold BOOLEAN,
        dateOfSale DATETIME
      );`;
  await database.run(createTableQuery);
};

app.get("/initialize-database", async (req, res) => {
  const url = "https://s3.amazonaws.com/roxiler.com/product_transaction.json";
  const response = await axios.get(url);
  const transactions = await response.data;
  for (const transaction of transactions) {
    const insertQuery = `INSERT OR IGNORE INTO transactions (id, title, price, description, category, image, sold, dateOfSale)
      VALUES (?, ?, ?, ?, ?, ?, ?, ?);`;

    await database.run(insertQuery, [
      transaction.id,
      transaction.title,
      transaction.price,
      transaction.description,
      transaction.category,
      transaction.image,
      transaction.sold,
      transaction.dateOfSale,
    ]);
  }
  res.send({ msg: "Initialized database with third party API" });
});

//API for Get all transactions
app.get("/transactions", async (req, res) => {
  const { month = "", s_query = "", limit = 10, offset = 0 } = req.query;
  const searchQuery = `
    SELECT *
    FROM transactions
    WHERE 
      (title LIKE ? OR description LIKE ? OR price LIKE ?) 
      AND strftime('%m', dateOfSale) LIKE ?
    LIMIT ? OFFSET ?;
  `;

  const params = [
    `%${s_query}%`,
    `%${s_query}%`,
    `%${s_query}%`,
    `%${month}%`,
    limit,
    offset,
  ];
  const totalItemQuery = `SELECT COUNT(id) AS total
  FROM transactions
  WHERE 
      (title LIKE ? OR description LIKE ? OR price LIKE ?) 
      AND strftime('%m', dateOfSale) LIKE ?;`;
  const totalParams = [
    `%${s_query}%`,
    `%${s_query}%`,
    `%${s_query}%`,
    `%${month}%`,
  ];
  const data = await database.all(searchQuery, params);
  const total = await database.get(totalItemQuery, totalParams);
  res.send({ transactions: data, total: total });
});

//API for statistics
app.get("/statistics", async (req, res) => {
  const { month = "" } = req.query;

  const totalSaleAmount = await database.get(`
    SELECT SUM(price) as total
    from transactions 
    where strftime('%m', dateOfSale) LIKE '%${month}';
  `);

  const soldItems = await database.get(`
    SELECT COUNT(id) as count 
    from transactions 
    where strftime('%m', dateOfSale) LIKE '%${month}' AND sold = 1;
  `);

  const notSoldItems = await database.get(`
  SELECT COUNT(id) as count 
  from transactions 
  where strftime('%m', dateOfSale) LIKE '%${month}' AND sold = 0;
`);
  res.json({ totalSaleAmount, soldItems, notSoldItems });
});

// API for bar chart
app.get("/bar-chart", async (req, res) => {
  const { month = "" } = req.query;

  const barChartData =
    await database.get(` SELECT SUM(CASE WHEN price BETWEEN 0 AND 100 THEN 1 ELSE 0 END) as "0-100"
,  SUM(CASE WHEN price BETWEEN 101 AND 200 THEN 1 ELSE 0 END) as "101-200"
,  SUM(CASE WHEN price BETWEEN 201 AND 300 THEN 1 ELSE 0 END) as "201-300"
,  SUM(CASE WHEN price BETWEEN 301 AND 400 THEN 1 ELSE 0 END) as "301-400"
,  SUM(CASE WHEN price BETWEEN 401 AND 500 THEN 1 ELSE 0 END) as "401-500"
,  SUM(CASE WHEN price BETWEEN 501 AND 600 THEN 1 ELSE 0 END) as "501-600"
,  SUM(CASE WHEN price BETWEEN 601 AND 700 THEN 1 ELSE 0 END) as "601-700"
,  SUM(CASE WHEN price BETWEEN 701 AND 800 THEN 1 ELSE 0 END) as "701-800"
,  SUM(CASE WHEN price BETWEEN 801 AND 900 THEN 1 ELSE 0 END) as "801-900"
,  SUM(CASE WHEN price > 901 THEN 1 ELSE 0 END) as "901-above"
  FROM transactions WHERE
  strftime('%m', dateOfSale) LIKE '%${month}';`);

  res.json({ barChartData });
});

// API for pie chart
app.get("/pie-chart", async (req, res) => {
  const { month = "" } = req.query;

  const pieChartData = await database.all(
    `SELECT category, COUNT(id) as count FROM transactions
    WHERE strftime('%m', dateOfSale) LIKE '%${month}%' 
    GROUP BY category;`
  );

  res.json({ pieChartData });
});

//API to fetch data from all the above APIs and combine the response

app.get("/combined-response", async (req, res) => {
  const { month = "", s_query = "", limit = 10, offset = 0 } = req.query;

  const initializeResponse = await axios.get(
    `https://roxiler-systems-assignment-lw5d.onrender.com/initialize-database`
  );
  const initializeResponseData = await initializeResponse.data;
  const listTransactionsResponse = await axios.get(
    `https://roxiler-systems-assignment-lw5d.onrender.com/transactions?month=${month}&s_query=${s_query}&limit=${limit}&offset=${offset}`
  );
  const listTransactionsResponseData = await listTransactionsResponse.data;
  const statisticsResponse = await axios.get(
    `https://roxiler-systems-assignment-lw5d.onrender.com/statistics?month=${month}`
  );
  const statisticsResponseData = await statisticsResponse.data;
  const barChartResponse = await axios.get(
    `https://roxiler-systems-assignment-lw5d.onrender.com/bar-chart?month=${month}`
  );
  const barChartResponseData = await barChartResponse.data;
  const pieChartResponse = await axios.get(
    `https://roxiler-systems-assignment-lw5d.onrender.com/pie-chart?month=${month}`
  );
  const pieChartResponseData = await pieChartResponse.data;

  const combinedResponse = {
    initialize: initializeResponseData,
    listTransactions: listTransactionsResponseData,
    statistics: statisticsResponseData,
    barChart: barChartResponseData,
    pieChart: pieChartResponseData,
  };

  res.json(combinedResponse);
});
