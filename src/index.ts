// Import necessary modules
import express from 'express';
import mysql from 'mysql2/promise';
import AWS from 'aws-sdk';
import pLimit from 'p-limit';
import { NseIndia } from 'stock-nse-india';
import { EquityDetails, EquityPriceInfo } from './interface';
import 'dotenv/config';

const nseIndia = new NseIndia();

AWS.config.update({
  accessKeyId: process.env.ACCESS_KEY,
  secretAccessKey: process.env.SECRET_KEY,
  region: 'us-east-1'
});

const sns = new AWS.SNS();
const topicArn = process.env.TOPIC; // Replace with your SNS Topic ARN


const dbConfig = {
  host: process.env.DB_HOST,  // RDS endpoint (e.g., 'mydb-instance.xyz.amazonaws.com')
  user: process.env.DB_USER,   // MySQL username
  password: process.env.DB_PASS, // MySQL password
  database: process.env.DB_NAME   // MySQL database name
};

// Create an Express app
const app = express();
const port = 3000; // You can change the port if needed

// Function to fetch stock data
const fetchStockData = async (symbol: string): Promise<EquityDetails> => {
  try {
    const stockData = await nseIndia.getEquityDetails(symbol); // Fetch stock data using stock-nse-india
    return stockData;
  } catch (error) {
    console.error('Error fetching stock data:', error);
    throw error;
  }
};

// Function to save data into MySQL
const saveToDatabase = async (symbol: any, price: any, volume: any) => {
  const connection = await mysql.createConnection(dbConfig);
  console.log('Connected');

  try {
    const query = 'INSERT INTO stock_data (stock_symbol, price, volume) VALUES (?, ?, ?)';
    await connection.execute(query, [symbol, price, volume]);
    console.log('Data saved to MySQL');
  } catch (error) {
    console.error('Error saving data to MySQL:', error);
  } finally {
    await connection.end();
  }
};

// Function to send an SNS message
const sendSNSMessage = async (message: any) => {
  const params = {
    Message: message,
    TopicArn: topicArn,
  };

  try {
    await sns.publish(params).promise();
    console.log('SNS message sent successfully');
  } catch (error) {
    console.error('Error sending SNS message:', error);
  }
};

// Main function to process stock data
const processStockData = async (symbol: any) => {
  try {
    console.log("Starting to process stock data...");
    
    // Fetch data
    const stockData = await fetchStockData(symbol);
    const { lastPrice, vwap } = stockData.priceInfo ?? {};  // Use optional chaining and nullish coalescing

    if (!stockData) {
      throw new Error('No stock data found');
    }

    console.log(`Fetched data: ${symbol} - Price: ${lastPrice}, Volume: ${vwap}`);

    // Save data to MySQL
    await saveToDatabase(symbol, lastPrice, vwap);

    // Send an SNS notification if price exceeds a threshold
    const message = `Stock Alert: ${symbol} price is now ${lastPrice}`;
    await sendSNSMessage(message);
  } catch (error) {
    console.error('Error processing stock data:', error);
  }
};

// API endpoint to trigger stock data fetching
app.get('/fetch-stock/:symbol', async (req, res) => {
  const { symbol } = req.params;

  try {
    const stockData = await fetchStockData(symbol);
    res.status(200).json(stockData);
  } catch (error) {
    res.status(500).json({ error: 'An error occurred while processing the stock data.' });
  }
});

app.get('/UpdateFNOStockList', async (req, res) => {
  try {
    const connection = await mysql.createConnection(dbConfig);
    const query = 'INSERT INTO `fno-stocks` (stock_name) VALUE (?)';
    const stockNames = await nseIndia.getAllStockSymbols(); // Fetch stock data using stock-nse-india
    stockNames.forEach(stock =>{
      try {
        nseIndia.getEquityDetails(stock).then(stockData =>{
          console.log("reading: "+ stock);   
          if (!stockData) {
            throw new Error('No stock data found');
          }
          if(stockData.info.isFNOSec == true && stockData.metadata.series == "EQ"){
            connection.execute(query, [stockData.info.symbol]).then(res =>{
              console.log(stockData.info.symbol);
            });
          }
        }); 
      } catch (error) {
        console.error('Error fetching stock data:', error);
        throw error;
      }
    });
    console.log('Stocks inserted successfully.');
  } catch (error) {
    res.status(500).json({ error: 'An error occurred while processing the stock data.' });
  }
});


app.get('/UpdateOHLStocks', async (req, res) => {
  const now = new Date();
  const formattedDate = now.toISOString().split('T')[0];
  const connection = await mysql.createConnection(dbConfig);
  const query = 'SELECT id, stock_name FROM `fno-stocks`'; 
  const insertQuery = 'INSERT INTO `OHL_Stocks` VALUE (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)';

  const limit = pLimit(10); // Limit to 10 concurrent calls

  try {
    const [rows] = await connection.execute(query) as [any[], any];

    // Process all rows with controlled concurrency
    const promises = rows.map((row) =>
      limit(async () => {
        try {
          const stockData = await nseIndia.getEquityDetails(row.stock_name);

          if (!stockData) {
            throw new Error('No stock data found');
          }

          if (
            stockData.priceInfo.open === stockData.priceInfo.intraDayHighLow.min ||
            stockData.priceInfo.open === stockData.priceInfo.intraDayHighLow.max
          ) {
            await connection.execute(insertQuery, [
              row.id,
              stockData.metadata.symbol,
              stockData.priceInfo.open,
              stockData.priceInfo.intraDayHighLow.min,
              stockData.priceInfo.intraDayHighLow.max,
              stockData.priceInfo.pChange,
              stockData.priceInfo.previousClose,
              stockData.priceInfo.weekHighLow.min,
              stockData.priceInfo.weekHighLow.max,
              formattedDate
            ]);

            console.log('Inserted stock:', stockData.metadata.symbol);
          }
        } catch (err) {
          console.error('Error processing stock:', row.stock_name, err);
        }
      })
    );

    await Promise.all(promises);
    res.status(200).json({ message: 'Stock data processed successfully' });
  } catch (error) {
    console.error('Error reading data from MySQL:', error);
    res.status(500).json({ error: 'Error processing stock data' });
  } finally {
    await connection.end();
  }
});


app.get('/test', async (req, res) => {
  res.status(200).json({ message: 'test successfully' });
});


// Start the Express server
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
  console.log(process.env.DB_HOST);
});
