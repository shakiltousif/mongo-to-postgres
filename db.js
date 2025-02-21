const mongoose = require("mongoose");
const { Client } = require("pg");
require("dotenv").config();


const mongoURI = process.env.MONGO_URI;
const pgURI = process.env.PG_URI;

console.log("🚀 MongoDB URI:", mongoURI);
console.log("🚀 PostgreSQL URI:", pgURI);
// MongoDB Connection
mongoose.connect(mongoURI, { useNewUrlParser: true, useUnifiedTopology: true });
const mongoDB = mongoose.connection;
mongoDB.on("error", console.error.bind(console, "MongoDB connection error:"));
mongoDB.once("open", () => console.log("✅ MongoDB Connected"));

// PostgreSQL Connection
const pgClient = new Client({ connectionString: pgURI });
pgClient.connect()
    .then(() => console.log("✅ PostgreSQL Connected"))
    .catch(err => console.error("PostgreSQL Connection Error", err));

const pgAdminClient = new Client({
    connectionString: process.env.PG_ADMIN_URI || "postgresql://your_user:your_pass@localhost:5432/postgres"
});

async function ensureDatabaseExists() {
    try {
        await pgAdminClient.connect();
        const dbName = process.env.PG_DB || "your_pg_db";

        // Check if the database exists
        const res = await pgAdminClient.query(`SELECT 1 FROM pg_database WHERE datname = $1`, [dbName]);

        if (res.rowCount === 0) {
            // Create the database if it doesn't exist
            await pgAdminClient.query(`CREATE DATABASE ${dbName}`);
            console.log(`✅ Database created: ${dbName}`);
        } else {
            console.log(`✅ Database already exists: ${dbName}`);
        }

    } catch (err) {
        console.error("❌ Error checking/creating database:", err);
    } finally {
        await pgAdminClient.end();
    }
}

module.exports = { mongoDB, pgClient, ensureDatabaseExists };
