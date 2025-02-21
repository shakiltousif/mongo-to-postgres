const { ensureDatabaseExists } = require("./db");
const { createPGTables, migrateInitialData, pollForChanges } = require("./sync");

(async () => {
    try {
        await ensureDatabaseExists();
        console.log("🚀 Starting MongoDB to PostgreSQL Sync...");

        await createPGTables();       // Step 1: Create Tables
        await migrateInitialData();   // Step 2: Initial Migration
        await pollForChanges();  // Step 3: Real-Time Sync

    } catch (err) {
        console.error("❌ Error:", err);
    }
})();
