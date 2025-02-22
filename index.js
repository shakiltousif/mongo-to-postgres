const { ensureDatabaseExists } = require("./db");
const { createPGTables, migrateInitialData, pollForChanges } = require("./sync");

(async () => {
    try {
        await ensureDatabaseExists();
        console.log("🚀 Starting MongoDB to PostgreSQL Sync...");

        await createPGTables();       // Step 1: Create Tables
        console.log("✅ PostgreSQL tables created");
        await migrateInitialData();   // Step 2: Initial Migration
        console.log("✅ Initial data migrated");
        await pollForChanges();  // Step 3: Real-Time Sync
        console.log("✅ Real-time sync started");

    } catch (err) {
        console.error("❌ Error:", err);
    }
})();
