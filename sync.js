const { mongoDB, pgClient } = require("./db");
const mongoose = require("mongoose");

async function getMongoCollections() {
    if (!mongoDB || !mongoDB.db) {
        throw new Error("MongoDB connection is not established");
    }

    const collections = await mongoDB.db.listCollections().toArray();
    console.log("✅ MongoDB Collections:", collections.map(col => col.name));
    return collections.map(col => col.name);
}

async function createPGTables() {
    const collections = await getMongoCollections();

    for (const collection of collections) {
        const tableName = `"${collection.toLowerCase()}"`; // Ensure proper quoting & lowercase names

        let model;
        try {
            model = mongoose.model(collection);
        } catch (error) {
            model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
        }

        // Fetch one sample document to detect fields
        const sampleDoc = await model.findOne().lean();
        if (!sampleDoc) continue; // Skip if collection is empty

        // Ensure base table exists
        let createTableQuery = `
            CREATE TABLE IF NOT EXISTS ${tableName} (
                id SERIAL PRIMARY KEY,
                mongo_id TEXT UNIQUE
            );
        `;
        await pgClient.query(createTableQuery);

        // Get existing columns in PostgreSQL
        const existingColumnsRes = await pgClient.query(`
            SELECT column_name FROM information_schema.columns
            WHERE table_name = ${tableName};
        `);
        const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

        // Add missing columns dynamically
        for (const [key, value] of Object.entries(sampleDoc)) {
            if (key === "_id" || existingColumns.includes(key)) continue; // Skip _id & existing columns

            let columnType = typeof value === "number" ? "NUMERIC" :
                typeof value === "boolean" ? "BOOLEAN" :
                    "TEXT"; // Default to TEXT

            // Ensure correct table alteration
            const alterTableQuery = `ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`;
            await pgClient.query(alterTableQuery);
            console.log(`✅ Added missing column: ${key} in ${tableName}`);
        }
    }
}

async function migrateInitialData() {
    const collections = await getMongoCollections();

    for (const collection of collections) {
        const tableName = `"${collection.toLowerCase()}"`; // Ensure proper quoting & lowercase names

        let model;
        try {
            model = mongoose.model(collection);
        } catch (error) {
            model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
        }

        const documents = await model.find().lean();
        if (documents.length === 0) continue; // Skip empty collections

        // Get existing columns
        const existingColumnsRes = await pgClient.query(`
            SELECT column_name FROM information_schema.columns
            WHERE table_name = ${tableName};
        `);
        const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

        for (const doc of documents) {
            const mongoId = doc._id.toString();
            delete doc._id;

            // Add missing columns dynamically before inserting
            for (const key of Object.keys(doc)) {
                if (!existingColumns.includes(key)) {
                    let columnType = typeof doc[key] === "number" ? "NUMERIC" :
                        typeof doc[key] === "boolean" ? "BOOLEAN" :
                            "TEXT";
                    await pgClient.query(`ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`);
                    existingColumns.push(key); // Update list to prevent duplicate ALTER queries
                    console.log(`✅ Added column: ${key} to ${tableName}`);
                }
            }

            // Prepare query dynamically
            const columns = ['mongo_id', ...Object.keys(doc).map(key => `"${key}"`)];
            const values = [mongoId, ...Object.values(doc)];

            const insertQuery = `
                INSERT INTO ${tableName} (${columns.join(", ")})
                VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                ON CONFLICT (mongo_id) DO UPDATE
                SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
            `;

            await pgClient.query(insertQuery, values);
        }

        console.log(`✅ Data migrated: ${collection} (${documents.length} records)`);
    }
}



async function pollForChanges() {
    const collections = await getMongoCollections();

    setInterval(async () => {
        console.log("🔄 Checking for changes...");

        for (const collection of collections) {
            const tableName = `"${collection.toLowerCase()}"`;

            let model;
            try {
                model = mongoose.model(collection);
            } catch (error) {
                model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
            }

            const latestDocs = await model.find().lean();

            // Get existing columns
            const existingColumnsRes = await pgClient.query(`
                SELECT column_name FROM information_schema.columns
                WHERE table_name = ${tableName};
            `);
            const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

            for (const doc of latestDocs) {
                const mongoId = doc._id.toString();
                delete doc._id; // Remove _id from document

                // Add missing columns dynamically
                for (const key of Object.keys(doc)) {
                    if (!existingColumns.includes(key)) {
                        let columnType = typeof doc[key] === "number" ? "NUMERIC" :
                            typeof doc[key] === "boolean" ? "BOOLEAN" :
                                "TEXT";
                        await pgClient.query(`ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`);
                        existingColumns.push(key);
                        console.log(`✅ Added column: ${key} to ${tableName}`);
                    }
                }

                // Prepare column names and values dynamically
                const columns = ['mongo_id', ...Object.keys(doc).map(key => `"${key}"`)];
                const values = [mongoId, ...Object.values(doc)];

                // Construct insert query
                const insertQuery = `
                    INSERT INTO ${tableName} (${columns.join(", ")})
                    VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                    ON CONFLICT (mongo_id) DO UPDATE
                    SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
                `;

                await pgClient.query(insertQuery, values);
            }
        }

        console.log("✅ Sync complete");

    }, 5000);  // Check for updates every 5 seconds
}








module.exports = { getMongoCollections, createPGTables, migrateInitialData, pollForChanges };
