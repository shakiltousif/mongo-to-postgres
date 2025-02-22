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
        let model;
        try {
            model = mongoose.model(collection);
        } catch (error) {
            model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
        }

        // Fetch one sample document to detect fields
        const sampleDoc = await model.findOne().lean();
        if (!sampleDoc) continue; // Skip if collection is empty

        // // Ensure base table exists
        // let createTableQuery = `
        //     CREATE TABLE IF NOT EXISTS ${collection} (
        //         id SERIAL PRIMARY KEY,
        //         mongo_id TEXT UNIQUE
        //     );
        // `;

        const tableName = `"${collection.toLowerCase()}"`;  // Ensure table names are safely quoted
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
            WHERE table_name = '${collection}';
        `);

        const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

        // Add missing columns dynamically
        for (const [key, value] of Object.entries(sampleDoc)) {
            if (key === "_id" || existingColumns.includes(key)) continue; // Skip _id & existing columns

            // let columnType = typeof value === "number" ? "NUMERIC" :
            //     typeof value === "boolean" ? "BOOLEAN" :
            //         "TEXT"; // Default to TEXT

            let columnType;
            if (typeof value === "number") {
                columnType = Number.isInteger(value) && value < Number.MAX_SAFE_INTEGER ? "INTEGER" : "BIGINT";
            } else if (typeof value === "boolean") {
                columnType = "BOOLEAN";
            } else if (typeof value === "string" && value.match(/^[0-9a-fA-F-]+$/)) { // Detect UUID-like strings
                columnType = "TEXT"; // Store UUIDs or unique keys as TEXT
            } else {
                columnType = "TEXT"; // Default fallback
            }



            // const alterTableQuery = `ALTER TABLE ${collection} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`;
            const alterTableQuery = `ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`;

            await pgClient.query(alterTableQuery);
            console.log(`✅ Added missing column: ${key} in ${collection}`);
        }
    }
}



async function migrateInitialData() {
    const collections = await getMongoCollections();

    for (const collection of collections) {
        let model;
        try {
            model = mongoose.model(collection); // Check if model already exists
        } catch (error) {
            model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
        }

        const documents = await model.find().lean();

        for (const doc of documents) {
            const mongoId = doc._id.toString();
            delete doc._id; // Remove _id to avoid duplicate fields

            // Construct INSERT query
            const columns = ['mongo_id', ...Object.keys(doc).map(key => `"${key}"`)];
            // const values = [mongoId, ...Object.values(doc)];
            const sanitizedValues = [mongoId, ...Object.entries(doc).map(([key, value]) => {
                if (typeof value === "number") {
                    return Number.isInteger(value) && value < Number.MAX_SAFE_INTEGER ? parseInt(value, 10) : parseFloat(value);
                } else if (typeof value === "boolean") {
                    return value;
                } else if (value instanceof Date) {
                    return value.toISOString();
                } else if (typeof value === "string" && value.match(/^[0-9a-fA-F-]+$/)) { // Convert UUID-like values
                    return value;
                } else {
                    return String(value);
                }
            })];


            // const insertQuery = `
            //     INSERT INTO ${collection} (${columns.join(", ")})
            //     VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
            //     ON CONFLICT (mongo_id) DO UPDATE
            //     SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
            // `;
            const tableName = `"${collection.toLowerCase()}"`;  // Ensure table names are safely quoted
            const insertQuery = `
                INSERT INTO ${tableName} (${columns.join(", ")})
                VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                ON CONFLICT (mongo_id) DO UPDATE
                SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
            `;


            await pgClient.query(insertQuery, sanitizedValues);
        }

        console.log(`✅ Data migrated: ${collection} (${documents.length} records)`);
    }
}


async function pollForChanges() {
    const collections = await getMongoCollections();

    setInterval(async () => {
        console.log("🔄 Checking for changes...");

        for (const collection of collections) {
            let model;
            try {
                model = mongoose.model(collection);
            } catch (error) {
                model = mongoose.model(collection, new mongoose.Schema({}, { strict: false }), collection);
            }

            const latestDocs = await model.find().lean();

            for (const doc of latestDocs) {
                const mongoId = doc._id.toString();
                delete doc._id; // Remove _id from document

                // Prepare column names and values dynamically
                const columns = ['mongo_id', ...Object.keys(doc).map(key => `"${key}"`)];
                // const values = [mongoId, ...Object.values(doc)];
                const sanitizedValues = [mongoId, ...Object.entries(doc).map(([key, value]) => {
                    if (typeof value === "number") {
                        return Number.isInteger(value) && value < Number.MAX_SAFE_INTEGER ? parseInt(value, 10) : parseFloat(value);
                    } else if (typeof value === "boolean") {
                        return value;
                    } else if (value instanceof Date) {
                        return value.toISOString();
                    } else if (typeof value === "string" && value.match(/^[0-9a-fA-F-]+$/)) { // Convert UUID-like values
                        return value;
                    } else {
                        return String(value);
                    }
                })];


                // Construct insert query
                // const insertQuery = `
                //     INSERT INTO ${collection} (${columns.join(", ")})
                //     VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                //     ON CONFLICT (mongo_id) DO UPDATE
                //     SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
                // `;
                const tableName = `"${collection.toLowerCase()}"`;  // Ensure table names are safely quoted
                const insertQuery = `
                    INSERT INTO ${tableName} (${columns.join(", ")})
                    VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                    ON CONFLICT (mongo_id) DO UPDATE
                    SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
                `;


                await pgClient.query(insertQuery, sanitizedValues);
            }
        }

        console.log("✅ Sync complete");

    }, 5000);  // Check for updates every 5 seconds
}






module.exports = { getMongoCollections, createPGTables, migrateInitialData, pollForChanges };
