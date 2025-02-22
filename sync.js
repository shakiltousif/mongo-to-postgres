const { mongoDB, pgClient } = require("./db");
const mongoose = require("mongoose");

async function getMongoCollections() {
    if (!mongoDB || !mongoDB.db) {
        throw new Error("MongoDB connection is not established");
    }

    const collections = await mongoDB.db.listCollections({}, { nameOnly: true }).toArray();
    // console.log("✅ MongoDB Collections:", collections.map(col => col.name));
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
        let sampleDoc = await model.findOne().lean();
        if (!sampleDoc) {
            // console.log(`⚠️ Collection "${collection}" is empty. Creating table with default structure.`);
            sampleDoc = {}; // Create an empty object to trigger table creation
            continue;
        }


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
                __id SERIAL PRIMARY KEY,
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
        const columnTypesRes = await pgClient.query(`
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name = '${collection}';
        `);
        const columnTypes = {};
        columnTypesRes.rows.forEach(row => {
            columnTypes[row.column_name] = row.data_type;
        });


        // Add missing columns dynamically
        for (const [key, value] of Object.entries(sampleDoc)) {
            if (key === "_id" || existingColumns.includes(key)) continue; // Skip _id & existing columns

            // let columnType = typeof value === "number" ? "NUMERIC" :
            //     typeof value === "boolean" ? "BOOLEAN" :
            //         "TEXT"; // Default to TEXT

            let columnType;
            if (typeof value === "number") {
                if (Number.isInteger(value)) {
                    columnType = value > 2147483647 ? "BIGINT" : "INTEGER"; // Use BIGINT if the number is too large
                } else {
                    columnType = "NUMERIC"; // Float values
                }
            } else if (typeof value === "boolean") {
                columnType = "BOOLEAN";
            } else {
                columnType = "TEXT"; // Default fallback
            }




            // const alterTableQuery = `ALTER TABLE ${collection} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`;

            const alterTableQuery = `ALTER TABLE ${tableName} ADD COLUMN IF NOT EXISTS "${key}" ${columnType};`;

            if (key.toLowerCase() === "id") {
                // Ensure id columns are converted to TEXT in existing tables
                const alterIdColumnQuery = `ALTER TABLE ${tableName} ALTER COLUMN "${key}" TYPE TEXT USING "${key}"::TEXT;`;
                try {
                    await pgClient.query(alterIdColumnQuery);
                    // console.log(`✅ Converted "id" column to TEXT in ${tableName}`);
                } catch (alterError) {
                    console.warn(`⚠️ Could not alter "id" column in ${tableName}:`, alterError.message);
                }
            }
            await pgClient.query(alterTableQuery);

            // console.log(`✅ Added missing column: ${key} in ${collection}`);
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
            // Fetch existing columns from PostgreSQL
            const existingColumnsRes = await pgClient.query(`
                SELECT column_name FROM information_schema.columns
                WHERE table_name = '${collection}';
            `);
            const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

            // Fetch column data types
            const columnTypesRes = await pgClient.query(`
                SELECT column_name, data_type FROM information_schema.columns
                WHERE table_name = '${collection}';
            `);
            const columnTypes = {};
            columnTypesRes.rows.forEach(row => {
                columnTypes[row.column_name] = row.data_type;
            });

            // Process data before inserting
            const sanitizedValues = [mongoId, ...Object.entries(doc).map(([key, value]) => {
                if (typeof value === "number") {
                    return Number.isInteger(value) && value < Number.MAX_SAFE_INTEGER ? parseInt(value, 10) : parseFloat(value);
                } else if (typeof value === "boolean") {
                    return value;
                } else if (value instanceof Date) {
                    return value.toISOString();
                } else if (typeof value === "string") {
                    if (value.trim() === "") {
                        if (existingColumns.includes(key) && columnTypes[key] === "BOOLEAN") {
                            return null; // Convert empty string to NULL for BOOLEAN fields
                        }
                        return null; // Convert all empty strings to NULL to avoid invalid syntax
                    }
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

            // console.log(`🔍 Inserting data into table "${collection}"`);
            // console.log("Columns:", columns);
            // console.log("Values:", sanitizedValues);
            await pgClient.query(insertQuery, sanitizedValues);
        }

        // console.log(`✅ Data migrated: ${collection} (${documents.length} records)`);
    }
}


async function pollForChanges() {
    setInterval(async () => {
        // console.log("🔄 Checking for changes...");

        const collections = await getMongoCollections(); // Fetch collections dynamically

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
                const columns = ['mongo_id', ...Object.keys(doc).map(key => `${key && key !== "" ? `"${key}"` : ""}`)];

                // Fetch existing columns from PostgreSQL
                const existingColumnsRes = await pgClient.query(`
                    SELECT column_name FROM information_schema.columns
                    WHERE table_name = '${collection}';
                `);

                const existingColumns = existingColumnsRes.rows.map(row => row.column_name);

                // Fetch column data types
                const columnTypesRes = await pgClient.query(`
                    SELECT column_name, data_type FROM information_schema.columns
                    WHERE table_name = '${collection}';
                `);
                const columnTypes = {};
                columnTypesRes.rows.forEach(row => {
                    columnTypes[row.column_name] = row.data_type;
                });

                const sanitizedValues = [mongoId, ...Object.entries(doc).map(([key, value]) => {
                    if (typeof value === "number") {
                        return Number.isInteger(value) && value < Number.MAX_SAFE_INTEGER ? parseInt(value, 10) : parseFloat(value);
                    } else if (typeof value === "boolean") {
                        return value;
                    } else if (value instanceof Date) {
                        return value.toISOString();
                    } else if (typeof value === "string") {
                        if (value.trim() === "") {
                            if (existingColumns.includes(key) && columnTypes[key] === "BOOLEAN") {
                                return null; // Convert empty string to NULL for BOOLEAN fields
                            }
                            return null; // Convert all empty strings to NULL to avoid invalid syntax
                        }
                        return value;
                    } else {
                        return String(value);
                    }
                })];



                // Ensure table exists before inserting data
                await createPGTables();

                // Construct insert query
                const tableName = `"${collection.toLowerCase()}"`;  // Ensure table names are safely quoted
                const insertQuery = `
                    INSERT INTO ${tableName} (${columns.join(", ")})
                    VALUES (${columns.map((_, i) => `$${i + 1}`).join(", ")})
                    ON CONFLICT (mongo_id) DO UPDATE
                    SET ${Object.keys(doc).map(key => `"${key}" = EXCLUDED."${key}"`).join(", ")};
                `;

                // console.log(`🔍 Inserting data into table "${collection}"`);
                // console.log("Columns:", columns);
                // console.log("Values:", sanitizedValues);
                await pgClient.query(insertQuery, sanitizedValues);
                console.log(`✅ Data inserted: ${collection} (${sanitizedValues.length} records)`);
            }
        }

        // console.log("✅ Sync complete");

    }, 5000);  // Check for updates every 5 seconds
}







module.exports = { getMongoCollections, createPGTables, migrateInitialData, pollForChanges };
