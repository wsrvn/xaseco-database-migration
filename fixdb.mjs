import postgres from 'pg'
import 'dotenv/config'

// PostgreSQL database
const pool = new postgres.Pool({
    user: process.env.POSTGRES_USER, // PostgreSQL database user
    password: process.env.POSTGRES_PASSWORD, // PostgreSQL database user password
    database: process.env.POSTGRES_DATABASE, // PostgreSQL database name
    host: process.env.POSTGRES_HOST, // PostgreSQL database host
    port: process.env.POSTGRES_PORT // PostgreSQL database port
})

/**
 * Gets the insert query
 * @param {Number} columns Columns amount
 * @param {Number} rows Rows amount
 * @returns The insert query
 */
function getInsertValuesString(columns, rows = 1) {
    let ret = `VALUES `
    let index = 1
    for (let i = 0; i < rows; i++) {
        ret += '($'
        for (let j = 1; j <= columns; j++) {
            ret += (index++).toString() + ',$'
        }
        ret = ret.slice(0, -2) + '),'
    }
    return ret.slice(0, -1)
}

let records = (await pool.query(`SELECT * FROM records;`)).rows
for (let i = 0; i < records.length; i++) {
    if (records[i].date[records[i].checkpoints.length - 1] === records[i].time) {
        records[i].checkpoints.pop()
    }
}

await pool.query(`DROP TABLE records`)

await pool.query(`CREATE TABLE records(
    map_id INT4 NOT NULL,
    player_id INT4 NOT NULL,
    time INT4 NOT NULL,
    checkpoints INT4[] NOT NULL,
    date TIMESTAMP NOT NULL,
    PRIMARY KEY(map_id, player_id),
    CONSTRAINT fk_player_id
      FOREIGN KEY(player_id) 
          REFERENCES player_ids(id),
    CONSTRAINT fk_map_id
      FOREIGN KEY(map_id)
        REFERENCES map_ids(id)
);`)

while (true) {
    const arr = records.slice(0, 1000)
    // Remove the already inserted entries
    records = records.slice(1000)
    // Nothing left to insert..
    if (arr.length === 0) {
        break
    }
    // Insert records
    await pool.query(`INSERT INTO records(map_id, player_id, time, checkpoints, date) ${getInsertValuesString(5, arr.length / 5)}`, arr)
}