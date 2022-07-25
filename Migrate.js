import mysql from 'mysql2/promise'
import postgres from 'pg'
import countries from 'Countries.json' assert { type: 'json' }

// MySQL database
const c = await mysql.createConnection({
    host: '', // MySQL database host
    user: '', // MySQL database user
    password: '', // MySQL database user password
    database: '' // MySQL database name
});

// PostgreSQL database
const pool = new postgres.Pool({
    user: '', // PostgreSQL database user
    password: '', // PostgreSQL database user password
    database: '', // PostgreSQL database name
    host: '', // PostgreSQL database host
    port: Number('') // PostgreSQL database port
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

/**
 * Gets all Player IDs from the database
 * @returns The database response
 */
async function getPlayerIds() {
    const query = `SELECT id, login FROM player_ids;`
    const res = await pool.query(query)
    return res.rows
}

/**
 * Gets all Map IDs from the database
 * @returns The database response
 */
async function getMapIds() {
    const query = `SELECT id, uid FROM map_ids;`
    const res = await pool.query(query)
    return res.rows
}

// Connect to the MySQL database
await c.connect()

// Create Player IDs table
await pool.query(`CREATE TABLE IF NOT EXISTS player_ids(
    id INT4 GENERATED ALWAYS AS IDENTITY,
    login VARCHAR(100) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);`)

// Create Players table
await pool.query(`CREATE TABLE IF NOT EXISTS players(
    id INT4 NOT NULL,
    nickname VARCHAR(100) NOT NULL,
    region VARCHAR(100) NOT NULL,
    wins INT4 NOT NULL,
    time_played INT4 NOT NULL,
    visits INT4 NOT NULL,
    is_united BOOLEAN NOT NULL,
    last_online TIMESTAMP,
    PRIMARY KEY(id),
    CONSTRAINT fk_player_id
      FOREIGN KEY(id) 
	      REFERENCES player_ids(id)
);`)

// Create Map IDs table
await pool.query(`CREATE TABLE IF NOT EXISTS map_ids(
    id INT4 GENERATED ALWAYS AS IDENTITY,
    uid VARCHAR(27) NOT NULL UNIQUE,
    PRIMARY KEY(id)
);`)

// Create Records table
await pool.query(`CREATE TABLE IF NOT EXISTS records(
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

// Create Votes table
await pool.query(`CREATE TABLE IF NOT EXISTS votes(
    map_id INT4 NOT NULL,
    player_id INT4 NOT NULL,
    vote INT2 NOT NULL,
    date TIMESTAMP NOT NULL,
    PRIMARY KEY(map_id, player_id),
    CONSTRAINT fk_player_id
      FOREIGN KEY(player_id) 
        REFERENCES player_ids(id),
    CONSTRAINT fk_map_id
      FOREIGN KEY(map_id)
        REFERENCES map_ids(id)
);`)

// Get all maps
const maps = (await c.query('SELECT * FROM challenges'))[0]

// Get all players, convert the nicknames in the process so they're displayed correctly.
// Conversion query taken from https://stackoverflow.com/a/9407998
let players = (await c.query('SELECT Id, Login, CONVERT(CAST(CONVERT(NickName USING LATIN1) AS BINARY) USING UTF8), Nation, Wins, TimePlayed, UpdatedAt FROM players'))[0]

// Get players extra values, only visits are used from here for now
const playersE = (await c.query('SELECT * FROM players_extra'))[0]

// Get all records that correlate to the players & maps from the database
let records = (await c.query(`SELECT Uid, Login, Score, Date, Checkpoints FROM records
INNER JOIN challenges ON challenges.Id=records.ChallengeId
INNER JOIN players ON players.Id=records.PlayerId `))[0]

// Get all votes
let votes = (await c.query(`SELECT Uid, Login, Score FROM rs_karma
INNER JOIN challenges ON challenges.Id=rs_karma.ChallengeId
INNER JOIN players ON players.Id=rs_karma.PlayerId `))[0]

// Insert all map IDs into the database
await pool.query(`INSERT INTO map_ids(uid) ${getInsertValuesString(1, maps.length)} 
ON CONFLICT (uid) DO NOTHING`, maps.map(a => a.Uid))

// Insert all player IDs into the database
await pool.query(`INSERT INTO player_ids(login) ${getInsertValuesString(1, players.length)} 
ON CONFLICT (login) DO NOTHING`, players.map(a => a.Login.split('/')[0]))

// Get both player & map IDs
const playerIds = await getPlayerIds()
const mapIds = await getMapIds()

// Queries need to be separated to not hit the PostgreSQL limit
// Players table stuff
for (let j = 0; j < 1000; j++) {
    const arr = []
    for (const [i, e] of players.entries()) {
        if (i === 1000) {
            break
        }
        arr.push(playerIds.find(a => a.login === e.Login.split('/')[0]).id, // Player ID
            e['CONVERT(CAST(CONVERT(NickName USING LATIN1) AS BINARY) USING UTF8)'], // Player nickname, very funny
            countries.find(a => a.code === e.Nation).name, // Country name, we store full location normally, but it's impossible to get from XASECO
            e.Wins, // Player wins amount
            e.TimePlayed, // Player total playtime
            playersE.find(a => e.Id === a.playerID).visits, // Player total visits
            false, // Whether the player has TMUF. Defaults to false, as this isn't stored by XASECO
            new Date(e.UpdatedAt)) // Player last update
    }
    // Remove the already inserted entries
    players = players.slice(1000)
    // Nothing left to insert..
    if (arr.length === 0) {
        break
    }
    // Insert players
    await pool.query(`INSERT INTO players(id, nickname, region, wins, time_played, visits, is_united, last_online) ${getInsertValuesString(8, arr.length / 8)}
    ON CONFLICT (id) DO NOTHING`, arr)
}

// Records table stuff
for (let j = 0; j < 1000; j++) {
    const arr = []
    for (const [i, e] of records.entries()) {
        if (i === 1000) {
            break
        }
        arr.push(mapIds.find(a => a.uid === e.Uid).id, // Map ID
            playerIds.find(a => a.login === e.Login.split('/')[0]).id, // Player ID
            e.Score, // Record time
            e.Checkpoints.split(',').map(a => Number(a)), // Player checkpoints, need to be reformatted as we store them in arrays
            new Date(e.Date)) // Record date
    }
    // Remove the already inserted entries
    records = records.slice(1000)
    // Nothing left to insert..
    if (arr.length === 0) {
        break
    }
    // Insert records
    await pool.query(`INSERT INTO records(map_id, player_id, time, checkpoints, date) ${getInsertValuesString(5, arr.length / 5)}
    ON CONFLICT (map_id, player_id) DO NOTHING`, arr)
}

// Votes table stuff
for (let j = 0; j < 1000; j++) {
    const arr = []
    for (const [i, e] of votes.entries()) {
        if (i === 1000) {
            break
        }
        arr.push(mapIds.find(a => a.uid === e.Uid).id, // Map ID
            playerIds.find(a => a.login === e.Login.split('/')[0]).id, // Player ID
            e.Score.map(a => Math.abs(a) === 6 ? a / 2 : a), // Player vote
            new Date()) // Vote date, no such thing in XASECO, so new date is inserted instead
    }
    // Remove the already inserted entries
    votes = votes.slice(1000)
    // Nothing left to insert..
    if (arr.length === 0) {
        break
    }
    await pool.query(`INSERT INTO votes(map_id, player_id, vote, date) ${getInsertValuesString(4, arr.length / 4)}
    ON CONFLICT (map_id, player_id) DO NOTHING`, arr)
}

// Exit the process on completion
console.log('Migration done. Check the database for errors.')
process.exit(0)