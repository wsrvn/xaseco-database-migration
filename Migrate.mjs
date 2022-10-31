import mysql from 'mysql2/promise'
import postgres from 'pg'
import { oraPromise } from 'ora'
import countries from './Countries.mjs'
import 'dotenv/config'

// MySQL database
const c = await mysql.createConnection({
    host: process.env.MYSQL_HOST, // MySQL database host
    user: process.env.MYSQL_USER, // MySQL database user
    password: process.env.MYSQL_PASSWORD, // MySQL database user password
    database: process.env.MYSQL_DATABASE // MySQL database name
})

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

/**
 * Gets all Player IDs from the database
 * @returns The database response
 */
async function getPlayerIds() {
    const query = `SELECT id, login FROM players;`
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

await oraPromise(async () => {
    // Connect to the MySQL database
    await c.connect()
}, { spinner: 'dots', text: `Connecting to the MySQL database ${process.env.MYSQL_DATABASE}@${process.env.MYSQL_HOST}` })

await oraPromise(async () => {
    // Create Players table
    await pool.query(`CREATE TABLE IF NOT EXISTS players(
        id INT4 GENERATED ALWAYS AS IDENTITY,
        login VARCHAR(100) NOT NULL UNIQUE,
        nickname VARCHAR(100) NOT NULL,
        region VARCHAR(100) NOT NULL,
        wins INT4 NOT NULL,
        time_played INT4 NOT NULL,
        visits INT4 NOT NULL,
        is_united BOOLEAN NOT NULL,
        average REAL NOT NULL,
        last_online TIMESTAMP,
        PRIMARY KEY(id)
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
          REFERENCES players(id),
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
        REFERENCES players(id),
    CONSTRAINT fk_map_id
      FOREIGN KEY(map_id)
        REFERENCES map_ids(id)
);`)

    // Create Best Sectors table
    await pool.query(`CREATE TABLE IF NOT EXISTS best_sector_records(
    map_id INT4 NOT NULL,
    player_id INT4 NOT NULL,
    index INT2 NOT NULL,
    sector INT4 NOT NULL,
    date TIMESTAMP NOT NULL,
    PRIMARY KEY(map_id, index),
    CONSTRAINT fk_player_id
      FOREIGN KEY(player_id) 
        REFERENCES players(id),
    CONSTRAINT fk_map_id
      FOREIGN KEY(map_id)
        REFERENCES map_ids(id)
  );`)

    // Create All Sectors table
    await pool.query(`CREATE TABLE IF NOT EXISTS sector_records(
    map_id INT4 NOT NULL,
    player_id INT4 NOT NULL,
    sectors INT4[] NOT NULL,
    PRIMARY KEY(map_id, player_id),
    CONSTRAINT fk_player_id
      FOREIGN KEY(player_id) 
        REFERENCES players(id),
    CONSTRAINT fk_map_id
      FOREIGN KEY(map_id)
        REFERENCES map_ids(id)
  );`)

    // Create Donations table
    await pool.query(`CREATE TABLE IF NOT EXISTS donations(
    player_id INT4 NOT NULL,
    amount INT4 NOT NULL,
    date TIMESTAMP NOT NULL,
    PRIMARY KEY(player_id, date),
    CONSTRAINT fk_player_id
      FOREIGN KEY(player_id)
        REFERENCES players(id)
  );`)
}, { spinner: 'dots', text: `Creating necessary tables in ${process.env.POSTGRES_DATABASE}@${process.env.POSTGRES_HOST}` })

// Get all maps
const maps = (await c.query('SELECT * FROM challenges'))[0]

// Get all players, convert the nicknames in the process so they're displayed correctly.
// Conversion query taken from https://stackoverflow.com/a/9407998
// This can be made unfunny with AS NICKNAME but I want it to be funny
const players = (await c.query('SELECT Id, Login, CONVERT(CAST(CONVERT(NickName USING LATIN1) AS BINARY) USING UTF8), Nation, Wins, TimePlayed, UpdatedAt FROM players'))[0]

// Break the reference
let p = [...players]

// Get players extra values, most are unused due to differences in the system
const playersE = (await c.query('SELECT * FROM players_extra'))[0]

// Get all records that correlate to the players & maps from the database
let records = (await c.query(`SELECT Uid, Login, Score, Date, Checkpoints FROM records
INNER JOIN challenges ON challenges.Id=records.ChallengeId
INNER JOIN players ON players.Id=records.PlayerId `))[0]

// Get all votes
let votes = (await c.query(`SELECT Uid, Login, Score FROM rs_karma
INNER JOIN challenges ON challenges.Id=rs_karma.ChallengeId
INNER JOIN players ON players.Id=rs_karma.PlayerId `))[0]

// Get all best sectors
let bestSecs = (await c.query(`SELECT ChallengeID, Sector, PlayerNick, Time FROM secrecs_all`))[0]

// Get all player sectors
let secs = (await c.query(`SELECT ChallengeID, Sector, PlayerNick, Time FROM secrecs_own`))[0]

// Insert all map IDs into the database
await pool.query(`INSERT INTO map_ids(uid) ${getInsertValuesString(1, maps.length)} 
ON CONFLICT (uid) DO NOTHING`, maps.map(a => a.Uid))

// Get both player & map IDs
const mapIds = await getMapIds()
const playerIds = await getPlayerIds()

// NOTE: Queries need to be separated to not hit the PostgreSQL limit

if (process.env.MIGRATE_PLAYERS === 'YES') {
    await oraPromise(async () => {
        // Players table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of p.entries()) {
                if (i === 1000) {
                    break
                }
                arr.push(
                    e.Login.split('/')[0],
                    e['CONVERT(CAST(CONVERT(NickName USING LATIN1) AS BINARY) USING UTF8)'], // Player nickname, very funny
                    countries.find(a => a.code === e.Nation).name, // Country name, we store full location normally, but it's impossible to get from XASECO
                    e.Wins, // Player wins amount
                    e.TimePlayed, // Player total playtime
                    playersE.find(a => e.Id === a.playerID).visits, // Player total visits
                    false, // Whether the player has TMUF. Defaults to false, as this isn't stored by XASECO
                    new Date(e.UpdatedAt), // Player last update
                    -1
                )
            }
            // Remove the already inserted entries
            p = p.slice(1000)
            // Nothing left to insert..
            if (arr.length === 0) {
                break
            }
            // Insert players
            await pool.query(`INSERT INTO players(login, nickname, region, wins, time_played, visits, is_united, last_online, average) ${getInsertValuesString(9, arr.length / 9)}
            ON CONFLICT (login) DO NOTHING`, arr)
        }
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:players to ${process.env.POSTGRES_DATABASE}:players` })
}

if (process.env.MIGRATE_RECORDS === 'YES') {
    await oraPromise(async () => {
        // Records table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of records.entries()) {
                if (i === 1000) {
                    break
                }
                const cps = e.Checkpoints.split(',').map(a => Number(a))
                cps.pop() // XASECO stores finish as checkpoint and trakman doesnt
                arr.push(
                    mapIds.find(a => a.uid === e.Uid).id, // Map ID
                    playerIds.find(a => a.login === e.Login.split('/')[0]).id, // Player ID
                    e.Score, // Record time
                    cps, // Player checkpoints, need to be reformatted as we store them in arrays
                    new Date(e.Date) // Record date
                )
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
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:records to ${process.env.POSTGRES_DATABASE}:records` })
}

if (process.env.MIGRATE_VOTES === 'YES') {
    await oraPromise(async () => {
        // Votes table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of votes.entries()) {
                if (i === 1000) {
                    break
                }
                arr.push(
                    mapIds.find(a => a.uid === e.Uid).id, // Map ID
                    playerIds.find(a => a.login === e.Login.split('/')[0]).id, // Player ID
                    Math.abs(e.Score) === 6 ? e.Score / 2 : e.Score, // Player vote
                    new Date() // Vote date, no such thing in XASECO, so new date is inserted instead
                )
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
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:rs_karma to ${process.env.POSTGRES_DATABASE}:votes` })
}

if (process.env.MIGRATE_BEST_SECTORS === 'YES') {
    await oraPromise(async () => {
        // Best Sectors table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of bestSecs.entries()) {
                if (i === 1000) {
                    break
                }
                const mapId = mapIds.find(a => a.uid === e.ChallengeID)?.id
                const playerId = playerIds.find(a => a.login === e.PlayerNick?.split('/')[0])?.id
                if (mapId === undefined || playerId === undefined) { continue }
                arr.push(
                    mapId, // Map ID
                    playerId, // Player ID
                    e.Sector, // Sector index
                    e.Time, // Sector time
                    new Date() // Sector date, no such thing in XASECO, so new date is inserted instead
                )
            }
            // Remove the already inserted entries
            bestSecs = bestSecs.slice(1000)
            // Nothing left to insert..
            if (arr.length === 0) {
                break
            }
            await pool.query(`INSERT INTO best_sector_records(map_id, player_id, index, sector, date) ${getInsertValuesString(5, arr.length / 5)}
    ON CONFLICT (map_id, index) DO NOTHING`, arr)
        }
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:secrecs_all to ${process.env.POSTGRES_DATABASE}:best_sector_records` })
}

if (process.env.MIGRATE_ALL_SECTORS === 'YES') {
    // Initialise the array
    let s = []

    await oraPromise(async () => {
        // Get each player's sectors for each map and store them as an array
        // This can take a very long time on huge databases, so be aware
        let index = 0
        while (secs.length > 0) {
            s[index] = {
                uid: secs[0].ChallengeID,
                login: secs[0].PlayerNick,
                sectors: []
            }
            s[index].sectors[secs[0].Sector] = secs[0].Time
            let i = 1
            while (true) {
                if (secs[i] === undefined) { break }
                if (secs[i].ChallengeID === secs[0].ChallengeID && secs[i].PlayerNick === secs[0].PlayerNick) {
                    s[index].sectors[secs[i].Sector] = secs[i].Time
                    secs.splice(i, 1)
                    i--
                }
                i++
            }
            secs.splice(0, 1)
            index++
        }
    }, { spinner: 'dots', text: `Building sector indexes from ${process.env.MYSQL_DATABASE}:secrecs_own` })

    await oraPromise(async () => {
        // Player Sectors table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of s.entries()) {
                if (i === 1000) {
                    break
                }
                const mapId = mapIds.find(a => a.uid === e.uid)?.id
                const playerId = playerIds.find(a => a.login === e.login?.split('/')[0])?.id
                if (mapId === undefined || playerId === undefined) { continue }
                arr.push(
                    mapId, // Map ID
                    playerId, // Player ID
                    e.sectors // All sectors array
                )
            }
            // Remove the already inserted entries
            s = s.slice(1000)
            // Nothing left to insert..
            if (arr.length === 0) {
                break
            }
            await pool.query(`INSERT INTO sector_records(map_id, player_id, sectors) ${getInsertValuesString(3, arr.length / 3)}
    ON CONFLICT (map_id, player_id) DO NOTHING`, arr)
        }
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:secrecs_own to ${process.env.POSTGRES_DATABASE}:sector_records` })
}

if (process.env.MIGRATE_DONATIONS === 'YES') {
    // Only get non-zero donations from the previous, very nicely optimised table
    let donations = playersE.filter(a => a.donations !== 0)
        .map(a => ({ amount: a.donations, login: players.find(b => b.Id === a.playerID)?.Login }))
        .filter(a => a.login !== undefined)

    await oraPromise(async () => {
        // Donations table stuff
        for (let j = 0; j < 1000; j++) {
            const arr = []
            for (const [i, e] of donations.entries()) {
                if (i === 1000) {
                    break
                }
                const playerId = playerIds.find(a => a.login === e.login?.split('/')[0])?.id
                if (playerId === undefined) { continue }
                arr.push(
                    playerId, // Player ID
                    e.amount, // Donation sum
                    new Date() // Donation date, not stored previously, hence it's the current date
                )
            }
            // Remove the already inserted entries
            donations = donations.slice(1000)
            // Nothing left to insert..
            if (arr.length === 0) {
                break
            }
            await pool.query(`INSERT INTO donations(player_id, amount, date) ${getInsertValuesString(3, arr.length / 3)}
    ON CONFLICT (player_id, date) DO NOTHING`, arr)
        }
    }, { spinner: 'dots', text: `Migrating table ${process.env.MYSQL_DATABASE}:players_extra.donations to ${process.env.POSTGRES_DATABASE}:donations` })
}

// Exit the process on completion
await oraPromise(() => { }, { text: `Migration complete. Check the database for possible errors.` })
process.exit(0)
