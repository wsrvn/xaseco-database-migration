# xaseco-database-migration
migrate your xaseco mysql database to the postgres counterpart of trakman

# how
1. `git clone` this repo or download source with the big green button
2. `npm i`
3. edit [settings](#settings) inside `.env`
4. `node Migrate.mjs`
5. ???
6. if youre lucky it worked gg and thanks for watching

# settings
### mysql db
- `MYSQL_HOST`: MySQL database host
- `MYSQL_USER`: MySQL database user
- `MYSQL_PASSWORD`: MySQL database user password
- `MYSQL_DATABASE`: MySQL database name
### postgres db
- `POSTGRES_HOST`: PostgreSQL database host
- `POSTGRES_USER`: PostgreSQL database user
- `POSTGRES_PASSWORD`: PostgreSQL database user password
- `POSTGRES_DATABASE`: PostgreSQL database name
- `POSTGRES_PORT`: PostgreSQL database host port
