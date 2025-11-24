import { Pool } from "pg";

const pool = new Pool({
    user: "postgres",
    host: "localhost",
    database: "ekatte",
    password: "pass",
    port: 5432,
    idleTimeoutMillis: 500,
});

export const client = pool.connect();

export default pool;
