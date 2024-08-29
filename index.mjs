import pg from "pg";
import { DateTime } from "luxon";
import { randomUUID } from "crypto";

const { Pool } = pg;

const delay = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

const pool = new Pool({
  connectionString: "postgres://postgres:password@localhost:5400/postgres",
});

const JOBS_TABLE_SCHEMA = `
DROP TABLE IF EXISTS jobs;
CREATE TABLE IF NOT EXISTS jobs (
  id SERIAL,
  data TEXT,
  completed_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ
);
`;

async function addJobs(pool) {
  const client = await pool.connect();

  await client.query(JOBS_TABLE_SCHEMA);
  for (let i = 0; i < 10; i++) {
    await client.query("INSERT INTO JOBS (data) VALUES ($1)", [
      Math.random().toString(),
    ]);
  }
  client.release();
}

class Worker {
  #id;
  #client;
  #stopping = false;
  constructor(pool) {
    this.#id = randomUUID();
    pool.connect().then((client) => {
      this.#client = client;
      this.start();
    });
  }

  async start() {
    while (!this.#stopping) {
      try {
        await this.#client.query("BEGIN");
        const result = await this.#client.query(
          "SELECT * FROM jobs WHERE completed_at is NULL FOR UPDATE SKIP LOCKED LIMIT 1",
        );
        const job = result.rows[0];

        if (!job) {
          console.log("no jobs found");
          await this.#client.query("COMMIT");
          await delay(200);
          continue;
        }
        console.log(`Worker ${this.#id}`, result.rows[0]);

        this.#client.query("UPDATE jobs SET completed_at = $1 WHERE id = $2", [
          new Date(),
          job.id,
        ]);

        console.log(`Worker ${this.#id} completed job id ${job.id}`);

        await this.#client.query("COMMIT");
      } catch (err) {
        console.log(err);
        this.#client.query("ROLLBACK");
      }

      await delay(200);
    }

    this.#stopping = false;
  }

  async stop() {
    this.#stopping = true;
    while (this.#stopping) {
      await delay(100);
    }
    this.#client.release();
  }
}

async function run() {
  await addJobs(pool);

  const workers = [new Worker(pool)];

  await delay(8000);

  await Promise.all(workers.map((worker) => worker.stop()));

  await pool.end();
}

run();
