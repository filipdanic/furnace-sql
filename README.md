# furnace-sql

`furnace-sql` is a typescript library for [bun](https://bun.sh/) that makes it easy to load your data from csv and parquet files into a SQLite database and run queries on top of it.

Currently under development. Read more about [in this blog post.](https://www.thesoftwarelounge.com/crunch-data-with-furnace-sql/)

## What’s This Good For?

- Crunching data on your local machine that’s in csv or parquet.
- Crunching data in the cloud via a cheap temporal compute like AWS Fargate
- Extremely fast with small data (1 million rows is small in this context)
- Ok with medium sized data (e.g crunch 8GB on AWS Fargate for $0.027)

## Example

```typescript
import { executeTransformation } from "furnace-sql/dist/src/index";

const t = await executeTransformation<MyType>(
  {
    transformationScript: "SELECT SUM(amount) FROM transactions;",
    tables: [
      {
        name: "transactions",
        schema: [
          { key: "id", type: "TEXT" },
          { key: "date", type: "TEXT" },
          { key: "amount", type: "INTEGER" },
        ],
        // you can add multiple files to the same table!
        files: [
          {
            type: "csv",
            path: `data/t1.csv`,
          },
          {
            type: "csv",
            path: `data/t2.csv`,
          },
        ],
      },
    ],
  },
  { debug: true },
);

console.log(t.result);

await t.db.query("…next query");
// or t.db.close() if you’re done
```

## What’s Left Before Stable Launch?

- More options to customize reading of csv and parquet files (pass-through to csv-parser and parquetjs)
- Benchmark and come up with better heuristic around serial vs parallel file processing.
- Optimize data loading/inserts
- More control over db schema
- Explore support for streaming files from S3, Azure, Vercel, etc.
