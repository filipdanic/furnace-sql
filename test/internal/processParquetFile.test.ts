import { processParquetFile } from "../../src/internal/processParquetFile";
import { test, expect, mock } from "bun:test";
import { prepareParquetSeedData, cleanUpSeedData } from "../seed";

test("reads parquet file", async () => {
  const fileCount = 1;
  const rows = 10;
  const path = "data/mocks";
  const prefix = "data_parquet";
  const t = await prepareParquetSeedData(fileCount, rows, path, prefix);
  const insertRowMock = mock(async () => {});
  await processParquetFile(
    t[0].files[0].path,
    t[0].schema.map((v) => v.key),
    insertRowMock,
  );
  await cleanUpSeedData(fileCount, path, prefix, "parquet");
  expect(insertRowMock).toHaveBeenCalledTimes(rows);
});
