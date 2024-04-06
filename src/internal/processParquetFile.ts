import type { IOptions } from "../lib";
import parquet from "parquetjs";

/**
 * Given a path to a local file, processes it line by line via streaming and calls inserter function
 */
export const processParquetFile = async (
  filePath: string,
  headers: string[],
  insertRow: (row: string[], options?: IOptions) => Promise<void>,
  options?: IOptions,
): Promise<void> => {
  const insertRowsStartTime = performance.now();
  let rowCount = 0;
  const reader = await parquet.ParquetReader.openFile(filePath);
  const cursor = reader.getCursor();
  let record: unknown;
  while ((record = await cursor.next())) {
    const row = headers.map(
      (header) => (record as Record<string, string>)[header],
    );
    insertRow(row);
    rowCount++;
  }
  const insertRowsEndTime = performance.now();
  const insertRowsElapsedTimeInSeconds =
    (insertRowsEndTime - insertRowsStartTime) / 1000;
  if (options?.debug) {
    console.log(
      `[processParquetFile] Inserted ${rowCount} rows in ${insertRowsElapsedTimeInSeconds.toPrecision(2)}s.`,
    );
  }
};
