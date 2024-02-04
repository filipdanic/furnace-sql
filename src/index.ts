
import { Database } from 'bun:sqlite';
import { IOptions, ITable, ITransformationConfig } from './lib_types';
import { processCsvFile } from './internal/processCsvFile';
import { processParquetFile } from './internal/processParquetFile';

const BATCH_SIZE = 100;

export const executeTransformation = async <T>(config: ITransformationConfig, options?: IOptions): Promise<{ result: T[], db: Database }> => {
  const db = new Database(':memory:');
  let batchQueue: string[][] = [];
  
  const flushBatch = async (tableName: string) => {
    if (batchQueue.length > 0) {
      const placeholders = batchQueue.map(row => `(${row.map(() => '?').join(', ')})`).join(',');
      const query = `INSERT INTO ${tableName} VALUES ${placeholders}`;
      await db.run(query, batchQueue.flat());
      batchQueue = [];
    }
  };

  const insertRowFn = (table: ITable) => async (row: string[]): Promise<void> => {
    batchQueue.push(row);
    if (batchQueue.length === BATCH_SIZE) { await flushBatch(table.name); }
  };

  for (const table of config.tables) {
    const createTable = `
      CREATE TABLE ${table.name} (${table.schema.map(_ => `${_.key} ${_.type}`).join(', ')});
    `;
    db.run(createTable);

    const insertRow = insertRowFn(table);

    const process = async (file: ITable['files'][0]) => {
      if (file.type === 'csv') {
        await processCsvFile(file.path, insertRow, options);
      } else if (file.type === 'parquet') {
        await processParquetFile(file.path, table.schema.map((s) => s.key), insertRow, options);
      }
      await flushBatch(table.name);
    }

    const processingMode = options?.processingMode || 'parallel';
    if (processingMode === 'parallel') {
      await Promise.all(table.files.map(process));
    } else {
      for (const file of table.files) {
        await process(file);
      }
    }
  }

  const sqlQueryStartTime = performance.now();
  const result = db.query<T, {}>(config.transformationScript).all({});
  const sqlQueryEndTime = performance.now();
  const sqlQueryElapsedTimeInSeconds = (sqlQueryEndTime - sqlQueryStartTime) / 1000;
  if (options?.debug) {
    console.log(`[executeTransformation] Executed query in ${sqlQueryElapsedTimeInSeconds.toPrecision(2)}s.`)
  }

  return { result, db };  
}
