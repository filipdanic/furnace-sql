import { test, expect } from 'bun:test';
import { executeTransformation } from '../src';
import { prepareCsvSeedData, prepareParquetSeedData, cleanUpSeedData } from './seed';

test('Joins 4 csv files and returns a union of all the data', async() => {
  const fileCount = 4;
  const rows = 10;
  const path = 'data/mocks';
  const prefix = 'data';
  const preparedTables = await prepareCsvSeedData(fileCount, rows, path, prefix);
  const tableNames = preparedTables.map((v) => v.name);
  const subQueries = tableNames.map(table => `SELECT * FROM ${table}`);
  const query = subQueries.join(' UNION ALL ');

  const r = await executeTransformation<any>({
    transformationScript: query,
    tables: preparedTables,   
  }, { debug: true });
  r.db.close();
  await cleanUpSeedData(fileCount, path, prefix, 'csv');

  expect(r.result.length).toEqual(fileCount * rows);
});

test('Joins 4 parquet files and returns a union of all the data', async() => {
  const fileCount = 4;
  const rows = 100;
  const path = 'data/mocks';
  const prefix = 'data';
  const preparedTables = await prepareParquetSeedData(fileCount, rows, path, prefix);
  const tableNames = preparedTables.map((v) => v.name);
  const subQueries = tableNames.map(table => `SELECT * FROM ${table}`);
  const query = subQueries.join(' UNION ALL ');

  const r = await executeTransformation<any>({
    transformationScript: query,
    tables: preparedTables,   
  }, { debug: true });
  r.db.close();
  await cleanUpSeedData(fileCount, path, prefix, 'parquet');

  // returned rows should be equal to number of rows in the files
  expect(r.result.length).toEqual(fileCount * rows);
});