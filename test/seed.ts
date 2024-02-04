import { unlink } from 'node:fs/promises';
import parquet from 'parquetjs';
import { ITransformationConfig } from '../src/lib_types';

// Function to generate a random date in YYYY-MM-DD format
const randomDate = () => {
  const year = Math.floor(Math.random() * (2023 - 2000 + 1)) + 2000; // Random year between 2000 and 2023
  const month = Math.floor(Math.random() * 12) + 1; // Random month between 1 and 12
  const day = Math.floor(Math.random() * 28) + 1; // Random day between 1 and 28

  // Format to YYYY-MM-DD
  return `${year}-${month.toString().padStart(2, '0')}-${day.toString().padStart(2, '0')}`;
};

// Function to generate a random amount between -1000000 and 1000000
const randomAmount = () => Math.floor(Math.random() * 2000001) - 1000000;

// Function to generate a random string of up to 240 characters
const randomString = () => {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';
  const length = Math.floor(Math.random() * 241);
  return Array(length).fill('').map(() => chars.charAt(Math.floor(Math.random() * chars.length))).join('');
};

// Function to generate a random country code
const randomCountry = () => {
  const countries = ['DE', 'UK', 'IT', 'FR', 'ES', 'US', 'CA', 'AU', 'NZ', 'BR'];
  return countries[Math.floor(Math.random() * countries.length)];
};

// Function to generate a random account number
const randomAccount = () => {
  const chars = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789';
  return Array(6).fill('').map(() => chars.charAt(Math.floor(Math.random() * chars.length))).join('');
};

export const prepareCsvSeedData = async(fileCount: number, rows: number, path: string, filePrefix: string) => {
  await cleanUpSeedData(fileCount, path, filePrefix, 'csv');
  for (let i = 0; i < fileCount; i++) {
    const file = `${path}/${filePrefix}-${i}.csv`
    const fileRef = Bun.file(file);
    const writer = fileRef.writer();
    writer.write('id,date,amount,description,country,account');
    for (let j = 0; j < rows; j++) {
      const row = `\n${crypto.randomUUID()},${randomDate()},${randomAmount()},${randomString()},${randomCountry()},${randomAccount()}`;
      writer.write(row);
      writer.flush();
    }
    writer.end();
  }

  return Array.from({ length: fileCount }).map((_, i) => ({
    name: `${filePrefix}_${i}`,
    schema: [
      { key: 'id', type: 'TEXT' },
      { key: 'date', type: 'TEXT' },
      { key: 'amount', type: 'INTEGER' },
      { key: 'description', type: 'TEXT' },
      { key: 'country', type: 'TEXT' },
      { key: 'account', type: 'TEXT' },
    ],
    files: [{
      type: 'csv',
      path: `${path}/${filePrefix}-${i}.csv`
    }],
  })) as ITransformationConfig['tables'];
}

export const prepareParquetSeedData = async(fileCount: number, rows: number, path: string, filePrefix: string) => {
  await cleanUpSeedData(fileCount, path, filePrefix, 'parquet');
  const schema = new parquet.ParquetSchema({
    id: { type: 'UTF8' },
    date: { type: 'UTF8' },
    amount: { type: 'INT64' },
    description: { type: 'UTF8' },
    country: { type: 'UTF8' },
    account: { type: 'UTF8' },
  });
  for (let i = 0; i < fileCount; i++) {
    const file = `${path}/${filePrefix}-${i}.parquet`
    const writer = await parquet.ParquetWriter.openFile(schema, file);
    for (let j = 0; j < rows; j++) {
      writer.appendRow({
        id: crypto.randomUUID(),
        date: randomDate(),
        amount: randomAmount(),
        description: randomString(),
        country: randomCountry(),
        account: randomAccount(),
      });
    }
    writer.close();
  }

  return Array.from({ length: fileCount }).map((_, i) => ({
    name: `${filePrefix}_${i}`,
    schema: [
      { key: 'id', type: 'TEXT' },
      { key: 'date', type: 'TEXT' },
      { key: 'amount', type: 'INTEGER' },
      { key: 'description', type: 'TEXT' },
      { key: 'country', type: 'TEXT' },
      { key: 'account', type: 'TEXT' },
    ],
    files: [{
      type: 'parquet',
      path: `${path}/${filePrefix}-${i}.parquet`
    }],
  })) as ITransformationConfig['tables'];
}

export const cleanUpSeedData = async(fileCount: number, path: string, filePrefix: string, extension: 'csv' | 'parquet') => {
  for (let i = 0; i < fileCount; i++) {
    await unlink(`${path}/${filePrefix}-${i}.${extension}`).catch(() => {});
  }
}
