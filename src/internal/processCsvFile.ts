import { createReadStream } from 'fs';
import { IOptions } from '../lib_types';
import csv from 'csv-parser';

/**
 * Given a path to a local file, processes it line by line via streaming and calls inserter function
 */
export const processCsvFile = (filePath: string, insertRow: (row: string[]) => Promise<void>, options?: IOptions): Promise<void> => {
  return new Promise((resolve) => {
    const insertRowsStartTime = performance.now();
    let rowCount = 0;
    let processing = false;
    const stream = createReadStream(filePath).pipe(csv({ headers: false, skipLines: 1 }));

    stream.on('data', async (data) => {
      if (!processing) {
        processing = true;
        stream.pause(); 
        const row = Object.values(data) as string[];
        rowCount++;
        await insertRow(row).catch((err) => {});
        processing = false;
        stream.resume();
      }
    });

    stream.on('end', () => {
      const finishProcessing = () => {
        const insertRowsEndTime = performance.now();
        const insertRowsElapsedTimeInSeconds = (insertRowsEndTime - insertRowsStartTime) / 1000;
        if (options?.debug) {
          console.log(`[processCsvFile] Inserted ${rowCount} rows in ${insertRowsElapsedTimeInSeconds.toPrecision(2)}s.`);
        }
        resolve();
      }
      if (!processing) {
        finishProcessing();
      } else {
        const checkProcessing = setInterval(() => {
          if (!processing) {
            clearInterval(checkProcessing);
            finishProcessing();
          }
        }, 100); 
      }
    });
  });
}