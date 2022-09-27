import { S3Event } from "aws-lambda";
import { S3Client, GetObjectCommand } from "@aws-sdk/client-s3";
// @ts-ignore:next-line
import csv from "csv-parser";
import { Readable } from "stream";
import { DocumentClient } from "aws-sdk/clients/dynamodb";
import short from "short-uuid";

const PK_PREFIX = "REGION#";
const SK_PREFIX = "DATE#";
const BATCH_WRITE_SIZE = 25;
const REGION = process.env.REGION!;
const TABLE_NAME = process.env.TABLE_NAME!;
const Headers: string[] =
  "tid,price,transferdate,postcode,propertytype,new,duration,paon,saon,street,locality,town,district,county,ppd,status".split(
    ","
  );

const db = new DocumentClient({
  region: REGION,
});

const logger = (msg: string) => console.log(msg);

const counters = { total: 0, retried: 0 };

const batchWithRetry = async (putRequestBatch: Array<Object>) => {
  return new Promise<void>(async (resolve) => {
    try {
      counters.total += putRequestBatch.length;
      const res = await db
        .batchWrite({
          RequestItems: {
            [TABLE_NAME]: putRequestBatch,
          },
        })
        .promise();
      if (res.UnprocessedItems && res.UnprocessedItems[TABLE_NAME]) {
        const unprocessed = res.UnprocessedItems[TABLE_NAME];
        counters.retried += unprocessed.length;
        await batchWithRetry(unprocessed);
      }
    } catch (err) {
      logger(`Error: ${err}`);
    }
    resolve();
  });
};

interface StringMap {
  [key: string]: string;
}

export const handler = async (event: S3Event) => {
  try {
    logger("Event received" + JSON.stringify(event));
    let dbUpdateResponses: Array<Promise<void>> = [];

    for (const record of event.Records) {
      const bucket: string = record.s3.bucket.name;
      const key: string = decodeURIComponent(
        event.Records[0].s3.object.key.replace(/\+/g, " ")
      );

      logger("Get file from S3 bucket: " + bucket + " key: " + key);

      const command = new GetObjectCommand({
        Bucket: bucket,
        Key: key,
      });

      const s3Client = new S3Client({ region: REGION });
      const { Body: csvData } = await s3Client.send(command);

      if (csvData && csvData instanceof Readable) {
        const csvReadStream = csvData.pipe(csv());

        let putRequestBatch: Array<Object> = [];

        for await (const csvRow of csvReadStream) {
          const postcode = csvRow[Headers[3]].split(" ")[0];
          const saleDate = `${new Date(
            csvRow[Headers[2]]
          ).toISOString()}#${short.generate()}`;

          if (postcode && saleDate) {
            const item: StringMap = {
              pk: PK_PREFIX + postcode,
              sk: SK_PREFIX + saleDate,
            };

            Headers.forEach((header, i) => {
              if (csvRow[header]) {
                item[header] = csvRow[header];
              }
            });

            putRequestBatch.push({
              PutRequest: {
                Item: item,
              },
            });
          } else {
            logger("Missing data from row: " + JSON.stringify(csvRow));
          }

          if (putRequestBatch.length === BATCH_WRITE_SIZE) {
            dbUpdateResponses.push(batchWithRetry(putRequestBatch));
            putRequestBatch = [];
          }
        }

        if (putRequestBatch.length > 0) {
          dbUpdateResponses.push(batchWithRetry(putRequestBatch));
          putRequestBatch = [];
        }
      }
    }

    await Promise.all(dbUpdateResponses);
    logger("Final:" + JSON.stringify(counters));
  } catch (err) {
    logger(`Error: ${err}`);
    throw new Error(`Error: ${err}`);
  }
};
