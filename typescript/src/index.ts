import { S3Client } from "@aws-sdk/client-s3"
import { S3ObjectInputStream } from "./s3-object-stream"
import * as readline from "readline"


async function readS3ObjectLines(bucket: string, key: string) {
  const s3Stream = S3ObjectInputStream(new S3Client({}), {
    Input: { 
      Bucket: bucket, 
      Key: key 
    }, 
    PreloadCount: 5, 
    RangeSize: 1024 
  })

  const rl = readline.createInterface({ input: s3Stream })
  for await (const line of rl) {
    console.log(`Line read: ${line}`)
  }
}

readS3ObjectLines('my-bucket', 'mykey')