import { GetObjectCommandInput, GetObjectOutput, S3, S3Client } from "@aws-sdk/client-s3"
import { Readable, ReadableOptions } from "stream"

type GetObjectOptions = {
  Input: Omit<GetObjectCommandInput, 'Range' | 'PartNumber'>
  Multipart?: boolean
  PreloadCount?: number
  RangeSize?: number
}

type GetObjectBody = GetObjectOutput["Body"]
function normalizeBody(body: GetObjectBody) {
  if (body == null || !(body instanceof Readable)) {
    return null
  }
  return body
}

type GetNextObjectPart = () => null | Promise<Readable | null>
async function* readS3ObjectGeneric(getObjectOptions: GetObjectOptions, getNextObjectPart: GetNextObjectPart) {

  const partsLoading: Promise<Readable | null>[] = []

  const preloadCount = getObjectOptions.PreloadCount ?? 1

  function preloadParts() {
    while (partsLoading.length < preloadCount) {
      const nextObjectPart = getNextObjectPart()
      if (nextObjectPart == null) {
        return
      }
      partsLoading.push(nextObjectPart)
    }
  }

  async function next(): Promise<Readable | null> {
    preloadParts()
    const nextPart = partsLoading.shift()
    if (!nextPart) {
      return null
    }
    return await nextPart
  }

  let currentStream: Readable | null
  while ((currentStream = await next()) != null) {
    for await (const data of currentStream) {
      yield data
    }
  }
}

async function* readS3ObjectRange(s3: S3, getObjectOptions: GetObjectOptions) {
  const { ContentLength } = await s3.headObject(getObjectOptions.Input)

  if (ContentLength == null) {
    return
  }

  type Range = { start: number, end: number }
  async function getObjectRange(range: Range): Promise<Readable | null> {
    const { Body } = await s3.getObject({
      ...getObjectOptions.Input,
      Range: `bytes=${range.start}-${range.end}`
    })

    return normalizeBody(Body)
  }

  const requiredContentLength: number = ContentLength
  const size: number = getObjectOptions.RangeSize ?? 5242880 // 5MB
  let currentRangeStart = 0

  function getNextObjectRange(): null | Promise<Readable | null> {
    if (currentRangeStart >= requiredContentLength) {
      return null
    }
    const rangeStart = currentRangeStart
    currentRangeStart += size
    return getObjectRange({
      start: rangeStart,
      end: Math.min(rangeStart + size, requiredContentLength) - 1
    })
  }

  yield* readS3ObjectGeneric(getObjectOptions, getNextObjectRange)
}

async function* readS3ObjectMultipart(s3: S3, getObjectOptions: GetObjectOptions) {
  const { PartsCount } = await s3.headObject({
    ...getObjectOptions.Input,
    PartNumber: 1
  })
  if (PartsCount == null) {
    yield* readS3ObjectRange(s3, getObjectOptions)
    return
  }

  async function getObjectPart(partNumber: number): Promise<Readable | null> {
    const { Body } = await s3.getObject({
      ...getObjectOptions.Input,
      PartNumber: partNumber
    })

    return normalizeBody(Body)
  }

  const requiredPartsCount: number = PartsCount
  let currentPartNumber = 1

  function getNextObjectPart(): null | Promise<Readable | null> {
    if (currentPartNumber > requiredPartsCount) {
      return null
    }
    const partNumber = currentPartNumber++
    return getObjectPart(partNumber)
  }

  yield* readS3ObjectGeneric(getObjectOptions, getNextObjectPart)
}

function readS3Object(s3Client: S3Client, getObjectOptions: GetObjectOptions) {
  const s3 = new S3(s3Client)
  if (getObjectOptions.Multipart) {
    return readS3ObjectMultipart(s3, getObjectOptions)
  }
  return readS3ObjectRange(s3, getObjectOptions)
}

export function S3ObjectInputStream(s3Client: S3Client, getObjectOptions: GetObjectOptions, readableOptions?: ReadableOptions) {
  return Readable.from(readS3Object(s3Client, getObjectOptions), readableOptions)
}