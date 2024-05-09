import {
  DeleteObjectCommand,
  PutObjectCommand,
  S3Client,
} from "@aws-sdk/client-s3";

const s3 = new S3Client({});
const sanitizeColumnName = (name: string): string => {
  // Remove invalid characters
  let sanitized = name.replace(/[^a-zA-Z0-9_]/g, "");

  // Prepend an underscore if the name starts with digits
  sanitized = sanitized.replace(/^(\d)/, "_$1");

  // Remove trailing underscores
  sanitized = sanitized.replace(/_+$/, "");

  // Convert to lowercase for uniformity
  return sanitized.toLowerCase();
};

const flattenImage = (image: Record<string, any>) => {
  const flat: Record<string, any> = {};
  for (const [key, value] of Object.entries(image)) {
    if (typeof value === "object") {
      flat[sanitizeColumnName(key)] = JSON.stringify(value);
    } else {
      flat[sanitizeColumnName(key)] = value;
    }
  }
  return flat;
};

const matchSchema = ({
  source,
  pk,
  sk,
}: {
  source: string;
  pk: string;
  sk: string;
}): string | void => {
  // logic here to match the schema based on the source, pk, and sk
  console.log({ source, pk, sk });
  if (source === "users") return "user";
  if (source === "blog" && pk.toLowerCase().startsWith("post")) return "post";
  if (source === "blog" && pk.toLowerCase().startsWith("comment")) return "comment";
  return "unknown";
};

export const handler = async (event: any) => {
  console.log(JSON.stringify(event, null, 2));
  const image = event.detail.data.newImage || event.detail.data.oldImage;
  if (!image) {
    return;
  }
  const bucket = process.env.CDC_ARCHIVE_BUCKET;
  const schema = matchSchema({
    source: event.source,
    pk: image.pk,
    sk: image.sk,
  });
  const key = `${event.source}/${schema}/${image.pk}###${image.sk}.json`;
  console.log(
    JSON.stringify({
      bucket,
      schema,
      key,
      source: event.source,
      pk: image.pk,
      sk: image.sk,
    })
  );
  if (!schema) {
    return;
  }
  try {
    // we flatten the image to make sure the column names are compatible with athena
    const flattenedImage = flattenImage(image);
    console.log(JSON.stringify(flattenedImage, null, 2));
    if (event.detail.data.operation === "REMOVE") {
      await s3.send(
        new DeleteObjectCommand({
          Bucket: bucket,
          Key: key,
        })
      );
    } else {
      await s3.send(
        new PutObjectCommand({
          Bucket: bucket,
          Key: key,
          Body: JSON.stringify({ ...flattenedImage, schema }),
        })
      );
    }
  } catch (e) {
    console.error(e);
  }
};
