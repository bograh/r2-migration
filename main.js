import "dotenv/config";
import {
    S3Client,
    ListObjectsV2Command,
    DeleteObjectCommand,
    HeadObjectCommand,
    GetObjectCommand,
} from "@aws-sdk/client-s3";
import { Upload } from "@aws-sdk/lib-storage";


const SOURCE_ACCOUNT_ID = process.env.SOURCE_ACCOUNT_ID;
const SOURCE_ACCESS_KEY_ID = process.env.SOURCE_ACCESS_KEY_ID;
const SOURCE_SECRET_ACCESS_KEY = process.env.SOURCE_SECRET_ACCESS_KEY;
const SOURCE_BUCKET = process.env.SOURCE_BUCKET;

const DEST_ACCOUNT_ID = process.env.DEST_ACCOUNT_ID;
const DEST_ACCESS_KEY_ID = process.env.DEST_ACCESS_KEY_ID;
const DEST_SECRET_ACCESS_KEY = process.env.DEST_SECRET_ACCESS_KEY;
const DEST_BUCKET = process.env.DEST_BUCKET;

const KEY_PREFIX = process.env.KEY_PREFIX ?? "";
const DRY_RUN = process.env.DRY_RUN === "true";
const DELETE_AFTER_COPY = process.env.DELETE_AFTER_COPY === "true";
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT ?? "5", 10);


const required = [
    "SOURCE_ACCOUNT_ID", "SOURCE_ACCESS_KEY_ID", "SOURCE_SECRET_ACCESS_KEY", "SOURCE_BUCKET",
    "DEST_ACCOUNT_ID", "DEST_ACCESS_KEY_ID", "DEST_SECRET_ACCESS_KEY", "DEST_BUCKET",
];
const missing = required.filter(k => !process.env[k]);

if (missing.length) {
    console.error(`❌  Missing required environment variables: ${missing.join(", ")}`);
    process.exit(1);
}


const sourceClient = new S3Client({
    region: "auto",
    endpoint: `https://${SOURCE_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: SOURCE_ACCESS_KEY_ID,
        secretAccessKey: SOURCE_SECRET_ACCESS_KEY,
    },
});

const destClient = new S3Client({
    region: "auto",
    endpoint: `https://${DEST_ACCOUNT_ID}.r2.cloudflarestorage.com`,
    credentials: {
        accessKeyId: DEST_ACCESS_KEY_ID,
        secretAccessKey: DEST_SECRET_ACCESS_KEY,
    },
});


/**
 * Yields all objects in the source bucket (handles pagination automatically).
 */
async function* listAllObjects(prefix = "") {
    let continuationToken;
    do {
        const response = await sourceClient.send(new ListObjectsV2Command({
            Bucket: SOURCE_BUCKET,
            Prefix: prefix || undefined,
            ContinuationToken: continuationToken,
        }));
        for (const obj of response.Contents ?? []) {
            yield obj;
        }
        continuationToken = response.IsTruncated ? response.NextContinuationToken : undefined;
    } while (continuationToken);
}

/**
 * Streams an object from the source bucket and uploads it to the destination.
 * Uses multipart upload via @aws-sdk/lib-storage to handle large files safely.
 */
async function streamCopyObject(key, contentType, contentLength) {
    const { Body } = await sourceClient.send(new GetObjectCommand({
        Bucket: SOURCE_BUCKET,
        Key: key,
    }));

    const upload = new Upload({
        client: destClient,
        params: {
            Bucket: DEST_BUCKET,
            Key: key,
            Body: Body,
            ...(contentType && { ContentType: contentType }),
            ...(contentLength && { ContentLength: contentLength }),
        },
        queueSize: 4,               // parallel multipart parts
        partSize: 10 * 1024 * 1024, // 10 MB per part
    });

    await upload.done();
}

/**
 * Verifies the destination object exists after upload.
 */
async function verifyDestination(key) {
    await destClient.send(new HeadObjectCommand({ Bucket: DEST_BUCKET, Key: key }));
}

/**
 * Deletes an object from the source bucket.
 */
async function deleteFromSource(key) {
    await sourceClient.send(new DeleteObjectCommand({ Bucket: SOURCE_BUCKET, Key: key }));
}

/**
 * Runs async task factories with bounded concurrency.
 */
async function runWithConcurrency(tasks, concurrency) {
    const executing = new Set();
    const results = [];

    for (const task of tasks) {
        const p = task().then(r => { executing.delete(p); return r; });
        executing.add(p);
        results.push(p);
        if (executing.size >= concurrency) {
            await Promise.race(executing);
        }
    }

    return Promise.allSettled(results);
}


const stats = { total: 0, copied: 0, deleted: 0, failed: 0 };

function formatBytes(bytes) {
    if (!bytes) return "0 B";
    const k = 1024;
    const sizes = ["B", "KB", "MB", "GB", "TB"];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return `${parseFloat((bytes / Math.pow(k, i)).toFixed(2))} ${sizes[i]}`;
}


async function main() {
    console.log("━".repeat(60));
    console.log("  R2 Cross-Account File Mover");
    console.log("━".repeat(60));
    console.log(`  Source      : ${SOURCE_BUCKET} (account: ${SOURCE_ACCOUNT_ID})`);
    console.log(`  Destination : ${DEST_BUCKET} (account: ${DEST_ACCOUNT_ID})`);
    console.log(`  Prefix      : ${KEY_PREFIX || "(all files)"}`);
    console.log(`  Mode        : ${DRY_RUN ? "DRY RUN (no changes)" : DELETE_AFTER_COPY ? "MOVE (copy + delete)" : "COPY ONLY"}`);
    console.log(`  Concurrency : ${MAX_CONCURRENT}`);
    console.log("━".repeat(60));
    console.log();

    console.log("📋 Listing objects in source bucket...");
    const objects = [];
    let totalBytes = 0;

    for await (const obj of listAllObjects(KEY_PREFIX)) {
        objects.push(obj);
        totalBytes += obj.Size ?? 0;
    }

    stats.total = objects.length;
    console.log(`   Found ${stats.total} object(s) (${formatBytes(totalBytes)})\n`);

    if (stats.total === 0) {
        console.log("✅  Nothing to move. Exiting.");
        return;
    }

    const tasks = objects.map(obj => async () => {
        const key = obj.Key;

        if (DRY_RUN) {
            console.log(`[DRY RUN] Would copy: ${key} (${formatBytes(obj.Size)})`);
            stats.copied++;
            return;
        }

        try {
            // 1. Stream from source → upload to destination
            await streamCopyObject(key, obj.ContentType, obj.Size);

            // 2. Verify destination
            await verifyDestination(key);
            stats.copied++;
            console.log(`✅  Copied : ${key} (${formatBytes(obj.Size)})`);

            // 3. Optionally delete from source
            if (DELETE_AFTER_COPY) {
                await deleteFromSource(key);
                stats.deleted++;
                console.log(`🗑️   Deleted: ${key}`);
            }
        } catch (err) {
            stats.failed++;
            console.error(`❌  Failed : ${key} — ${err.message}`);
        }
    });

    await runWithConcurrency(tasks, MAX_CONCURRENT);

    console.log();
    console.log("━".repeat(60));
    console.log("  Summary");
    console.log("━".repeat(60));
    console.log(`  Total objects : ${stats.total}`);
    console.log(`  Copied        : ${stats.copied}`);
    if (DELETE_AFTER_COPY) {
        console.log(`  Deleted       : ${stats.deleted}`);
    }
    console.log(`  Failed        : ${stats.failed}`);
    console.log("━".repeat(60));

    if (stats.failed > 0) {
        console.error("\n⚠️  Some files failed. Check the logs above.");
        process.exit(1);
    } else {
        console.log("\n✅  All done!");
    }
}

main().catch(err => {
    console.error("Fatal error:", err);
    process.exit(1);
});
