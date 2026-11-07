// The backup of I-4, its step bodies stubbed, on a memory or file store:
//
//   node --conditions=substantial-source typescript/examples/backup.ts memory://backup
//   node --conditions=substantial-source typescript/examples/backup.ts file:/tmp/substantial-backup-ts
//
// Two approvals arrive at once, so the wait ends at the first replay that binds the handler; without
// them, a day later, the rollback deletes the upload then the archive.
import * as s from "substantial";

const deleteArchive = async (archive: string, _files: string[], _tag: string) => {
  console.log(`undo: delete ${archive}`);
};
const deleteUpload = async (url: string, _archive: string, _target: string) => {
  console.log(`undo: delete ${url}`);
};

const listFiles = s.step(
  "list_files",
  async (source: string): Promise<string[]> => [`${source}/a.txt`, `${source}/b.txt`],
);
const makeArchive = s.step(
  "make_archive",
  async (_files: string[], tag: string): Promise<string> => `/tmp/backup-${tag}.tar`,
  { timeout: s.minutes(10), undo: deleteArchive },
);
const upload = s.step(
  "upload",
  async (archive: string, target: string): Promise<string> =>
    `${target}/${archive.split("/").at(-1)}`,
  {
    retry: { maxAttempts: 5, initial: s.seconds(1), max: s.minutes(1), multiplier: 2 },
    undo: deleteUpload,
  },
);
const checkArchive = s.step("check_archive", async (_archive: string): Promise<boolean> => true);

export const verify = s.workflow("verify", async (_c, input: { archive: string }) =>
  checkArchive(input.archive),
);

export const backup = s.workflow("backup", async (c, input: { source: string; target: string }) => {
  const tag = await c.uuid();
  const files = await listFiles(input.source);
  const archive = await makeArchive(files, tag);
  const url = await upload(archive, input.target);
  c.log.info(`archived ${files.length} files`);

  const approvedBy: string[] = [];
  c.handle<string>("approve", (name) => {
    if (!approvedBy.includes(name)) approvedBy.push(name);
  });
  if (!(await c.waitUntil(() => approvedBy.length >= 2, { timeout: s.days(1) }))) {
    await c.rollback("not approved within a day");
  }

  const check = await c.start(verify, { archive });
  return { url, approved_by: approvedBy, check: await check.result() };
});

export async function main(url: string): Promise<unknown> {
  await using sub = await s.Substantial.create(url, { partitions: 1 });
  await using _worker = sub.worker([backup, verify], { preset: "script" });
  const run = await sub.start(
    backup,
    { source: "/data", target: "s3://archive" },
    { id: "backup-2026-10-31" },
  );
  await run.send("approve", "alice", { id: "approve-alice" });
  await run.send("approve", "bob", { id: "approve-bob" });
  return await run.result({ timeout: s.minutes(1) });
}

if (import.meta.main) {
  const url = process.argv[2];
  if (url === undefined) {
    console.error("usage: backup.ts <memory://ns | file:path>");
    process.exit(2);
  }
  console.log(JSON.stringify(await main(url)));
}
