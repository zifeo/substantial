// The two S3 answers the adapter reads: a ListObjectsV2 page and an error. S3 answers are
// generated XML without CDATA or comments, so matching elements is enough.

const ENTITIES: Record<string, string> = { lt: "<", gt: ">", amp: "&", quot: '"', apos: "'" };

function unescapeXml(text: string): string {
  return text.replace(/&(#x[0-9a-fA-F]+|#\d+|\w+);/g, (whole, entity: string) => {
    if (entity.startsWith("#x")) return String.fromCodePoint(Number.parseInt(entity.slice(2), 16));
    if (entity.startsWith("#")) return String.fromCodePoint(Number.parseInt(entity.slice(1), 10));
    return ENTITIES[entity] ?? whole;
  });
}

function elements(xml: string, tag: string): string[] {
  const pattern = new RegExp(`<(?:\\w+:)?${tag}>([\\s\\S]*?)</(?:\\w+:)?${tag}>`, "g");
  return Array.from(xml.matchAll(pattern), (m) => m[1] as string);
}

export function errorCode(body: Uint8Array): string | undefined {
  const code = elements(new TextDecoder().decode(body), "Code")[0];
  return code === undefined ? undefined : unescapeXml(code);
}

/** An S3 error answer as `Code: Message`, or the start of a body that is not one. */
export function errorText(body: Uint8Array): string {
  const xml = new TextDecoder().decode(body);
  const code = elements(xml, "Code")[0]?.trim();
  if (!code) return xml.slice(0, 200);
  const message = elements(xml, "Message")[0]?.trim();
  return message ? `${unescapeXml(code)}: ${unescapeXml(message)}` : unescapeXml(code);
}

/** The full keys of a ListObjectsV2 page and whether more follow; `undefined` when the body is
 * not such a page. */
export function parseListing(body: Uint8Array): { keys: string[]; truncated: boolean } | undefined {
  const xml = new TextDecoder().decode(body);
  if (!/<(?:\w+:)?ListBucketResult[\s>]/.test(xml)) return undefined;
  const keys = elements(xml, "Contents").map((contents) =>
    unescapeXml(elements(contents, "Key")[0] ?? ""),
  );
  return { keys, truncated: elements(xml, "IsTruncated")[0]?.trim() === "true" };
}
