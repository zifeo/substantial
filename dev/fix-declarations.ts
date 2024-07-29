// Copyright Metatype OÜ, licensed under the Mozilla Public License Version 2.0.
// SPDX-License-Identifier: MPL-2.0

import {
    dirname,
    fromFileUrl,
    join,
    relative,
    resolve,
} from "https://deno.land/std@0.224.0/path/mod.ts";
import { expandGlobSync } from "https://deno.land/std@0.224.0/fs/mod.ts";

export const thisDir = dirname(fromFileUrl(import.meta.url));

type Replacer = {
    path: string;
    op: (s: string) => string;
};

const basePath = "../substantial/deno/gen";

const replacements = [
    // Imports should refer to the actual file
    ...Array.from(
        expandGlobSync(join(basePath, "/**/*.d.ts"), {
            root: thisDir,
            includeDirs: false,
            globstar: true,
        }),
    ).map(({ path }) => ({
        path,
        op: (s: string) => s.replace(/^(import .*)(\.js)\';$/, "$1.d.ts';"),
    })),
    // Remove exports aliases
    {
        path: resolve(thisDir, basePath, "substantial.js"),
        op: (s: string) => s.replaceAll(/,\s*\w+ as '[\w:\/]+'/g, ""),
    },
    // Normalize native node imports
    {
        path: resolve(thisDir, basePath, "substantial.js"),
        op: (s: string) =>
            s.replaceAll(/["']fs\/promises["']/g, "'node:fs/promises'"),
    },
] as Array<Replacer>;

console.log("Fixing declarations..");
for (const { path, op } of replacements) {
    const text = Deno.readTextFileSync(path);
    const rewrite = [...text.split("\n")];

    for (let i = 0; i < rewrite.length; i += 1) {
        rewrite[i] = op(rewrite[i]);
    }

    const newText = rewrite.join("\n");
    if (text != newText) {
        console.log(`  Fixed generated code at ${relative(thisDir, path)}`);
        Deno.writeTextFileSync(path, newText);
    }
}

console.log("Merge types");
// Merge everything at interfaces/*
const merged = Array.from(
    expandGlobSync(join(basePath, "/interfaces/*.d.ts"), {
        root: thisDir,
        includeDirs: false,
        globstar: true,
    }),
).reduce((curr, { path }) => {
    console.log(`  < ${path}`);
    const next = `
  ${
        Deno.readTextFileSync(path)
            .replaceAll(/import type {.+} from ['"].+\.d\.ts['"];/g, (m) =>
                `// ${m}`)
            .replaceAll(/export {.+};/g, (m) =>
                `// ${m}`)
    }
  `;
    return curr + next;
}, "");

const hintMainPath = join(thisDir, basePath, "/substantial.d.ts");
const mergedContent = `
  // interfaces begin
  ${merged}
  // interfaces end
  
  // common
  `;

Deno.writeTextFileSync(hintMainPath, mergedContent);

await Deno.remove(join(thisDir, basePath, "/interfaces"), { recursive: true })
    .catch((_) => {})
    .finally(() => console.log("cleanup.."));
