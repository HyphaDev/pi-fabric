// Measures MeshStore read costs against a large event log and a large state file.
//
//   node scripts/benchmark-mesh-read.mjs [--events-mb 30] [--state-mb 3] [--iterations 20]
//
// Builds a temporary mesh root with a realistic events.jsonl (increasing
// sequence numbers, ~300-byte lines) and a state.json with many entries, then
// times the read paths a poller exercises. The store is bundled from
// `src/mesh/store.ts` with esbuild (the published bundle splits it into an
// unnamed chunk), so the script measures the working tree without a build.
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { performance } from "node:perf_hooks";
import { fileURLToPath, pathToFileURL } from "node:url";

const repo = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const loadStore = async () => {
  const { build } = await import("esbuild");
  const bundle = await build({
    stdin: { resolveDir: repo, loader: "ts", contents: 'export { MeshStore } from "./src/mesh/store.ts";' },
    bundle: true, platform: "node", format: "esm", packages: "external", write: false, logLevel: "silent",
  });
  const bundlePath = path.join(fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-bench-bundle-")), "store.mjs");
  fs.writeFileSync(bundlePath, bundle.outputFiles[0].text);
  try {
    return (await import(pathToFileURL(bundlePath).href)).MeshStore;
  } finally {
    fs.rmSync(path.dirname(bundlePath), { recursive: true, force: true });
  }
};
const MeshStore = await loadStore();

const argument = (name, fallback) => {
  const index = process.argv.indexOf(name);
  return index >= 0 && process.argv[index + 1] !== undefined ? Number(process.argv[index + 1]) : fallback;
};
const EVENTS_MB = argument("--events-mb", 30);
const STATE_MB = argument("--state-mb", 3);
const ITERATIONS = Math.max(1, argument("--iterations", 20));
const MAX_EVENT_BYTES = 256 * 1024;
const MAX_READ_EVENTS = 100;
const RARE_TOPIC = "bench.rare";

const root = fs.mkdtempSync(path.join(os.tmpdir(), "pi-fabric-mesh-bench-"));
const eventsPath = path.join(root, "events.jsonl");
const statePath = path.join(root, "state.json");

const identity = { id: "session:bench", name: "bench", kind: "main", sessionId: "bench" };
const writeEvents = () => {
  const target = EVENTS_MB * 1024 * 1024;
  const descriptor = fs.openSync(eventsPath, "w");
  let sequence = 0;
  let written = 0;
  let lastRare = 0;
  try {
    while (written < target) {
      const lines = [];
      for (let index = 0; index < 1000; index++) {
        sequence += 1;
        const rare = sequence % 5000 === 0;
        if (rare) lastRare = sequence;
        lines.push(JSON.stringify({
          id: `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`,
          sequence,
          topic: rare ? RARE_TOPIC : `team.worker-${sequence % 7}`,
          kind: "message",
          from: identity,
          ...(sequence % 3 === 0 ? { to: `session:peer-${sequence % 11}` } : {}),
          text: `heartbeat ${sequence} ` + "x".repeat(120),
          data: { step: sequence, state: "active", load: (sequence % 100) / 100 },
          createdAt: 1_700_000_000_000 + sequence,
        }));
      }
      const block = lines.join("\n") + "\n";
      fs.writeSync(descriptor, block);
      written += Buffer.byteLength(block, "utf8");
    }
  } finally {
    fs.closeSync(descriptor);
  }
  fs.writeFileSync(path.join(root, "sequence"), JSON.stringify(sequence));
  return { sequence, lastRare, bytes: written };
};

const writeState = () => {
  const target = STATE_MB * 1024 * 1024;
  const entries = {};
  const versions = {};
  let index = 0;
  let bytes = 0;
  while (bytes < target) {
    index += 1;
    const key = `participants/session:bench-${index}`;
    const entry = {
      key,
      value: {
        id: `session:bench-${index}`,
        name: `worker-${index}`,
        kind: "worker",
        labels: { role: "worker", lane: `lane-${index % 9}` },
        heartbeatAt: 1_700_000_000_000 + index,
        description: "y".repeat(400),
      },
      version: 1,
      updatedAt: 1_700_000_000_000 + index,
      updatedBy: identity,
    };
    entries[key] = entry;
    versions[key] = 1;
    bytes += JSON.stringify(entry, null, 2).length + 40;
  }
  const state = { format: 1, revisionFormat: 2, entries, versions, highWater: index };
  fs.writeFileSync(statePath, JSON.stringify(state, null, 2));
  return { entries: index, bytes: fs.statSync(statePath).size };
};

const time = (label, run) => {
  run();
  const samples = [];
  for (let index = 0; index < ITERATIONS; index++) {
    const started = performance.now();
    run();
    samples.push(performance.now() - started);
  }
  samples.sort((left, right) => left - right);
  const mean = samples.reduce((sum, value) => sum + value, 0) / samples.length;
  const median = samples[Math.floor(samples.length / 2)];
  console.log(`${label.padEnd(44)} mean ${mean.toFixed(2).padStart(9)} ms   median ${median.toFixed(2).padStart(9)} ms   max ${samples[samples.length - 1].toFixed(2).padStart(9)} ms`);
};

try {
  const events = writeEvents();
  const state = writeState();
  console.log(`events.jsonl: ${(events.bytes / 1024 / 1024).toFixed(1)} MB, ${events.sequence} events; state.json: ${(state.bytes / 1024 / 1024).toFixed(1)} MB, ${state.entries} entries; ${ITERATIONS} iterations`);
  const store = new MeshStore(root, MAX_EVENT_BYTES, MAX_READ_EVENTS, { maxEventLogBytes: 1024 * 1024 * 1024 });
  const nearTail = events.sequence - 50;
  time("read({after: tail-50, limit: 100})", () => {
    const result = store.read({ after: nearTail, limit: 100 });
    if (result.length !== 50) throw new Error(`expected 50 events, got ${result.length}`);
  });
  time("read({after: last, limit: 100}) (nothing new)", () => {
    const result = store.read({ after: events.sequence, limit: 100 });
    if (result.length !== 0) throw new Error(`expected 0 events, got ${result.length}`);
  });
  time("read({after: last-5000, topic: rare})", () => {
    const result = store.read({ after: events.sequence - 5000, topic: RARE_TOPIC, limit: 100 });
    if (result.length !== 1) throw new Error(`expected 1 rare event, got ${result.length}`);
  });
  time("read({after: 0, topic: rare}) (full scan)", () => {
    const result = store.read({ after: 0, topic: RARE_TOPIC, limit: 100 });
    if (result.length === 0) throw new Error("expected rare events");
  });
  time("read({after: 0, limit: 100}) (head)", () => {
    const result = store.read({ after: 0, limit: 100 });
    if (result.length !== 100) throw new Error(`expected 100 events, got ${result.length}`);
  });
  time("read() (recent, no after)", () => {
    const result = store.read({ limit: 100 });
    if (result.length !== 100) throw new Error(`expected 100 events, got ${result.length}`);
  });
  let touch = 0;
  const firstKey = "participants/session:bench-1";
  time("get(key) after state.json mtime change", () => {
    touch += 1;
    const when = new Date(1_700_000_000_000 + touch * 2000);
    fs.utimesSync(statePath, when, when);
    if (store.get(firstKey) === undefined) throw new Error("missing state entry");
  });
  time("get(key) with unchanged state.json (cached)", () => {
    if (store.get(firstKey) === undefined) throw new Error("missing state entry");
  });
  time("list('participants/', 100) (cached state)", () => {
    if (store.list("participants/", 100).length !== 100) throw new Error("missing entries");
  });
} finally {
  fs.rmSync(root, { recursive: true, force: true });
}
