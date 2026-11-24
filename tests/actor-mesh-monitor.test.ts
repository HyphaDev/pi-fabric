import { MeshStore } from "../src/mesh/store.js";

import fs, { type FSWatcher } from "node:fs";
import os from "node:os";
import path from "node:path";
import { EventEmitter } from "node:events";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ActorMeshMonitor } from "../src/actors/mesh-monitor.js";
import type { MeshEvent } from "../src/mesh/store.js";

const roots: string[] = [];
const monitors: ActorMeshMonitor[] = [];
afterEach(() => {
  for (const monitor of monitors.splice(0)) monitor.close();
  vi.restoreAllMocks();
  vi.useRealTimers();
  for (const root of roots.splice(0)) fs.rmSync(root, { recursive: true, force: true });
});

function setup(cursor?: string) {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-monitor-"));
  roots.push(root);
  const cursorPath = path.join(root, "cursor.json");
  if (cursor !== undefined) fs.writeFileSync(cursorPath, cursor);
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
  vi.spyOn(fs, "watch").mockReturnValue(watcher as unknown as FSWatcher);
  const event = { topic: "test" } as MeshEvent;
  const mesh = { root, latestOffset: vi.fn(() => 10), latestSequence: vi.fn(() => 0), tail: vi.fn(() => ({ events: [event], nextOffset: 20 })) };
  const beforePoll = vi.fn(() => true);
  const onEvent = vi.fn();
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 7 }, { cursorPath, beforePoll, onEvent });
  monitors.push(monitor);
  return { root, cursorPath, watcher, mesh, beforePoll, onEvent, monitor };
}

const flush = async () => { await Promise.resolve(); await Promise.resolve(); };

it("seeds a legacy cursor before compaction without replaying retained events", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-legacy-"));
  roots.push(root);
  const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 6000, retainedEventLogBytes: 4000 });
  const publish = () => mesh.publish({ topic: "test", from: { id: "peer", name: "peer", kind: "main" }, text: "x".repeat(300) });
  for (let i = 0; i < 6; i++) await publish();
  const sequence = mesh.latestSequence();
  const cursorPath = path.join(root, "cursor.json");
  fs.writeFileSync(cursorPath, JSON.stringify({ format: 1, cursor: mesh.latestOffset() }));
  const delivered: number[] = [];
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 100 }, {
    cursorPath, beforePoll: () => true, onEvent: event => { delivered.push(event.sequence); },
  });
  monitors.push(monitor);
  while (!fs.existsSync(path.join(root, "generation"))) await publish();
  const fresh = await publish();
  monitor.schedule();
  await flush();
  expect(delivered).toEqual(Array.from({ length: fresh.sequence - sequence }, (_, i) => sequence + i + 1));
});
it("resumes a persisted actor cursor after compaction without redelivery", async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-generation-"));
  roots.push(root);
  const mesh = new MeshStore(root, 1024, 100, { maxEventLogBytes: 6000, retainedEventLogBytes: 4000 });
  const publish = () => mesh.publish({ topic: "test", from: { id: "session:test", name: "test", kind: "main" }, text: "x".repeat(300) });
  for (let i = 0; i < 6; i++) await publish();
  const cursorPath = path.join(root, "cursor.json");
  const config = { enabled: true, actorPollMs: 2000, maxReadEvents: 100 };
  const delivered: number[] = [];
  const callbacks = { cursorPath, beforePoll: () => true, onEvent: (event: MeshEvent) => { delivered.push(event.sequence); } };
  const first = new ActorMeshMonitor(mesh, config, callbacks);
  monitors.push(first);
  first.schedule();
  await flush();
  first.close();
  const sequence = mesh.latestSequence();
  const generationPath = path.join(root, "generation");
  while (!fs.existsSync(generationPath)) await publish();
  const fresh = await publish();
  const resumed = new ActorMeshMonitor(mesh, config, callbacks);
  monitors.push(resumed);
  resumed.schedule();
  await flush();
  expect(delivered).toEqual(Array.from({ length: fresh.sequence - sequence }, (_, i) => sequence + i + 1));
  resumed.schedule();
  await flush();
  expect(delivered.filter((value) => value === fresh.sequence)).toHaveLength(1);
});
it("detects appends despite missed watcher notifications without idle tail reads", async () => {
  vi.useFakeTimers();
  const root = fs.mkdtempSync(path.join(os.tmpdir(), "actor-missed-watch-"));
  roots.push(root);
  const mesh = new MeshStore(root, 1024, 100);
  const watcher = Object.assign(new EventEmitter(), { close: vi.fn() });
  vi.spyOn(fs, "watch").mockReturnValue(watcher as unknown as FSWatcher);
  const delivered: number[] = [];
  const monitor = new ActorMeshMonitor(mesh, { enabled: true, actorPollMs: 50, maxReadEvents: 100 }, {
    beforePoll: () => true, onEvent: event => { delivered.push(event.sequence); },
  });
  monitors.push(monitor);
  monitor.start();
  await flush();
  const tail = vi.spyOn(mesh, "tail");
  await vi.advanceTimersByTimeAsync(100);
  expect(tail).not.toHaveBeenCalled();
  const event = await mesh.publish({ topic: "test", from: { id: "peer", name: "peer", kind: "main" }, text: "missed notification" });
  await vi.advanceTimersByTimeAsync(50);
  expect(delivered).toEqual([event.sequence]);
  tail.mockClear();
  await vi.advanceTimersByTimeAsync(100);
  expect(tail).not.toHaveBeenCalled();
});
describe("ActorMeshMonitor", () => {
  it("coalesces notifications, preserves a halted cursor, and resumes in order", async () => {
    const s = setup('{"format":1,"cursor":3}');
    s.beforePoll.mockReturnValue(false);
    s.monitor.start();
    await flush();
    expect(s.mesh.tail).not.toHaveBeenCalled();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    s.beforePoll.mockReturnValue(true);
    s.monitor.schedule();
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledExactlyOnceWith(3, 7, { afterSequence: 0 });
    expect(s.onEvent).toHaveBeenCalledOnce();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8"))).toEqual({ format: 1, cursor: 20 });
  });

  it.skipIf(process.platform === "win32")("polls on a watch event without a filename, as Bun reports undefined", async () => {
    const s = setup();
    s.monitor.start();
    await flush();
    s.mesh.tail.mockClear();
    // The mock records the (path, options, listener) overload the monitor calls.
    const listener = (vi.mocked(fs.watch).mock.calls[0] as unknown[])[2] as (event: string, filename: string | null | undefined) => void;
    expect(() => listener("rename", undefined)).not.toThrow();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledOnce();
    listener("change", "unrelated.json");
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledOnce();
  });

  it.skipIf(process.platform === "win32")("falls back after watcher errors and closes timers and queued work", async () => {
    const s = setup();
    s.monitor.start();
    await flush();
    s.watcher.emit("error", new Error("watch failed"));
    await flush();
    expect(s.watcher.close).toHaveBeenCalledOnce();
    const count = s.mesh.tail.mock.calls.length;
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail).toHaveBeenCalledTimes(count + 1);
    s.monitor.schedule();
    s.monitor.close();
    s.monitor.close();
    s.watcher.emit("error", new Error("late error"));
    await vi.advanceTimersByTimeAsync(5000);
    expect(s.mesh.tail).toHaveBeenCalledTimes(count + 1);
    expect(vi.getTimerCount()).toBe(0);
    expect(s.watcher.close).toHaveBeenCalledOnce();
  });

  it("uses polling when watch creation fails and ignores malformed cursors", async () => {
    const s = setup('{"format":2,"cursor":3}');
    vi.mocked(fs.watch).mockImplementation(() => { throw new Error("unsupported"); });
    s.monitor.start();
    s.monitor.start();
    await flush();
    expect(s.mesh.tail).toHaveBeenCalledExactlyOnceWith(10, 7, { afterSequence: 0 });
    await vi.advanceTimersByTimeAsync(50);
    expect(s.mesh.tail).toHaveBeenCalledTimes(2);
    expect(vi.getTimerCount()).toBe(1);
  });

  it("does not commit a cursor after dispatch failure and tolerates cursor write failure", async () => {
    const s = setup('{"format":1,"cursor":3}');
    s.onEvent.mockImplementationOnce(() => { throw new Error("dispatch"); });
    s.monitor.start();
    await flush();
    expect(JSON.parse(fs.readFileSync(s.cursorPath, "utf8")).cursor).toBe(3);
    fs.rmSync(s.cursorPath);
    fs.mkdirSync(s.cursorPath);
    s.monitor.schedule();
    await flush();
    expect(s.mesh.tail).toHaveBeenLastCalledWith(20, 7, { afterSequence: 0 });
    expect(s.onEvent).toHaveBeenCalledTimes(2);
  });
});
