import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { SessionManager } from "@earendil-works/pi-coding-agent";
import { pageSessionHistory, __historyPagingTest } from "../src/memory/history-paging.js";

const message = (text: string) => ({ role: "user" as const, content: text, timestamp: 1 });
const large = "λ漢字\\n\"".repeat(8000);

describe("inactive session payload paging", () => {
  it("rotates at the cap and cold-reads both unlinked archives", async () => {
    __historyPagingTest.setMaxArchiveBytes(1);
    const opened = vi.spyOn(fs, "openSync");
    const cold = vi.spyOn(WeakRef.prototype, "deref").mockReturnValue(undefined);
    try {
      const manager = SessionManager.inMemory();
      const ids = [manager.appendMessage(message(`old:${large}`)), manager.appendMessage(message(`new:${large}`))];
      manager.appendCompaction("summary", null, 10000);
      expect((await pageSessionHistory(manager)).entries).toBe(2);
      const paths = opened.mock.calls.map(call => String(call[0])).filter(file => file.endsWith("payloads.jsonl"));
      expect(paths).toHaveLength(2);
      if (process.platform !== "win32") for (const file of paths) {
        fs.rmSync(file, { force: true });
        expect(fs.existsSync(file)).toBe(false);
      }
      for (const [index, id] of ids.entries()) {
        const cached = Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value;
        expect(cached.content).toBe(`${index === 0 ? "old" : "new"}:${large}`);
      }
    } finally { __historyPagingTest.setMaxArchiveBytes(); cold.mockRestore(); opened.mockRestore(); }
  });
  it("keeps cold payloads readable after path removal across 50 paging passes", async () => {
    const written = vi.spyOn(fs, "writeSync");
    const cold = vi.spyOn(WeakRef.prototype, "deref").mockReturnValue(undefined);
    try {
      const manager = SessionManager.inMemory();
      const retained: { content: string }[] = [];
      for (let turn = 0; turn < 50; turn++) {
        const content = `${turn}: ${large}`;
        const id = manager.appendMessage(message(content));
        manager.appendCompaction("summary", null, 10000);
        expect((await pageSessionHistory(manager)).entries).toBe(1);
        const cached = Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value as { content: string };
        retained.push(cached);
        const fds = new Set(written.mock.calls.map(call => call[0]));
        expect(fds.size).toBeGreaterThan(0);
        if (process.platform !== "win32") for (const fd of fds) {
          expect(fs.fstatSync(fd).nlink).toBe(0);
        }
        for (let earlier = 0; earlier <= turn; earlier++) {
          expect(retained[earlier]!.content).toBe(`${earlier}: ${large}`);
          expect(retained[earlier]!.content).toBe(`${earlier}: ${large}`);
        }
      }
    } finally { cold.mockRestore(); written.mockRestore(); }
  });

  it("releases an aborted pass's writer lease", async () => {
    __historyPagingTest.setMaxArchiveBytes(1);
    try {
      const manager = SessionManager.inMemory();
      for (let i = 0; i < 96; i++) manager.appendMessage(message(`${i}:${large}`));
      manager.appendCompaction("summary", null, 10000);
      const controller = new AbortController();
      const pending = pageSessionHistory(manager, controller.signal);
      const archive = __historyPagingTest.archives().at(-1)!;
      expect(archive.writers).toBe(1);
      controller.abort();
      const result = await pending;
      expect(result.entries).toBeGreaterThan(0);
      expect(result.entries).toBeLessThan(96);
      expect(archive.writers).toBe(0);
      expect(() => archive.write("later")).toThrow("closed for writing");
    } finally { __historyPagingTest.setMaxArchiveBytes(); }
  });

  it("closes an ownerless archive on write failure and preserves the cause", async () => {
    __historyPagingTest.setMaxArchiveBytes(1);
    const manager = SessionManager.inMemory();
    const id = manager.appendMessage(message(large));
    manager.appendCompaction("summary", null, 10000);
    const cause = new Error("ENOSPC: no space left");
    let archive: ReturnType<typeof __historyPagingTest.archives>[number] | undefined;
    const write = vi.spyOn(fs, "writeSync").mockImplementation(() => {
      archive = __historyPagingTest.archives().at(-1);
      throw cause;
    });
    try {
      await expect(pageSessionHistory(manager)).rejects.toMatchObject({
        message: "Historical payload could not be written: ENOSPC: no space left", cause,
      });
      expect(archive).toBeDefined();
      expect(archive!.writers).toBe(0);
      expect(archive!.closed).toBe(true);
      expect(archive!.fd).toBeUndefined();
      expect(Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value.content).toBe(large);
    } finally { write.mockRestore(); __historyPagingTest.setMaxArchiveBytes(); }
  });

  it("shares the newest archive between concurrent sessions with intact cold reads", async () => {
    const managers = [SessionManager.inMemory(), SessionManager.inMemory()];
    const ids = managers.map((manager, session) => {
      const ids = Array.from({ length: 40 }, (_, index) => manager.appendMessage(message(`${session}:${index}:${large}`)));
      manager.appendCompaction("summary", null, 10000);
      return ids;
    });
    const newest = __historyPagingTest.archives().at(-1);
    const write = vi.spyOn(fs, "writeSync");
    const cold = vi.spyOn(WeakRef.prototype, "deref").mockReturnValue(undefined);
    try {
      const results = await Promise.all(managers.map(manager => pageSessionHistory(manager)));
      expect(results.map(result => result.entries)).toEqual([40, 40]);
      const fds = new Set(write.mock.calls.map(call => call[0]));
      expect(fds.size).toBe(1);
      if (newest && newest.offset < 64 * 1024 * 1024) expect([...fds]).toEqual([newest.fd]);
      for (const [session, manager] of managers.entries()) for (const [index, id] of ids[session]!.entries()) {
        const cached = Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value;
        expect(cached.content).toBe(`${session}:${index}:${large}`);
      }
    } finally { write.mockRestore(); cold.mockRestore(); }
  });
  it.each(["short read", "EBADF"])("reports a cold %s failure without hiding content loss", async failure => {
    const manager = SessionManager.inMemory();
    const id = manager.appendMessage(message(large));
    manager.appendCompaction("summary", null, 10000);
    await pageSessionHistory(manager);
    const cached = Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value as { content: string };
    const cold = vi.spyOn(WeakRef.prototype, "deref").mockReturnValue(undefined);
    const read = vi.spyOn(fs, "readSync");
    if (failure === "short read") read.mockReturnValue(0);
    else read.mockImplementation(() => { throw new Error("EBADF: bad file descriptor"); });
    try {
      expect(() => cached.content).toThrow(failure === "short read"
        ? "Historical payload could not be read: Historical payload archive is incomplete"
        : "Historical payload could not be read: EBADF: bad file descriptor");
    } finally { read.mockRestore(); cold.mockRestore(); }
    expect(cached.content).toBe(large);
  });

  it("preserves entry identities, JSON, branch navigation and current context", async () => {
    const manager = SessionManager.inMemory();
    const first = manager.appendMessage(message(large));
    manager.appendMessage(message("small old message"));
    const current = manager.appendMessage(message("current task"));
    manager.appendCompaction("Summary", current, 10000);
    const cachedEntries = manager.getEntries(); // pi-tps and other history consumers retain these.
    const before = JSON.stringify(manager.getEntries());
    const context = JSON.stringify(manager.buildSessionContext());
    const live = manager.getEntry(first)!;
    const cachedMessage = Object.getOwnPropertyDescriptor(live, "message")!.value as object;
    const result = await pageSessionHistory(manager);
    expect(result.entries).toBe(1);
    expect(result.bytes).toBeGreaterThan(16000);
    expect(manager.getEntry(first)).toBe(live);
    expect(cachedEntries[0]).toBe(live);
    expect(Object.getOwnPropertyDescriptor(live, "message")?.value).toBe(cachedMessage);
    expect(Object.getOwnPropertyDescriptor(cachedMessage, "content")?.get).toBeTypeOf("function");
    expect(JSON.stringify(manager.getEntries())).toBe(before);
    expect(JSON.stringify(manager.buildSessionContext())).toBe(context);
    expect(await pageSessionHistory(manager)).toEqual({ entries: 0, bytes: 0 });
    manager.branch(first);
    expect(manager.buildSessionContext().messages[0]).toEqual(message(large));
    expect((await pageSessionHistory(manager)).entries).toBe(0);
  });

  it("does not rewrite the source file and preserves labels, tree and extension state", async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), "history-paging-test-"));
    try {
      const seed = SessionManager.inMemory();
      const old = seed.appendMessage(message(large));
      seed.appendLabelChange(old, "keep me");
      seed.appendCustomEntry("extension-state", { keep: true });
      seed.appendCompaction("summary", null, 10000);
      const file = path.join(root, "session.jsonl");
      const original = [seed.getHeader(), ...seed.getEntries()].map(row => JSON.stringify(row)).join("\n") + "\n";
      fs.writeFileSync(file, original);
      const manager = SessionManager.open(file, root);
      const tree = JSON.stringify(manager.getTree());
      expect((await pageSessionHistory(manager)).entries).toBe(1);
      expect(fs.readFileSync(file, "utf8")).toBe(original);
      expect(manager.getLabel(old)).toBe("keep me");
      expect(JSON.stringify(manager.getTree())).toBe(tree);
      expect(manager.getEntries().find(row => row.type === "custom")).toMatchObject({ data: { keep: true } });
    } finally { fs.rmSync(root, { recursive: true, force: true }); }
  });
  it("keeps cached message identity and metadata reads independent of the cold body", async () => {
    const manager = SessionManager.inMemory();
    const id = manager.appendMessage(message(large));
    manager.appendCompaction("summary", null, 10000);
    const cached = Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value as { role: string; content: unknown; timestamp: number };
    await pageSessionHistory(manager);
    const descriptor = Object.getOwnPropertyDescriptor(cached, "content")!;
    const readBody = vi.fn(descriptor.get);
    Object.defineProperty(cached, "content", { ...descriptor, get: readBody });
    expect(Object.getOwnPropertyDescriptor(manager.getEntry(id)!, "message")!.value).toBe(cached);
    expect(cached.role).toBe("user");
    expect(cached.timestamp).toBe(1);
    expect(readBody).not.toHaveBeenCalled();
    expect(cached.content).toBe(large);
    expect(readBody).toHaveBeenCalledOnce();
  });
  it("never pages active messages or changes custom extension state", async () => {
    const manager = SessionManager.inMemory();
    const id = manager.appendMessage(message(large));
    const custom = manager.appendCustomEntry("extension-state", { data: large });
    expect(await pageSessionHistory(manager)).toEqual({ entries: 0, bytes: 0 });
    expect(Object.getOwnPropertyDescriptor(manager.getEntry(id), "message")?.value).toEqual(message(large));
    expect(manager.getEntry(custom)).toMatchObject({ data: { data: large } });
  });

  it("supports whole-message replacement and does not replace foreign getters", async () => {
    const manager = SessionManager.inMemory();
    const id = manager.appendMessage(message(large));
    manager.appendCompaction("summary", null, 10000);
    await pageSessionHistory(manager);
    const entry = manager.getEntry(id)! as unknown as { message: unknown };
    entry.message = message("replacement");
    expect(entry.message).toEqual(message("replacement"));
    const getter = vi.fn(() => message(large));
    Object.defineProperty(entry, "message", { get: getter, configurable: true, enumerable: true });
    expect(await pageSessionHistory(manager)).toEqual({ entries: 0, bytes: 0 });
    expect(getter).not.toHaveBeenCalled();
  });

  it("aborts cooperatively without losing any history", async () => {
    const manager = SessionManager.inMemory();
    for (let i = 0; i < 96; i++) manager.appendMessage(message(`${i}: ${large}`));
    manager.appendCompaction("summary", null, 10000);
    const before = JSON.stringify(manager.getEntries());
    const controller = new AbortController();
    const pending = pageSessionHistory(manager, controller.signal);
    controller.abort();
    const result = await pending;
    expect(result.entries).toBeLessThanOrEqual(32);
    expect(JSON.stringify(manager.getEntries())).toBe(before);
    expect(await pageSessionHistory(manager, controller.signal)).toEqual({ entries: 0, bytes: 0 });
  });
});
