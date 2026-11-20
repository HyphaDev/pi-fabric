import { describe, expect, it, vi } from "vitest";
import { AgentMessageRouter } from "../src/providers/agents-message-router.js";
import type { FabricInvocationContext } from "../src/protocol.js";
import type { FabricParticipantInfo } from "../src/topology/types.js";

type Ports = ConstructorParameters<typeof AgentMessageRouter>;
const remoteAgent = (overrides: Partial<FabricParticipantInfo> = {}): FabricParticipantInfo => ({
  format: 1, id: "remote-run", kind: "agent", rootId: "session:peer",
  ownerHostId: "runtime:owner", ownerIdentityId: "owner-agent",
  ownerIncarnation: "owner-generation", parentId: "owner-agent", name: "researcher",
  status: "running", runner: "pi", transport: "process",
  capabilities: ["steer", "followUp", "stop"], startedAt: 1, updatedAt: 2,
  controlProtocol: "v1", local: false, stale: false, ...overrides,
});
const setup = (
  participant: FabricParticipantInfo | undefined,
  withControl = true,
  stale: FabricParticipantInfo[] = [],
) => {
  const manager: Ports[0] = {
    status: vi.fn(() => { throw new Error("Unknown Fabric agent"); }),
    steer: vi.fn(), followUp: vi.fn(), stop: vi.fn(),
  };
  const actors: Ports[1] = {
    identity: { id: "caller", name: "caller", kind: "agent" },
    status: vi.fn(() => { throw new Error("Unknown Fabric actor"); }),
    validateDirectMessage: vi.fn(), tell: vi.fn(), ask: vi.fn(), stop: vi.fn(),
    steerRemote: vi.fn(), resolveBinding: vi.fn(),
  };
  const main: Ports[2] = {
    id: "session:main", local: false,
    matches: (id) => id === "main" || id === "session:main", deliverAgent: vi.fn(),
  };
  const participants: Ports[3] = {
    get: (id) => id === participant?.id ? participant : undefined,
    list: vi.fn((options) => options?.includeStale ? stale : []),
    scheduleRefresh: vi.fn(),
  };
  const request = vi.fn<NonNullable<Ports[4]>["request"]>().mockResolvedValue({
    queued: true, messageId: "receipt", routed: "mesh", acknowledged: true,
  });
  const router = new AgentMessageRouter(
    manager, actors, main, participants, withControl ? { request } : undefined,
    (binding) => binding,
  );
  return { router, request, actors };
};

describe("remote one-shot agent routing", () => {
  it.each(["steer", "followUp"] as const)("routes %s to the exact execution owner", async (kind) => {
    const participant = remoteAgent();
    const { router, request, actors } = setup(participant);
    await expect(router.routeMessage(participant.id, "protect the preview", { scope: "UX" }, kind,
      undefined, { triggerTurn: false })).resolves.toMatchObject({ routed: "mesh", acknowledged: true });
    expect(request).toHaveBeenCalledWith("runtime:owner", "remote-run", kind, {
      message: "protect the preview", data: { scope: "UX" }, triggerTurn: false,
      ownerIncarnation: "owner-generation",
    }, "owner-agent");
    expect(actors.tell).not.toHaveBeenCalled();
    expect(actors.steerRemote).not.toHaveBeenCalled();
  });

  it.each(["steer", "followUp"] as const)("labels a peer %s with its project label before acknowledgement", async kind => {
    const participant = remoteAgent({ id: "session:peer", kind: "root", label: "FAB-4", name: "main", transport: "host" });
    const { router, request } = setup(participant);
    const activity = vi.fn(), attachPreview = vi.fn();
    await router.routeMessage(participant.id, "Protect the preview", undefined, kind,
      { cwd: "/repo", update() {}, activity, attachPreview } as unknown as FabricInvocationContext, { triggerTurn: true });
    expect(activity).toHaveBeenCalledWith({ type: "entity", id: participant.id, kind: "agent", name: "FAB-4" });
    expect(attachPreview).toHaveBeenCalledWith({ kind: "fabric-agent-target", id: participant.id, name: "FAB-4" });
    expect(request).toHaveBeenCalledWith(participant.ownerHostId, participant.id, kind,
      expect.objectContaining({ triggerTurn: true }), participant.ownerIdentityId);
  });

  it("also routes a sibling owned by the caller's Main peer", async () => {
    const participant = remoteAgent({ rootId: "session:main", ownerHostId: "session:main", ownerIdentityId: "session:main" });
    const { router, request } = setup(participant);
    await router.routeMessage(participant.id, "status please", undefined, "followUp");
    expect(request).toHaveBeenCalledWith("session:main", participant.id, "followUp", expect.any(Object), "session:main");
  });

  it("refuses a capability withdrawn by the owner", async () => {
    const participant = remoteAgent({ capabilities: [] });
    const { router, request } = setup(participant);
    await expect(router.routeMessage(participant.id, "too late", undefined, "steer"))
      .rejects.toThrow("does not support steer");
    expect(request).not.toHaveBeenCalled();
  });

  it.each(["legacy", "missing"])("refuses a %s control channel instead of broadcasting", async (mode) => {
    const participant = remoteAgent(mode === "legacy" ? { controlProtocol: "legacy" } : {});
    const { router, request, actors } = setup(participant, mode !== "missing");
    await expect(router.routeMessage(participant.id, "hello", undefined, "steer"))
      .rejects.toThrow("has no control channel");
    expect(request).not.toHaveBeenCalled();
    expect(actors.steerRemote).not.toHaveBeenCalled();
  });

  it("does not reinterpret an unknown local run as remote", async () => {
    const participant = remoteAgent({ local: true });
    const { router, request } = setup(participant);
    await expect(router.routeMessage(participant.id, "hello", undefined, "steer"))
      .rejects.toThrow("Unknown Fabric participant");
    expect(request).not.toHaveBeenCalled();
  });
});

// A busy host can miss lease renewals under mesh lock contention. Its canonical
// record then reads stale while the session is alive and still drains control.
describe("routing to a live root whose lease lapsed", () => {
  const root = (overrides: Partial<FabricParticipantInfo> = {}): FabricParticipantInfo => remoteAgent({
    id: "session:main", kind: "root", rootId: "session:main", ownerHostId: "session:main",
    ownerIdentityId: "session:main", ownerIncarnation: "main-generation", name: "main", transport: "host",
    capabilities: ["steer", "followUp", "fabric"], ...overrides,
  });
  const peer = {
    id: "session:peer", rootId: "session:peer", ownerHostId: "session:peer", ownerIdentityId: "session:peer",
  };

  it("delivers a child's followUp to its Main through the stale canonical record", async () => {
    const { router, request } = setup(undefined, true, [root({ stale: true })]);
    await expect(router.routeMessage("main", "done", undefined, "followUp"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
    expect(request).toHaveBeenCalledWith("session:main", "session:main", "followUp",
      expect.objectContaining({ message: "done", ownerIncarnation: "main-generation" }), "session:main");
  });

  it("delivers a child's followUp to its Main by identity when no record is readable", async () => {
    const { router, request } = setup(undefined);
    await expect(router.routeMessage("main", "done", undefined, "followUp"))
      .resolves.toMatchObject({ routed: "mesh" });
    expect(request).toHaveBeenCalledWith("session:main", "session:main", "followUp",
      expect.objectContaining({ message: "done" }), "session:main");
  });

  it("routes a peer root seen only through its session record over its stale control record", async () => {
    const { ownerIncarnation: _incarnation, ...legacy } = root({ ...peer, controlProtocol: "legacy" });
    const { router, request, actors } = setup(legacy, true, [root({ ...peer, stale: true })]);
    await expect(router.routeMessage("session:peer", "hello", undefined, "steer"))
      .resolves.toMatchObject({ routed: "mesh", acknowledged: true });
    expect(request).toHaveBeenCalledWith("session:peer", "session:peer", "steer",
      expect.objectContaining({ message: "hello" }), "session:peer");
    expect(actors.steerRemote).not.toHaveBeenCalled();
  });

  it("relays to a legacy peer root over the mesh steering topic", async () => {
    const { router, request, actors } = setup(root({ ...peer, controlProtocol: "legacy" }));
    vi.mocked(actors.steerRemote).mockResolvedValue({ queued: true, messageId: "relayed", routed: "mesh" });
    await expect(router.routeMessage("session:peer", "hello", undefined, "followUp"))
      .resolves.toMatchObject({ messageId: "relayed", routed: "mesh" });
    expect(actors.steerRemote).toHaveBeenCalledWith("session:peer", "hello", "followUp", undefined);
    expect(request).not.toHaveBeenCalled();
  });

  it.each([true, false])("keeps triggerTurn %s on the legacy relay to a peer root", async (triggerTurn) => {
    const { router, request, actors } = setup(root({ ...peer, controlProtocol: "legacy" }));
    vi.mocked(actors.steerRemote).mockResolvedValue({ queued: true, messageId: "relayed", routed: "mesh" });
    await router.routeMessage("session:peer", "hello", { n: 1 }, "steer", undefined, { triggerTurn });
    expect(actors.steerRemote).toHaveBeenCalledWith("session:peer", "hello", "steer", { n: 1 }, { triggerTurn });
    expect(request).not.toHaveBeenCalled();
  });

  it.each([true, false])("keeps triggerTurn %s on the legacy relay to a remote Main", async (triggerTurn) => {
    const { router, request, actors } = setup(root({ controlProtocol: "legacy" }));
    vi.mocked(actors.steerRemote).mockResolvedValue({ queued: true, messageId: "relayed", routed: "mesh" });
    await router.routeMessage("main", "done", undefined, "followUp", undefined, { triggerTurn });
    expect(actors.steerRemote).toHaveBeenCalledWith("session:main", "done", "followUp", undefined, { triggerTurn });
    expect(request).not.toHaveBeenCalled();
  });
});
