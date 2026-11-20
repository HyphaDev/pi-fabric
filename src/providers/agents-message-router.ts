import type { AgentManager } from "../agents/manager.js";
import type { ActorManager } from "../actors/manager.js";
import type { FabricActorInfo, FabricActorRunBinding } from "../actors/types.js";
import type { FabricAgentMessageResult, FabricMainAgentTarget } from "../main-agent.js";
import type { MeshIdentity } from "../mesh/store.js";
import type { FabricInvocationContext } from "../protocol.js";
import type { FabricControlPlane, FabricControlCommand, FabricControlAcceptance } from "../topology/control-plane.js";
import type { FabricParticipantInfo, FabricParticipantSource } from "../topology/types.js";
import type { FabricAgentRunner } from "../config.js";
// Route messages using only the ownership, delivery, and binding ports needed here.
export class AgentMessageRouter {
  constructor(
    readonly manager: Pick<AgentManager, "status" | "steer" | "followUp" | "stop">,
    readonly actorManager: Pick<ActorManager, "identity" | "status" | "validateDirectMessage" | "tell" | "ask" | "stop" | "steerRemote" | "resolveBinding">,
    readonly mainAgent: Pick<FabricMainAgentTarget, "matches" | "local" | "id" | "deliverAgent">,
    readonly participants: Pick<FabricParticipantSource, "get" | "scheduleRefresh"> &
      Partial<Pick<FabricParticipantSource, "list">>,
    readonly control: Pick<FabricControlPlane, "request"> | undefined,
    readonly resolvePiRunBinding: (binding: FabricActorRunBinding, runner: FabricAgentRunner, context: FabricInvocationContext) => FabricActorRunBinding,
  ) {}

  #reportTarget(participant: FabricParticipantInfo, context?: FabricInvocationContext): void {
    const name = participant.label || participant.name || participant.id;
    context?.activity?.({ type: "entity", id: participant.id, kind: "agent", name });
    context?.attachPreview?.({ kind: "fabric-agent-target", id: participant.id, name });
  }

  /**
   * The newest canonical control record for `id`, even when its owner's lease
   * lapsed. A busy host can miss renewals under mesh lock contention while its
   * control plane keeps draining commands addressed to it.
   */
  #staleControlRecord(id: string): FabricParticipantInfo | undefined {
    return this.participants.list?.({ scope: "project", includeStale: true })
      .find((participant) => participant.id === id && participant.controlProtocol === "v1");
  }

  /**
   * Main's control endpoint. A Main session hosts its own control plane under
   * its identity, so a child can address it even when no live record is
   * readable; the owner acknowledges or the request reports it unconfirmed.
   */
  #mainEndpoint(): FabricParticipantInfo {
    const id = this.mainAgent.id;
    const live = this.participants.get(id);
    if (live?.controlProtocol === "v1") return live;
    const stale = this.#staleControlRecord(id);
    if (stale) return { ...stale, local: false };
    if (live) return live;
    return {
      format: 1, id, kind: "root", rootId: id, ownerHostId: id, ownerIdentityId: id,
      name: "Main", status: "running", runner: "pi", transport: "host",
      capabilities: ["steer", "followUp", "fabric"], startedAt: 0, updatedAt: 0,
      controlProtocol: "v1", local: false, stale: true,
    };
  }

  async routeMessage(
    id: string,
    message: string,
    data: unknown,
    kind: "steer" | "followUp",
    context?: FabricInvocationContext,
    options: {
      from?: MeshIdentity;
      triggerTurn?: boolean;
      binding?: FabricActorRunBinding;
    } = {},
  ): Promise<FabricAgentMessageResult> {
    if (this.mainAgent.matches(id)) {
      if (this.mainAgent.local) {
        context?.activity?.({
          type: "entity",
          id: this.mainAgent.id,
          kind: "agent",
          name: "Main",
        });
        return this.mainAgent.deliverAgent({
          from: options.from ?? this.actorManager.identity,
          message,
          delivery: kind,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(data === undefined ? {} : { data }),
        });
      }
      const participant = this.#mainEndpoint();
      if (!participant.capabilities.includes(kind)) {
        throw new Error(`Fabric participant ${participant.id} does not support ${kind}`);
      }
      if (!this.control || participant.controlProtocol === "legacy") {
        return this.actorManager.steerRemote(this.mainAgent.id, message, kind, data);
      }
      this.#reportTarget(participant, context);
      return this.control.request(
        participant.ownerHostId,
        participant.id,
        kind,
        {
          message,
          data,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(participant.ownerIncarnation ? { ownerIncarnation: participant.ownerIncarnation } : {}),
        },
        participant.ownerIdentityId,
      );
    }

    // Local one-shot agent: forward between its turns via the worker's
    // steer.jsonl channel, preserving the child's accumulated context.
    try {
      const status = this.manager.status(id);
      context?.activity?.({ type: "entity", id, kind: "agent", name: status.name });
      const result =
        kind === "steer"
          ? this.manager.steer(id, message, data)
          : this.manager.followUp(id, message, data);
      return { queued: true, messageId: result.messageId, routed: "local" };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) throw error;
    }

    // Remote roots and one-shot agents are not actors. This includes siblings
    // managed by the caller's Main and runs belonging to another peer's lineage.
    const remote = this.participants.get(id);
    if ((remote?.kind === "root" || remote?.kind === "agent") && !remote.local) {
      if (!remote.capabilities.includes(kind)) {
        throw new Error(`Fabric participant ${remote.id} does not support ${kind}`);
      }
      // A root read through its session record alone still drains control when
      // its canonical record only lapsed; a true legacy root takes the relay.
      const endpoint = remote.kind === "root" && remote.controlProtocol !== "v1"
        ? this.#staleControlRecord(remote.id) ?? remote
        : remote;
      if (endpoint.kind === "root" && (!this.control || endpoint.controlProtocol !== "v1")) {
        return this.actorManager.steerRemote(endpoint.id, message, kind, data);
      }
      if (!this.control || endpoint.controlProtocol !== "v1") {
        throw new Error(`Fabric ${endpoint.kind} ${endpoint.id} has no control channel`);
      }
      this.#reportTarget(endpoint, context);
      return this.control.request(
        endpoint.ownerHostId,
        endpoint.id,
        kind,
        {
          message,
          data,
          ...(typeof options.triggerTurn === "boolean"
            ? { triggerTurn: options.triggerTurn }
            : {}),
          ...(endpoint.ownerIncarnation ? { ownerIncarnation: endpoint.ownerIncarnation } : {}),
        },
        endpoint.ownerIdentityId,
      );
    }

    // Persistent actors consume both delivery modes through their serial mailbox.
    this.actorManager.validateDirectMessage(message, data);
    let target: { actor?: FabricActorInfo; participant?: FabricParticipantInfo };
    try {
      target = this.resolveActorTarget(id);
    } catch (error) {
      if (error instanceof Error && /Unknown Fabric actor/.test(error.message)) {
        throw new Error(`Unknown Fabric participant: ${id}`);
      }
      throw error;
    }
    const { actor, participant } = target;
    const localActor = Boolean(actor && (!participant || participant.local));
    const binding = options.binding && context && localActor
      ? this.resolvePiRunBinding(options.binding, actor!.runner, context)
      : options.binding;
    if (actor && localActor) {
      context?.activity?.({ type: "entity", id: actor.id, kind: "actor", name: actor.name });
      const result = this.actorManager.tell(actor.id, message, data, {
        ...(binding ? { overrides: binding } : {}),
      });
      return { queued: true, messageId: result.messageId, routed: "local" };
    }
    if (!participant) throw new Error(`Fabric actor ${actor!.id} has no live execution owner`);
    if (!participant.capabilities.includes(kind)) {
      throw new Error(`Fabric participant ${participant.id} does not support ${kind}`);
    }
    const sessionBinding = actor?.binding;
    const resolvedBinding = actor
      ? this.actorManager.resolveBinding(actor.id, binding)
      : binding;
    const needsBinding = Boolean(
      resolvedBinding?.model ||
        resolvedBinding?.thinking ||
        sessionBinding?.model ||
        sessionBinding?.thinking,
    );
    if (needsBinding && !participant.capabilities.includes("actor-bindings")) {
      throw new Error(`Fabric actor owner ${participant.ownerHostId} does not support session bindings`);
    }
    if (!this.control || participant.controlProtocol === "legacy") {
      if (needsBinding) {
        throw new Error(`Fabric actor owner ${participant.ownerHostId} has no binding control channel`);
      }
      return this.actorManager.steerRemote(participant.id, message, kind, data);
    }
    return this.control.request(
      participant.ownerHostId,
      participant.id,
      kind,
      {
        message,
        data,
        ...(typeof options.triggerTurn === "boolean"
          ? { triggerTurn: options.triggerTurn }
          : {}),
        ...(needsBinding && resolvedBinding ? { binding: resolvedBinding } : {}),
        ...(participant.ownerIncarnation ? { ownerIncarnation: participant.ownerIncarnation } : {}),
      },
      participant.ownerIdentityId,
    );
  }

  async acceptControl(
    command: FabricControlCommand,
    from: MeshIdentity,
    signal?: AbortSignal,
  ): Promise<FabricControlAcceptance> {
    if (command.operation === "cancel") {
      return { accepted: false, error: "Cancel commands are handled by the control plane" };
    }
    if (command.operation === "stop") {
      try {
        await this.manager.stop(command.targetId);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
          return { accepted: false, error: error instanceof Error ? error.message : String(error) };
        }
      }
      try {
        const actor = this.actorManager.status(command.targetId);
        const ownership = this.participants.get(actor.id);
        if (ownership && !ownership.local) {
          return { accepted: false, error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}` };
        }
        await this.actorManager.stop(actor.id);
        this.participants.scheduleRefresh();
        return { accepted: true, messageId: command.commandId };
      } catch (error) {
        if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) {
          return { accepted: false, error: error instanceof Error ? error.message : String(error) };
        }
      }
      return { accepted: false, error: `Owner does not control Fabric participant ${command.targetId}` };
    }

    const message = command.message?.trim();
    if (!message) return { accepted: false, error: "Fabric control message must not be empty" };
    if (command.operation === "ask") {
      try {
        const actor = this.actorManager.status(command.targetId);
        const ownership = this.participants.get(actor.id);
        if (ownership && !ownership.local) {
          return {
            accepted: false,
            error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}`,
          };
        }
        const result = await this.actorManager.ask(
          actor.id,
          message,
          command.data,
          signal,
          { ...(command.binding !== undefined ? { binding: command.binding } : {}), sender: command.sender ?? null },
        );
        return { accepted: true, messageId: result.id, result };
      } catch (error) {
        return {
          accepted: false,
          error: error instanceof Error ? error.message : String(error),
        };
      }
    }
    if (this.mainAgent.local && this.mainAgent.matches(command.targetId)) {
      const result = this.mainAgent.deliverAgent({
        from,
        message,
        delivery: command.operation,
        ...(typeof command.triggerTurn === "boolean"
          ? { triggerTurn: command.triggerTurn }
          : {}),
        ...(command.data === undefined ? {} : { data: command.data }),
      });
      return { accepted: true, messageId: result.messageId };
    }
    try {
      this.manager.status(command.targetId);
      const result =
        command.operation === "steer"
          ? this.manager.steer(command.targetId, message, command.data)
          : this.manager.followUp(command.targetId, message, command.data);
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric agent/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
    }
    try {
      const actor = this.actorManager.status(command.targetId);
      const ownership = this.participants.get(actor.id);
      if (ownership && !ownership.local) {
        return { accepted: false, error: `Participant ${actor.id} is owned by ${ownership.ownerHostId}` };
      }
      const result = this.actorManager.tell(
        actor.id,
        message,
        command.data,
        { ...(command.binding !== undefined ? { binding: command.binding } : {}), sender: command.sender ?? null },
      );
      return { accepted: true, messageId: result.messageId };
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) {
        return { accepted: false, error: error instanceof Error ? error.message : String(error) };
      }
    }
    return { accepted: false, error: `Owner does not control Fabric participant ${command.targetId}` };
  }

  resolveActorTarget(id: string): {
    actor?: FabricActorInfo;
    participant?: FabricParticipantInfo;
  } {
    let actor: FabricActorInfo | undefined;
    try {
      actor = this.actorManager.status(id);
    } catch (error) {
      if (!(error instanceof Error && /Unknown Fabric actor/.test(error.message))) throw error;
    }
    const participant = this.participants.get(actor?.id ?? id);
    if (!actor && (!participant || participant.kind !== "actor")) {
      throw new Error(`Unknown Fabric actor: ${id}`);
    }
    return {
      ...(actor ? { actor } : {}),
      ...(participant?.kind === "actor" ? { participant } : {}),
    };
  }

}
