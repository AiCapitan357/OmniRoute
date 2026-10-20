import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

// Regression guard for the Perplexity error-boundary flake (#17290).
//
// Since #14725 the streaming tail handed `onPipelineStreamError` / `onClientDisconnectFinalize`
// back to chatCore through `carry`, i.e. only after `runStreamingTail` had returned. chatCore's
// stream controller reads those handlers through getters at event time, so for the whole window
// between "pipeline assembled and pumping" and "tail returned" the getter answered `null`: an
// upstream failure pumped through the pipeline during the tail's remaining awaits was dropped —
// no stream-failure finalization, no `onStreamFailure` for the caller. Slow CI runners widened
// the window enough to fail three of six PRs in a row on 2026-10-20.
//
// The fix publishes both handlers into a holder shared with chatCore the moment they exist,
// before the pipeline starts pumping — restoring the pre-#14725 closure timing.

const ROOT = path.resolve(import.meta.dirname, "../../..");
const read = (p: string) => readFileSync(path.join(ROOT, p), "utf8");
const LEAF = "open-sse/handlers/chatCore/streamingTail.ts";
const BARREL = "open-sse/handlers/chatCore.ts";

test("chatCore reads the late-bound stream handlers through a holder shared with the tail", () => {
  const barrel = read(BARREL);
  assert.match(barrel, /const lateBoundStreamHandlers\b/);
  assert.match(
    barrel,
    /getPipelineStreamError: \(\) => lateBoundStreamHandlers\.onPipelineStreamError/
  );
  assert.match(
    barrel,
    /getClientDisconnectFinalize: \(\) => lateBoundStreamHandlers\.onClientDisconnectFinalize/
  );
  assert.doesNotMatch(
    barrel,
    /let onPipelineStreamError\b/,
    "the handler must not live in a chatCore-local binding the tail cannot reach"
  );
  assert.match(
    barrel,
    /runStreamingTail\(\{[\s\S]*?\n\s+lateBoundStreamHandlers,\n[\s\S]*?\}\)/,
    "the holder must be handed to the tail as a dep"
  );
});

test("the tail publishes both handlers into the holder before the pipeline is assembled", () => {
  const leaf = read(LEAF);
  const publishError = leaf.indexOf(
    "lateBoundStreamHandlers.onPipelineStreamError = onPipelineStreamError"
  );
  const publishDisconnect = leaf.indexOf(
    "lateBoundStreamHandlers.onClientDisconnectFinalize = onClientDisconnectFinalize"
  );
  const assemble = leaf.indexOf("assembleStreamingPipeline({");
  assert.ok(publishError > 0, "the tail must publish onPipelineStreamError into the holder");
  assert.ok(
    publishDisconnect > 0,
    "the tail must publish onClientDisconnectFinalize into the holder"
  );
  assert.ok(assemble > 0, "the tail must still assemble the pipeline");
  assert.ok(
    publishError < assemble,
    "onPipelineStreamError must be visible to the stream controller before the pipeline pumps"
  );
  assert.ok(
    publishDisconnect < assemble,
    "onClientDisconnectFinalize must be visible to the stream controller before the pipeline pumps"
  );
});

test("a pipeline error raised once the tail installed its handler reaches that handler", async () => {
  const { createChatStreamController } =
    await import("../../../open-sse/handlers/chatCore/chatStreamController.ts");
  const holder: {
    onPipelineStreamError:
      | ((event: {
          error: unknown;
          message: string;
          statusCode: number;
          duration: number;
        }) => boolean)
      | null;
    onClientDisconnectFinalize: ((event: { reason: string; duration: number }) => boolean) | null;
  } = { onPipelineStreamError: null, onClientDisconnectFinalize: null };
  const seen: string[] = [];
  const controller = createChatStreamController({
    getClientDisconnectFinalize: () => holder.onClientDisconnectFinalize,
    getPipelineStreamError: () => holder.onPipelineStreamError,
    pendingScope: {},
    finalizePendingScope: () => undefined,
    onDisconnect: null,
    provider: "perplexity-web",
    model: "pplx-auto",
    connectionId: null,
    pendingRequestId: "late-bound-test",
    clientResponseFormat: "openai",
    clientAbortSignal: null,
    allowCompletedToolHandoffGrace: false,
  });

  // What the holder guarantees: the handler is installed as soon as the tail creates it, so an
  // error the pipeline pumps afterwards (while the tail is still awaiting) is finalized.
  holder.onPipelineStreamError = (event) => {
    seen.push(event.message);
    return true;
  };
  controller.handleError(new Error("upstream died mid-answer"));
  assert.deepEqual(seen, ["upstream died mid-answer"]);
});
