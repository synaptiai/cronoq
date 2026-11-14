import assert from "node:assert/strict";
import { test } from "node:test";

import { create } from "@bufbuild/protobuf";
import { createRouterTransport } from "@connectrpc/connect";

import { Client, CronoqError } from "../src/index.js";
import * as pb from "../src/gen/cronoq/v1/cronoq_pb.js";

test("reject is not silently ignored by an older server", async () => {
  // What a 0.2.2 server does with a submission: it does not know onKeyConflict, returns the
  // task that holds a key as a duplicate, and sets no outcome. Key "taken" belongs to task 7;
  // any other key is free.
  const old = createRouterTransport(({ service }) => {
    service(pb.Tasks, {
      submit: (req) => {
        const taken = req.task?.key === "taken";
        return create(pb.SubmitResponseSchema, { id: taken ? 7n : 8n, duplicate: taken });
      },
    });
  });
  const c = new Client(old);
  const unimplemented = (e: unknown) => e instanceof CronoqError && e.code === "unimplemented" && !e.keyConflict;
  // The key is in use, or free and now taken by this submission: either way the old server
  // did not compare anything, and the caller is told.
  await assert.rejects(c.submit({ payload: "edit-2", key: "taken", onKeyConflict: "reject" }), unimplemented);
  await assert.rejects(c.submit({ payload: "edit-2", key: "free", onKeyConflict: "reject" }), unimplemented);
  // Without the option the same answer is fine.
  const plain = await c.submit({ payload: "edit-2", key: "taken" });
  assert.equal(plain.id, 7n);
  assert.equal(plain.duplicate, true);
});
