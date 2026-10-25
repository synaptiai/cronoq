import assert from "node:assert/strict";
import { test } from "node:test";

import { Code, ConnectError } from "@connectrpc/connect";

import { CronoqError, wrap } from "../src/errors.js";

test("error codes use the gRPC status names", () => {
  const name = (code: Code) => (wrap(new ConnectError("x", code)) as CronoqError).code;
  assert.equal(name(Code.Canceled), "cancelled");
  assert.equal(name(Code.DeadlineExceeded), "deadline_exceeded");
  assert.equal(name(Code.FailedPrecondition), "failed_precondition");
  assert.equal(name(Code.Unauthenticated), "unauthenticated");
});
