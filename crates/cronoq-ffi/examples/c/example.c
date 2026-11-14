/*
 * Embedding cronoq from C: submit tasks, take them in priority order, and use ordering
 * keys, replace by key, pause and release.
 *
 * Build and run (from the repository root, after `cargo build -p cronoq-ffi --release`):
 *
 *   cc -I crates/cronoq-ffi/include crates/cronoq-ffi/examples/c/example.c \
 *      target/release/libcronoq_ffi.a -lpthread -ldl -lm -o example && ./example
 *
 * The program checks what it gets and exits with 1 if anything is not as expected, so it is
 * also the test of the C API from C.
 */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "cronoq.h"

static cronoq_scheduler *sched;

#define CHECK(cond)                                                              \
  do {                                                                           \
    if (!(cond)) {                                                               \
      fprintf(stderr, "%s:%d: failed: %s (%s)\n", __FILE__, __LINE__, #cond,     \
              cronoq_last_error());                                              \
      exit(1);                                                                   \
    }                                                                            \
  } while (0)

static uint64_t submit(const char *payload, const char *options) {
  uint64_t id = 0;
  CHECK(cronoq_submit(sched, (const uint8_t *)payload, strlen(payload), options, &id,
                      NULL) == CRONOQ_OK);
  return id;
}

/* Take the next task and check that it is `expected`. Returns the lease id. */
static uint64_t take(const char *expected) {
  cronoq_lease lease;
  CHECK(cronoq_next_lease(sched, NULL, 5000, &lease) == CRONOQ_OK);
  if (lease.payload_len != strlen(expected) ||
      memcmp(lease.payload, expected, lease.payload_len) != 0) {
    fprintf(stderr, "expected task '%s', got '%.*s'\n", expected, (int)lease.payload_len,
            (const char *)lease.payload);
    exit(1);
  }
  printf("  %-10s attempt %u\n", expected, lease.attempt);
  uint64_t id = lease.lease;
  /* The lease's pointers are the library's: copy what is needed, then free. */
  cronoq_lease_free(&lease);
  return id;
}

static void run(const char *expected) {
  CHECK(cronoq_complete(sched, take(expected)) == CRONOQ_OK);
}

int main(void) {
  CHECK(cronoq_abi_version() == CRONOQ_ABI_VERSION);
  printf("cronoq %s, ABI %u\n", cronoq_version(), cronoq_abi_version());

  CHECK(cronoq_new("{\"pools\": [{\"name\": \"default\", \"retry\": {\"max_attempts\": 1}}]}",
                   &sched) == CRONOQ_OK);

  printf("priority order:\n");
  submit("low", "{\"priority\": \"low\"}");
  submit("normal", NULL);
  submit("critical", "{\"priority\": \"critical\"}");
  submit("high", "{\"priority\": \"high\"}");
  run("critical");
  run("high");
  run("normal");
  run("low");

  printf("an ordering key keeps its tasks in order:\n");
  submit("chat-1/a", "{\"ordering_key\": \"chat-1\", \"priority\": \"low\"}");
  submit("chat-1/b", "{\"ordering_key\": \"chat-1\", \"priority\": \"high\"}");
  submit("other", NULL);
  /* The high task lifts the low one in front of it: the key runs first, in order. */
  run("chat-1/a");
  run("chat-1/b");
  run("other");

  printf("replace by key:\n");
  int outcome = -1;
  uint64_t first = submit("title v1", "{\"key\": \"title\"}");
  uint64_t second = 0;
  CHECK(cronoq_submit(sched, (const uint8_t *)"title v2", 8,
                      "{\"key\": \"title\", \"on_key_conflict\": \"replace_queued\"}", &second,
                      &outcome) == CRONOQ_OK);
  CHECK(second == first && outcome == 2);
  run("title v2");

  printf("pause and resume:\n");
  submit("held", NULL);
  CHECK(cronoq_pause(sched, NULL, -1, NULL, "relay deploy", NULL) == CRONOQ_OK);
  cronoq_lease none;
  CHECK(cronoq_next_lease(sched, NULL, 50, &none) == CRONOQ_PAUSED);
  CHECK(strcmp(cronoq_last_error(), "relay deploy") == 0);
  printf("  paused: %s\n", cronoq_last_error());
  CHECK(cronoq_resume(sched, NULL, -1, NULL, NULL) == CRONOQ_OK);
  run("held");

  printf("release does not use up a retry:\n");
  submit("released", NULL);
  /* max_attempts is 1: a failure would end the task. A release does not. */
  CHECK(cronoq_release(sched, take("released"), 0) == CRONOQ_OK);
  run("released");

  cronoq_buf snapshot;
  CHECK(cronoq_snapshot(sched, &snapshot) == CRONOQ_OK);
  CHECK(strstr((const char *)snapshot.ptr, "\"releases\":1") != NULL);
  cronoq_buf_free(&snapshot);

  CHECK(cronoq_shutdown(sched, 1000) == CRONOQ_OK);
  CHECK(cronoq_submit(sched, NULL, 0, NULL, NULL, NULL) == CRONOQ_SHUTDOWN);
  cronoq_free(sched);
  printf("ok\n");
  return 0;
}
