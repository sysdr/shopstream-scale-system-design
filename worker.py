"""Receipt worker: a consumer in the "mailers" group of the "receipts" stream.

- New jobs:      XREADGROUP ... >        (each job goes to one worker)
- Done:          XACK                    (only after the receipt is sent)
- Failed/crashed jobs stay pending; after CLAIM_MS any worker takes them
  over with XAUTOCLAIM, which also counts the delivery.
- After MAX_TRIES deliveries a job goes to the dead-letter stream.
- Idempotent: a receipt already sent for an order is never sent again.
"""
import os
import socket
import sys
import time

import receipts
import resp

NAME = sys.argv[1] if len(sys.argv) > 1 else f"worker-{socket.gethostname()}-{os.getpid()}"
CLAIM_MS = int(os.environ.get("CLAIM_MS", "3000"))
MAX_TRIES = int(os.environ.get("MAX_TRIES", "5"))
CRASH_AFTER = int(os.environ.get("CRASH_AFTER", "0"))  # demo: die before acking job N+1
r = resp.Redis()


def setup():
    try:
        r.call("XGROUP", "CREATE", receipts.STREAM, receipts.GROUP, "0", "MKSTREAM")
    except RuntimeError as e:
        if "BUSYGROUP" not in str(e):
            raise


def as_dict(flat):
    return dict(zip(flat[::2], flat[1::2]))


def deliveries(job_id):
    info = r.call("XPENDING", receipts.STREAM, receipts.GROUP, job_id, job_id, 1)
    return info[0][3] if info else 0


def handle(job_id, order, done):
    tries = deliveries(job_id)
    if tries > MAX_TRIES:
        r.call("XADD", receipts.DEAD, "*", "job", job_id, "tries", tries - 1, *sum(order.items(), ()))
        r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)
        print(f"{NAME}: {job_id} dead-lettered after {tries - 1} tries", flush=True)
        return
    if r.call("EXISTS", f"receipt_done:{order['order_id']}"):
        r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)  # sent before we crashed: don't send twice
        return
    try:
        receipts.send_receipt(order)
    except receipts.SendError as e:
        print(f"{NAME}: try {tries} failed: {e}", flush=True)
        return  # stays pending; retried after CLAIM_MS
    r.call("SET", f"receipt_done:{order['order_id']}", 1, "EX", 86400)
    if CRASH_AFTER and done >= CRASH_AFTER:
        print(f"{NAME}: crashed after sending {job_id} but before XACK", flush=True)
        os._exit(1)
    r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)


def main():
    setup()
    print(f"{NAME}: waiting for receipts", flush=True)
    done = 0
    while True:
        # 1) take over jobs another worker failed or abandoned
        _, stolen, *_ = r.call("XAUTOCLAIM", receipts.STREAM, receipts.GROUP, NAME, CLAIM_MS, "0-0", "COUNT", 10)
        jobs = list(stolen or [])
        # 2) new jobs
        if not jobs:
            reply = r.call("XREADGROUP", "GROUP", receipts.GROUP, NAME, "COUNT", 10, "BLOCK", 1000,
                           "STREAMS", receipts.STREAM, ">")
            jobs = reply[0][1] if reply else []
        for job_id, flat in jobs:
            if flat is None:  # deleted while pending
                r.call("XACK", receipts.STREAM, receipts.GROUP, job_id)
                continue
            handle(job_id, as_dict(flat), done)
            done += 1


if __name__ == "__main__":
    main()
