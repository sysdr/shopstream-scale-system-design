"""Send an order receipt — the slow, flaky work we want off the request path.

The "email provider" takes SEND_MS per message and fails FAIL_RATE of the
time. Orders from user "poison" always fail (a message that can never succeed).
"""
import os
import random
import time

import resp

SEND_MS = int(os.environ.get("SEND_MS", "300"))
FAIL_RATE = float(os.environ.get("FAIL_RATE", "0"))
STREAM, GROUP, DEAD = "receipts", "mailers", "receipts:dead"


class SendError(Exception):
    pass


def send_receipt(order):
    time.sleep(SEND_MS / 1000)
    if order["user_id"] == "poison" or random.random() < FAIL_RATE:
        raise SendError(f"provider rejected receipt for {order['order_id']}")
    resp.client().call("INCR", f"receipts_sent:{order['order_id']}")  # how many times this customer got mail


def enqueue(order):
    """Add a receipt job to the stream. Returns the job id."""
    fields = []
    for k, v in order.items():
        fields += [k, v]
    return resp.client().call("XADD", STREAM, "MAXLEN", "~", "100000", "*", *fields)
