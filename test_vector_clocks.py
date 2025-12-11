# test_concurrent_vc.py
import asyncio
import httpx
import json
import random
from pprint import pprint

NODES = [
    "127.0.0.1:60001",
    "127.0.0.1:60003",
    "127.0.0.1:60004",
]

async def put(node, key, value):
    async with httpx.AsyncClient() as client:
        try:
            r = await client.put(f"http://{node}/put/{key}", json={"value": value}, timeout=5.0)
            return r.json()
        except Exception as e:
            return {"error": str(e)}

async def get(node, key):
    async with httpx.AsyncClient() as client:
        try:
            r = await client.get(f"http://{node}/get/{key}", timeout=5.0)
            return r.json()
        except Exception as e:
            return {"error": str(e)}

async def main():
    key = "concurrent-key-" + str(random.randint(1,1000000))
    nodeA = NODES[0]
    nodeB = NODES[1]
    print("Issuing two concurrent PUTs to", nodeA, "and", nodeB)

    # fire two puts concurrently (no await between them)
    t1 = asyncio.create_task(put(nodeA, key, "A_CONCURRENT"))
    t2 = asyncio.create_task(put(nodeB, key, "B_CONCURRENT"))

    resp1, resp2 = await asyncio.gather(t1, t2)
    print("\nPUT responses:")
    print("resp1:")
    pprint(resp1)
    print("resp2:")
    pprint(resp2)

    used_vc1 = resp1.get("used_vc")
    used_vc2 = resp2.get("used_vc")
    print("\nused_vc1:", used_vc1)
    print("used_vc2:", used_vc2)

    # Now do a GET from a third node to observe siblings
    print("\nDoing a quorum GET from node", NODES[2])
    gresp = await get(NODES[2], key)
    pprint(gresp)

    resolved = gresp.get("resolved_versions", [])
    print(f"\nNumber of resolved versions: {len(resolved)}")
    for v in resolved:
        print("value:", v.get("value"), "vc:", v.get("vc"))

    # If two siblings present, demonstrate a FINAL write that merges them:
    if len(resolved) > 1:
        print("\nWriting FINAL to merge siblings (coordinator will sample parents).")
        # Do a normal PUT; coordinator will sample parents and create merged VC.
        fresp = await put(NODES[2], key, "FINAL_MERGE")
        print("FINAL PUT resp:")
        pprint(fresp)
        print("\nGET after FINAL:")
        g2 = await get(NODES[2], key)
        pprint(g2)

if __name__ == "__main__":
    asyncio.run(main())
