import sys
import asyncio

def testCentralGetCentrifugoToken():
    from satorilib.server import SatoriServerClient
    from satorilib.wallet import EvrmoreWallet
    urlServer='http://137.184.38.160'
    w = EvrmoreWallet.create('/Satori/Neuron/wallet/wallet.yaml')
    server = SatoriServerClient(w, urlServer)
    payload = server.getCentrifugoToken()
    assert payload is not None
    print(f"Centrifugo payload: {payload}")
    return payload

async def testCentrifugoToken(payload):
    from satorineuron import logging
    from satorilib.centrifugo import create_centrifugo_client
    client = await create_centrifugo_client(
        token=payload['token'],
        ws_url=payload['ws_url'],
        on_connected_callback=lambda x: logging.info(f"Centrifugo Connected: {x}"),
        on_disconnected_callback=lambda x: logging.info(f"Centrifugo Disconnected: {x}"))
    await client.connect()
    return client

async def testCentrifugoSubscription(client, streamUuidSub):
    from satorineuron import logging
    from satorilib.centrifugo import create_subscription_handler, subscribe_to_stream
    subscription_handler = create_subscription_handler(
        stream_uuid=streamUuidSub, 
        value_callback=lambda x, y: logging.info(f"Centrifugo Publication: {x}, {y}")
    )    
    subscription = await subscribe_to_stream(client, streamUuidSub, subscription_handler)
    print(subscription)
    
async def testCentrifugoPublication(client, payload, streamUuidPub):
    from satorilib.centrifugo import publish_to_stream_rest
    import json
    from datetime import datetime, timezone
    
    # Create proper data structure that matches what neurons actually send
    observation_data = {
        "topic": f"stream_{streamUuidPub}",
        "date": "0.10103",  # Note: "date" field contains the actual data value (legacy naming)
        "time": datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),  # ISO format timestamp
        "hash": "test_hash_123"
    }
    
    # Send as JSON string (how neurons actually send data)
    publish_to_stream_rest(
        stream_uuid=streamUuidPub, 
        data=json.dumps(observation_data), 
        token=payload['token']
    )

async def waitForLogs(client):
    ''' keep alive - simulate the engine continuing to run '''
    x = 0
    while True:
        await asyncio.sleep(1)
        x += 1
        if x > 60*20:
            break
    await client.disconnect()

async def testCentrifugo(test_subcription: bool = True, test_publication: bool = False):
    payload = testCentralGetCentrifugoToken()
    client = await testCentrifugoToken(payload)
    if test_subcription:
        await testCentrifugoSubscription(client, streamUuidSub='003c3ae6-c2d0-5793-95bc-f9de0a279522') # remotePublishers
    if test_publication:
        await testCentrifugoPublication(client, payload, streamUuidPub='24325852-b12e-5a06-b293-60f1010d9b06')  # Use a stream from hostInfo (your owned streams)
    await waitForLogs(client)


# if streams go inactive, you can run `cd /Satori/Neuron/satorineuron/web/ && bash start.sh` to get these logs (make sure `logging level: debug` in /Satori/config/config.yaml) and replace the above uuids with ones from active streams assigned to this Neuron:
#[NEURON] hostInfo {'0c6d4691-f0c2-5455-ad65-87358a21943a': [], ... }
#[NEURON] remotePublishers {'003c3ae6-c2d0-5793-95bc-f9de0a279522': [], ... }

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python centrifugo.py <test_subcription> <test_publication>")
        print("Example: python centrifugo.py True False")
        sys.exit(1)
    test_subcription = True if sys.argv[1].lower() in ['true', '1', 'yes', 'y', 't'] else False
    test_publication = True if sys.argv[2].lower() in ['true', '1', 'yes', 'y', 't'] else False
    print(f"test_subcription: {test_subcription}, test_publication: {test_publication}")
    asyncio.run(testCentrifugo(test_subcription=test_subcription, test_publication=test_publication))
