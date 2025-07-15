import asyncio

def testCentralGetCentrifugoToken():
    from satorilib.server import SatoriServerClient
    from satorilib.wallet import EvrmoreWallet
    urlServer='http://137.184.38.160'
    w = EvrmoreWallet.create('/Satori/Neuron/wallet/wallet.yaml')
    server = SatoriServerClient(w, urlServer)
    payload = server.getCentrifugoToken()
    assert payload is not None
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
    publish_to_stream_rest(stream_uuid=streamUuidPub, data='0.10101', token=payload['token'])

async def waitForLogs(client):
    ''' keep alive - simulate the engine continuing to run '''
    x = 0
    while True:
        await asyncio.sleep(1)
        x += 1
        if x > 60*20:
            break
    await client.disconnect()

async def testCentrifugo():
    payload = testCentralGetCentrifugoToken()
    client = await testCentrifugoToken(payload)
    # uncomment what you'd like to test:
    await testCentrifugoSubscription(client, streamUuidSub='003c3ae6-c2d0-5793-95bc-f9de0a279522') # remotePublishers
    #await testCentrifugoPublication(client, payload, streamUuidPub='4de07abf-8aa9-57f3-8733-284beaf54d51')
    await waitForLogs(client)


asyncio.run(testCentrifugo())


#[NEURON] hostInfo {'1de04a26-13c1-59cf-8516-a6d56476351e': ['65.130.22.251:24600'], '2fc025e9-6cad-5f5f-8247-41f78c2f82be': ['65.130.22.251:24600'], '3f5f43ab-94e1-5789-85fb-143e3ece642d': ['65.130.22.251:24600'], '422d87cb-d83a-5ebd-9329-64f650e980ea': ['65.130.22.251:24600'], '4de07abf-8aa9-57f3-8733-284beaf54d51': ['65.130.22.251:24600'], '53cd0846-bebc-5ad2-9dc9-1178d86edf7a': ['65.130.22.251:24600'], '6edd21f2-a6ad-5e30-8e74-d2289329f608': ['65.130.22.251:24600'], '81c6cf0b-6161-59d4-b9f3-3e46ab3dcd2c': ['65.130.22.251:24600'], '923ef17f-4052-5561-808e-89dd85d7559e': ['65.130.22.251:24600'], '98e88e84-9af4-5c8d-b452-6b018254cde2': ['65.130.22.251:24600'], '9f095c38-fc64-52ad-82a2-ad8ab65e833b': ['65.130.22.251:24600'], 'ae046e24-eac5-56f2-9c68-474ae239bb54': ['65.130.22.251:24600'], 'c238ee85-9909-5765-a148-a4afa398998b': ['65.130.22.251:24600'], 'c359eb44-b373-5805-b86c-409040a2600d': ['65.130.22.251:24600'], 'c7759d7a-5c48-5979-91a7-a4dfeb7e63b2': ['65.130.22.251:24600'], 'e7af70fb-e64b-518c-b779-12245d4c2538': ['65.130.22.251:24600'], 'effcc66c-3f20-577f-91b6-96904aaad6ec': ['65.130.22.251:24600'], 'fea43ce5-8b40-57b8-94a2-c11c2283c21c': ['65.130.22.251:24600']}
#[NEURON] remotePublishers {'003c3ae6-c2d0-5793-95bc-f9de0a279522': ['65.21.174.154:24600'], '031dd6af-b8a3-5569-97ee-5cd19b24aa77': ['37.27.109.113:24600'], '05612fac-7711-53e3-9729-582d6cb84e33': ['37.27.109.113:24600'], '0b50eac6-125c-5180-8c51-81c324df5751': ['208.65.163.217:24600'], '2f308c17-be73-577b-ba2e-f4597a41151d': ['109.76.243.85:24600'], '531908d0-7eec-5ffc-9b20-0430ecf4aee2': ['37.60.224.157:24600'], '64c56680-b8ad-5246-b966-954de87a8b67': ['109.76.243.85:24600'], '76a8da5b-8844-5338-84e6-ea8965bc95bf': ['132.147.156.166:24600'], '9aa7d943-3b65-567b-8097-a882bef955ec': ['65.21.174.154:24600'], 'b4cb9674-1f68-5ad5-b1ac-647a364f1769': ['37.60.224.157:24600'], 'b55173bf-d428-50ea-8b33-9764b3f15387': ['220.245.202.78:24600']}
#[NEURON] Host Ip And Port ['65.130.22.251:24600']
