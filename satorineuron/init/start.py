from typing import Union, Callable
import os
import time
import json
import random
import threading
from queue import Queue
from eth_account import Account
import asyncio
import pandas as pd
from satorilib.concepts.structs import (
    StreamId,
    Stream,
    StreamPairs,
    StreamOverview)
from satorilib import disk
from satorilib.concepts import constants
from satorilib.wallet import EvrmoreWallet
from satorilib.wallet.evrmore.identity import EvrmoreIdentity
from satorilib.server.api import CheckinDetails

# P2P Integration: Config-based networking mode selection
# Supports: 'central' (default/legacy), 'hybrid' (P2P with fallback), 'p2p' (pure P2P)
def _get_server_client_class():
    """Get the appropriate SatoriServerClient class based on networking mode."""
    try:
        from satorineuron import config as neuron_config
        networking_mode = neuron_config.get().get('networking mode', 'central')
    except Exception:
        networking_mode = 'central'

    if networking_mode in ('hybrid', 'p2p', 'p2p_only'):
        try:
            from satorip2p.integration import P2PSatoriServerClient
            return P2PSatoriServerClient
        except ImportError:
            pass  # Fall back to central

    from satorilib.server import SatoriServerClient
    return SatoriServerClient

SatoriServerClient = _get_server_client_class()

def _get_networking_mode() -> str:
    """Get the current networking mode from config."""
    try:
        from satorineuron import config as neuron_config
        return neuron_config.get().get('networking mode', 'central').lower().strip()
    except Exception:
        return 'central'

# P2P Module Imports (lazy-loaded for optional dependency)
# These are imported when needed but defined here for documentation
def _get_p2p_modules():
    """
    Get all available P2P modules from satorip2p.
    Returns a dict of module references, or empty dict if satorip2p not installed.

    Available modules:
    - Peers: Core P2P networking
    - EvrmoreIdentityBridge: Wallet-to-P2P identity
    - UptimeTracker, Heartbeat: Node uptime and relay bonus tracking
    - ConsensusManager, ConsensusVote: Stake-weighted voting
    - SignerNode: Multi-sig signing (3-of-5)
    - DistributionTrigger: Reward distribution coordination
    - SatoriScorer, RewardCalculator: Local reward calculation
    - OracleNetwork: P2P observation publishing
    - PredictionProtocol: Commit-reveal predictions
    - PeerRegistry, StreamRegistry: P2P discovery
    """
    try:
        from satorip2p import (
            Peers,
            EvrmoreIdentityBridge,
            SatoriScorer,
            RewardCalculator,
            RoundDataStore,
            PredictionInput,
            ScoreBreakdown,
            NetworkingMode,
            get_networking_mode as p2p_get_networking_mode,
        )
        from satorip2p.protocol.uptime import (
            UptimeTracker,
            Heartbeat,
            RELAY_UPTIME_THRESHOLD,
            HEARTBEAT_INTERVAL,
        )
        from satorip2p.protocol.consensus import (
            ConsensusManager,
            ConsensusVote,
            ConsensusPhase,
        )
        from satorip2p.protocol.signer import (
            SignerNode,
            MULTISIG_THRESHOLD,
            AUTHORIZED_SIGNERS,
        )
        from satorip2p.protocol.distribution_trigger import (
            DistributionTrigger,
        )
        from satorip2p.protocol.peer_registry import PeerRegistry
        from satorip2p.protocol.stream_registry import StreamRegistry
        from satorip2p.protocol.oracle_network import OracleNetwork
        from satorip2p.protocol.prediction_protocol import PredictionProtocol
        from satorip2p.protocol.lending import LendingManager, PoolConfig, LendRegistration
        from satorip2p.protocol.delegation import DelegationManager, DelegationRecord, CharityUpdate
        from satorip2p.signing import (
            EvrmoreWallet as P2PEvrmoreWallet,
            sign_message,
            verify_message,
        )
        return {
            'available': True,
            'Peers': Peers,
            'EvrmoreIdentityBridge': EvrmoreIdentityBridge,
            'UptimeTracker': UptimeTracker,
            'Heartbeat': Heartbeat,
            'RELAY_UPTIME_THRESHOLD': RELAY_UPTIME_THRESHOLD,
            'HEARTBEAT_INTERVAL': HEARTBEAT_INTERVAL,
            'ConsensusManager': ConsensusManager,
            'ConsensusVote': ConsensusVote,
            'ConsensusPhase': ConsensusPhase,
            'SignerNode': SignerNode,
            'MULTISIG_THRESHOLD': MULTISIG_THRESHOLD,
            'AUTHORIZED_SIGNERS': AUTHORIZED_SIGNERS,
            'DistributionTrigger': DistributionTrigger,
            'SatoriScorer': SatoriScorer,
            'RewardCalculator': RewardCalculator,
            'RoundDataStore': RoundDataStore,
            'PredictionInput': PredictionInput,
            'ScoreBreakdown': ScoreBreakdown,
            'PeerRegistry': PeerRegistry,
            'StreamRegistry': StreamRegistry,
            'OracleNetwork': OracleNetwork,
            'PredictionProtocol': PredictionProtocol,
            'LendingManager': LendingManager,
            'PoolConfig': PoolConfig,
            'LendRegistration': LendRegistration,
            'DelegationManager': DelegationManager,
            'DelegationRecord': DelegationRecord,
            'CharityUpdate': CharityUpdate,
            'P2PEvrmoreWallet': P2PEvrmoreWallet,
            'sign_message': sign_message,
            'verify_message': verify_message,
            'NetworkingMode': NetworkingMode,
        }
    except ImportError:
        return {'available': False}

# Cache the P2P modules (lazy loaded on first access)
_p2p_modules_cache = None

def get_p2p_module(name: str):
    """Get a specific P2P module by name, or None if not available."""
    global _p2p_modules_cache
    if _p2p_modules_cache is None:
        _p2p_modules_cache = _get_p2p_modules()
    return _p2p_modules_cache.get(name)

def is_p2p_available() -> bool:
    """Check if satorip2p is installed and available."""
    global _p2p_modules_cache
    if _p2p_modules_cache is None:
        _p2p_modules_cache = _get_p2p_modules()
    return _p2p_modules_cache.get('available', False)

from satorilib.centrifugo import publish_to_stream_rest
from satorilib.asynchronous import AsyncThread
# import satoriengine
from satoriengine.veda.data.structs import StreamForecast
import satorineuron
from satorineuron import VERSION
from satorineuron import logging
from satorineuron import config
# from satorineuron.init import engine
from satorineuron.init.tag import LatestTag, Version
from satorineuron.init.wallet import WalletManager
from satorineuron.common.structs import ConnectionTo
from satorineuron.relay import RawStreamRelayEngine, ValidateRelayStream
from satorineuron.structs.start import RunMode, UiEndpoint, StartupDagStruct
from satorilib.datamanager import DataClient, DataServerApi, DataClientApi, Message, Subscription
from satorilib.utils.ip import getPublicIpv4UsingCurl
from io import StringIO

def getStart():
    """returns StartupDag singleton"""
    return StartupDag()


class SingletonMeta(type):
    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(SingletonMeta, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class StartupDag(StartupDagStruct, metaclass=SingletonMeta):
    """a DAG of startup tasks."""

    _holdingBalanceBase_cache = None
    _holdingBalanceBase_timestamp = 0

    @classmethod
    async def create(
        cls,
        *args,
        env: str = 'dev',
        runMode: str = None,
        sendToUI: Callable = None,
        urlServer: str = None,
        urlMundo: str = None,
        isDebug: bool = False,
    ) -> 'StartupDag':
        '''Factory method to create and initialize StartupDag asynchronously'''
        startupDag = cls(
            *args,
            env=env,
            runMode=runMode,
            sendToUI=sendToUI,
            urlServer=urlServer,
            urlMundo=urlMundo,
            isDebug=isDebug)
        await startupDag.startFunction()
        return startupDag

    def __init__(
        self,
        *args,
        env: str = 'dev',
        runMode: str = None,
        sendToUI: Callable = None,
        urlServer: str = None,
        urlMundo: str = None,
        isDebug: bool = False,
    ):
        super(StartupDag, self).__init__(*args)
        self.needsRestart: Union[str, None] = None
        self.version = Version(VERSION)
        # TODO: test and turn on with new installer
        # self.watchForVersionUpdates()
        self.env = env
        self.runMode = RunMode.choose(runMode or config.get().get('mode', None))
        self.sendToUI = sendToUI or (lambda x: None)
        logging.debug(f'mode: {self.runMode.name}', print=True)
        self.userInteraction = time.time()
        self.walletManager: WalletManager
        self.asyncThread: AsyncThread = AsyncThread()
        self.isDebug: bool = isDebug
        # self.chatUpdates: BehaviorSubject = BehaviorSubject(None)
        self.chatUpdates: Queue = Queue()
        # dictionary of connection statuses {ConnectionTo: bool}
        self.latestConnectionStatus: dict = {}
        self.urlServer: str = urlServer
        self.urlMundo: str = urlMundo
        self.paused: bool = False
        self.pauseThread: Union[threading.Thread, None] = None
        self.details: CheckinDetails = CheckinDetails(raw={})
        self.balances: dict = {}
        self.key: str
        self.oracleKey: str
        self.idKey: str
        self.subscriptionKeys: str
        self.publicationKeys: str
        # self.ipfs: Ipfs = Ipfs()
        self.relayValidation: ValidateRelayStream
        self.dataServerIp: str =  ''
        self.dataServerPort: Union[int, None] =  None
        self.dataClient: Union[DataClient, None] = None
        self.allOracleStreams = None
        self.relay: RawStreamRelayEngine = None
        # self.engine: satoriengine.Engine
        self.publications: list[Stream] = []
        self.subscriptions: list[Stream] = []
        self.pubSubMapping: dict = {}
        self.centrifugoToken: str = None
        self.centrifugoPayload: dict = None
        self.identity: EvrmoreIdentity = EvrmoreIdentity(config.walletPath('wallet.yaml'))
        self.data: dict[str, dict[pd.DataFrame, pd.DataFrame]] = {}
        self.streamDisplay: list = []
        self.udpQueue: Queue = Queue()  # TODO: remove
        self.stakeStatus: bool = False
        self.miningMode: bool = False
        self.stopAllSubscriptions = threading.Event()
        self.lastBlockTime = time.time()
        self.lastBridgeTime = 0
        self.poolIsAccepting: bool = False
        self.transferProtocol: Union[str, None] = None
        self.invitedBy: str = None
        self.setInvitedBy()
        self.latestObservationTime: float = 0
        self.configRewardAddress: str = None
        self.setRewardAddress()
        self.setEngineVersion()
        self.setupWalletManager()
        self.ip = getPublicIpv4UsingCurl()
        self.restartQueue: Queue = Queue()
        # self.checkinCheckThread = threading.Thread( # TODO: clean up after making sure the newer async works well
        #     target=self.checkinCheck,
        #     daemon=True)
        # self.checkinCheckThread.start()
        asyncio.create_task(self.checkinCheck())
        # self.delayedStart()
        alreadySetup: bool = os.path.exists(config.walletPath("wallet.yaml"))
        if not alreadySetup:
            threading.Thread(target=self.delayedEngine).start()
        self.ranOnce = False
        self.startFunction = self.start
        if self.runMode == RunMode.normal:
            self.startFunction = self.start
        elif self.runMode == RunMode.worker:
            self.startFunction = self.startWorker
        elif self.runMode == RunMode.wallet:
            self.startFunction = self.startWalletOnly
        # TODO: this logic must be removed - auto restart functionality should
        #       happen ouside neuron
        if not config.get().get("disable restart", False):
            self.restartThread = threading.Thread(
                target=self.restartEverythingPeriodic,
                daemon=True)
            self.restartThread.start()
        #while True:
        #    if self.asyncThread.loop is not None:
        #        self.checkinThread = self.asyncThread.repeatRun(
        #            task=startFunction,
        #            interval=60 * 60 * 24 if alreadySetup else 60 * 60 * 12,
        #        )
        #        break
        #    time.sleep(60 * 15)

        # TODO: after pubsubmap is provided to the data server,
        #       get the data for each datastream, grab all (optimize later)
        #       subscribe to everything, add the data to our in memory datasets
        #       (that updates the ui)

    @property
    def walletOnlyMode(self) -> bool:
        return self.runMode == RunMode.wallet

    @property
    def rewardAddress(self) -> str:
        if isinstance(self.details, CheckinDetails):
            return self.details.get('rewardaddress')
        return self.configRewardAddress
        #if isinstance(self.details, CheckinDetails):
        #    return self.details.rewardAddress
        #return self.configRewardAddress

    @property
    def network(self) -> str:
        return 'main' if self.env in ['prod', 'local', 'testprod'] else 'test'

    @property
    def vault(self) -> EvrmoreWallet:
        return self.walletManager.vault

    @property
    def wallet(self) -> EvrmoreWallet:
        return self.walletManager.wallet

    @property
    def holdingBalance(self) -> float:
        if self.wallet.balance.amount > 0:
            self._holdingBalance = round(
                self.wallet.balance.amount
                + (self.vault.balance.amount if self.vault is not None else 0),
                8)
        else:
            self._holdingBalance = self.getBalance()
        return self._holdingBalance

    def refreshBalance(self, threaded: bool = True, forWallet: bool = True, forVault: bool = True):
        self.walletManager.connect()
        if forWallet and isinstance(self.wallet, EvrmoreWallet):
            if threaded:
                threading.Thread(target=self.wallet.get).start()
            else:
                self.wallet.get()
        if forVault and isinstance(self.vault, EvrmoreWallet):
            if threaded:
                threading.Thread(target=self.vault.get).start()
            else:
                self.vault.get()
        return self.holdingBalance

    def refreshUnspents(self, threaded: bool = True, forWallet: bool = True, forVault: bool = True):
        self.walletManager.connect()
        if forWallet and isinstance(self.wallet, EvrmoreWallet):
            if threaded:
                threading.Thread(target=self.wallet.getReadyToSend).start()
            else:
                self.wallet.getReadyToSend()
        if forVault and isinstance(self.vault, EvrmoreWallet):
            if threaded:
                threading.Thread(target=self.vault.getReadyToSend).start()
            else:
                self.vault.getReadyToSend()
        return self._holdingBalance

 #  api basescan version
 #  @property
 #   def holdingBalanceBase(self) -> float:
 #       """
 #       Get Satori from Base 5 min interval
 #       """
 #       import requests
 #       current_time = time.time()
 #       cache_timeout = 60 * 5
 #       if self._holdingBalanceBase_cache is not None and (current_time - self._holdingBalanceBase_timestamp) < cache_timeout:
 #           return self._holdingBalanceBase_cache
 #       eth_address = self.vault.ethAddress
 #       #base_api_url = "https://api.basescan.org/api"
 #       params = {
 #           "module": "account",
 #           "action": "tokenbalance",
 #           "contractaddress": "0xc1c37473079884CbFCf4905a97de361FEd414B2B",
 #           "address": eth_address,
 #           "tag": "latest",
 #           "apikey": "xxx"
 #       }
 #       try:
 #           response = requests.get(base_api_url, params=params)
 #           response.raise_for_status()
 #           data = response.json()
 #           if data.get("status") == "1":
 #               token_balance = int(data.get("result", 0)) / (10 ** 18)
 #               self._holdingBalanceBase_cache = token_balance
 #               self._holdingBalanceBase_timestamp = current_time
 #               print(f"Connecting to Base node OK")
 #               return token_balance
 #           else:
 #               print(f"Error API: {data.get('message', 'Unknown Error')}")
 #               return 0
 #       except requests.RequestException as e:
 #           print(f"Error connecting to API: {e}")
 #           return 0

    @property
    def holdingBalanceBase(self) -> float:
        """
        Get Satori from Base with 5-minute interval cache
        """
        # TEMPORARY DISABLE
        return 0
        
        import requests
        import time

        current_time = time.time()
        cache_timeout = 60 * 5
        if self._holdingBalanceBase_cache is not None and (current_time - self._holdingBalanceBase_timestamp) < cache_timeout:
            return self._holdingBalanceBase_cache
        eth_address = self.vault.ethAddress
        base_api_url = "https://base-mainnet.g.alchemy.com/v2/wdwSzh0cONBj81_XmHWvODOBq-wuQiAi"
        payload = {
            "jsonrpc": "2.0",
            "method": "alchemy_getTokenBalances",
            "params": [
                eth_address,
                ["0xc1c37473079884CbFCf4905a97de361FEd414B2B"]
            ],
            "id": 1
        }
        headers = {"Content-Type": "application/json"}
        try:
            response = requests.post(base_api_url, json=payload, headers=headers)
            response.raise_for_status()
            data = response.json()
            if "result" in data and "tokenBalances" in data["result"]:
                balance_hex = data["result"]["tokenBalances"][0]["tokenBalance"]
                token_balance = int(balance_hex, 16) / (10 ** 18)
                self._holdingBalanceBase_cache = token_balance
                self._holdingBalanceBase_timestamp = current_time
                print(f"Connecting to Base node OK")
                return token_balance
            else:
                print(f"API Error: Unexpected response format")
                return 0
        except requests.RequestException as e:
            print(f"Error connecting to API: {e}")
            return 0

    @property
    def ethaddressforward(self) -> str:
        eth_address = self.vault.ethAddress
        if eth_address:
            return eth_address
        else:
        #    print("Ethereum address not found")
            return ""
        
    @property
    def evrvaultaddressforward(self) -> str:
        evrvaultaddress = self.details.wallet.get('vaultaddress', '')
        if evrvaultaddress:
            return evrvaultaddress
        else:
        #    print("EVR Vault address not found")
            return ""

    def setupWalletManager(self):
        self.walletManager = WalletManager.create(
            updateConnectionStatus=self.updateConnectionStatus)

    def shutdownWallets(self):
        self.walletManager._electrumx = None
        self.walletManager._wallet = None
        self.walletManager._vault = None

    def closeVault(self):
        self.walletManager.closeVault()

    def openVault(self, password: Union[str, None] = None, create: bool = False):
        return self.walletManager.openVault(password=password, create=create)

    def getWallet(self, **kwargs):
        return self.walletManager.wallet

    def getVault(self, password: Union[str, None] = None, create: bool = False) -> Union[EvrmoreWallet, None]:
        return self.walletManager.openVault(password=password, create=create)

    def electrumxCheck(self):
        if self.walletManager.isConnected():
            self.updateConnectionStatus(
                connTo=ConnectionTo.electrumx, 
                status=True)
            return True
        else:
            self.updateConnectionStatus(
                connTo=ConnectionTo.electrumx, 
                status=False)
            return False

    def watchForVersionUpdates(self):
        """
        if we notice the code version has updated, download code restart
        in order to restart we have to kill the main thread manually.
        """

        def getPidByName(name: str) -> Union[int, None]:
            """
            Returns the PID of a process given a name or partial command name match.
            If multiple matches are found, returns the first match.
            Returns None if no process is found with the given name.
            """
            import psutil

            for proc in psutil.process_iter(["pid", "cmdline"]):
                try:
                    # Check if the process command line matches the target name
                    if name in " ".join(proc.info["cmdline"]):
                        return proc.info["pid"]
                except (psutil.NoSuchProcess, psutil.AccessDenied):
                    continue  # Process terminated or access denied, skip
            return None  # No process found with the given name

        def terminatePid(pid: int):
            import signal
            os.kill(pid, signal.SIGTERM)  # Send SIGTERM to the process

        def watchForever():
            latestTag = LatestTag(self.version, serverURL=self.urlServer)
            while True:
                time.sleep(60 * 60 * 6)
                if latestTag.mustUpdate():
                    terminatePid(getPidByName("satori.py"))

        self.watchVersionThread = threading.Thread(
            target=watchForever,
            daemon=True)
        self.watchVersionThread.start()

    def delayedEngine(self):
        time.sleep(60 * 60 * 6)
        self.buildEngine()

    # TODO: clean up after making sure the newer async works well
    # def checkinCheck(self):
    #     while True:
    #         time.sleep(60 * 60 * 6)
    #         # loop through streams, if I haven't had an observation on a stream
    #         # in more than 24 hours, delete it. and restart
    #         # for stream in self.subscriptions:
    #         #    ts = timestampToSeconds(
    #         #        self.cacheOf(stream.streamId).getLatestObservationTime()
    #         #    )
    #         #    if ts > 0 and ts + 60*60*24 < time.time():
    #         #        self.server.removeStream(stream.streamId.jsonId)
    #         #        self.triggerRestart()
    #         if self.latestObservationTime + 60*60*6 < time.time():
    #             self.triggerRestart()
    #         if self.server.checkinCheck():
    #             self.triggerRestart()

    async def checkinCheck(self):
        while True:
            await asyncio.sleep(60 * 60 * 6) 
            current_time = time.time()
            if self.latestObservationTime and (current_time - self.latestObservationTime > 60*60*6):
                logging.warning("No observations in 6 hours, restarting")
                self.triggerRestart()
            if hasattr(self.server, 'checkinCheck') and self.server.checkinCheck():
                logging.warning("Server check failed, restarting") 
                self.triggerRestart()

    def networkIsTest(self, network: str = None) -> bool:
        return network.lower().strip() in ("testnet", "test", "ravencoin", "rvn")

    async def dataServerFinalize(self):
        await self.sharePubSubInfo()
        await self.populateData()
        await self.subscribeToRawData()
        await self.subscribeToEngineUpdates()

    async def start(self):
        """start the satori engine."""
        await self.connectToDataServer()
        asyncio.create_task(self.stayConnectedForever())
        # while True:
        if self.ranOnce:
            time.sleep(60 * 60)
        self.ranOnce = True
        if self.env == 'prod' and self.serverConnectedRecently():
            last_checkin = config.get().get('server checkin')
            elapsed_minutes = (time.time() - last_checkin) / 60
            wait_minutes = max(0, 10 - elapsed_minutes)
            if wait_minutes > 0:
                logging.info(f"Server connected recently, waiting {wait_minutes:.1f} minutes")
                await asyncio.sleep(wait_minutes * 60)
        self.recordServerConnection()
        networking_mode = _get_networking_mode()
        if self.walletOnlyMode:
            self.createServerConn()
            if networking_mode != 'p2p':
                self.checkin()  # Skip in pure P2P mode
            await self.announceToNetwork()  # P2P announcement (hybrid/p2p modes)
            await self.initializeP2PComponents()  # Initialize consensus, rewards, etc.
            if networking_mode != 'p2p':
                self.getBalances()
            logging.info("in WALLETONLYMODE")
            return
        self.setMiningMode()
        self.createRelayValidation()
        self.createServerConn()
        if networking_mode != 'p2p':
            self.checkin()  # Skip in pure P2P mode
        await self.announceToNetwork()  # P2P announcement (hybrid/p2p modes)
        await self.initializeP2PComponents()  # Initialize consensus, rewards, etc.
        if networking_mode != 'p2p':
            self.getBalances()
        await self.centrifugoConnect()
        await self.dataServerFinalize() 
        if self.isDebug:
            return
        self.startRelay()

    def startWalletOnly(self):
        """start the satori engine."""
        logging.info("running in walletOnly mode", color="blue")
        self.createServerConn()
        return

    async def startWorker(self):
        """start the satori engine."""
        logging.info("running in worker mode", color="blue")
        await self.connectToDataServer()
        asyncio.create_task(self.stayConnectedForever())
        networking_mode = _get_networking_mode()
        self.setMiningMode()
        self.createRelayValidation()
        self.createServerConn()
        if networking_mode != 'p2p':
            self.checkin()  # Skip in pure P2P mode
        await self.announceToNetwork()  # P2P announcement (hybrid/p2p modes)
        await self.initializeP2PComponents()  # Initialize consensus, rewards, etc.
        if networking_mode != 'p2p':
            self.getBalances()
        #await self.centrifugoConnect()
        await self.dataServerFinalize() 
        if self.isDebug:
            return
        self.startRelay()
        await asyncio.Event().wait() # probably place this at the end of satori.py?

    def updateConnectionStatus(self, connTo: ConnectionTo, status: bool):
        # logging.info('connTo:', connTo, status, color='yellow')
        self.latestConnectionStatus = {
            **self.latestConnectionStatus,
            **{connTo.name: status}}
        # Add P2P-specific info when updating P2P status
        if connTo.name == 'p2p' and hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
            try:
                peer_count = len(self._p2p_peers.connected_peers) if hasattr(self._p2p_peers, 'connected_peers') else 0
                nat_type = getattr(self._p2p_peers, 'nat_type', 'Unknown')
                self.latestConnectionStatus['peer_count'] = peer_count
                self.latestConnectionStatus['nat_type'] = nat_type
                # Add consensus phase if available (from distribution_trigger or signer_node)
                consensus_phase = ''
                if hasattr(self, '_distribution_trigger') and self._distribution_trigger is not None:
                    phase = getattr(self._distribution_trigger, 'current_phase', None)
                    if phase is not None:
                        consensus_phase = str(phase.name) if hasattr(phase, 'name') else str(phase)
                elif hasattr(self, '_signer_node') and self._signer_node is not None:
                    phase = getattr(self._signer_node, 'current_phase', None)
                    if phase is not None:
                        consensus_phase = str(phase.name) if hasattr(phase, 'name') else str(phase)
                self.latestConnectionStatus['consensus_phase'] = consensus_phase
            except Exception:
                pass
        self.sendToUI(UiEndpoint.connectionStatus, self.latestConnectionStatus)

    def createRelayValidation(self):
        self.relayValidation = ValidateRelayStream()
        logging.info("started relay validation engine", color="green")

    def serverConnectedRecently(self, threshold_minutes: int = 10) -> bool:
        """Check if server was connected to recently without side effects."""
        last_checkin = config.get().get('server checkin')
        if last_checkin is None:
            return False
        elapsed_seconds = time.time() - last_checkin
        return elapsed_seconds < (threshold_minutes * 60)

    def recordServerConnection(self) -> None:
        """Record the current time as the last server connection time."""
        config.add(data={'server checkin': time.time()})

    def createServerConn(self):
        logging.debug(self.urlServer, color="teal")
        self.server = SatoriServerClient(
            self.wallet, url=self.urlServer, sendingUrl=self.urlMundo
        )

    def checkin(self):
        try:
            referrer = (
                open(config.root("config", "referral.txt"), mode="r")
                .read()
                .strip())
        except Exception as _:
            referrer = None
        x = 30
        attempt = 0
        while True:
            attempt += 1
            try:
                vault = self.getVault()
                self.details = CheckinDetails(
                    self.server.checkin(
                        referrer=referrer,
                        ip=self.ip,
                        vaultInfo={
                            'vaultaddress': vault.address, 
                            'vaultpubkey': vault.pubkey,
                        } if isinstance(vault, EvrmoreWallet) else None))
                
                if self.details.get('sponsor') != self.invitedBy:
                    if self.invitedBy is None:
                        self.setInvitedBy(self.details.get('sponsor'))
                    if isinstance(self.invitedBy, str) and len(self.invitedBy) == 34 and self.invitedBy.startswith('E'):
                        self.server.invitedBy(self.invitedBy)

                if config.get().get('prediction stream', 'notExisting') == 'notExisting':
                    config.add(data={'prediction stream': None})

                self.server.setDataManagerPort(self.dataServerPort)

                if self.details.get('rewardaddress') != self.configRewardAddress:
                    if self.configRewardAddress is None:
                        self.setRewardAddress(address=self.details.get('rewardaddress'))
                    else:
                        self.setRewardAddress(globally=True)

                self.updateConnectionStatus(
                    connTo=ConnectionTo.central, 
                    status=True)
                # logging.debug(self.details, color='magenta')
                self.key = self.details.key
                self.poolIsAccepting = bool(
                    self.details.wallet.get("accepting", False))
                self.oracleKey = self.details.oracleKey
                self.idKey = self.details.idKey
                self.subscriptionKeys = self.details.subscriptionKeys
                self.publicationKeys = self.details.publicationKeys
                self.subscriptions = [
                    Stream.fromMap(x)
                    for x in json.loads(self.details.subscriptions)]
                if (
                    attempt < 5 and (
                        self.details is None or len(self.subscriptions) == 0)
                ):
                    time.sleep(30)
                    continue
                logging.debug("subscriptions:", len(
                    self.subscriptions), print=True)
                self.publications = [
                    Stream.fromMap(x)
                    for x in json.loads(self.details.publications)]
                logging.debug(
                    "publications:",
                    len(self.publications),
                    print=True)
                logging.info("checked in with Satori", color="green")
                break
            except Exception as e:
                self.updateConnectionStatus(
                    connTo=ConnectionTo.central,
                    status=False)
                logging.warning(f"connecting to central err: {e}")
            x = x * 1.5 if x < 60 * 60 * 6 else 60 * 60 * 6
            logging.warning(f"trying again in {x}")
            time.sleep(x)

    async def announceToNetwork(self, capabilities: list = None):
        """
        Announce our presence to the P2P network.

        Called alongside checkin() in hybrid mode, or instead of checkin() in pure P2P mode.
        In central mode, this does nothing.

        Args:
            capabilities: List of capabilities (e.g., ["predictor", "oracle"])
        """
        networking_mode = _get_networking_mode()

        # Skip in central mode
        if networking_mode == 'central':
            return

        try:
            from satorip2p.protocol.peer_registry import PeerRegistry
            from satorip2p import Peers

            # Get or create P2P peers instance
            if not hasattr(self, '_p2p_peers') or self._p2p_peers is None:
                self._p2p_peers = Peers(
                    identity=self.identity,
                    listen_port=config.get().get('p2p port', 24600),
                )
                await self._p2p_peers.start()

            # Create registry and announce
            if not hasattr(self, '_peer_registry') or self._peer_registry is None:
                self._peer_registry = PeerRegistry(self._p2p_peers)
                await self._peer_registry.start()

            # Announce with capabilities
            capabilities = capabilities or ["predictor"]
            announcement = await self._peer_registry.announce(capabilities=capabilities)

            if announcement:
                logging.info(
                    f"Announced to P2P network: {announcement.evrmore_address[:16]}...",
                    color="cyan"
                )
                self.updateConnectionStatus(connTo=ConnectionTo.p2p, status=True)
            else:
                logging.warning("Failed to announce to P2P network")
                self.updateConnectionStatus(connTo=ConnectionTo.p2p, status=False)

        except ImportError:
            logging.debug("satorip2p not available, skipping P2P announcement")
        except Exception as e:
            logging.warning(f"P2P announcement failed: {e}")
            if hasattr(self, 'updateConnectionStatus'):
                self.updateConnectionStatus(connTo=ConnectionTo.p2p, status=False)

    async def initializeP2PComponents(self):
        """
        Initialize all P2P components for consensus, rewards, and distribution.

        Called after announceToNetwork() has created the _p2p_peers instance.
        Only runs in hybrid/p2p mode.

        Components initialized:
        - _uptime_tracker: Heartbeat-based uptime tracking for relay bonus
        - _reward_calculator: Local reward calculation and tracking
        - _consensus_manager: Stake-weighted voting coordination
        - _distribution_trigger: Automatic distribution on consensus
        - _signer_node: Multi-sig signing (if this node is an authorized signer)
        """
        networking_mode = _get_networking_mode()

        # Skip in central mode
        if networking_mode == 'central':
            return

        # Need P2P peers to be initialized first
        if not hasattr(self, '_p2p_peers') or self._p2p_peers is None:
            logging.debug("P2P peers not available, skipping component initialization")
            return

        try:
            # Get P2P modules
            modules = _get_p2p_modules()
            if not modules.get('available'):
                logging.debug("satorip2p not available, skipping component initialization")
                return

            # Get classes from modules
            UptimeTracker = modules.get('UptimeTracker')
            RewardCalculator = modules.get('RewardCalculator')
            ConsensusManager = modules.get('ConsensusManager')
            DistributionTrigger = modules.get('DistributionTrigger')
            SignerNode = modules.get('SignerNode')
            AUTHORIZED_SIGNERS = modules.get('AUTHORIZED_SIGNERS', [])

            # 1. Initialize Uptime Tracker (for relay bonus qualification)
            if UptimeTracker and (not hasattr(self, '_uptime_tracker') or self._uptime_tracker is None):
                try:
                    self._uptime_tracker = UptimeTracker(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                    )
                    await self._uptime_tracker.start()
                    logging.info("P2P uptime tracker initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize uptime tracker: {e}")

            # 2. Initialize Reward Calculator (for local score computation)
            if RewardCalculator and (not hasattr(self, '_reward_calculator') or self._reward_calculator is None):
                try:
                    self._reward_calculator = RewardCalculator()
                    logging.info("P2P reward calculator initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize reward calculator: {e}")

            # 3. Initialize Consensus Manager (for stake-weighted voting)
            if ConsensusManager and (not hasattr(self, '_consensus_manager') or self._consensus_manager is None):
                try:
                    self._consensus_manager = ConsensusManager(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                    )
                    await self._consensus_manager.start()
                    logging.info("P2P consensus manager initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize consensus manager: {e}")

            # 4. Initialize Distribution Trigger (for automatic distribution)
            if DistributionTrigger and (not hasattr(self, '_distribution_trigger') or self._distribution_trigger is None):
                try:
                    self._distribution_trigger = DistributionTrigger(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                        consensus_manager=getattr(self, '_consensus_manager', None),
                    )
                    await self._distribution_trigger.start()
                    logging.info("P2P distribution trigger initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize distribution trigger: {e}")

            # 5. Initialize Signer Node (only if this node is an authorized signer)
            my_address = self.identity.address if hasattr(self.identity, 'address') else None
            is_authorized_signer = my_address and my_address in AUTHORIZED_SIGNERS

            if SignerNode and is_authorized_signer and (not hasattr(self, '_signer_node') or self._signer_node is None):
                try:
                    self._signer_node = SignerNode(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                    )
                    await self._signer_node.start()
                    logging.info(f"P2P signer node initialized (authorized signer)", color="green")
                except Exception as e:
                    logging.warning(f"Failed to initialize signer node: {e}")
            elif SignerNode and not is_authorized_signer:
                logging.debug("Not an authorized signer, skipping signer node initialization")

            # 6. Initialize Oracle Network (for P2P observations)
            OracleNetwork = modules.get('OracleNetwork')
            if OracleNetwork and (not hasattr(self, '_oracle_network') or self._oracle_network is None):
                try:
                    self._oracle_network = OracleNetwork(self._p2p_peers)
                    await self._oracle_network.start()
                    logging.info("P2P oracle network initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize oracle network: {e}")

            # 7. Initialize Lending Manager (for P2P pool operations)
            LendingManager = modules.get('LendingManager')
            if LendingManager and (not hasattr(self, '_lending_manager') or self._lending_manager is None):
                try:
                    self._lending_manager = LendingManager(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                    )
                    await self._lending_manager.start()
                    logging.info("P2P lending manager initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize lending manager: {e}")

            # 8. Initialize Delegation Manager (for P2P proxy/delegation)
            DelegationManager = modules.get('DelegationManager')
            if DelegationManager and (not hasattr(self, '_delegation_manager') or self._delegation_manager is None):
                try:
                    self._delegation_manager = DelegationManager(
                        peers=self._p2p_peers,
                        wallet=self.identity,
                    )
                    await self._delegation_manager.start()
                    logging.info("P2P delegation manager initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize delegation manager: {e}")

            # 9. Initialize Stream Registry (for P2P stream discovery)
            StreamRegistry = modules.get('StreamRegistry')
            if StreamRegistry and (not hasattr(self, '_stream_registry') or self._stream_registry is None):
                try:
                    self._stream_registry = StreamRegistry(self._p2p_peers)
                    await self._stream_registry.start()
                    logging.info("P2P stream registry initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize stream registry: {e}")

            # 11. Initialize Prediction Protocol (for P2P commit-reveal predictions)
            PredictionProtocol = modules.get('PredictionProtocol')
            if PredictionProtocol and (not hasattr(self, '_prediction_protocol') or self._prediction_protocol is None):
                try:
                    self._prediction_protocol = PredictionProtocol(self._p2p_peers)
                    await self._prediction_protocol.start()
                    logging.info("P2P prediction protocol initialized", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to initialize prediction protocol: {e}")

            # 10. Wire managers to P2PSatoriServerClient if available
            if hasattr(self, 'server') and self.server is not None:
                try:
                    if hasattr(self.server, 'set_lending_manager'):
                        self.server.set_lending_manager(getattr(self, '_lending_manager', None))
                    if hasattr(self.server, 'set_delegation_manager'):
                        self.server.set_delegation_manager(getattr(self, '_delegation_manager', None))
                    if hasattr(self.server, 'set_oracle_network'):
                        self.server.set_oracle_network(getattr(self, '_oracle_network', None))
                    logging.info("P2P managers wired to server client", color="cyan")
                except Exception as e:
                    logging.warning(f"Failed to wire P2P managers to server: {e}")

            # 12. Wire P2P events to WebSocket for real-time UI updates
            self._wire_p2p_events_to_websocket()

            logging.info("P2P components initialization complete", color="cyan")

        except Exception as e:
            logging.warning(f"P2P component initialization failed: {e}")

    def _wire_p2p_events_to_websocket(self):
        """
        Wire P2P component callbacks to WebSocket event emission.

        This enables real-time updates in the web dashboard for:
        - Heartbeats from network peers
        - Predictions published/received
        - Observations from oracles
        - Consensus phase changes
        - Pool/lending updates
        - Delegation changes
        """
        if not callable(self.sendToUI):
            logging.debug("sendToUI not available, skipping WebSocket wiring")
            return

        sendToUI = self.sendToUI

        # Heartbeat callback
        def on_heartbeat(heartbeat):
            """Called when a heartbeat is received from the network."""
            try:
                node_id = getattr(heartbeat, 'node_id', '') or ''
                address = getattr(heartbeat, 'evrmore_address', '') or ''
                sendToUI('heartbeat', {
                    'node_id': node_id[:16] + '...' if len(node_id) > 16 else node_id,
                    'address': address[:12] + '...' if len(address) > 12 else address,
                    'timestamp': getattr(heartbeat, 'timestamp', 0),
                    'roles': getattr(heartbeat, 'roles', []),
                })
            except Exception as e:
                logging.debug(f"Failed to emit heartbeat: {e}")

        # Prediction callback
        def on_prediction(prediction):
            """Called when a prediction is received."""
            try:
                stream_id = getattr(prediction, 'stream_id', '') or ''
                predictor = getattr(prediction, 'predictor', '') or ''
                sendToUI('prediction', {
                    'stream_id': stream_id[:16] + '...' if len(stream_id) > 16 else stream_id,
                    'predictor': predictor[:12] + '...' if len(predictor) > 12 else predictor,
                    'value': getattr(prediction, 'value', None),
                    'timestamp': getattr(prediction, 'timestamp', None),
                    'target_time': getattr(prediction, 'target_time', None),
                })
            except Exception as e:
                logging.debug(f"Failed to emit prediction: {e}")

        # Observation callback
        def on_observation(observation):
            """Called when an observation is received from an oracle."""
            try:
                stream_id = getattr(observation, 'stream_id', '') or ''
                oracle = getattr(observation, 'oracle', '') or ''
                sendToUI('observation', {
                    'stream_id': stream_id[:16] + '...' if len(stream_id) > 16 else stream_id,
                    'oracle': oracle[:12] + '...' if len(oracle) > 12 else oracle,
                    'value': getattr(observation, 'value', None),
                    'timestamp': getattr(observation, 'timestamp', None),
                    'hash': getattr(observation, 'hash', None),
                })
            except Exception as e:
                logging.debug(f"Failed to emit observation: {e}")

        # Consensus callback
        def on_consensus_change(phase, round_id=None):
            """Called when consensus phase changes."""
            try:
                sendToUI('consensus', {
                    'phase': str(phase),
                    'round_id': round_id,
                    'timestamp': int(time.time()),
                })
            except Exception as e:
                logging.debug(f"Failed to emit consensus: {e}")

        # Pool/lending callback
        def on_pool_update(update_type, data):
            """Called when pool/lending status changes."""
            try:
                sendToUI('pool_update', {
                    'type': update_type,
                    'data': data if isinstance(data, dict) else str(data),
                    'timestamp': int(time.time()),
                })
            except Exception as e:
                logging.debug(f"Failed to emit pool update: {e}")

        # Delegation callback
        def on_delegation_update(update_type, data):
            """Called when delegation status changes."""
            try:
                sendToUI('delegation_update', {
                    'type': update_type,
                    'data': data if isinstance(data, dict) else str(data),
                    'timestamp': int(time.time()),
                })
            except Exception as e:
                logging.debug(f"Failed to emit delegation update: {e}")

        # Wire callbacks to P2P components
        try:
            # Wire uptime tracker for heartbeats
            if hasattr(self, '_uptime_tracker') and self._uptime_tracker:
                tracker = self._uptime_tracker
                if hasattr(tracker, 'on_heartbeat_received'):
                    tracker.on_heartbeat_received = on_heartbeat
                elif hasattr(tracker, 'set_callback'):
                    tracker.set_callback('heartbeat', on_heartbeat)

            # Wire prediction protocol
            if hasattr(self, '_prediction_protocol') and self._prediction_protocol:
                proto = self._prediction_protocol
                if hasattr(proto, 'on_prediction_received'):
                    proto.on_prediction_received = on_prediction
                elif hasattr(proto, 'set_callback'):
                    proto.set_callback('prediction', on_prediction)

            # Wire oracle network
            if hasattr(self, '_oracle_network') and self._oracle_network:
                oracle = self._oracle_network
                if hasattr(oracle, 'on_observation_received'):
                    oracle.on_observation_received = on_observation
                elif hasattr(oracle, 'set_callback'):
                    oracle.set_callback('observation', on_observation)

            # Wire consensus manager
            if hasattr(self, '_consensus_manager') and self._consensus_manager:
                consensus = self._consensus_manager
                if hasattr(consensus, 'on_phase_change'):
                    consensus.on_phase_change = on_consensus_change
                elif hasattr(consensus, 'set_callback'):
                    consensus.set_callback('phase_change', on_consensus_change)

            # Wire lending manager
            if hasattr(self, '_lending_manager') and self._lending_manager:
                lending = self._lending_manager
                if hasattr(lending, 'on_update'):
                    lending.on_update = lambda data: on_pool_update('lending', data)
                elif hasattr(lending, 'set_callback'):
                    lending.set_callback('update', lambda data: on_pool_update('lending', data))

            # Wire delegation manager
            if hasattr(self, '_delegation_manager') and self._delegation_manager:
                delegation = self._delegation_manager
                if hasattr(delegation, 'on_update'):
                    delegation.on_update = lambda data: on_delegation_update('delegation', data)
                elif hasattr(delegation, 'set_callback'):
                    delegation.set_callback('update', lambda data: on_delegation_update('delegation', data))

            logging.info("P2P event callbacks wired to WebSocket", color="cyan")

        except Exception as e:
            logging.warning(f"Failed to wire P2P callbacks: {e}")

    async def discoverStreams(
        self,
        source: str = None,
        datatype: str = None,
        use_p2p: bool = True,
        use_central: bool = True
    ) -> list:
        """
        Discover available streams from P2P network and/or central server.

        In hybrid mode, queries both and merges results.
        In P2P mode, only queries P2P network.
        In central mode, only queries central server.

        Args:
            source: Filter by source (e.g., "exchange/binance")
            datatype: Filter by datatype (e.g., "price")
            use_p2p: Whether to query P2P network
            use_central: Whether to query central server

        Returns:
            List of stream definitions
        """
        networking_mode = _get_networking_mode()
        streams = []

        # Query P2P network if enabled
        if use_p2p and networking_mode in ('hybrid', 'p2p'):
            try:
                from satorip2p.protocol.stream_registry import StreamRegistry

                # Ensure P2P peers and registry are initialized
                if not hasattr(self, '_stream_registry') or self._stream_registry is None:
                    if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                        self._stream_registry = StreamRegistry(self._p2p_peers)
                        await self._stream_registry.start()

                if hasattr(self, '_stream_registry') and self._stream_registry is not None:
                    p2p_streams = await self._stream_registry.discover_streams(
                        source=source,
                        datatype=datatype
                    )
                    # Convert to common format
                    for s in p2p_streams:
                        streams.append({
                            'stream_id': s.stream_id,
                            'source': s.source,
                            'stream': s.stream,
                            'target': s.target,
                            'datatype': s.datatype,
                            'cadence': s.cadence,
                            'from_p2p': True,
                        })
                    logging.debug(f"Discovered {len(p2p_streams)} streams from P2P network")

            except ImportError:
                logging.debug("satorip2p not available for stream discovery")
            except Exception as e:
                logging.warning(f"P2P stream discovery failed: {e}")

        # Query central server if enabled
        if use_central and networking_mode in ('central', 'hybrid'):
            try:
                # Use existing central server stream discovery
                if hasattr(self, 'server') and self.server is not None:
                    central_streams, _ = self.getPaginatedOracleStreams(
                        per_page=100,
                        searchText=source
                    )
                    for s in central_streams:
                        # Avoid duplicates from P2P
                        stream_id = s.get('uuid', s.get('stream_id', ''))
                        if not any(existing.get('stream_id') == stream_id for existing in streams):
                            streams.append({
                                **s,
                                'from_p2p': False,
                            })
                    logging.debug(f"Discovered {len(central_streams)} streams from central server")

            except Exception as e:
                logging.warning(f"Central server stream discovery failed: {e}")

        return streams

    async def claimStream(self, stream_id: str, slot_index: int = None) -> bool:
        """
        Claim a predictor slot on a stream (P2P mode).

        Args:
            stream_id: Stream to claim
            slot_index: Specific slot (None = first available)

        Returns:
            True if claimed successfully
        """
        networking_mode = _get_networking_mode()

        if networking_mode == 'central':
            logging.debug("Stream claiming not available in central mode")
            return False

        try:
            from satorip2p.protocol.stream_registry import StreamRegistry

            # Ensure stream registry is initialized
            if not hasattr(self, '_stream_registry') or self._stream_registry is None:
                if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                    self._stream_registry = StreamRegistry(self._p2p_peers)
                    await self._stream_registry.start()

            if hasattr(self, '_stream_registry') and self._stream_registry is not None:
                claim = await self._stream_registry.claim_stream(
                    stream_id=stream_id,
                    slot_index=slot_index
                )
                if claim:
                    logging.info(f"Claimed stream {stream_id[:16]}... slot {claim.slot_index}")
                    return True

        except ImportError:
            logging.debug("satorip2p not available for stream claiming")
        except Exception as e:
            logging.warning(f"Stream claiming failed: {e}")

        return False

    async def getMyClaimedStreams(self) -> list:
        """
        Get list of streams we've claimed in P2P mode.

        Returns:
            List of StreamClaim objects
        """
        if hasattr(self, '_stream_registry') and self._stream_registry is not None:
            try:
                return await self._stream_registry.get_my_streams()
            except Exception as e:
                logging.warning(f"Failed to get claimed streams: {e}")
        return []

    async def subscribeToP2PData(
        self,
        stream_id: str,
        callback: callable
    ) -> bool:
        """
        Subscribe to stream data via P2P network.

        In hybrid mode, receives data from both P2P and central.
        In P2P mode, only receives from P2P network.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with each observation

        Returns:
            True if subscribed successfully
        """
        networking_mode = _get_networking_mode()

        if networking_mode == 'central':
            logging.debug("P2P data subscription not available in central mode")
            return False

        try:
            from satorip2p.protocol.oracle_network import OracleNetwork

            # Ensure oracle network is initialized
            if not hasattr(self, '_oracle_network') or self._oracle_network is None:
                if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                    self._oracle_network = OracleNetwork(self._p2p_peers)
                    await self._oracle_network.start()

            if hasattr(self, '_oracle_network') and self._oracle_network is not None:
                success = await self._oracle_network.subscribe_to_stream(
                    stream_id=stream_id,
                    callback=callback
                )
                if success:
                    logging.info(f"Subscribed to P2P data for stream {stream_id[:16]}...")
                return success

        except ImportError:
            logging.debug("satorip2p not available for P2P data subscription")
        except Exception as e:
            logging.warning(f"P2P data subscription failed: {e}")

        return False

    async def publishObservation(
        self,
        stream_id: str,
        value: float,
        timestamp: int = None,
        to_p2p: bool = True,
        to_central: bool = True
    ) -> bool:
        """
        Publish an observation to the network.

        In hybrid mode, publishes to both P2P and central server.
        In P2P mode, only publishes to P2P network.

        Args:
            stream_id: Stream to publish to
            value: Observed value
            timestamp: Observation timestamp (default: now)
            to_p2p: Whether to publish to P2P network
            to_central: Whether to publish to central server

        Returns:
            True if published successfully to at least one destination
        """
        import time as time_module
        networking_mode = _get_networking_mode()
        timestamp = timestamp or int(time_module.time())
        success = False

        # Publish to P2P network
        if to_p2p and networking_mode in ('hybrid', 'p2p'):
            try:
                from satorip2p.protocol.oracle_network import OracleNetwork

                # Ensure oracle network is initialized
                if not hasattr(self, '_oracle_network') or self._oracle_network is None:
                    if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                        self._oracle_network = OracleNetwork(self._p2p_peers)
                        await self._oracle_network.start()

                if hasattr(self, '_oracle_network') and self._oracle_network is not None:
                    observation = await self._oracle_network.publish_observation(
                        stream_id=stream_id,
                        value=value,
                        timestamp=timestamp
                    )
                    if observation:
                        logging.debug(f"Published observation to P2P: {stream_id[:16]}... = {value}")
                        success = True

            except ImportError:
                logging.debug("satorip2p not available for P2P observation publishing")
            except Exception as e:
                logging.warning(f"P2P observation publishing failed: {e}")

        # Publish to central server
        if to_central and networking_mode in ('central', 'hybrid'):
            try:
                if hasattr(self, 'server') and self.server is not None:
                    # Use existing publish mechanism
                    # Note: The stream_id needs to be converted to topic format
                    self.server.publish(
                        topic=stream_id,
                        data=str(value),
                        observationTime=str(timestamp),
                        observationHash="",  # Will be computed by server
                        isPrediction=False
                    )
                    logging.debug(f"Published observation to central: {stream_id[:16]}... = {value}")
                    success = True
            except Exception as e:
                logging.warning(f"Central server observation publishing failed: {e}")

        return success

    async def getP2PObservations(
        self,
        stream_id: str,
        limit: int = 100
    ) -> list:
        """
        Get cached P2P observations for a stream.

        Args:
            stream_id: Stream to get observations for
            limit: Maximum observations to return

        Returns:
            List of Observation objects
        """
        if hasattr(self, '_oracle_network') and self._oracle_network is not None:
            try:
                return self._oracle_network.get_cached_observations(stream_id, limit)
            except Exception as e:
                logging.warning(f"Failed to get P2P observations: {e}")
        return []

    async def publishP2PPrediction(
        self,
        stream_id: str,
        value: float,
        target_time: int,
        confidence: float = 0.0,
        to_p2p: bool = True,
        to_central: bool = True
    ) -> bool:
        """
        Publish a prediction to the network.

        In hybrid mode, publishes to both P2P and central server.
        In P2P mode, only publishes to P2P network.

        Args:
            stream_id: Stream to predict
            value: Predicted value
            target_time: When this prediction is for
            confidence: Confidence level (0-1)
            to_p2p: Whether to publish to P2P network
            to_central: Whether to publish to central server

        Returns:
            True if published successfully
        """
        networking_mode = _get_networking_mode()
        success = False

        # Publish to P2P network
        if to_p2p and networking_mode in ('hybrid', 'p2p'):
            try:
                from satorip2p.protocol.prediction_protocol import PredictionProtocol

                # Ensure prediction protocol is initialized
                if not hasattr(self, '_prediction_protocol') or self._prediction_protocol is None:
                    if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                        self._prediction_protocol = PredictionProtocol(self._p2p_peers)
                        await self._prediction_protocol.start()

                if hasattr(self, '_prediction_protocol') and self._prediction_protocol is not None:
                    prediction = await self._prediction_protocol.publish_prediction(
                        stream_id=stream_id,
                        value=value,
                        target_time=target_time,
                        confidence=confidence
                    )
                    if prediction:
                        logging.debug(f"Published prediction to P2P: {stream_id[:16]}... = {value}")
                        success = True

            except ImportError:
                logging.debug("satorip2p not available for P2P prediction publishing")
            except Exception as e:
                logging.warning(f"P2P prediction publishing failed: {e}")

        # Publish to central server (existing behavior)
        if to_central and networking_mode in ('central', 'hybrid'):
            # Central server predictions go through handlePredictionData flow
            # which calls self.server.publish with isPrediction=True
            pass

        return success

    async def subscribeToP2PPredictions(
        self,
        stream_id: str,
        callback: callable
    ) -> bool:
        """
        Subscribe to predictions from other predictors via P2P.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with each prediction

        Returns:
            True if subscribed successfully
        """
        networking_mode = _get_networking_mode()

        if networking_mode == 'central':
            logging.debug("P2P prediction subscription not available in central mode")
            return False

        try:
            from satorip2p.protocol.prediction_protocol import PredictionProtocol

            # Ensure prediction protocol is initialized
            if not hasattr(self, '_prediction_protocol') or self._prediction_protocol is None:
                if hasattr(self, '_p2p_peers') and self._p2p_peers is not None:
                    self._prediction_protocol = PredictionProtocol(self._p2p_peers)
                    await self._prediction_protocol.start()

            if hasattr(self, '_prediction_protocol') and self._prediction_protocol is not None:
                success = await self._prediction_protocol.subscribe_to_predictions(
                    stream_id=stream_id,
                    callback=callback
                )
                if success:
                    logging.info(f"Subscribed to P2P predictions for {stream_id[:16]}...")
                return success

        except ImportError:
            logging.debug("satorip2p not available for P2P prediction subscription")
        except Exception as e:
            logging.warning(f"P2P prediction subscription failed: {e}")

        return False

    async def getP2PPredictions(
        self,
        stream_id: str,
        limit: int = 100
    ) -> list:
        """
        Get cached P2P predictions for a stream.

        Args:
            stream_id: Stream to get predictions for
            limit: Maximum predictions to return

        Returns:
            List of Prediction objects
        """
        if hasattr(self, '_prediction_protocol') and self._prediction_protocol is not None:
            try:
                return self._prediction_protocol.get_cached_predictions(stream_id, limit)
            except Exception as e:
                logging.warning(f"Failed to get P2P predictions: {e}")
        return []

    async def getPredictorScore(self, predictor: str, stream_id: str = None) -> float:
        """
        Get average prediction score for a predictor.

        Args:
            predictor: Evrmore address of predictor
            stream_id: Optional stream filter

        Returns:
            Average score (0-1)
        """
        if hasattr(self, '_prediction_protocol') and self._prediction_protocol is not None:
            try:
                return self._prediction_protocol.get_predictor_average_score(predictor, stream_id)
            except Exception as e:
                logging.warning(f"Failed to get predictor score: {e}")
        return 0.0

    # ========== P2P Rewards (Phase 5/6) ==========

    async def getPendingRewards(self, stream_id: str = None) -> dict:
        """
        Get pending (unclaimed) rewards for this predictor.

        Works in hybrid/p2p mode by querying the reward data store.
        In central mode, queries the central server.

        Args:
            stream_id: Optional stream filter

        Returns:
            Dict with pending rewards info:
            {
                'total_pending': float,
                'rounds': [
                    {'round_id': str, 'stream_id': str, 'amount': float, 'score': float},
                    ...
                ]
            }
        """
        mode = _get_networking_mode()

        if mode in ('hybrid', 'p2p', 'p2p_only'):
            # P2P mode: Query reward data store
            try:
                from satorip2p.protocol.rewards import RoundDataStore
                if hasattr(self, '_peers') and self._peers is not None:
                    store = RoundDataStore(self._peers)
                    # Get our address
                    our_address = ""
                    if hasattr(self, 'wallet') and self.wallet:
                        our_address = self.wallet.address
                    elif hasattr(self, '_peers') and self._peers._identity_bridge:
                        our_address = self._peers._identity_bridge.evrmore_address

                    # Query recent rounds from local cache
                    pending = {'total_pending': 0.0, 'rounds': []}

                    # Check local cache for recent round data
                    for key, summary in store._local_cache.items():
                        if stream_id and summary.stream_id != stream_id:
                            continue
                        # Find our reward in this round
                        for reward in summary.rewards:
                            if reward.address == our_address and reward.amount > 0:
                                pending['rounds'].append({
                                    'round_id': summary.round_id,
                                    'stream_id': summary.stream_id,
                                    'amount': reward.amount,
                                    'score': reward.score,
                                    'rank': reward.rank,
                                })
                                pending['total_pending'] += reward.amount

                    return pending
            except Exception as e:
                logging.warning(f"Failed to get pending rewards from P2P: {e}")

        # Central mode or fallback: Query server
        try:
            if hasattr(self, 'server') and self.server:
                success, data = self.server.getPendingRewards(stream_id=stream_id)
                if success:
                    return data
        except Exception as e:
            logging.warning(f"Failed to get pending rewards from server: {e}")

        return {'total_pending': 0.0, 'rounds': []}

    async def getRewardHistory(
        self,
        stream_id: str = None,
        limit: int = 100,
        offset: int = 0
    ) -> list:
        """
        Get historical reward claims for this predictor.

        Args:
            stream_id: Optional stream filter
            limit: Max records to return
            offset: Pagination offset

        Returns:
            List of reward history entries:
            [
                {
                    'round_id': str,
                    'stream_id': str,
                    'amount': float,
                    'score': float,
                    'rank': int,
                    'tx_hash': str,
                    'claimed_at': int,
                },
                ...
            ]
        """
        mode = _get_networking_mode()

        if mode in ('hybrid', 'p2p', 'p2p_only'):
            # P2P mode: Query DHT for historical round data
            try:
                from satorip2p.protocol.rewards import RoundDataStore
                if hasattr(self, '_peers') and self._peers is not None:
                    store = RoundDataStore(self._peers)
                    our_address = ""
                    if hasattr(self, 'wallet') and self.wallet:
                        our_address = self.wallet.address
                    elif hasattr(self, '_peers') and self._peers._identity_bridge:
                        our_address = self._peers._identity_bridge.evrmore_address

                    history = []
                    # Note: In production, would query DHT for historical rounds
                    # For now, return from local cache
                    for key, summary in list(store._local_cache.items())[:limit]:
                        if stream_id and summary.stream_id != stream_id:
                            continue
                        for reward in summary.rewards:
                            if reward.address == our_address:
                                history.append({
                                    'round_id': summary.round_id,
                                    'stream_id': summary.stream_id,
                                    'amount': reward.amount,
                                    'score': reward.score,
                                    'rank': reward.rank,
                                    'tx_hash': summary.evrmore_tx_hash,
                                    'claimed_at': summary.created_at,
                                })
                    return history[offset:offset + limit]
            except Exception as e:
                logging.warning(f"Failed to get reward history from P2P: {e}")

        # Central mode or fallback: Query server
        try:
            if hasattr(self, 'server') and self.server:
                success, data = self.server.getRewardHistory(
                    stream_id=stream_id,
                    limit=limit,
                    offset=offset
                )
                if success:
                    return data
        except Exception as e:
            logging.warning(f"Failed to get reward history from server: {e}")

        return []

    async def claimRewards(
        self,
        round_ids: list = None,
        stream_id: str = None,
        claim_address: str = None
    ) -> dict:
        """
        Claim pending rewards.

        In P2P mode, rewards are distributed automatically at round end.
        This method is primarily for:
        - Querying claim status
        - Overriding claim address
        - Manual claiming in transition phase

        Args:
            round_ids: Specific rounds to claim (None = all pending)
            stream_id: Filter by stream
            claim_address: Override default reward address

        Returns:
            Dict with claim result:
            {
                'success': bool,
                'claimed_amount': float,
                'tx_hash': str or None,
                'message': str,
            }
        """
        mode = _get_networking_mode()

        # Determine claim address
        if not claim_address:
            # Try config reward address first
            claim_address = getattr(self, 'configRewardAddress', None)
            if not claim_address and hasattr(self, 'wallet') and self.wallet:
                claim_address = self.wallet.address

        if not claim_address:
            return {
                'success': False,
                'claimed_amount': 0.0,
                'tx_hash': None,
                'message': 'No claim address available'
            }

        if mode in ('hybrid', 'p2p', 'p2p_only'):
            # P2P mode: Rewards are auto-distributed
            # This queries status and can request manual claim if needed
            try:
                pending = await self.getPendingRewards(stream_id=stream_id)

                if pending['total_pending'] == 0:
                    return {
                        'success': True,
                        'claimed_amount': 0.0,
                        'tx_hash': None,
                        'message': 'No pending rewards to claim'
                    }

                # In fully decentralized mode, rewards are auto-distributed
                # This is informational - rewards will arrive at claim_address
                return {
                    'success': True,
                    'claimed_amount': pending['total_pending'],
                    'tx_hash': None,
                    'message': f"Rewards ({pending['total_pending']:.8f} SATORI) will be distributed to {claim_address}"
                }
            except Exception as e:
                logging.warning(f"Failed to claim rewards via P2P: {e}")

        # Central mode or fallback: Request claim from server
        try:
            if hasattr(self, 'server') and self.server:
                success, data = self.server.claimRewards(
                    round_ids=round_ids,
                    stream_id=stream_id,
                    claim_address=claim_address,
                    signature=self.wallet.sign(claim_address) if hasattr(self, 'wallet') else None,
                    pubkey=self.wallet.pubkey if hasattr(self, 'wallet') else None
                )
                if success:
                    return data
                return {
                    'success': False,
                    'claimed_amount': 0.0,
                    'tx_hash': None,
                    'message': data.get('message', 'Claim failed')
                }
        except Exception as e:
            logging.warning(f"Failed to claim rewards from server: {e}")

        return {
            'success': False,
            'claimed_amount': 0.0,
            'tx_hash': None,
            'message': 'Failed to process claim'
        }

    async def subscribeToRewardNotifications(
        self,
        stream_id: str,
        callback: Callable = None
    ) -> bool:
        """
        Subscribe to reward distribution notifications for a stream.

        Receives notifications when rewards are distributed for rounds
        you participated in.

        Args:
            stream_id: Stream to subscribe to
            callback: Function called with notification dict:
                {
                    'type': 'round_complete',
                    'round_id': str,
                    'merkle_root': str,
                    'total_rewards': float,
                    'tx_hash': str,
                }

        Returns:
            True if subscribed successfully
        """
        mode = _get_networking_mode()

        if mode not in ('hybrid', 'p2p', 'p2p_only'):
            logging.debug("Reward notifications only available in P2P mode")
            return False

        try:
            from satorip2p.protocol.rewards import RoundDataStore
            if hasattr(self, '_peers') and self._peers is not None:
                store = RoundDataStore(self._peers)

                async def notification_handler(data):
                    """Handle reward notification."""
                    if callback:
                        try:
                            callback(data)
                        except Exception as e:
                            logging.debug(f"Reward notification callback error: {e}")

                    # Log notification
                    logging.info(
                        f"Reward notification for {stream_id}: "
                        f"round={data.get('round_id')}, "
                        f"total={data.get('total_rewards', 0):.4f} SATORI"
                    )

                return await store.subscribe_to_rewards(stream_id, notification_handler)
        except Exception as e:
            logging.warning(f"Failed to subscribe to reward notifications: {e}")

        return False

    async def getMyRewardScore(self, stream_id: str = None) -> dict:
        """
        Get this predictor's scoring statistics.

        Returns:
            Dict with scoring stats:
            {
                'average_score': float,
                'total_predictions': int,
                'total_rewards': float,
                'rank': int or None,
            }
        """
        our_address = ""
        if hasattr(self, 'wallet') and self.wallet:
            our_address = self.wallet.address
        elif hasattr(self, '_peers') and self._peers and self._peers._identity_bridge:
            our_address = self._peers._identity_bridge.evrmore_address

        if not our_address:
            return {
                'average_score': 0.0,
                'total_predictions': 0,
                'total_rewards': 0.0,
                'rank': None,
            }

        # Get score from prediction protocol
        avg_score = await self.getPredictorScore(our_address, stream_id)

        # Get reward history for totals
        history = await self.getRewardHistory(stream_id=stream_id, limit=1000)
        total_rewards = sum(h.get('amount', 0) for h in history)

        return {
            'average_score': avg_score,
            'total_predictions': len(history),
            'total_rewards': total_rewards,
            'rank': None,  # Would need leaderboard query
        }

    def getBalances(self):
        '''
        we get this from the server, not electrumx
        example:
        {
            'currency': 100,
            'chain_balance': 0,
            'liquidity_balance': None,
        }
        '''
        success, self.balances = self.server.getBalances()
        if not success:
            logging.warning("Failed to get balances from server")
        return self.getBalance()
    
    def getBalance(self, currency: str = 'currency') -> float:
        return self.balances.get(currency, 0)

    def setRewardAddress(
        self,
        address: Union[str, None] = None,
        globally: bool = False
    ) -> bool:
        if EvrmoreWallet.addressIsValid(address):
            self.configRewardAddress = address
            config.add(data={'reward address': address})
            if isinstance(self.details, CheckinDetails):
                self.details.setRewardAddress(address)
            if not globally:
                return True
        else:
            self.configRewardAddress: str = str(config.get().get('reward address', ''))
        if (
            globally and
            self.env in ['prod', 'local', 'testprod'] and
            EvrmoreWallet.addressIsValid(self.configRewardAddress)
        ):
            self.server.setRewardAddress(
                signature=self.wallet.sign(self.configRewardAddress),
                pubkey=self.wallet.pubkey,
                address=self.configRewardAddress)
            return True
        return False

    async def populateData(self):
        """ save real and prediction data in neuron """
        for k in self.pubSubMapping.keys():
            if k != 'transferProtocol' and k != 'transferProtocolPayload' and k != 'transferProtocolKey':
                realDataDf = None
                predictionDataDf = None
                try:
                    realData = await self.dataClient.getLocalStreamData(k)
                    if realData.status == DataServerApi.statusSuccess.value and isinstance(realData.data, pd.DataFrame):
                        realDataDf = realData.data.tail(100)
                    else:
                        raise Exception(realData.senderMsg)
                except Exception as e:
                    # logging.error(e)
                    pass
                try:
                    predictionData = await self.dataClient.getLocalStreamData(self.pubSubMapping[k]['publicationUuid'])
                    if predictionData.status == DataServerApi.statusSuccess.value and isinstance(predictionData.data, pd.DataFrame):
                        predictionDataDf = predictionData.data.tail(100)
                    else:
                        raise Exception(predictionData.senderMsg)
                except Exception as e:
                    # logging.error(e)
                    pass

                self.data[k] = {
                    'realData': realDataDf.tail(100) if realDataDf is not None else pd.DataFrame([]),
                    'predictionData': predictionDataDf.tail(100) if predictionDataDf is not None else pd.DataFrame([])
                }
                if ((realDataDf is not None and not realDataDf.empty) or 
                (predictionDataDf is not None and not predictionDataDf.empty)):
                    self.updateStreamDisplay(self.findMatchingPubSubStream(k).streamId)

    @staticmethod
    def predictionStreams(streams: list[Stream]):
        """filter down to prediciton publications"""
        # todo: doesn't work
        return [s for s in streams if s.predicting is not None]

    @staticmethod
    def oracleStreams(streams: list[Stream]):
        """filter down to prediciton publications"""
        return [s for s in streams if s.predicting is None]

    def streamDisplayer(self, subsription: Stream, publication: Stream):
        return StreamOverview(
            streamId=subsription.streamId,
            value="",
            prediction="",
            values=[],
            predictions=[],
            price_per_obs=publication.price_per_obs if publication is not None else 0.0)
    
    def removePair(self, pub: StreamId, sub: StreamId):
        self.publications = [p for p in self.publications if p.streamId != pub]
        self.subscriptions = [s for s in self.subscriptions if s.streamId != sub]

    def addToEngine(self, stream: Stream, publication: Stream):
        if self.aiengine is not None:
            self.aiengine.addStream(stream, publication)

    def getMatchingStream(self, streamId: StreamId) -> Union[StreamId, None]:
        for stream in self.publications:
            if stream.streamId == streamId:
                return stream.predicting  # predicting is already a StreamId
            if stream.predicting == streamId:  # comparing StreamId objects directly
                return stream.streamId
        return None
    
    def removePair(self, pub: StreamId, sub: StreamId):
        self.publications = [p for p in self.publications if p.streamId != pub]
        self.subscriptions = [s for s in self.subscriptions if s.streamId != sub]

    def addToEngine(self, stream: Stream, publication: Stream):
        if self.aiengine is not None:
            self.aiengine.addStream(stream, publication)


    async def centrifugoConnect(self):
        self.centrifugoPayload = self.server.getCentrifugoToken()
        self.centrifugoToken = self.centrifugoPayload.get('token')
        if self.centrifugoToken is None:
            logging.error("Failed to get centrifugo token")
            return
        # Token will be passed to Engine via transferProtocolPayload
        # Neuron only publishes, Engine subscribes

    @property
    def isConnectedToServer(self):
        if hasattr(self, 'dataClient') and self.dataClient is not None:
            return self.dataClient.isConnected()
        return False

    async def connectToDataServer(self):
        ''' connect to server, retry if failed '''

        async def authenticate() -> bool:
            response = await self.dataClient.authenticate(islocal='neuron')
            if response.status == DataServerApi.statusSuccess.value:
                logging.info("Local Neuron successfully connected to Server Ip at :", self.dataServerIp, color="green")
                return True
            return False

        async def initiateServerConnection() -> bool:
            ''' local neuron client authorization '''
            self.dataClient = DataClient(self.dataServerIp, self.dataServerPort, identity=self.identity)
            return await authenticate()

        waitingPeriod = 10
        while not self.isConnectedToServer:
            try:
                self.dataServerIp = config.get().get('server ip', '0.0.0.0')
                self.dataServerPort = int(config.get().get('server port', 24600))
                if await initiateServerConnection():
                    return True
            except Exception as e:
                # logging.error("Error connecting to server ip in config : ", e)
                try:
                    self.dataServerIp = self.server.getPublicIp().text.split()[-1]
                    if await initiateServerConnection():
                        return True
                except Exception as e:
                    logging.warning(f'Failed to find a valid Server Ip, retrying in {waitingPeriod}')
                    await asyncio.sleep(waitingPeriod)

    async def stayConnectedForever(self):
        ''' alternative to await asyncio.Event().wait() '''
        while True:
            await asyncio.sleep(30)
            if not self.isConnectedToServer:
                await self.connectToDataServer()
                await self.dataServerFinalize()

    def determineTransferProtocol(self, ipAddr: str, port: int = 24600) -> str:
        '''
        determine the transfer protocol to be used for data transfer
        default: p2p
        p2p - use the data server and data clients
        p2p-proactive
            - use the data server and data clients.
            - connect to my subscribers, send them the data streams they want.
                - sync historic datasets?
        pubsub
            - use the pubsub connection to subscribe to data streams.
            - does not include pub
        '''
        return config.get().get(
            'transfer protocol',
            'p2p-pubsub' if self.server.loopbackCheck(ipAddr, port) else 'p2p-proactive-pubsub')
    
    def determineInternalNatIp(self) -> str:
        """Determine the internal NAT IP address using multiple methods."""
        
        import socket
        import subprocess
        import re
        import ipaddress

        # Method 1: Using socket
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            ip = s.getsockname()[0]
            s.close()
            if ipaddress.ip_address(ip).is_private:
                return ip
        except Exception as e:
            logging.debug(f"Socket method failed: {e}")
        
        # Method 2: Parse output of ip address command
        try:
            result = subprocess.run(["ip", "addr", "show"], capture_output=True, text=True)
            output = result.stdout
            interfaces = {}
            current_iface = None
            
            for line in output.splitlines():
                iface_match = re.match(r'^\d+:\s+(\w+):', line)
                if iface_match:
                    current_iface = iface_match.group(1)
                    if current_iface == 'lo' or current_iface.startswith(('docker', 'br-', 'veth')):
                        current_iface = None
                elif current_iface and 'inet ' in line:
                    ip_match = re.search(r'inet\s+(\d+\.\d+\.\d+\.\d+)', line)
                    if ip_match:
                        ip = ip_match.group(1)
                        if not ip.startswith('127.') and ipaddress.ip_address(ip).is_private:
                            interfaces[current_iface] = ip
            
            # First try specific interface prefixes
            priority_prefixes = ['eth', 'ens', 'eno', 'enp', 'wlan']
            for prefix in priority_prefixes:
                for iface, ip in interfaces.items():
                    if iface.startswith(prefix):
                        return ip
            
            # Then try any available private IP
            if interfaces:
                return next(iter(interfaces.values()))
                    
        except Exception as e:
            logging.error(f"IP command method failed: {e}")
        
        # Method 3: Fallback to hostname command
        try:
            result = subprocess.run(["hostname", "-I"], capture_output=True, text=True)
            ips = result.stdout.strip().split()
            for ip in ips:
                if ipaddress.ip_address(ip).is_private:
                    return ip
        except Exception as e:
            logging.error(f"Hostname method failed: {e}")
        
        return "0.0.0.0"


    async def sharePubSubInfo(self):
        ''' set Pub-Sub mapping in the authorized server '''
        def matchPubSub():
            ''' matchs related pub/sub stream '''
            streamPairs = StreamPairs(
                self.subscriptions,
                StartupDag.predictionStreams(self.publications))
            subscriptions, publications = streamPairs.get_matched_pairs()
            # this is the one that's called (todo: refactor other paths out)
            self.streamDisplay = [
                self.streamDisplayer(subscription, publication)
                for subscription, publication in zip(subscriptions, publications)]
            predictionStreamsToPredict = config.get().get('prediction stream', None)
            if predictionStreamsToPredict is not None:
                streamsLen = int(predictionStreamsToPredict)
                logging.info(f"Length of Streams reduced from {len(subscriptions)} to {streamsLen}")
            else:
                streamsLen = len(subscriptions)
            subList = [sub.streamId.uuid for sub in subscriptions[:streamsLen]]
            pubList = [pub.streamId.uuid for pub in publications[:streamsLen]]
            pubListAll = [pub.streamId.uuid for pub in self.publications]
            _, fellowSubscribers = self.server.getStreamsSubscribers(subList)
            success, mySubscribers = self.server.getStreamsSubscribers(pubListAll)
            _, remotePublishers = self.server.getStreamsPublishers(subList)
            _, meAsPublisher = self.server.getStreamsPublishers(pubList)
            _, hostInfo = self.server.getStreamsPublishers(pubListAll)

            # removing duplicates ( same ip and port )
            for data in [fellowSubscribers, mySubscribers, remotePublishers, meAsPublisher]:
                for key in data:
                    seen = set()
                    data[key] = [x for x in data[key] if not (x in seen or seen.add(x))]

            # logging.debug("My Subscribers", mySubscribers, print=True)
            # logging.debug("hostInfo", hostInfo, print=True)
            # logging.debug("remotePublishers", remotePublishers, print=True)

            hostIpAndPort = next((value for value in hostInfo.values() if value), [])

            # Handle empty `hostInfo` or hostIpAndPort is not known case
            if not hostInfo or not hostIpAndPort:
                logging.warning("Host Info is empty. Using default Pro-active protocol.")
                self.transferProtocol = 'p2p-proactive-pubsub' # a good usecase for 'pubsub'?
            else:
                logging.debug('Host Ip And Port', hostIpAndPort, print=True)
                hostIp = hostIpAndPort[0].split(':')[0]
                for k, v in remotePublishers.items():
                    publisherIp = v[0].split(':')[0]
                    if publisherIp == hostIp:
                        uuidOfMatchedIp = k
                        portOfMatchedIp = v[0].split(':')[1]
                        internalNatIp = self.determineInternalNatIp()
                        publisherToBeAppended = internalNatIp + ':' + portOfMatchedIp
                        for k, v in fellowSubscribers.items():
                            # Appending the internal NAT ip if remotePublisher has same ip as of the host
                            if k == uuidOfMatchedIp:
                                logging.debug("Appended remotePublisher Ip with Port:", publisherToBeAppended, print=True)
                                v.append(publisherToBeAppended)
                self.transferProtocol = self.determineTransferProtocol(
                    hostIp, self.dataServerPort)
                logging.debug('transferProtocol', self.transferProtocol, print=True)
                    
            subInfo = {
                uuid: {
                    'subscribers': fellowSubscribers.get(uuid, []),
                    'publishers': remotePublishers.get(uuid, [])}
                for uuid in subList
            }
            pubInfo = {
                uuid: {
                    'subscribers': mySubscribers.get(uuid, []),
                    'publishers': meAsPublisher.get(uuid, [])}
                for uuid in pubList
            }

            self.pubSubMapping = {
                sub_uuid: {
                    'publicationUuid': pub_uuid,
                    'supportiveUuid': [],
                    'dataStreamSubscribers': subInfo[sub_uuid]['subscribers'],
                    'dataStreamPublishers': subInfo[sub_uuid]['publishers'],
                    'predictiveStreamSubscribers': pubInfo[pub_uuid]['subscribers'],
                    'predictiveStreamPublishers': pubInfo[pub_uuid]['publishers']
                }
                for sub_uuid, pub_uuid in zip(subInfo.keys(), pubInfo.keys())
            }
            self.pubSubMapping['transferProtocol'] = self.transferProtocol

            # Include Centrifugo token for all transfer protocols if available
            centrifugoData = None
            if self.centrifugoPayload:
                centrifugoData = {
                    'centrifugo': {
                        'token': self.centrifugoPayload.get('token'),
                        'ws_url': self.centrifugoPayload.get('ws_url'),
                        'expires_at': self.centrifugoPayload.get('expires_at'),
                        'user_id': self.centrifugoPayload.get('user_id')
                    }
                }

            if self.transferProtocol == 'p2p-proactive-pubsub': # p2p-proactive-pubsub
                self.pubSubMapping['transferProtocolPayload'] = {
                    **(mySubscribers if success else {}),
                    **(centrifugoData or {})
                }
                self.pubSubMapping['transferProtocolKey'] = self.key
            elif self.transferProtocol == 'p2p-pubsub':
                self.pubSubMapping['transferProtocolKey'] = self.key
                self.pubSubMapping['transferProtocolPayload'] = centrifugoData
            else:
                self.pubSubMapping['transferProtocolPayload'] = centrifugoData

        async def _sendPubSubMapping():
            """ send pub-sub mapping with peer informations to the DataServer """
            try:
                response = await self.dataClient.setPubsubMap(self.pubSubMapping)
                if response.status == DataServerApi.statusSuccess.value:
                    logging.debug(response.senderMsg, print=True)
                else:
                    raise Exception(response.senderMsg)
            except Exception as e:
                logging.error(f"Failed to set pub-sub mapping, {e}")

        matchPubSub()
        await _sendPubSubMapping()

    async def subscribeToRawData(self):
        ''' local neuron client subscribes to engine predication data '''

        for k in self.pubSubMapping.keys():
            if k!= 'transferProtocol' and k!= 'transferProtocolPayload' and k != 'transferProtocolKey':
                response = await self.dataClient.subscribe(
                    uuid=k,
                    callback=self.handleRawData)
                if response.status == DataServerApi.statusSuccess.value:
                    logging.debug('Subscribed to Raw Data', response.senderMsg)
                else:
                    logging.warning('Failed to Subscribe: ', response.senderMsg )

    async def subscribeToEngineUpdates(self):
        ''' local neuron client subscribes to engine predication data '''

        for k, v in self.pubSubMapping.items():
            if k!= 'transferProtocol' and k!= 'transferProtocolPayload' and k != 'transferProtocolKey':
                response = await self.dataClient.subscribe(
                    uuid=v['publicationUuid'],
                    callback=self.handlePredictionData)
                if response.status == DataServerApi.statusSuccess.value:
                    logging.debug('Subscribed to Prediction Data', response.senderMsg)
                else:
                    logging.warning('Failed to Subscribe: ', response.senderMsg )

    async def handleRawData(self, subscription: Subscription, message: Message):

        if message.status != DataClientApi.streamInactive.value:
            logging.info('Raw Data Subscribtion Message',message.to_dict(True), color='green')
            updated_data  = pd.concat([
                self.data[message.uuid]['realData'],
                message.data
            ])
            self.data[message.uuid]['realData'] = updated_data.tail(100)
            self.latestObservationTime = time.time()
        else:
            logging.warning('Raw Data Subscribtion Message',message.to_dict(True))

    async def handlePredictionData(self, subscription: Subscription, message: Message):

        def findMatchingSubUuid(pubUuid) -> str:
            for key in self.pubSubMapping.keys():
                if pubUuid == self.pubSubMapping.get(key, {}).get('publicationUuid'):
                    return key

        if message.status != DataClientApi.streamInactive.value:
            logging.info('Prediction Data Subscribtion Message',message.to_dict(True), color='green')
            matchedSubUuid = findMatchingSubUuid(message.uuid)
            matchedPubStream = self.findMatchingPubSubStream(message.uuid, False)
            matchedSubStream = self.findMatchingPubSubStream(matchedSubUuid)

            if not message.replace: # message.replace is False if its prediction on cadence
                self.server.publish(
                    topic=matchedPubStream.streamId.jsonId,
                    data=str(message.data['value'].iloc[0]),
                    observationTime=str(message.data.index[0]),
                    observationHash=str(message.data['hash'].iloc[0]),
                    isPrediction=True,
                    useAuthorizedCall=self.version >= Version("0.2.6"))
            
            updatedPredictionData  = pd.concat([
                self.data[matchedSubUuid]['predictionData'],
                message.data
            ])
            self.data[matchedSubUuid]['predictionData'] = updatedPredictionData.tail(100)
            self.updateStreamDisplay(matchedSubStream.streamId)
        else:
            logging.warning('Prediction Data Subscribtion Message',message.to_dict(True))

    def startRelay(self):
        def append(streams: list[Stream]):
            relays = satorineuron.config.get("relay")
            rawStreams = []
            for x in streams:
                topic = x.streamId.jsonId
                if topic in relays.keys():
                    x.uri = relays.get(topic).get("uri")
                    x.headers = relays.get(topic).get("headers")
                    x.payload = relays.get(topic).get("payload")
                    x.hook = relays.get(topic).get("hook")
                    x.history = relays.get(topic).get("history")
                    rawStreams.append(x)
            return rawStreams

        if self.relay is not None:
            self.relay.kill()
        self.relay = RawStreamRelayEngine(streams=append(self.publications))
        self.relay.run()
        logging.info("started relay engine", color="green")

    def findMatchingPubSubStream(self, uuid: str, sub: bool = True) -> Stream:
            if sub:
                for sub in self.subscriptions:
                    if sub.streamId.uuid == uuid:
                        return sub
            else:
                for pub in self.publications:
                    if pub.streamId.uuid == uuid:
                        return pub

    def updateStreamDisplay(self, streamId: StreamId) -> None:
        """
        Update stream display with new value and prediction 
        """
        for stream_display in self.streamDisplay:
            if stream_display.streamId == streamId:
                uuid = streamId.uuid
                if uuid in self.data:
                    realData = self.data[uuid]['realData']
                    predData = self.data[uuid]['predictionData']
                    alignedRealData, alignedPredData = self.alignPredDataWithRealData(realData, predData)
                    if alignedRealData.empty or alignedPredData.empty:
                        continue
                    stream_display.value = str(alignedRealData['value'].iloc[-1])
                    stream_display.values = [str(val) for val in alignedRealData['value']]
                    stream_display.prediction = str(predData['value'].iloc[-1])
                    stream_display.predictions = [str(val) for val in alignedPredData['value']]
                    self.addModelUpdate(stream_display)
                    break

    def addWorkingUpdate(self, data: str):
        ''' tell ui we are working on something '''
        self.sendToUI(UiEndpoint.workingUpdate, data)

    def addModelUpdate(self, data: StreamOverview):
        ''' tell ui about model changes '''
        self.sendToUI(UiEndpoint.modelUpdate, data)

    def populateStreamDisplay(self):

        def streamDisplayer(subsription: Stream):
            # Get the price from the stream
            price_per_obs = 0.0
            
            # Debug logging
            print(f"Creating StreamOverview for: {subsription.streamId.stream}")
            print(f"  StreamId details: source={subsription.streamId.source}, author={subsription.streamId.author}, stream={subsription.streamId.stream}, target={subsription.streamId.target}")
            print(f"  StreamId UUID: {subsription.streamId.uuid}")
            print(f"  StreamId UUID type: {type(subsription.streamId.uuid)}")
            print(f"  StreamId UUID length: {len(subsription.streamId.uuid) if subsription.streamId.uuid else 0}")
            
            # If this is a prediction stream (has predicting field), get its price
            if hasattr(subsription, 'predicting') and subsription.predicting is not None:
                if hasattr(subsription, 'price_per_obs') and subsription.price_per_obs is not None:
                    price_per_obs = subsription.price_per_obs
            else:
                # This is an oracle stream, find its prediction stream to get the price
                for pub in self.publications:
                    if hasattr(pub, 'predicting') and pub.predicting == subsription.streamId:
                        if hasattr(pub, 'price_per_obs') and pub.price_per_obs is not None:
                            price_per_obs = pub.price_per_obs
                        break
            
            return StreamOverview(
                streamId=subsription.streamId,
                value="",
                prediction="",
                values=[],
                predictions=[],
                price_per_obs=price_per_obs)

        # Create StreamOverview objects for subscriptions (oracle streams)
        self.streamDisplay = [
            streamDisplayer(subscription)
            for subscription in self.subscriptions]

    def pause(self, timeout: int = 60):
        """pause the engine."""
        self.paused = True
        # self.engine.pause()
        self.pauseThread = self.asyncThread.delayedRun(
            task=self.unpause, delay=timeout)
        logging.info("AI engine paused", color="green")

    def unpause(self):
        """pause the engine."""
        # self.engine.unpause()
        self.paused = False
        if self.pauseThread is not None:
            self.asyncThread.cancelTask(self.pauseThread)
        self.pauseThread = None
        logging.info("AI engine unpaused", color="green")

    def delayedStart(self):
        alreadySetup: bool = os.path.exists(config.walletPath("wallet.yaml"))
        if alreadySetup:
            threading.Thread(target=self.delayedEngine).start()
        # while True:
        #    if self.asyncThread.loop is not None:
        #        self.restartThread = self.asyncThread.repeatRun(
        #            task=self.start,
        #            interval=60*60*24 if alreadySetup else 60*60*12)
        #        break
        #    time.sleep(1)

    def triggerRestart(self, return_code=1):
        # 0 = shutdown, 1 = restart container, 2 = restart app
        os._exit(return_code)

    def emergencyRestart(self):
        import time
        logging.warning("restarting in 10 minutes", print=True)
        time.sleep(60 * 10)
        self.triggerRestart()

    def restartEverythingPeriodic(self):
        import random
        restartTime = time.time() + config.get().get(
            "restartTime", random.randint(60 * 60 * 21, 60 * 60 * 24)
        )
        # # removing tag check because I think github might block vps or
        # # something when multiple neurons are hitting it at once. very
        # # strange, but it was unreachable for someone and would just hang.
        # latestTag = LatestTag()
        while True:
            if time.time() > restartTime:
                self.triggerRestart()
            time.sleep(random.randint(60 * 60, 60 * 60 * 4))
            # time.sleep(random.randint(10, 20))
            # latestTag.get()
            # if latestTag.isNew:
            #    self.triggerRestart()

    def publish(
        self,
        topic: str,
        data: str,
        observationTime: str,
        observationHash: str,
        toCentral: bool = False,
        isPrediction: bool = False,
    ) -> True:
        """publishes to all the pubsub servers"""
        
        # publishing to centrifugo REST API
        if self.centrifugoToken and not isPrediction:
            try:
                streamId = StreamId.fromTopic(topic)
                if streamId.uuid:
                    # Format data to include all fields - ensure all values are serializable
                    centrifugo_data = {
                        'value': str(data),
                        'time': observationTime,
                        'hash': str(observationHash)
                    }
                    response = publish_to_stream_rest(
                        stream_uuid=streamId.uuid,
                        data=centrifugo_data,
                        token=self.centrifugoToken
                    )
                    if response.status_code != 200:
                        logging.warning(f"Centrifugo publish failed: {response.status_code}")
                        raise Exception
                    else:
                        logging.info(f"Centrifugo publish successful: {response.status_code}")
            except Exception as e:
                toCentral = True
                logging.error(f"Error publishing to Centrifugo: {e}")

        # publishing to centrifugo WEBSOCKET (todo: move websocket connection to Engine, and use rest api for publishing here.)
        #if self.centrifugo is not None and hasattr(self, 'centrifugoSubscriptions'):
        #    # TODO: we need to figure public to the correct stream according to this topic (by uuid ideally)
        #    streamId = StreamId.fromTopic(topic)
        #    # Find the subscription for this stream
        #    for subscription in self.centrifugoSubscriptions:
        #        if subscription.channel == f"streams:{streamId.uuid}":
        #            # Publish your predictions
        #            # run in background, don't wait
        #            asyncio.create_task(subscription.publish(data))
        #            break
        #    # alternatively, we could use the `asyncio.run(subscription.publish(data))`
        #    # alternatively, we could make publish an async function and call this with `await subscription.publish(data)`

        if toCentral:
            self.server.publish(
                topic=topic,
                data=data,
                observationTime=observationTime,
                observationHash=observationHash,
                isPrediction=isPrediction,
                useAuthorizedCall=self.version >= Version("0.2.6"))

    def performStakeCheck(self):
        self.stakeStatus = self.server.stakeCheck()
        return self.stakeStatus

    def setMiningMode(self, miningMode: Union[bool, None] = None):
        miningMode = (
            miningMode
            if isinstance(miningMode, bool)
            else config.get().get('mining mode', True))
        self.miningMode = miningMode
        config.add(data={'mining mode': self.miningMode})
        if hasattr(self, 'server') and self.server is not None:
            self.server.setMiningMode(miningMode)
        return self.miningMode

    def setEngineVersion(self, version: Union[str, None] = None) -> str:
        default = 'v2'
        version = (
            version
            if version in ['v1', 'v2']
            else config.get().get('engine version', default))
        self.engineVersion = version if version in ['v1', 'v2'] else default
        config.add(data={'engine version': self.engineVersion})
        return self.engineVersion

    def setInvitedBy(self, address: Union[str, None] = None) -> str:
        address = address or config.get().get('invited by', address)
        if address:
            self.invitedBy = address
            config.add(data={'invited by': self.invitedBy})
        return self.invitedBy

    def poolAccepting(self, status: bool):
        success, result = self.server.poolAccepting(status)
        if success:
            self.poolIsAccepting = status
        return success, result

    # def getAllOracleStreams(self, searchText: Union[str, None] = None, fetch: bool = False):
    #     if fetch or self.allOracleStreams is None:
    #         self.allOracleStreams = self.server.getSearchStreams(
    #             searchText=searchText)
    #     return self.allOracleStreams


    def getPaginatedOracleStreams(self, page: int = 1, per_page: int = 100, searchText: Union[str, None] = None, 
                            sort_by: str = 'popularity', order: str = 'desc', force_refresh: bool = False) -> tuple[list, dict]:
        """ Get paginated oracle streams (recommended approach) """
        try:
            page = max(1, page)
            per_page = min(max(1, per_page), 200)
            
            offset = (page - 1) * per_page
            
            streams, total_count = self.server.getSearchStreamsPaginated(
                searchText=searchText,
                page=page,
                per_page=per_page,
                sort_by=sort_by,
                order=order)
            
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
            has_prev = page > 1
            has_next = page < total_pages
            
            pagination_info = {
                'current_page': page,
                'total_pages': total_pages,
                'total_count': total_count,
                'has_prev': has_prev,
                'has_next': has_next,
                'per_page': per_page}
            return streams, pagination_info
            
        except Exception as e:
            logging.error(f"Error in getPaginatedOracleStreams: {str(e)}")
            return [], {
                'current_page': page,
                'total_pages': 0,
                'total_count': 0,
                'has_prev': False,
                'has_next': False,
                'per_page': per_page}

    def getPaginatedPredictionStreams(self, page: int = 1, per_page: int = 100, searchText: Union[str, None] = None, 
                            sort_by: str = 'popularity', order: str = 'desc', force_refresh: bool = False) -> tuple[list, dict]:
        """ Get paginated prediction streams (recommended approach) """
        try:
            page = max(1, page)
            per_page = min(max(1, per_page), 200)
            
            offset = (page - 1) * per_page
            
            streams, total_count = self.server.getSearchPredictionStreamsPaginated(
                searchText=searchText,
                page=page,
                per_page=per_page,
                sort_by=sort_by,
                order=order)
            
            total_pages = (total_count + per_page - 1) // per_page if total_count > 0 else 1
            has_prev = page > 1
            has_next = page < total_pages
            
            pagination_info = {
                'current_page': page,
                'total_pages': total_pages,
                'total_count': total_count,
                'has_prev': has_prev,
                'has_next': has_next,
                'per_page': per_page}
            return streams, pagination_info
            
        except Exception as e:
            logging.error(f"Error in getPaginatedPredictionStreams: {str(e)}")
            return [], {
                'current_page': page,
                'total_pages': 0,
                'total_count': 0,
                'has_prev': False,
                'has_next': False,
                'per_page': per_page}

    def ableToBridge(self):
        if self.lastBridgeTime < time.time() + 60*60*1:
            return True, ''
        return False, 'Please wait at least an hour before Bridging Satori'

    @property
    def stakeRequired(self) -> float:
        return self.details.stakeRequired or constants.stakeRequired

    @property
    def admin(self) -> float:
        ''' check if the wallet is an admin, shows admin abilities '''
        if self.wallet is None:
            return False
        return self.wallet.address in ['EKHXCC6vGU3VfrGsRFnfBGLkvm6EENsXaB']
    
    def alignPredDataWithRealData(self, realDataDf, predictionDataDf) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Align prediction data with the NEXT real data point. Used for aligning the raw data with pred data in the UI
        """
        if realDataDf.empty or predictionDataDf.empty:
            return realDataDf, predictionDataDf
        
        # Ensure datetime index and sort
        realDataDf.index = pd.to_datetime(realDataDf.index)
        predictionDataDf.index = pd.to_datetime(predictionDataDf.index)
        realDataDf = realDataDf.sort_index()
        predictionDataDf = predictionDataDf.sort_index()
        
        # Calculate cadence from real data (median time difference between consecutive points)
        if len(realDataDf) < 2:
            cadence_minutes = 10  # Default fallback
        else:
            time_diffs = realDataDf.index.to_series().diff().dropna()
            cadence_seconds = time_diffs.median().total_seconds()
            cadence_minutes = max(1, cadence_seconds / 60)  # At least 1 minute
        
        # We need at least 2 real data points for this alignment strategy
        if len(realDataDf) < 2:
            return pd.DataFrame(), pd.DataFrame()
        
        aligned_real = []
        aligned_predictions = []
        
        # Start from the second real data point (index 1) since we need the previous point
        for i in range(1, len(realDataDf)):
            current_real_timestamp = realDataDf.index[i]
            previous_real_timestamp = realDataDf.index[i-1]
            current_real_row = realDataDf.iloc[i]
            
            # Define prediction window: from previous real data timestamp to just before current
            window_start = previous_real_timestamp
            window_end = current_real_timestamp - pd.Timedelta(seconds=1)
            
            # Find predictions within this window
            prediction_mask = (
                (predictionDataDf.index >= window_start) & 
                (predictionDataDf.index <= window_end)
            )
            
            window_predictions = predictionDataDf[prediction_mask]
            
            if not window_predictions.empty:
                # Take the latest prediction in the window (closest to current real data)
                latest_prediction = window_predictions.iloc[-1]
                
                aligned_real.append(current_real_row)
                aligned_predictions.append(latest_prediction)
        
        # Convert to DataFrames with sequential indices
        if aligned_real:
            aligned_real_df = pd.DataFrame(aligned_real)
            aligned_predictions_df = pd.DataFrame(aligned_predictions)
            
            # Reset indices to be sequential
            aligned_real_df.reset_index(drop=True, inplace=True)
            aligned_predictions_df.reset_index(drop=True, inplace=True)
            
            return aligned_real_df, aligned_predictions_df
        else:
            return pd.DataFrame(), pd.DataFrame()
