import asyncio
import aiohttp
import json
import logging
import os
import time
from dataclasses import dataclass
from pathlib import Path
from solana.rpc.async_api import AsyncClient
from solana.keypair import Keypair
from solana.transaction import Transaction
from solders.signature import Signature

# Configuration setup
@dataclass
class Config:
    rpc_url: str
    profit_threshold: float
    max_trade_amount: float
    trading_pairs: list
    jupiter_url: str
    slippage_bps: int
    max_retries: int
    retry_delay: float

    @classmethod
    def load(cls, path="config.json"):
        with open(path) as f:
            data = json.load(f)
        return cls(**data)

# Initialize logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("arbitrage.log"),
        logging.StreamHandler()
    ]
)

class ArbitrageBot:
    def __init__(self, config):
        self.config = config
        self.client = AsyncClient(config.rpc_url)
        self.keypair = self._load_keypair()
        self.session = aiohttp.ClientSession()
        self.prices = {}
        self.profit_log = []

    def _load_keypair(self):
        keypath = Path("wallet.json")
        if not keypath.exists():
            kp = Keypair()
            keypath.write_text(json.dumps(list(kp.secret_key)))
            os.chmod(keypath, 0o600)
            logging.info("New wallet created")
            return kp
        return Keypair.from_json(json.loads(keypath.read_text()))

    async def fetch_prices(self):
        """Fetch prices from Jupiter API asynchronously"""
        while True:
            try:
                tasks = [self._get_price(pair) for pair in self.config.trading_pairs]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for pair, result in zip(self.config.trading_pairs, results):
                    if not isinstance(result, Exception):
                        self.prices[pair] = result
                
                await asyncio.sleep(0.1)  # 100ms update interval
            except Exception as e:
                logging.error(f"Price fetch error: {e}")
                await asyncio.sleep(1)

    async def _get_price(self, pair):
        url = f"{self.config.jupiter_url}/quote?inputMint={pair[0]}&outputMint={pair[1]}&amount={self.config.max_trade_amount}"
        async with self.session.get(url) as resp:
            data = await resp.json()
            return float(data["outAmount"]) / 10**9  # Assuming 9 decimals

    def find_arbitrage(self):
        """Identify triangular arbitrage opportunities"""
        opportunities = []
        pairs = self.config.trading_pairs
        
        # Simple triangular arbitrage check
        for a in pairs:
            for b in pairs:
                if a[1] == b[0]:
                    for c in pairs:
                        if b[1] == c[0] and c[1] == a[0]:
                            rate = (self.prices.get(a, 0) * 
                                   self.prices.get(b, 0) * 
                                   self.prices.get(c, 0))
                            if rate > 1 + self.config.profit_threshold:
                                opportunities.append((a, b, c, rate))
        return opportunities

    async def execute_trade(self, route):
        """Execute arbitrage trade through Jupiter"""
        tx = Transaction()
        for pair in route:
            swap_ix = await self._prepare_swap_instruction(pair)
            tx.add(swap_ix)
        
        retries = 0
        while retries < self.config.max_retries:
            try:
                txid = await self.client.send_transaction(tx, self.keypair)
                confirmation = await self.client.confirm_transaction(txid)
                if confirmation.value[0].err is None:
                    profit = await self._calculate_profit(route)
                    logging.info(f"Arbitrage success! TX: {txid} Profit: {profit:.4f} SOL")
                    self._log_transaction(txid, profit, route)
                    return True
            except Exception as e:
                logging.warning(f"Transaction failed (attempt {retries+1}): {e}")
                await asyncio.sleep(self.config.retry_delay * (2 ** retries))
                retries += 1
        return False

    async def _prepare_swap_instruction(self, pair):
        # Implement actual swap instruction preparation using Jupiter API
        # This is a simplified placeholder implementation
        return {"instruction": "swap", "pair": pair}

    async def _calculate_profit(self, route):
        # Implement actual profit calculation
        return 0.03  # Placeholder value

    def _log_transaction(self, txid, profit, route):
        entry = {
            "timestamp": time.time(),
            "txid": str(txid),
            "profit": profit,
            "route": route,
            "wallet": str(self.keypair.public_key)
        }
        self.profit_log.append(entry)
        with open("transactions.log", "a") as f:
            f.write(json.dumps(entry) + "\n")

    async def run(self):
        """Main trading loop"""
        price_task = asyncio.create_task(self.fetch_prices())
        
        try:
            while True:
                opportunities = self.find_arbitrage()
                if opportunities:
                    best = max(opportunities, key=lambda x: x[3])
                    logging.info(f"Opportunity found: {best[3]-1:.2%} profit")
                    await self.execute_trade(best[:3])
                
                await asyncio.sleep(0.05)  # 50ms analysis interval
        finally:
            price_task.cancel()
            await self.session.close()
            await self.client.close()

if __name__ == "__main__":
    config = Config.load()
    bot = ArbitrageBot(config)
    
    try:
        asyncio.run(bot.run())
    except KeyboardInterrupt:
        logging.info("Bot stopped by user")