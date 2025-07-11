import asyncio
import aiohttp
import requests
import json
import time
import pandas as pd
from typing import List, Dict, Optional, Any
from datetime import datetime
import logging
from loguru_logger import logger
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, AsyncConnection
from sqlalchemy.orm import sessionmaker
from config import *
from models import *
from solders.pubkey import Pubkey
from sqlalchemy.dialects.postgresql import insert as pg_insert
import traceback
from utils_tmp_table import ensure_tmp_tables
from dotenv import load_dotenv
from web3 import Web3
import os
from decimal import Decimal

load_dotenv()

# 將 loguru 預設等級設為 INFO
logger.remove()
logger.add(lambda msg: print(msg, end=""), level="INFO")

SOL_TOKENS = {"So11111111111111111111111111111111111111112", "So11111111111111111111111111111111111111111"}
STABLECOINS = {"EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERk6LsGkZ7zJ2E4zi6tLQ6t5uCwFctb9d"}

# BNB/WBNB/穩定幣常量
BNB_ADDRESSES = [
    "0x0000000000000000000000000000000000000000",  # BNB (原生)
    "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",  # WBNB
    "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c"   # WBNB (小寫)
]
BSC_STABLECOIN_ADDRESSES = [
    "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",  # USD1
    "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  # USD1 (小寫)
    "0x55d398326f99059fF775485246999027B3197955",  # USDT
    "0x55d398326f99059ff775485246999027b3197955",  # USDT (小寫)
    "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",  # USDC
    "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC (小寫)
    "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",  # BUSD
    "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD (小寫)
    "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",  # DAI
    "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",  # DAI (小寫)
    "0x14016E85a25aeb13065688cAFB43044C2ef86784",  # TUSD
    "0x14016e85a25aeb13065688cafb43044c2ef86784",  # TUSD (小寫)
    "0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40",  # FRAX
    "0x90c97f71e18723b0cf0dfa30ee176ab653e89f40",  # FRAX (小寫)
    "0xd17479997F34cC09Bc8D90EaE5A2954b8C89c0b5",  # USDD
    "0xd17479997f34cc09bc8d90eae5a2954b8c89c0b5"   # USDD (小寫)
]
STABLECOIN_SYMBOLS = ["USDT", "USDC", "USD1", "BUSD", "DAI", "TUSD", "FRAX", "USDD"]

def now_utc8():
    """獲取UTC+8當前時間"""
    from datetime import timezone, timedelta
    tz_utc8 = timezone(timedelta(hours=8))
    return datetime.now(tz_utc8)

def to_checksum_if_bsc(address, chain_id):
    if chain_id in (56, 9006):
        try:
            return Web3.toChecksumAddress(address)
        except Exception:
            return address
    return address

async def get_bnb_price() -> Decimal:
    url = "https://www.binance.com/api/v3/ticker/price?symbol=BNBUSDT"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            data = await response.json()
            return Decimal(data['price'])

def get_bsc_balance(address: str) -> float:
    bsc_rpc = os.getenv('BSC_RPC_URL')
    if not bsc_rpc:
        raise Exception("BSC_RPC_URL not set")
    w3 = Web3(Web3.HTTPProvider(bsc_rpc))
    try:
        checksum_address = w3.to_checksum_address(address)
        balance_wei = w3.eth.get_balance(checksum_address)
        balance_eth = Web3.from_wei(balance_wei, 'ether')
        return float(balance_eth)
    except Exception as e:
        print(f'查詢BSC餘額失敗: {e}')
        return 0.0

class TradeDataFetcher:
    """
    交易數據查詢器
    用於從數據庫中查詢錢包的交易記錄
    """
    
    def __init__(self):
        self.Ian_engine = None
        self.SmartMoney_engine = None
        self.Ian_session_factory = None
        self.SmartMoney_session_factory = None
        self._init_db_connection()
    
    def _init_db_connection(self):
        """初始化數據庫連接"""
        try:
            # 使用Ian的數據庫連接
            self.Ian_engine = create_async_engine(
                DATABASE_URI_Ian,
                echo=False,
                future=True,
                pool_size=10,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True
            )

            self.SmartMoney_engine = create_async_engine(
                DATABASE_URI_SWAP_SOL,
                echo=False,
                future=True,
                pool_size=10,
                max_overflow=10,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True
            )
            
            self.Ian_session_factory = sessionmaker(
                bind=self.Ian_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )

            self.SmartMoney_session_factory = sessionmaker(
                bind=self.SmartMoney_engine,
                class_=AsyncSession,
                expire_on_commit=False
            )
            
            logger.info("數據庫連接初始化成功")
            
        except Exception as e:
            logger.error(f"數據庫連接初始化失敗: {e}")
            raise
    
    async def fetch_wallet_trades(self, wallet_address: str, chain_id: int = 501, days: int = 30) -> List[Dict[str, Any]]:
        try:
            db_chain_id = 9006 if chain_id in (56, 9006) else chain_id
            # BSC查詢時直接用小寫比對
            if db_chain_id == 9006:
                wallet_address_query = wallet_address.lower()
                trades_sql = text("""
                    SELECT * FROM dex_query_v1.trades 
                    WHERE chain_id = :chain_id AND LOWER(signer) = :wallet_address 
                    ORDER BY timestamp DESC
                """).bindparams(chain_id=db_chain_id, wallet_address=wallet_address_query)
            else:
                trades_sql = text("""
                    SELECT * FROM dex_query_v1.trades 
                    WHERE signer = :wallet_address 
                    ORDER BY timestamp DESC
                """).bindparams(wallet_address=wallet_address)
            async with self.Ian_session_factory() as session:
                await session.execute(text("SET search_path TO dex_query_v1;"))
                result = await session.execute(trades_sql)
                all_trades = []
                for row in result:
                    # 預設欄位
                    payload = None
                    is_buy = None
                    payload_token_address = None
                    payload_token_amount_ui = None
                    try:
                        if hasattr(row, 'payload') and row.payload:
                            if isinstance(row.payload, str):
                                payload = json.loads(row.payload)
                            else:
                                payload = row.payload
                            is_buy = payload.get('isBuy')
                            payload_token_address = payload.get('tokenAddress')
                            payload_token_amount_ui = payload.get('tokenAmountUI')
                    except Exception as e:
                        logger.warning(f"解析payload失敗: {e}, row: {row}")
                        payload = None
                    # 預設side
                    side = None
                    if is_buy is not None:
                        side = 'buy' if is_buy else 'sell'
                    else:
                        side = 'buy' if row.side == 0 else 'sell'
                    # token邏輯
                    if is_buy is not None:
                        # 買入
                        if is_buy:
                            # token_out 以payload為主
                            token_out = payload_token_address
                            # token_in 先用原本trades表的token_in，如果與payload tokenAddress相同則用token_out
                            token_in = row.token_in
                            if token_in == token_out:
                                token_in = row.token_out
                            # amount_out 以payload tokenAmountUI為主
                            amount_out = payload_token_amount_ui
                            # amount_in 先用原本trades表的amount_in/10**decimals_in，如果與tokenAmountUI相同則用amount_out/10**decimals_out
                            decimals_in = row.decimals_in or 0
                            decimals_out = row.decimals_out or 0
                            try:
                                amount_in_calc = float(row.amount_in) / (10 ** decimals_in) if row.amount_in is not None else 0
                            except Exception:
                                amount_in_calc = 0
                            try:
                                amount_out_calc = float(row.amount_out) / (10 ** decimals_out) if row.amount_out is not None else 0
                            except Exception:
                                amount_out_calc = 0
                            if abs(amount_in_calc - payload_token_amount_ui) < 1e-6:
                                amount_in = amount_out_calc
                            else:
                                amount_in = amount_in_calc
                        else:
                            # 賣出
                            token_in = payload_token_address
                            token_out = row.token_out
                            if token_out == token_in:
                                token_out = row.token_in
                            amount_in = payload_token_amount_ui
                            decimals_in = row.decimals_in or 0
                            decimals_out = row.decimals_out or 0
                            try:
                                amount_out_calc = float(row.amount_out) / (10 ** decimals_out) if row.amount_out is not None else 0
                            except Exception:
                                amount_out_calc = 0
                            try:
                                amount_in_calc = float(row.amount_in) / (10 ** decimals_in) if row.amount_in is not None else 0
                            except Exception:
                                amount_in_calc = 0
                            if abs(amount_out_calc - payload_token_amount_ui) < 1e-6:
                                amount_out = amount_in_calc
                            else:
                                amount_out = amount_out_calc
                    else:
                        # 沒有payload，維持原本邏輯
                        token_in = row.token_in
                        token_out = row.token_out
                        amount_in = row.amount_in
                        amount_out = row.amount_out
                        decimals_in = row.decimals_in or 0
                        decimals_out = row.decimals_out or 0
                    trade_data = {
                        'signer': row.signer,
                        'token_in': token_in,
                        'token_out': token_out,
                        'token_address': payload_token_address if payload_token_address else (row.token_out if row.side == 0 else row.token_in),
                        'side': side,
                        'amount_in': amount_in,
                        'amount_out': amount_out,
                        'price': row.price,
                        'price_usd': row.price_usd,
                        'decimals_in': decimals_in,
                        'decimals_out': decimals_out,
                        'timestamp': row.timestamp,
                        'tx_hash': row.tx_hash,
                        'chain_id': row.chain_id
                    }
                    all_trades.append(trade_data)
                return all_trades
        except Exception as e:
            logger.error(f"查詢交易記錄失敗: {str(e)}")
            return []
    
    async def close(self):
        """關閉數據庫連接"""
        if self.Ian_engine:
            await self.Ian_engine.dispose()
        if self.SmartMoney_engine:
            await self.SmartMoney_engine.dispose()

class OKXCrawler:
    """
    OKX聰明錢包爬蟲
    用於爬取OKX Web3平台的聰明錢包排名數據
    """
    
    def __init__(self):
        self.base_url = "https://web3.okx.com/priapi/v1/dx/market/v2/smartmoney/ranking/content"
        self.session = None
        self.trade_fetcher = TradeDataFetcher()
        self.wallet_token_state_cache = {}  # 錢包代幣狀態緩存
        self.pending_state_updates = 0
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'zh-TW,zh;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
        }
        self.token_info_cache = {}
        self.token_info_expiry = {}
        self.token_cache_ttl = 3600  # 1小時
        self.token_fetch_locks = {}
        self.wallet_balance_cache = {}  # 新增錢包餘額快取
        self.wallet_balance_expiry = {}
        self.wallet_balance_cache_ttl = 60  # 1分鐘
        self.sol_price_cache = None  # 類別級快取
        self.sol_price_cache_time = None
        self.sol_price_cache_ttl = 3600  # 1小時
        self.start_time = time.time()  # 添加 start_time 屬性
        
    async def __aenter__(self):
        """異步上下文管理器入口"""
        self.session = aiohttp.ClientSession(headers=self.headers)
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """異步上下文管理器出口"""
        if self.session:
            await self.session.close()
        await self.trade_fetcher.close()
    
    def _build_url(self, rank_start: int, rank_end: int, chain_id: int = 501) -> str:
        """
        構建API請求URL
        
        Args:
            rank_start: 排名起始位置
            rank_end: 排名結束位置
            chain_id: 區塊鏈ID (501 = SOLANA, 9006 = BSC)
            
        Returns:
            完整的API URL
        """
        timestamp = int(time.time() * 1000)  # 毫秒級時間戳
        params = {
            'chainId': chain_id,
            'rankStart': rank_start,
            'periodType': 5,  # 固定值
            'rankBy': 1,      # 固定值
            'label': 'all',   # 固定值
            'desc': 'true',   # 降序排列
            'rankEnd': rank_end,
            't': timestamp
        }
        
        # 構建查詢字符串
        query_string = '&'.join([f"{k}={v}" for k, v in params.items()])
        return f"{self.base_url}?{query_string}"
    
    async def fetch_page(self, rank_start: int, rank_end: int, chain_id: int = 501) -> Optional[Dict]:
        """
        獲取單頁數據
        
        Args:
            rank_start: 排名起始位置
            rank_end: 排名結束位置
            chain_id: 區塊鏈ID
            
        Returns:
            解析後的JSON數據，如果失敗返回None
        """
        try:
            url = self._build_url(rank_start, rank_end, chain_id)
            logger.info(f"正在爬取頁面: rank_start={rank_start}, rank_end={rank_end}, chain_id={chain_id}")
            
            async with self.session.get(url) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    # 檢查響應是否成功
                    if data.get('code') == 0 and 'data' in data:
                        return data['data']
                    else:
                        logger.warning(f"API響應異常: {data}")
                        return None
                else:
                    logger.error(f"HTTP請求失敗: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"爬取頁面時發生錯誤: {e}")
            return None
    
    def extract_wallet_data(self, ranking_infos: List[Dict], chain_id: int = 501) -> List[Dict]:
        wallets = []
        for info in ranking_infos:
            try:
                # 強制BSC寫入9006
                db_chain_id = 9006 if chain_id in (56, 9006) else chain_id
                chain_name = 'SOLANA' if db_chain_id == 501 else 'BSC' if db_chain_id == 9006 else 'Unknown'
                kol_info = self._extract_kol_info(info.get('t', []))
                twitter_name = kol_info.get('twitter_name', None)
                twitter_username = kol_info.get('twitter_username', None)
                wallet_data = {
                    'wallet_address': info.get('walletAddress', ''),
                    'wallet_name': info.get('walletName', ''),
                    'wallet_icon_url': info.get('walletIconUrl', ''),
                    'chain_id': db_chain_id,
                    'chain': chain_name,
                    'labels': info.get('labels', []),
                    'last_time': info.get('lastTime', 0),
                    'period_type': info.get('periodType', 5),
                    'tx_count': info.get('tx', 0),
                    'volume': float(info.get('volume', 0)),
                    'avg_buy_size': float(info.get('avgBuySize', 0)),
                    'win_rate': float(info.get('winRate', 0)),
                    'pnl': float(info.get('pnl', 0)),
                    'roi': float(info.get('roi', 0)),
                    'pnl_history': info.get('pnlHistory', []),
                    'top_tokens': info.get('topTokens', []),
                    'kol_info': kol_info,
                    'crawled_at': datetime.now().isoformat(),
                    'twitter_name': twitter_name,
                    'twitter_username': twitter_username,
                }
                wallets.append(wallet_data)
            except Exception as e:
                logger.error(f"提取錢包數據時發生錯誤: {e}, 數據: {info}")
                continue
        return wallets
    
    def _extract_kol_info(self, tags: List[Dict]) -> Dict:
        kol_info = {
            'twitter_name': None,
            'twitter_username': None,
            'twitter_link': None,
            'twitter_image': None,
            'is_kol': False,
            'is_sniper': False,
            'is_dev': False
        }
        for tag in tags:
            tag_type = tag.get('k', '')
            tag_data = tag.get('e', {})
            if tag_type == 'kol':
                kol_info['is_kol'] = True
                kol_info['twitter_name'] = tag_data.get('name', None)
                kol_info['twitter_link'] = tag_data.get('kolTwitterLink', None)
                link = tag_data.get('kolTwitterLink', '')
                if link and link.startswith('https://x.com/'):
                    kol_info['twitter_username'] = link.split('/')[-1]
                else:
                    kol_info['twitter_username'] = tag_data.get('name', None)
                kol_info['twitter_image'] = tag_data.get('kolTwitterImageFullPath', None)
            elif tag_type == 'sniper':
                kol_info['is_sniper'] = True
            elif tag_type == 'dev':
                kol_info['is_dev'] = True
        return kol_info
    
    async def process_wallet_activities(self, wallet_address: str, chain_id: int = 501, activities: List[Dict[str, Any]] = None, twitter_name: str = None, twitter_username: str = None) -> List[Dict[str, Any]]:
        try:
            db_chain_id = 9006 if chain_id in (56, 9006) else chain_id
            if db_chain_id == 9006:
                logging.info(f"[BSC分析] {wallet_address}，chain_id={chain_id}，將寫入 {Transaction.__tablename__}")
            else:
                logging.info(f"[SOLANA分析] {wallet_address}，chain_id={chain_id}，將寫入 {Transaction.__tablename__}")
            wallet_address = to_checksum_if_bsc(wallet_address, db_chain_id)
            if activities is not None:
                trades_from_db = activities
            else:
                logger.info(f"嘗試從資料庫查詢錢包 {wallet_address} 的交易記錄")
                trades_from_db = await self.trade_fetcher.fetch_wallet_trades(wallet_address, chain_id=db_chain_id)
            if trades_from_db:
                logger.info(f"從資料庫查詢到 {len(trades_from_db)} 筆交易記錄")
                # 查詢餘額：BSC用get_bsc_balance並查BNB價格，計算USD
                if db_chain_id == 9006:
                    bnb_price = await get_bnb_price()
                    wallet_balance = get_bsc_balance(wallet_address)
                    wallet_balance_usd = float(wallet_balance) * float(bnb_price)
                else:
                    wallet_balance = await self._get_wallet_balance_cached(wallet_address)
                    wallet_balance_usd = float(wallet_balance)
                transaction_records = await self.convert_trades_to_transactions(trades_from_db, wallet_address, db_chain_id, wallet_balance=wallet_balance, wallet_balance_usd=wallet_balance_usd)
                if transaction_records:
                    try:
                        # logger.info(f"準備調用 save_transactions_batch，交易記錄數量: {len(transaction_records)}")
                        save_result = await self.save_transactions_batch(transaction_records)
                        logger.info(f"批量保存交易記錄結果: {save_result}")
                        
                        # 只有在成功保存交易記錄時才進行後續統計
                        if save_result:
                            # 寫入後自動統計錢包數據
                            try:
                                from WalletAnalysis import update_smart_money_data
                                async with self.trade_fetcher.SmartMoney_session_factory() as session:
                                    await session.execute(text("SET search_path TO dex_query_v1;"))
                                    kwargs = {}
                                    if twitter_name:
                                        kwargs['twitter_name'] = twitter_name
                                    if twitter_username:
                                        kwargs['twitter_username'] = twitter_username
                                    await update_smart_money_data(
                                        session,
                                        wallet_address,
                                        chain='SOLANA' if db_chain_id == 501 else 'BSC',
                                        is_smart_wallet=True,
                                        wallet_type=None,
                                        days=30,
                                        limit=100,
                                        **kwargs
                                    )
                                    logger.info(f"已自動統計並更新錢包 {wallet_address} 的錢包統計與持倉數據")
                            except Exception as e:
                                logger.error(f"自動統計錢包 {wallet_address} 數據失敗: {str(e)}")
                        else:
                            logger.warning(f"交易記錄保存失敗，跳過錢包統計更新")
                            
                    except Exception as e:
                        logger.error(f"保存交易記錄失敗: {str(e)}")
                        logger.warning(f"由於保存失敗，跳過錢包統計更新")
                    
                    return transaction_records
                if trades_from_db:
                    logger.info(f"從資料庫查詢到交易記錄但轉換後為空")
                    # 即使轉換後為空，也要更新錢包基本信息
                    try:
                        from WalletAnalysis import update_smart_money_data
                        async with self.trade_fetcher.SmartMoney_session_factory() as session:
                            await session.execute(text("SET search_path TO dex_query_v1;"))
                            kwargs = {}
                            if twitter_name:
                                kwargs['twitter_name'] = twitter_name
                            if twitter_username:
                                kwargs['twitter_username'] = twitter_username
                            await update_smart_money_data(
                                session,
                                wallet_address,
                                chain='SOLANA' if db_chain_id == 501 else 'BSC',
                                is_smart_wallet=True,
                                wallet_type=None,
                                days=30,
                                limit=100,
                                **kwargs
                            )
                            logger.info(f"完成錢包 {wallet_address} 的數據更新/寫入（無交易記錄，僅更新時間）")
                    except Exception as e:
                        logger.error(f"更新錢包 {wallet_address} 基本信息失敗: {str(e)}")
                    return []
            
            # 完全沒有交易記錄的情況
            logger.info(f"錢包 {wallet_address} 沒有找到交易記錄")
            # 即使沒有交易記錄，也要更新錢包基本信息
            try:
                from WalletAnalysis import update_smart_money_data
                async with self.trade_fetcher.SmartMoney_session_factory() as session:
                    await session.execute(text("SET search_path TO dex_query_v1;"))
                    kwargs = {}
                    if twitter_name:
                        kwargs['twitter_name'] = twitter_name
                    if twitter_username:
                        kwargs['twitter_username'] = twitter_username
                    await update_smart_money_data(
                        session,
                        wallet_address,
                        chain='SOLANA' if db_chain_id == 501 else 'BSC',
                        is_smart_wallet=True,
                        wallet_type=None,
                        days=30,
                        limit=100,
                        **kwargs
                    )
                    logger.info(f"完成錢包 {wallet_address} 的數據更新/寫入（無交易記錄，僅更新時間）")
            except Exception as e:
                logger.error(f"更新錢包 {wallet_address} 基本信息失敗: {str(e)}")
            return []
        except Exception as e:
            logger.error(f"處理錢包 {wallet_address} 活動時發生錯誤: {str(e)}")
            return []
    
    def _get_common_token_info(self, token_address):
        """
        針對常見 token_address 直接回傳對應資訊，不查快取、不查資料庫。
        支援 SOLANA 和 BSC 鏈的常見代幣。
        """
        mapping = {
            # SOLANA 常見代幣
            "So11111111111111111111111111111111111111112": {
                "address": "So11111111111111111111111111111111111111112",
                "name": "Wrapped SOL",
                "symbol": "WSOL",
                "decimals": 9,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111112/logo.png"
            },
            "So11111111111111111111111111111111111111111": {
                "address": "So11111111111111111111111111111111111111111",
                "name": "SOLANA",
                "symbol": "SOL",
                "decimals": 9,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/So11111111111111111111111111111111111111111/logo.png"
            },
            # USDC (SOLANA)
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "address": "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",
                "name": "USD Coin",
                "symbol": "USDC",
                "decimals": 6,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png"
            },
            # USDT (SOLANA)
            "Es9vMFrzaCERk6LsGkZ7zJ2E4zi6tLQ6t5uCwFctb9d": {
                "address": "Es9vMFrzaCERk6LsGkZ7zJ2E4zi6tLQ6t5uCwFctb9d",
                "name": "Tether USD",
                "symbol": "USDT",
                "decimals": 6,
                "icon": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/Es9vMFrzaCERk6LsGkZ7zJ2E4zi6tLQ6t5uCwFctb9d/logo.png"
            },
            
            # BSC 常見代幣
            # BNB (原生代幣)
            "0x0000000000000000000000000000000000000000": {
                "address": "0x0000000000000000000000000000000000000000",
                "name": "BNB",
                "symbol": "BNB",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/bnb-bnb-logo.png"
            },
            # WBNB (Wrapped BNB)
            "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c": {
                "address": "0xbb4CdB9CBd36B01bD1cBaEBF2De08d9173bc095c",
                "name": "Wrapped BNB",
                "symbol": "WBNB",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/bnb-bnb-logo.png"
            },
            "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c": {
                "address": "0xbb4cdb9cbd36b01bd1cbaebf2de08d9173bc095c",
                "name": "Wrapped BNB",
                "symbol": "WBNB",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/bnb-bnb-logo.png"
            },
            
            # USDT (BSC)
            "0x55d398326f99059fF775485246999027B3197955": {
                "address": "0x55d398326f99059fF775485246999027B3197955",
                "name": "Tether USD",
                "symbol": "USDT",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/tether-usdt-logo.png"
            },
            "0x55d398326f99059ff775485246999027b3197955": {
                "address": "0x55d398326f99059ff775485246999027b3197955",
                "name": "Tether USD",
                "symbol": "USDT",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/tether-usdt-logo.png"
            },
            
            # USDC (BSC)
            "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d": {
                "address": "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",
                "name": "USD Coin",
                "symbol": "USDC",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usd-coin-usdc-logo.png"
            },
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d": {
                "address": "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",
                "name": "USD Coin",
                "symbol": "USDC",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usd-coin-usdc-logo.png"
            },
            
            # BUSD (BSC)
            "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56": {
                "address": "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",
                "name": "BUSD Token",
                "symbol": "BUSD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/binance-usd-busd-logo.png"
            },
            "0xe9e7cea3dedca5984780bafc599bd69add087d56": {
                "address": "0xe9e7cea3dedca5984780bafc599bd69add087d56",
                "name": "BUSD Token",
                "symbol": "BUSD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/binance-usd-busd-logo.png"
            },
            
            # USD1 (BSC)
            "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d": {
                "address": "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",
                "name": "USD1",
                "symbol": "USD1",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usd-coin-usdc-logo.png"
            },
            "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d": {
                "address": "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",
                "name": "USD1",
                "symbol": "USD1",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usd-coin-usdc-logo.png"
            },
            
            # DAI (BSC)
            "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3": {
                "address": "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",
                "name": "Dai Token",
                "symbol": "DAI",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/multi-collateral-dai-dai-logo.png"
            },
            "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3": {
                "address": "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",
                "name": "Dai Token",
                "symbol": "DAI",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/multi-collateral-dai-dai-logo.png"
            },
            
            # TUSD (BSC)
            "0x14016E85a25aeb13065688cAFB43044C2ef86784": {
                "address": "0x14016E85a25aeb13065688cAFB43044C2ef86784",
                "name": "TrueUSD",
                "symbol": "TUSD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/trueusd-tusd-logo.png"
            },
            "0x14016e85a25aeb13065688cafb43044c2ef86784": {
                "address": "0x14016e85a25aeb13065688cafb43044c2ef86784",
                "name": "TrueUSD",
                "symbol": "TUSD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/trueusd-tusd-logo.png"
            },
            
            # FRAX (BSC)
            "0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40": {
                "address": "0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40",
                "name": "Frax",
                "symbol": "FRAX",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/frax-frax-logo.png"
            },
            "0x90c97f71e18723b0cf0dfa30ee176ab653e89f40": {
                "address": "0x90c97f71e18723b0cf0dfa30ee176ab653e89f40",
                "name": "Frax",
                "symbol": "FRAX",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/frax-frax-logo.png"
            },
            
            # USDD (BSC)
            "0xd17479997F34cC09Bc8D90EaE5A2954b8C89c0b5": {
                "address": "0xd17479997F34cC09Bc8D90EaE5A2954b8C89c0b5",
                "name": "Decentralized USD",
                "symbol": "USDD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usdd-usdd-logo.png"
            },
            "0xd17479997f34cc09bc8d90eae5a2954b8c89c0b5": {
                "address": "0xd17479997f34cc09bc8d90eae5a2954b8c89c0b5",
                "name": "Decentralized USD",
                "symbol": "USDD",
                "decimals": 18,
                "icon": "https://cryptologos.cc/logos/usdd-usdd-logo.png"
            },
        }
        return mapping.get(token_address)

    async def get_token_info_async(self, token_address: str) -> Optional[Dict[str, Any]]:
        # 先判斷是否為常見 token
        common = self._get_common_token_info(token_address)
        if common:
            return common
        from datetime import datetime, timedelta
        now = datetime.now()
        # 先查快取
        if token_address in self.token_info_cache:
            if self.token_info_expiry[token_address] > now:
                return self.token_info_cache[token_address]
        # 避免重複查詢
        if token_address not in self.token_fetch_locks:
            self.token_fetch_locks[token_address] = asyncio.Lock()
        async with self.token_fetch_locks[token_address]:
            # 再查一次快取
            if token_address in self.token_info_cache:
                if self.token_info_expiry[token_address] > now:
                    return self.token_info_cache[token_address]
            # 查 Ian 資料庫
            async with self.trade_fetcher.Ian_session_factory() as session:
                await session.execute(text("SET search_path TO dex_query_v1;"))
                sql = text("""
                    SELECT address, name, symbol, decimals, logo
                    FROM dex_query_v1.tokens WHERE address = :address LIMIT 1
                """)
                result = await session.execute(sql, {"address": token_address})
                row = result.fetchone()
                if row:
                    info = {
                        "address": row.address,
                        "name": row.name,
                        "symbol": row.symbol,
                        "decimals": row.decimals or 9,
                        "icon": row.logo
                    }
                    self.token_info_cache[token_address] = info
                    self.token_info_expiry[token_address] = now + timedelta(seconds=self.token_cache_ttl)
                    return info
        return None
    
    async def _get_wallet_balance_cached(self, wallet_address: str) -> float:
        from datetime import datetime, timedelta
        now = datetime.now()
        # 先查快取
        if wallet_address in self.wallet_balance_cache:
            if self.wallet_balance_expiry[wallet_address] > now:
                return self.wallet_balance_cache[wallet_address]
        # 沒有快取或過期，重新查詢
        balance = await self._get_wallet_balance_safely(wallet_address)
        self.wallet_balance_cache[wallet_address] = balance
        self.wallet_balance_expiry[wallet_address] = now + timedelta(seconds=self.wallet_balance_cache_ttl)
        return balance
    
    async def _get_wallet_balance_safely(self, wallet_address: str) -> float:
        try:
            balance_data = await self.get_sol_balance(wallet_address)
            wallet_balance = balance_data.get("balance", {}).get("float", 0)
            return wallet_balance
        except Exception as e:
            logger.warning(f"獲取錢包 {wallet_address} 餘額失敗: {e}，使用預設值 0")
            return 0
    
    async def get_sol_balance(self, wallet_address: str) -> dict:
        import base58
        from solana.rpc.async_api import AsyncClient
        fallback_rpcs = [
            "https://rpc.ankr.com/solana",
            "https://solana-mainnet.rpc.extrnode.com"
        ]
        client = None
        max_retries = 3
        default_balance = {"decimals": 9, "balance": {"int": 0, "float": 0.0}, "lamports": 0}
        try:
            pubkey = Pubkey(base58.b58decode(wallet_address))
        except Exception as e:
            logger.error(f"转换钱包地址到Pubkey时失败: {str(e)}")
            return default_balance
        rpcs_to_try = [RPC_URL, RPC_URL_backup] + fallback_rpcs
        for retry in range(max_retries):
            for rpc_index, rpc_url in enumerate(rpcs_to_try):
                try:
                    if not rpc_url.startswith(("http://", "https://")):
                        rpc_url = f"https://{rpc_url}"
                    if retry > 0 or rpc_index > 0:
                        logger.info(f"尝试使用 RPC {rpc_index+1}/{len(rpcs_to_try)} (尝试 {retry+1}/{max_retries}): {rpc_url[:30]}...")
                    client = AsyncClient(rpc_url, timeout=15)
                    balance_response = await client.get_balance(pubkey=pubkey)
                    # 只查一次 SOL 價格，之後都用快取
                    sol_info = self.get_sol_info("So11111111111111111111111111111111111111112")
                    sol_price = sol_info.get("priceUsd", 155.53)
                    lamports = balance_response.value
                    sol_balance = lamports / 10**9
                    usd_value = sol_balance * sol_price
                    logger.info(f"钱包 {wallet_address} 的 SOL 余额: {sol_balance:.6f} (${usd_value:.2f})")
                    return {
                        'decimals': 9,
                        'balance': {
                            'int': sol_balance,
                            'float': float(usd_value)
                        },
                        'lamports': lamports
                    }
                except Exception as e:
                    rpc_name = "主" if rpc_index == 0 else "备用" if rpc_index == 1 else f"额外 #{rpc_index-1}"
                    logger.error(f"使用{rpc_name} RPC 获取 SOL 余额时发生异常: {str(e)}")
                    if client:
                        try:
                            await client.close()
                        except:
                            pass
                        client = None
                    if rpc_index == len(rpcs_to_try) - 1 and retry < max_retries - 1:
                        wait_time = (retry + 1) * 1.5
                        logger.info(f"所有RPC都失败，等待 {wait_time:.1f} 秒后重试...")
                        await asyncio.sleep(wait_time)
        logger.warning(f"无法获取钱包 {wallet_address} 的余额，所有尝试均失败")
        return default_balance
    
    def determine_transaction_type(self, wallet_address: str, token_address: str, is_buy: bool, amount: float) -> str:
        try:
            state = self.get_wallet_token_state(wallet_address, token_address)
            current_amount = state["current_amount"]
            if is_buy:
                if current_amount <= 0:
                    return "build"
                else:
                    return "buy"
            else:
                if current_amount <= 0:
                    return "clean"
                elif amount >= current_amount:
                    return "clean"
                else:
                    return "sell"
        except Exception as e:
            logger.error(f"判斷交易類型時發生錯誤: {str(e)}")
            return "buy" if is_buy else "sell"
    
    def filter_stablecoin_trades(self, trades):
        """
        過濾穩定幣價格異常的交易，並對同 tx_hash 只保留 price_usd 最接近 1 的那筆。
        支援 SOLANA 和 BSC 鏈的穩定幣。
        """
        # SOLANA 穩定幣
        sol_stablecoins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERk6LsGkZ7zJ2E4zi6tLQ6t5uCwFctb9d"   # USDT
        }
        
        # BSC 穩定幣（大小寫不敏感）
        bsc_stablecoins = {
            "0x55d398326f99059fF775485246999027B3197955",  # USDT
            "0x55d398326f99059ff775485246999027b3197955",  # USDT (小寫)
            "0x8AC76a51cc950d9822D68b83fE1Ad97B32Cd580d",  # USDC
            "0x8ac76a51cc950d9822d68b83fe1ad97b32cd580d",  # USDC (小寫)
            "0xe9e7CEA3DedcA5984780Bafc599bD69ADd087D56",  # BUSD
            "0xe9e7cea3dedca5984780bafc599bd69add087d56",  # BUSD (小寫)
            "0x8d0D000Ee44948FC98c9B98A4FA4921476f08B0d",  # USD1
            "0x8d0d000ee44948fc98c9b98a4fa4921476f08b0d",  # USD1 (小寫)
            "0x1AF3F329e8BE154074D8769D1FFa4eE058B1DBc3",  # DAI
            "0x1af3f329e8be154074d8769d1ffa4ee058b1dbc3",  # DAI (小寫)
            "0x14016E85a25aeb13065688cAFB43044C2ef86784",  # TUSD
            "0x14016e85a25aeb13065688cafb43044c2ef86784",  # TUSD (小寫)
            "0x90C97F71E18723b0Cf0dfa30ee176Ab653E89F40",  # FRAX
            "0x90c97f71e18723b0cf0dfa30ee176ab653e89f40",  # FRAX (小寫)
            "0xd17479997F34cC09Bc8D90EaE5A2954b8C89c0b5",  # USDD
            "0xd17479997f34cc09bc8d90eae5a2954b8c89c0b5"   # USDD (小寫)
        }
        
        # 合併所有穩定幣地址
        stablecoins = sol_stablecoins | bsc_stablecoins
        
        tx_hash_map = {}
        for t in trades:
            if t['token_address'] in stablecoins:
                try:
                    price_usd = float(t.get('price_usd', 0))
                except Exception:
                    price_usd = 0
                # 只保留價格在合理範圍的
                if not (0.95 <= price_usd <= 1.05):
                    continue
                key = t['tx_hash']
                if key in tx_hash_map:
                    prev = tx_hash_map[key]
                    prev_price = 0
                    try:
                        prev_price = float(prev.get('price_usd', 0))
                    except Exception:
                        pass
                    if abs(price_usd - 1) < abs(prev_price - 1):
                        tx_hash_map[key] = t
                else:
                    tx_hash_map[key] = t
            else:
                # 非穩定幣直接保留（tx_hash+token_address 保證唯一）
                tx_hash_map[t['tx_hash'] + str(t['token_address'])] = t
        return list(tx_hash_map.values())

    def get_main_token(self, token_in, token_out, side, chain_id=None):
        # 支援 BSC 主幣/穩定幣
        bsc_main_tokens = set(addr.lower() for addr in BNB_ADDRESSES + BSC_STABLECOIN_ADDRESSES)
        sol_main_tokens = SOL_TOKENS | STABLECOINS

        def is_bsc_token(addr):
            return (addr or '').lower() in bsc_main_tokens

        def is_sol_token(addr):
            return addr in sol_main_tokens

        # 根據 chain_id 決定主幣集合
        if chain_id in (56, 9006):
            is_main_token = is_bsc_token
        else:
            is_main_token = is_sol_token

        # 若有 memecoin 參與，token_address 一定是 memecoin
        if not is_main_token(token_in) and is_main_token(token_out):
            return token_in  # 賣出 memecoin
        if not is_main_token(token_out) and is_main_token(token_in):
            return token_out  # 買入 memecoin
        # 兩邊都是主幣/穩定幣，維持原邏輯
        return token_out if (side == 0 or side == 'buy') else token_in

    async def convert_trades_to_transactions(self, trades: List[Dict[str, Any]], wallet_address: str, chain_id: int = 501, wallet_balance: float = None, wallet_balance_usd: float = None) -> List[Dict[str, Any]]:
        db_chain_id = 9006 if chain_id in (56, 9006) else chain_id
        if not trades:
            return []
        
        # 過濾穩定幣交易
        trades = self.filter_stablecoin_trades(trades)
        
        try:
            # logger.info(f"開始轉換 {wallet_address} 的 {len(trades)} 筆交易紀錄為 Transaction 格式")
            transaction_rows = []
            
            # 餘額處理
            if wallet_balance is None:
                if db_chain_id == 9006:
                    wallet_balance = get_bsc_balance(wallet_address)
                else:
                    wallet_balance = await self._get_wallet_balance_cached(wallet_address)
            
            if wallet_balance_usd is None:
                if db_chain_id == 9006:
                    bnb_price = await get_bnb_price()
                    wallet_balance_usd = float(wallet_balance) * float(bnb_price)
                else:
                    wallet_balance_usd = float(wallet_balance)
            else:
                wallet_balance_usd = float(wallet_balance_usd)
            
            # 收集所有代幣地址並獲取代幣信息
            token_addresses = set()
            for trade in trades:
                token_addresses.add(trade['token_in'])
                token_addresses.add(trade['token_out'])
            
            # 統一從 tokens 表獲取代幣信息
            token_info_map = await self.get_multiple_token_info(list(token_addresses))
            logger.info(f"token info 批量查詢完成，開始處理交易")
            
            # 主幣價格快取
            bnb_price = None
            sol_price = None
            if db_chain_id == 9006:
                bnb_price = await get_bnb_price()
            elif db_chain_id == 501:
                sol_info = self.get_sol_info("So11111111111111111111111111111111111111112")
                sol_price = Decimal(str(sol_info.get("priceUsd", 0)))
            
            # 按時間戳排序交易
            sorted_trades = sorted(trades, key=lambda x: x['timestamp'])
            
            # 持倉追蹤map：{token_address: {'amount': Decimal, 'cost': Decimal}}
            token_holding_map = {}
            processed_signatures = set()
            
            for trade in sorted_trades:
                try:
                    # 避免重複處理
                    if trade['tx_hash'] in processed_signatures:
                        continue
                    
                    from_token_address = trade['token_in']
                    dest_token_address = trade['token_out']

                    # logger.info(f"[DEBUG] trade: tx_hash={trade['tx_hash']} side={trade['side']} token_in={trade['token_in']} token_out={trade['token_out']} amount_in={trade['amount_in']} amount_out={trade['amount_out']} price_usd={trade.get('price_usd')}")

                    main_token = self.get_main_token(trade['token_in'], trade['token_out'], trade['side'], chain_id=db_chain_id)
                    # logger.info(f"[DEBUG] main_token={main_token} (get_main_token結果)")

                    # --- 修正 side 判斷 ---
                    side = trade['side']
                    is_buy = (side == 0 or side == 'buy')
                    is_sell = (side == 1 or side == 'sell')

                    if is_buy:
                        amount = Decimal(str(trade['amount_out'])) / Decimal(10 ** trade['decimals_out'])
                        decimals = trade['decimals_out']
                        token_address = trade['token_out']
                        from_token_amount = Decimal(str(trade['amount_in'])) / Decimal(10 ** trade['decimals_in'])
                        dest_token_amount = Decimal(str(trade['amount_out'])) / Decimal(10 ** trade['decimals_out'])
                    elif is_sell:
                        amount = Decimal(str(trade['amount_in'])) / Decimal(10 ** trade['decimals_in'])
                        decimals = trade['decimals_in']
                        token_address = trade['token_in']
                        from_token_amount = Decimal(str(trade['amount_in'])) / Decimal(10 ** trade['decimals_in'])
                        dest_token_amount = Decimal(str(trade['amount_out'])) / Decimal(10 ** trade['decimals_out'])
                    else:
                        logger.error(f"[ERROR] 未知 side: {side}，跳過本交易: {trade}")
                        continue

                    price_usd = Decimal(str(trade['price_usd']))
                    value = amount * price_usd

                    # --- 修正 symbol 取值防止 None ---
                    from_token_info = self._get_common_token_info(from_token_address) or {}
                    from_token_symbol = from_token_info.get('symbol', '')
                    dest_token_info = self._get_common_token_info(dest_token_address) or {}
                    dest_token_symbol = dest_token_info.get('symbol', '')

                    # 取得處理前的持倉與成本
                    prev_holding = token_holding_map.get(token_address, {'amount': Decimal('0'), 'cost': Decimal('0')})
                    prev_amount = prev_holding['amount']
                    prev_cost = prev_holding['cost']
                    avg_buy_price = prev_cost / prev_amount if prev_amount > 0 else Decimal('0')

                    # logger.info(f"[DEBUG] token={token_address} prev_amount={prev_amount} prev_cost={prev_cost} avg_buy_price={avg_buy_price}")

                    realized_profit = Decimal('0')
                    realized_profit_percentage = Decimal('0')
                    holding_percentage = Decimal('0')

                    if is_buy:
                        if prev_amount == 0:
                            transaction_type = 'build'
                        else:
                            transaction_type = 'buy'
                        new_amount = prev_amount + amount
                        new_cost = prev_cost + value
                        realized_profit = Decimal('0')
                        realized_profit_percentage = Decimal('0')
                    elif is_sell:
                        if prev_amount > 0:
                            holding_percentage = min((amount / prev_amount) * Decimal('100'), Decimal('100'))
                        after_amount = prev_amount - amount
                        if prev_amount > 0:
                            realized_profit = (price_usd - avg_buy_price) * amount
                            if avg_buy_price > 0:
                                realized_profit_percentage = ((price_usd / avg_buy_price - 1) * 100)
                                realized_profit_percentage = max(realized_profit_percentage, Decimal('-100'))
                            else:
                                realized_profit_percentage = Decimal('0')
                        else:
                            realized_profit = Decimal('0')
                            realized_profit_percentage = Decimal('0')
                        if after_amount <= 0:
                            transaction_type = 'clean'
                            new_amount = Decimal('0')
                            new_cost = Decimal('0')
                        else:
                            transaction_type = 'sell'
                            # 防止持倉數量變為負值
                            new_amount = max(Decimal('0'), after_amount)
                            new_cost = prev_cost * (new_amount / prev_amount) if prev_amount > 0 else Decimal('0')

                    # logger.info(f"[DEBUG] token={token_address} new_amount={new_amount} new_cost={new_cost} transaction_type={transaction_type} realized_profit={realized_profit} realized_profit_percentage={realized_profit_percentage}")
                    # if transaction_type == 'clean':
                    #     logger.warning(f"[CLEAN事件] token={token_address} tx_hash={trade['tx_hash']} prev_amount={prev_amount} amount={amount} after_amount={after_amount if is_sell else 'N/A'}")

                    token_holding_map[token_address] = {'amount': new_amount, 'cost': new_cost}
                    
                    # 處理時間戳
                    ts = int(trade['timestamp']) if trade['timestamp'] else int(time.time())
                    if ts > 1e12:
                        ts = int(ts / 1000)
                    
                    # 計算 marketcap - 安全地處理 token_info
                    marketcap = Decimal('0')
                    token_info = token_info_map.get(token_address)
                    if token_info:
                        try:
                            supply = Decimal(str(token_info.get('supply', 0)))
                            marketcap = price_usd * supply if supply > 0 else Decimal('0')
                            # 過濾異常大的 marketcap
                            if marketcap > Decimal('10000000000'):
                                marketcap = Decimal('0')
                        except Exception as e:
                            logger.warning(f"計算 marketcap 失敗: {e}")
                            marketcap = Decimal('0')
                    
                    # 安全地獲取 token 信息
                    token_icon = token_info.get('icon', '') if token_info else ''
                    token_name = token_info.get('symbol', '') or token_info.get('name', '') if token_info else ''
                    
                    # 構建交易記錄
                    tx_data = {
                        'wallet_address': wallet_address,
                        'wallet_balance': float(wallet_balance) if wallet_balance else 0.0,
                        'wallet_balance_usd': wallet_balance_usd,
                        'token_address': token_address,
                        'token_icon': token_icon,
                        'token_name': token_name,
                        'price': float(price_usd),
                        'amount': float(amount),
                        'marketcap': float(marketcap),
                        'value': float(value),
                        'holding_percentage': float(holding_percentage),
                        'chain': self.get_chain_name(db_chain_id),
                        'chain_id': db_chain_id,
                        'realized_profit': float(realized_profit),
                        'realized_profit_percentage': float(realized_profit_percentage),
                        'transaction_type': transaction_type,
                        'transaction_time': ts,
                        'time': now_utc8().replace(tzinfo=None),
                        'signature': trade['tx_hash'],
                        'from_token_address': from_token_address,
                        'from_token_symbol': from_token_symbol,
                        'from_token_amount': float(from_token_amount),
                        'dest_token_address': dest_token_address,
                        'dest_token_symbol': dest_token_symbol,
                        'dest_token_amount': float(dest_token_amount),
                    }
                    
                    transaction_rows.append(tx_data)
                    processed_signatures.add(trade['tx_hash'])
                    
                    # 更新錢包代幣狀態緩存
                    self._update_wallet_token_state_after_transaction(
                        wallet_address, token_address, transaction_type,
                        float(amount), float(value), float(price_usd), ts
                    )
                    
                except Exception as e:
                    logger.error(f"處理交易時發生錯誤: {str(e)}")
                    logger.info(f"問題交易數據: {trade}")
                    continue
            
            logger.info(f"錢包 {wallet_address} 交易轉換完成，共 {len(transaction_rows)} 筆")
            return transaction_rows
            
        except Exception as e:
            logger.error(f"轉換交易記錄失敗: {str(e)}")
            return []
    
    def _convert_to_float(self, value) -> float:
        """安全地轉換為float類型"""
        try:
            if value is None:
                return 0.0
            return float(value)
        except (ValueError, TypeError):
            return 0.0
    
    def get_chain_name(self, chain_id: int) -> str:
        """根據chain_id獲取鏈名稱"""
        chain_map = {
            501: "SOLANA",
            9006: "BSC",
            1: "ETH",
            137: "Polygon"
        }
        return chain_map.get(chain_id, "Unknown")
    
    def _update_wallet_token_state_after_transaction(self, wallet_address, token_address, transaction_type, amount, value, price, timestamp):
        """更新錢包代幣狀態緩存"""
        cache_key = f"{wallet_address}:{token_address}"
        
        # 獲取當前狀態，如果不存在則創建預設狀態
        if cache_key not in self.wallet_token_state_cache:
            self.wallet_token_state_cache[cache_key] = self._get_default_wallet_token_state()
        
        state = self.wallet_token_state_cache[cache_key]
        current_amount = state["current_amount"]
        current_cost = state["current_total_cost"]
        current_avg_buy_price = state["current_avg_buy_price"]
        position_opened_at = state["position_opened_at"]
        historical_buy_amount = state["historical_buy_amount"]
        historical_sell_amount = state["historical_sell_amount"]
        historical_buy_cost = state["historical_buy_cost"]
        historical_sell_value = state["historical_sell_value"]
        historical_realized_pnl = state["historical_realized_pnl"]
        historical_buy_count = state["historical_buy_count"]
        historical_sell_count = state["historical_sell_count"]

        if transaction_type in ["buy"]:
            # 買入交易
            if position_opened_at is None:
                # 建倉時設置首次建倉時間
                position_opened_at = timestamp
            
            # 更新當前持倉
            current_amount += amount
            current_cost += value
            current_avg_buy_price = current_cost / current_amount if current_amount > 0 else 0
            
            # 更新歷史數據
            historical_buy_amount += amount
            historical_buy_cost += value
            historical_buy_count += 1
            
        elif transaction_type in ["sell"]:
            # 賣出交易
            if current_amount > 0:
                # 計算賣出比例
                sell_ratio = min(1.0, amount / current_amount)
                
                # 計算對應的成本
                cost_basis = current_cost * sell_ratio
                
                # 計算已實現盈虧
                realized_pnl = value - cost_basis
                historical_realized_pnl += realized_pnl
                
                # 更新當前持倉
                current_amount -= amount
                if current_amount <= 0:
                    # 完全清倉
                    current_amount = 0
                    current_cost = 0
                    current_avg_buy_price = 0
                    position_opened_at = None
                else:
                    # 部分減倉
                    current_cost -= cost_basis
                    current_avg_buy_price = current_cost / current_amount if current_amount > 0 else 0
                
                # 防止持倉數量變為負值
                current_amount = max(0, current_amount)
            
            # 更新歷史數據
            historical_sell_amount += amount
            historical_sell_value += value
            historical_sell_count += 1

        # 更新最後交易時間
        state["last_transaction_time"] = timestamp
        
        # 更新狀態數據
        state["current_amount"] = current_amount
        state["current_total_cost"] = current_cost
        state["current_avg_buy_price"] = current_avg_buy_price
        state["position_opened_at"] = position_opened_at
        state["historical_buy_amount"] = historical_buy_amount
        state["historical_sell_amount"] = historical_sell_amount
        state["historical_buy_cost"] = historical_buy_cost
        state["historical_sell_value"] = historical_sell_value
        state["historical_realized_pnl"] = historical_realized_pnl
        state["historical_buy_count"] = historical_buy_count
        state["historical_sell_count"] = historical_sell_count
        
        # 更新緩存
        self.wallet_token_state_cache[cache_key] = state
    
    def _get_default_wallet_token_state(self) -> Dict[str, Any]:
        """獲取預設的錢包代幣狀態"""
        return {
            "current_amount": 0.0,
            "current_total_cost": 0.0,
            "current_avg_buy_price": 0.0,
            "position_opened_at": None,
            "historical_buy_amount": 0.0,
            "historical_sell_amount": 0.0,
            "historical_buy_cost": 0.0,
            "historical_sell_value": 0.0,
            "historical_realized_pnl": 0.0,
            "historical_buy_count": 0,
            "historical_sell_count": 0,
            "last_transaction_time": 0
        }
    
    async def save_transactions_batch(self, transactions: List[Dict[str, Any]]) -> bool:
        """批量保存交易記錄"""
        # logger.info(f"[進入] save_transactions_batch 方法，交易記錄數量: {len(transactions) if transactions else 0}")
        
        if not transactions:
            logger.info("[退出] 交易記錄為空，直接返回 False")
            return False
        
        try:
            # 動態獲取表名，支援臨時表切換
            from models import Transaction
            table_name = Transaction.__tablename__
            
            # logger.info(f"[寫入] save_transactions_batch 將寫入 {table_name}，第一筆: {transactions[0] if transactions else None}")
        except Exception as e:
            logger.error(f"[錯誤] 導入 Transaction 模型失敗: {str(e)}")
            logger.error(f"完整錯誤堆疊: {traceback.format_exc()}")
            return False
        
        # 減少批量大小到 500，因為每條記錄大約需要 24 個參數
        batch_size = 500
        total_transactions = len(transactions)
        success_count = 0
        
        # logger.info(f"嘗試批量保存 {len(transactions)} 筆交易到 {table_name}")
        
        try:
            async with self.trade_fetcher.SmartMoney_session_factory() as session:
                await session.execute(text("SET search_path TO dex_query_v1;"))
                
                for i in range(0, total_transactions, batch_size):
                    batch = transactions[i:i + batch_size]
                    batch_num = i//batch_size + 1
                    # logger.info(f"[寫入] 批次 {batch_num} 寫入 {table_name}: {i} ~ {i + len(batch)} 筆")
                    
                    try:
                        # 調試：檢查第一筆數據的格式
                        if batch and len(batch) > 0:
                            first_record = batch[0]
                            # logger.info(f"[調試] 第一筆數據格式檢查: {first_record}")
                            # logger.info(f"[調試] time 欄位類型: {type(first_record.get('time'))}, 值: {first_record.get('time')}")
                        
                        # 動態構建插入語句，使用正確的表名
                        insert_stmt = text(f"""
                            INSERT INTO dex_query_v1.{table_name} (
                                wallet_address, wallet_balance, wallet_balance_usd, token_address, token_icon, token_name,
                                price, amount, marketcap, value, holding_percentage, chain, chain_id,
                                realized_profit, realized_profit_percentage, transaction_type,
                                transaction_time, time, signature, from_token_address, from_token_symbol,
                                from_token_amount, dest_token_address, dest_token_symbol, dest_token_amount
                            ) VALUES (
                                :wallet_address, :wallet_balance, :wallet_balance_usd, :token_address, :token_icon, :token_name,
                                :price, :amount, :marketcap, :value, :holding_percentage, :chain, :chain_id,
                                :realized_profit, :realized_profit_percentage, :transaction_type,
                                :transaction_time, :time, :signature, :from_token_address, :from_token_symbol,
                                :from_token_amount, :dest_token_address, :dest_token_symbol, :dest_token_amount
                            )
                            ON CONFLICT (signature, wallet_address, token_address, transaction_time) DO UPDATE SET
                                wallet_balance = EXCLUDED.wallet_balance,
                                wallet_balance_usd = EXCLUDED.wallet_balance_usd,
                                token_icon = EXCLUDED.token_icon,
                                token_name = EXCLUDED.token_name,
                                price = EXCLUDED.price,
                                amount = EXCLUDED.amount,
                                marketcap = EXCLUDED.marketcap,
                                value = EXCLUDED.value,
                                holding_percentage = EXCLUDED.holding_percentage,
                                chain = EXCLUDED.chain,
                                chain_id = EXCLUDED.chain_id,
                                realized_profit = EXCLUDED.realized_profit,
                                realized_profit_percentage = EXCLUDED.realized_profit_percentage,
                                transaction_type = EXCLUDED.transaction_type,
                                time = EXCLUDED.time,
                                from_token_address = EXCLUDED.from_token_address,
                                from_token_symbol = EXCLUDED.from_token_symbol,
                                from_token_amount = EXCLUDED.from_token_amount,
                                dest_token_address = EXCLUDED.dest_token_address,
                                dest_token_symbol = EXCLUDED.dest_token_symbol,
                                dest_token_amount = EXCLUDED.dest_token_amount
                        """)
                        
                        # 執行批量插入
                        await session.execute(insert_stmt, batch)
                        await session.commit()
                        success_count += len(batch)
                        # logger.info(f"[成功] 批次 {batch_num} 寫入 {table_name} 成功: {len(batch)} 筆")
                        
                    except Exception as e:
                        logger.error(f"[錯誤] 批次 {batch_num} 處理失敗 ({i} ~ {i + len(batch)}): {str(e)}")
                        logger.error(f"目標表: {table_name}")
                        logger.error(f"錯誤詳情: {traceback.format_exc()}")
                        await session.rollback()
                        # 不要 continue，讓錯誤繼續傳播
                        raise
                
            success_rate = success_count / total_transactions if total_transactions > 0 else 0
            logger.info(f"[完成] 交易記錄保存完成。目標表: {table_name}, 成功率: {success_rate:.2%} ({success_count}/{total_transactions})")
            
            return success_count > 0
            
        except Exception as e:
            logger.error(f"[錯誤] 保存交易記錄時發生錯誤: {str(e)}")
            logger.error(f"目標表: {table_name}")
            logger.error(f"完整錯誤堆疊: {traceback.format_exc()}")
            return False
    
    async def crawl_all_pages(self, chain_id: int = 501, page_size: int = 20, fetch_trades: bool = True) -> List[Dict]:
        """
        爬取所有頁面的數據
        
        Args:
            chain_id: 區塊鏈ID
            page_size: 每頁數據量
            fetch_trades: 是否同時查詢交易記錄
            
        Returns:
            所有錢包數據的列表
        """
        all_wallets = []
        rank_start = 0
        rank_end = page_size
        
        async with self:
            while True:
                # 添加延遲避免請求過於頻繁
                await asyncio.sleep(1)
                
                # 獲取當前頁面數據
                page_data = await self.fetch_page(rank_start, rank_end, chain_id)
                
                if not page_data:
                    logger.warning(f"頁面數據為空，停止爬取: rank_start={rank_start}")
                    break
                
                ranking_infos = page_data.get('rankingInfos', [])
                
                # 如果沒有數據，說明已經到最後一頁
                if not ranking_infos:
                    logger.info(f"沒有更多數據，爬取完成: rank_start={rank_start}")
                    break
                
                # 提取錢包數據
                wallets = self.extract_wallet_data(ranking_infos, chain_id)
                
                # 如果需要查詢交易記錄
                if fetch_trades:
                    for wallet in wallets:
                        wallet_address = wallet['wallet_address']
                        logger.info(f"正在查詢錢包 {wallet_address} 的交易記錄...")
                        
                        # 查詢交易記錄
                        trades = await self.trade_fetcher.fetch_wallet_trades(
                            wallet_address, 
                            chain_id=chain_id,
                            days=30
                        )
                        
                        # 添加交易記錄到錢包數據中
                        wallet['trades'] = trades
                        wallet['trade_count'] = len(trades)
                        
                        # 計算一些交易統計
                        if trades:
                            buy_trades = [t for t in trades if t['side'] == 'buy']
                            sell_trades = [t for t in trades if t['side'] == 'sell']
                            
                            wallet['db_buy_count'] = len(buy_trades)
                            wallet['db_sell_count'] = len(sell_trades)
                            wallet['db_total_volume'] = sum(float(t.get('price_usd', 0)) for t in trades)
                        else:
                            wallet['db_buy_count'] = 0
                            wallet['db_sell_count'] = 0
                            wallet['db_total_volume'] = 0
                        
                        # 處理錢包活動並保存到數據庫
                        if trades:
                            await self.process_wallet_activities(wallet_address, chain_id, activities=trades, twitter_name=wallet.get('twitter_name'), twitter_username=wallet.get('twitter_username'))
                
                all_wallets.extend(wallets)
                
                logger.info(f"成功爬取 {len(wallets)} 個錢包，累計 {len(all_wallets)} 個")
                
                # 更新下一頁的參數
                rank_start = rank_end
                rank_end += page_size
        
        logger.info(f"爬取完成，總共獲取 {len(all_wallets)} 個錢包數據")
        return all_wallets
    
    async def crawl_single_page(self, rank_start: int, rank_end: int, chain_id: int = 501, fetch_trades: bool = True) -> List[Dict]:
        """
        爬取單頁數據
        
        Args:
            rank_start: 排名起始位置
            rank_end: 排名結束位置
            chain_id: 區塊鏈ID
            fetch_trades: 是否同時查詢交易記錄
            
        Returns:
            錢包數據列表
        """
        async with self:
            page_data = await self.fetch_page(rank_start, rank_end, chain_id)
            
            if not page_data:
                return []
            
            ranking_infos = page_data.get('rankingInfos', [])
            wallets = self.extract_wallet_data(ranking_infos, chain_id)
            
            # 如果需要查詢交易記錄
            if fetch_trades:
                for wallet in wallets:
                    wallet_address = wallet['wallet_address']
                    logger.info(f"正在查詢錢包 {wallet_address} 的交易記錄...")
                    
                    # 查詢交易記錄
                    trades = await self.trade_fetcher.fetch_wallet_trades(
                        wallet_address, 
                        chain_id=chain_id,
                        days=30
                    )
                    
                    # 添加交易記錄到錢包數據中
                    wallet['trades'] = trades
                    wallet['trade_count'] = len(trades)
                    
                    # 計算一些交易統計
                    if trades:
                        buy_trades = [t for t in trades if t['side'] == 'buy']
                        sell_trades = [t for t in trades if t['side'] == 'sell']
                        
                        wallet['db_buy_count'] = len(buy_trades)
                        wallet['db_sell_count'] = len(sell_trades)
                        wallet['db_total_volume'] = sum(float(t.get('price_usd', 0)) for t in trades)
                    else:
                        wallet['db_buy_count'] = 0
                        wallet['db_sell_count'] = 0
                        wallet['db_total_volume'] = 0
                    
                    # 處理錢包活動並保存到數據庫
                    if trades:
                        await self.process_wallet_activities(wallet_address, chain_id, activities=trades, twitter_name=wallet.get('twitter_name'), twitter_username=wallet.get('twitter_username'))
            
            return wallets

    def get_wallet_token_state(self, wallet_address, token_address):
        cache_key = f"{wallet_address}:{token_address}"
        if cache_key not in self.wallet_token_state_cache:
            self.wallet_token_state_cache[cache_key] = self._get_default_wallet_token_state()
        return self.wallet_token_state_cache[cache_key]
    
    def get_sol_info(self, token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        import time
        # 快取邏輯：只查一次，之後都用快取
        if self.sol_price_cache is not None:
            return self.sol_price_cache
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    result = {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
                    self.sol_price_cache = result
                    self.sol_price_cache_time = time.time()
                    return result
        except Exception as e:
            logger.warning(f"查詢 SOL 價格失敗: {e}")
            return {"priceUsd": 155.53}
        logger.warning("查詢 SOL 價格失敗，使用預設值")
        return {"priceUsd": 155.53}

    async def get_multiple_token_info(self, token_addresses: list) -> dict:
        """
        批量查詢多個 token_address 的資訊，統一從 tokens 表查詢並快取到 dict。
        支援 BSC 地址的大小寫處理。
        """
        from datetime import datetime, timedelta
        now = datetime.now()
        
        # 初始化結果字典
        result = {}
        to_query = []
        
        # 處理每個地址
        for addr in token_addresses:
            if not addr:
                continue
                
            # 先從常見 token 直接給
            common = self._get_common_token_info(addr)
            if common:
                result[addr] = common
                continue
            
            # 檢查緩存
            if addr in self.token_info_cache and self.token_info_expiry[addr] > now:
                result[addr] = self.token_info_cache[addr]
                continue
            
            # 需要查詢的地址
            to_query.append(addr)
        
        # 如果沒有需要查詢的地址，直接返回
        if not to_query:
            return result
        
        # 批量查詢 tokens 表 - 優化查詢效率
        try:
            async with self.trade_fetcher.Ian_session_factory() as session:
                await session.execute(text("SET search_path TO dex_query_v1;"))
                
                # 分離 BSC 和 SOLANA 地址，使用更高效的查詢方式
                bsc_addresses = []
                solana_addresses = []
                
                for addr in to_query:
                    if addr and len(addr) == 42 and addr.startswith('0x'):
                        bsc_addresses.append(addr)
                    else:
                        solana_addresses.append(addr)
                
                # 執行查詢
                all_rows = []
                
                # 查詢 BSC 地址（使用 IN 子句）
                if bsc_addresses:
                    # 構建大小寫不敏感的查詢
                    bsc_conditions = []
                    for addr in bsc_addresses:
                        bsc_conditions.append(f"LOWER(address) = LOWER('{addr}')")
                    
                    bsc_sql = text(f"""
                        SELECT address, name, symbol, decimals, logo, supply
                        FROM dex_query_v1.tokens 
                        WHERE {' OR '.join(bsc_conditions)}
                    """)
                    
                    bsc_rows = await session.execute(bsc_sql)
                    all_rows.extend(bsc_rows)
                
                # 查詢 SOLANA 地址（使用 IN 子句）
                if solana_addresses:
                    solana_sql = text(f"""
                        SELECT address, name, symbol, decimals, logo, supply
                        FROM dex_query_v1.tokens 
                        WHERE address = ANY(:addresses)
                    """)
                    
                    solana_rows = await session.execute(solana_sql, {"addresses": solana_addresses})
                    all_rows.extend(solana_rows)
                
                # 處理查詢結果
                for row in all_rows:
                    info = {
                        "address": row.address,
                        "name": row.name or '',
                        "symbol": row.symbol or '',
                        "decimals": row.decimals or 9,
                        "icon": row.logo or '',
                        "supply": row.supply or 0
                    }
                    
                    # 更新緩存
                    self.token_info_cache[row.address] = info
                    self.token_info_expiry[row.address] = now + timedelta(seconds=self.token_cache_ttl)
                    
                    # 添加到結果中
                    result[row.address] = info
                    
                    # 對於 BSC 地址，同時緩存小寫版本
                    if row.address and len(row.address) == 42 and row.address.startswith('0x'):
                        lower_addr = row.address.lower()
                        if lower_addr != row.address:
                            self.token_info_cache[lower_addr] = info
                            self.token_info_expiry[lower_addr] = now + timedelta(seconds=self.token_cache_ttl)
                            result[lower_addr] = info
                
        except Exception as e:
            logger.error(f"批量查詢代幣信息失敗: {e}")
        
        # 為沒有查詢到的地址設置空值
        for addr in to_query:
            if addr not in result:
                result[addr] = None
                # 同時設置空值到緩存，避免重複查詢
                self.token_info_cache[addr] = None
                self.token_info_expiry[addr] = now + timedelta(seconds=self.token_cache_ttl)
        
        return result

async def main():
    """自動依序爬多條鏈（BSC、SOLANA）並合併tmp表"""
    for chain in ["SOLANA", "BSC"]:
        session_factory = sessions.get(chain)
        if session_factory:
            async with session_factory() as session:
                await ensure_tmp_tables(session)
    crawler = OKXCrawler()
    print("開始爬取 BSC 鏈...")
    await crawler.crawl_all_pages(chain_id=56, page_size=20, fetch_trades=True)
    print("BSC 鏈爬取完成，開始爬取 SOLANA 鏈...")
    await crawler.crawl_all_pages(chain_id=501, page_size=20, fetch_trades=True)
    print("SOLANA 鏈爬取完成！")
    
    # 開始合併tmp表到主表
    print("\n==== 開始合併tmp表到主表 ====")
    from merge_tmp import merge_all_chains
    await merge_all_chains()
    print("==== 所有流程完成！====\n")

# 主程式啟動時log ORM表名
# logging.info(f"[啟動] USE_TMP_TABLE={USE_TMP_TABLE}")
# logging.info(f"[啟動] WalletSummary.__tablename__ = {WalletSummary.__tablename__}")
# logging.info(f"[啟動] Transaction.__tablename__ = {Transaction.__tablename__}")
# logging.info(f"[啟動] Holding.__tablename__ = {Holding.__tablename__}")
# logging.info(f"[啟動] TokenBuyData.__tablename__ = {TokenBuyData.__tablename__}")
# logging.info(f"[啟動] TokenState.__tablename__ = {TokenState.__tablename__}")

if __name__ == "__main__":
    asyncio.run(main()) 