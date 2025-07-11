import base58
import re
import httpx
import logging
import traceback
from loguru_logger import logger
from datetime import datetime, timedelta, timezone
from solders.pubkey import Pubkey
from sqlalchemy.ext.asyncio import AsyncSession
from token_info import TokenUtils
from models import *
from solana.rpc.async_api import AsyncClient
from config import RPC_URL, RPC_URL_backup, HELIUS_API_KEY
# from smart_wallet_filter import calculate_statistics2
# from WalletHolding import calculate_token_statistics
from collections import defaultdict
from loguru_logger import *
from test_Helius import WalletAnalyzer
import time
from sqlalchemy import text
from sqlalchemy.dialects.postgresql import insert as pg_insert
from models import make_naive_time
from utils_tmp_table import merge_tmp_to_main, clear_tmp_table
from okx_crawler import get_bsc_balance, get_bnb_price
import asyncio

# 配置參數
LIMIT = 100                      # 每次請求的交易數量
DAYS_TO_ANALYZE = 30             # 查詢過去多少天的交易
token_supply_cache = {}

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
WSOL_MINT = "So11111111111111111111111111111111111111112"

async def get_client():
    """
    轮流尝试RPC_URL和RPC_URL_backup，返回一个有效的客户端
    """
    # 尝试使用两个 URL，轮替
    for url in [RPC_URL, RPC_URL_backup]:
        try:
            client = AsyncClient(url)
            # 简单请求RPC服务进行健康检查
            resp = await client.is_connected()
            if resp:
                return client
            else:
                logging.warning(f"RPC连接失败，尝试下一个 URL: {url}")
        except Exception as e:
            logging.warning(f"请求RPC URL {url}时发生错误: {e}")
            continue

    logging.error("所有RPC URL都不可用")
    raise Exception("无法连接到任何 RPC URL")

async def get_token_supply(client, mint: str) -> float:
    if mint in token_supply_cache:
        return token_supply_cache[mint]

    try:
        # 檢查是否為 Solana 地址格式（Base58）
        if len(mint) == 44 and all(c in '123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz' for c in mint):
            supply_data = await client.get_token_supply(Pubkey(base58.b58decode(mint)))
            supply = int(supply_data.value.amount) / (10 ** supply_data.value.decimals)
            token_supply_cache[mint] = supply
            return supply
        else:
            # BSC 或其他鏈的代幣，返回預設值
            return 1000000000
    except Exception:
        return 1000000000
    
async def process_transactions_concurrently(all_transactions, address, async_session, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain):
    # 創建異步任務列表
    wallet_transactions = defaultdict(list)
    
    # 順序處理交易，保持原子性
    for tx in all_transactions:
        if tx["type"] != "SWAP":
            continue
        
        try:
            # 在一個事務中處理每筆交易
            async with async_session.begin():
                tasks = []
                for tx in all_transactions:
                    if tx["type"] == "SWAP":
                        tasks.append(analyze_swap_transaction(
                            tx, address, async_session, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain
                        ))
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # 安全地檢查和處理交易數據
                if not results:
                    continue
                
                token_address = results.get("token_address", "")

                if not token_address or token_address == "So11111111111111111111111111111111111111112":
                    continue
                
                transaction_record = {
                    "buy_amount": results.get("amount", 0) if results.get("transaction_type") == "buy" else 0,
                    "sell_amount": results.get("amount", 0) if results.get("transaction_type") == "sell" else 0,
                    "cost": results.get("price", 0) * results.get("amount", 0) if results.get("transaction_type") == "buy" else 0,
                    "profit": results.get("realized_profit", 0),
                    "marketcap": results.get("marketcap", 0)
                }
                
                wallet_transactions[address].append({
                    token_address: transaction_record,
                    "timestamp": results["transaction_time"]
                })
        
        except Exception as e:
            print(f"處理交易失敗: {e}")
            # 如果一筆交易失敗，繼續處理下一筆
            continue
    
    return wallet_transactions

@async_log_execution_time
async def fetch_transactions_within_30_days(
        async_session: AsyncSession,
        address: str,
        chain: str,
        api_key: str,
        is_smart_wallet: bool = None,  # 设置为可选参数
        wallet_type: int = None,       # 设置为可选参数
        days: int = 30,
        limit: int = 100        
    ):
    """
    查詢指定錢包過去30天內的所有交易數據（分頁處理），
    最後按照最舊到最新排序後逐一進行分析。
    如果交易記錄超過2000筆，則跳過該錢包。
    """
    client = await get_client()
    
    # 根據鏈類型獲取餘額
    wallet_balance_usdt = 0
    try:
        if chain.upper() == 'BSC':
            # BSC 鏈使用不同的餘額查詢方法
            from okx_crawler import get_bsc_balance, get_bnb_price
            
            # 獲取 BNB 餘額
            bnb_balance = get_bsc_balance(address)
            
            # 獲取 BNB 價格並計算 USD 價值
            bnb_price = await get_bnb_price()
            wallet_balance_usdt = float(bnb_balance) * float(bnb_price)
            
            logger.info(f"[BSC餘額] {address}: {bnb_balance} BNB (${wallet_balance_usdt:.2f} USD)")
        else:
            # SOLANA 鏈使用原有的方法
            sol_balance = await TokenUtils.get_usd_balance(client, address)
            wallet_balance_usdt = sol_balance.get("balance_usd", 0)
            logger.info(f"[SOLANA餘額] {address}: {wallet_balance_usdt:.2f} USD")
    except Exception as e:
        logger.warning(f"獲取錢包餘額失敗: {e}")
        wallet_balance_usdt = 0

    print(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")
    
    try:
        # 直接創建 WalletAnalyzer 並從 API 獲取交易數據
        wallet_analyzer = WalletAnalyzer(address)
        
        # 從 API 獲取交易數據，檢查是否超過2000筆
        transaction_count = await wallet_analyzer.fetch_transactions(api_key)
        
        # 確保transaction_count不為None並且是可比較的數字
        if transaction_count is None:
            transaction_count = 0
            logging.warning(f"錢包 {address} 的交易記錄數量為None，設置為0")
        
        # 如果交易記錄超過2000筆，跳過該錢包
        if transaction_count > 2000:
            print(f"錢包 {address} 的交易記錄超過2000筆 ({transaction_count}筆)，跳過分析...")
            # 記錄到日誌
            logging.warning(f"錢包 {address} 的交易記錄超過2000筆 ({transaction_count}筆)，已跳過分析")
            return False  # 返回False表示處理未完成
        
        # 處理交易數據
        wallet_analyzer.process_transactions()
        
        # 篩選智能錢包
        is_smart_wallet = await wallet_analyzer.filter_smart_money(async_session, client, wallet_type)
        # await wallet_analyzer.save_transactions_to_db(client, async_session, chain, wallet_balance_usdt)

        if is_smart_wallet:
            # 保存交易數據到數據庫
            await reset_wallet_buy_data(address, async_session, chain)
            await wallet_analyzer.save_transactions_to_db(client, async_session, chain, wallet_balance_usdt)
        
        return True  # 返回True表示處理完成
    
    except Exception as e:
        error_msg = f"分析或保存交易數據時出錯：{str(e)}"
        print(error_msg)
        logging.error(f"{error_msg}\n詳細錯誤堆疊：\n{traceback.format_exc()}")
        return False  # 發生錯誤時返回False

@async_log_execution_time
async def fetch_transactions_within_30_days_for_smartmoney(
        async_session: AsyncSession,
        address: str,
        chain: str,
        api_key: str,
        is_smart_wallet: bool = None,  # 设置为可选参数
        wallet_type: int = None,       # 设置为可选参数
        days: int = 30,
        limit: int = 100        
    ):
    """
    查詢指定錢包過去30天內的所有交易數據（分頁處理），
    最後按照最舊到最新排序後逐一進行分析。
    """
    client = await get_client()
    
    # 根據鏈類型獲取餘額
    wallet_balance_usdt = 0
    try:
        if chain.upper() == 'BSC':
            # BSC 鏈使用不同的餘額查詢方法
            from okx_crawler import get_bsc_balance, get_bnb_price
            
            # 獲取 BNB 餘額
            bnb_balance = get_bsc_balance(address)
            
            # 獲取 BNB 價格並計算 USD 價值
            bnb_price = await get_bnb_price()
            wallet_balance_usdt = float(bnb_balance) * float(bnb_price)
            
            logger.info(f"[BSC餘額] {address}: {bnb_balance} BNB (${wallet_balance_usdt:.2f} USD)")
        else:
            # SOLANA 鏈使用原有的方法
            sol_balance = await TokenUtils.get_usd_balance(client, address)
            wallet_balance_usdt = sol_balance.get("balance_usd", 0)
            logger.info(f"[SOLANA餘額] {address}: {wallet_balance_usdt:.2f} USD")
    except Exception as e:
        logger.warning(f"獲取錢包餘額失敗: {e}")
        wallet_balance_usdt = 0

    print(f"正在查詢 {address} 錢包 {days} 天內的交易數據...")
    
    try:
        # 直接創建 WalletAnalyzer 並從 API 獲取交易數據
        wallet_analyzer = WalletAnalyzer(address)
        
        # 從 API 獲取交易數據，檢查是否超過2000筆
        transaction_count = await wallet_analyzer.fetch_transactions(api_key)
        
        # 確保transaction_count不為None並且是可比較的數字
        if transaction_count is None:
            transaction_count = 0
            logging.warning(f"錢包 {address} 的交易記錄數量為None，設置為0")
        
        # 如果交易記錄超過2000筆，跳過該錢包
        if transaction_count > 2000:
            print(f"錢包 {address} 的交易記錄超過2000筆 ({transaction_count}筆)，跳過分析...")
            # 記錄到日誌
            logging.warning(f"錢包 {address} 的交易記錄超過2000筆 ({transaction_count}筆)，已跳過分析")
            return False  # 返回False表示處理未完成
        
        # 處理交易數據
        wallet_analyzer.process_transactions()
        
        # 篩選智能錢包
        is_smart_wallet = await wallet_analyzer.filter_smart_money(async_session, client, wallet_type)
        # await wallet_analyzer.save_transactions_to_db(client, async_session, chain, wallet_balance_usdt)

        if is_smart_wallet:
            # 保存交易數據到數據庫
            await reset_wallet_buy_data(address, async_session, chain)
            await wallet_analyzer.save_transactions_to_db(client, async_session, chain, wallet_balance_usdt)
        
        return True  # 返回True表示處理完成
    
    except Exception as e:
        error_msg = f"分析或保存交易數據時出錯：{str(e)}"
        print(error_msg)
        logging.error(f"{error_msg}\n詳細錯誤堆疊：\n{traceback.format_exc()}")
        return False  # 發生錯誤時返回False

def convert_transaction_format(old_transactions, wallet_address, sol_price_usd):
    """
    將原始交易數據轉換為calculate_statistics2需要的格式
    """
    # 檢查傳入的數據格式
    new_transactions = {wallet_address: []}
    
    # 如果 old_transactions[wallet_address] 已經是列表格式，且內容是字典形式的交易資料
    if isinstance(old_transactions.get(wallet_address), list) and len(old_transactions[wallet_address]) > 0:
        # 檢查第一筆交易是否已經符合我們需要的格式
        first_tx = old_transactions[wallet_address][0]
        
        # 判斷是否已經是正確格式的交易記錄
        if 'token_mint' in first_tx and 'transaction_type' in first_tx and 'token_amount' in first_tx:
            # 資料已經是正確格式，只需確保 transaction_type 是小寫並返回
            for tx in old_transactions[wallet_address]:
                tx_copy = tx.copy()  # 複製交易記錄以避免修改原始數據
                
                # 確保 transaction_type 是小寫
                if 'transaction_type' in tx_copy:
                    tx_copy['transaction_type'] = tx_copy['transaction_type'].lower()
                
                new_transactions[wallet_address].append(tx_copy)
            
            return new_transactions
    
    # 如果還不是正確格式，使用原始轉換邏輯
    try:
        for tx in old_transactions[wallet_address]:
            timestamp = tx.get('timestamp', 0)
            
            # 處理扁平格式的交易記錄 (token_mint 作為主體)
            if 'token_mint' in tx and 'transaction_type' in tx:
                tx_type = tx['transaction_type'].lower()
                
                new_tx = {
                    'token_mint': tx['token_mint'],
                    'timestamp': timestamp,
                    'transaction_type': tx_type,
                    'token_amount': tx.get('token_amount', 0),
                    'value': tx.get('value', 0),
                    'sol_price_usd': sol_price_usd
                }
                
                # 對賣出交易添加 realized_profit
                if tx_type == 'sell' and 'realized_profit' in tx:
                    new_tx['realized_profit'] = tx['realized_profit']
                
                new_transactions[wallet_address].append(new_tx)
                continue
            
            # 處理巢狀格式的交易記錄
            for token_mint, token_data in tx.items():
                if token_mint == 'timestamp' or not isinstance(token_data, dict):
                    continue
                    
                # 檢查 token_data 是否為字典
                if not isinstance(token_data, dict):
                    print(f"跳過非字典類型的 token_data: {token_data} for {token_mint}")
                    continue
                    
                # 處理買入交易
                if token_data.get('buy_amount', 0) > 0:
                    new_tx = {
                        'token_mint': token_mint,
                        'timestamp': timestamp,
                        'transaction_type': 'buy',
                        'token_amount': token_data['buy_amount'],
                        'value': token_data.get('cost', 0),
                        'sol_price_usd': sol_price_usd
                    }
                    new_transactions[wallet_address].append(new_tx)
                    
                # 處理賣出交易
                if token_data.get('sell_amount', 0) > 0:
                    new_tx = {
                        'token_mint': token_mint,
                        'timestamp': timestamp,
                        'transaction_type': 'sell',
                        'token_amount': token_data['sell_amount'],
                        'value': token_data.get('profit', 0),
                        'realized_profit': token_data.get('profit', 0) - (token_data.get('cost', 0) * token_data.get('sell_amount', 0) / token_data.get('buy_amount', 1) if token_data.get('buy_amount', 0) > 0 else 0),
                        'sol_price_usd': sol_price_usd
                    }
                    new_transactions[wallet_address].append(new_tx)
    except Exception as e:
        print(f"轉換交易格式時出錯: {e}")
        import traceback
        traceback.print_exc()
    
    print(f"轉換後的交易數據: {new_transactions}")
    return new_transactions

async def update_smart_money_data(session, address, chain, is_smart_wallet=None, wallet_type=None, days=30, limit=100, twitter_name=None, twitter_username=None):
    """分析錢包過去30天內的所有交易數據並更新錢包信息"""
    client = await get_client()
    # print(f"正在分析 {address} 錢包 {days} 天內的交易數據...")
    try:
        if twitter_name and twitter_username:
            is_kol = True
        else:
            is_kol = False

        # 2. 從 Transaction 表獲取交易記錄
        transactions = await get_transactions_for_wallet(
            session=session, chain=chain, wallet_address=address, days=days
        )

        if not transactions:
            await session.execute(
                update(WalletSummary)
                .where(WalletSummary.wallet_address == address)
                .values(update_time=datetime.now())
            )
            await session.commit()
            print(f"完成錢包 {address} 的數據更新/寫入（無交易記錄，僅更新時間）")
            return

        # 3. 初始化統計數據
        stats = {
            'is_active': True,
            'is_smart_wallet': True,
            'avg_cost_30d': 0,
            'avg_cost_7d': 0,
            'avg_cost_1d': 0,
            'total_transaction_num_30d': 0,
            'total_transaction_num_7d': 0,
            'total_transaction_num_1d': 0,
            'buy_num_30d': 0,  # 包括 build 和 buy
            'buy_num_7d': 0,
            'buy_num_1d': 0,
            'sell_num_30d': 0,  # 包括 clean 和 sell
            'sell_num_7d': 0,
            'sell_num_1d': 0,
            'win_rate_30d': 0,
            'win_rate_7d': 0,
            'win_rate_1d': 0,
            'pnl_30d': 0,
            'pnl_7d': 0,
            'pnl_1d': 0,
            'pnl_percentage_30d': 0,
            'pnl_percentage_7d': 0,
            'pnl_percentage_1d': 0,
            'unrealized_profit_30d': 0,
            'unrealized_profit_7d': 0,
            'unrealized_profit_1d': 0,
            'total_cost_30d': 0,
            'total_cost_7d': 0,
            'total_cost_1d': 0,
            'avg_realized_profit_30d': 0,
            'avg_realized_profit_7d': 0,
            'avg_realized_profit_1d': 0,
            'last_transaction_time': 0
        }

        # 4. 計算時間區間
        now = int(time.time())
        day_ago = now - 24*60*60
        week_ago = now - 7*24*60*60
        month_ago = now - 30*24*60*60

        # 5. 分析交易數據
        profitable_trades_30d = profitable_trades_7d = profitable_trades_1d = 0
        
        # 用於追蹤最近交易的代幣
        recent_tokens = []
        
        for tx in transactions:
            tx_time = tx['transaction_time']
            tx_type = tx['transaction_type'].lower()
            token_address = tx['token_address']
            
            # 更新最後交易時間
            stats['last_transaction_time'] = max(stats['last_transaction_time'], tx_time)
            
            # 追蹤最近交易的代幣（不重複）
            if token_address and token_address not in recent_tokens:
                recent_tokens.append(token_address)
                if len(recent_tokens) > 3:
                    recent_tokens.pop(0)
            
            # 30天數據
            if tx_time > month_ago:
                stats['total_transaction_num_30d'] += 1
                
                if tx_type in ['buy', 'build']:
                    stats['buy_num_30d'] += 1
                    stats['total_cost_30d'] += tx['value']
                
                elif tx_type in ['sell', 'clean']:
                    stats['sell_num_30d'] += 1
                    if (tx.get('realized_profit') or 0) > 0:
                        profitable_trades_30d += 1
                    stats['pnl_30d'] += (tx.get('realized_profit') or 0)
            
            # 7天數據
            if tx_time > week_ago:
                stats['total_transaction_num_7d'] += 1
                
                if tx_type in ['buy', 'build']:
                    stats['buy_num_7d'] += 1
                    stats['total_cost_7d'] += tx['value']
                
                elif tx_type in ['sell', 'clean']:
                    stats['sell_num_7d'] += 1
                    if (tx.get('realized_profit') or 0) > 0:
                        profitable_trades_7d += 1
                    stats['pnl_7d'] += (tx.get('realized_profit') or 0)
            
            # 1天數據
            if tx_time > day_ago:
                stats['total_transaction_num_1d'] += 1
                
                if tx_type in ['buy', 'build']:
                    stats['buy_num_1d'] += 1
                    stats['total_cost_1d'] += tx['value']
                
                elif tx_type in ['sell', 'clean']:
                    stats['sell_num_1d'] += 1
                    if (tx.get('realized_profit') or 0) > 0:
                        profitable_trades_1d += 1
                    stats['pnl_1d'] += (tx.get('realized_profit') or 0)

        # 6. 計算平均值和比率
        if stats['buy_num_30d'] > 0:
            stats['avg_cost_30d'] = stats['total_cost_30d'] / stats['buy_num_30d']
        if stats['buy_num_7d'] > 0:
            stats['avg_cost_7d'] = stats['total_cost_7d'] / stats['buy_num_7d']
        if stats['buy_num_1d'] > 0:
            stats['avg_cost_1d'] = stats['total_cost_1d'] / stats['buy_num_1d']

        # 計算勝率（只考慮賣出和清倉的交易）
        if stats['sell_num_30d'] > 0:
            stats['win_rate_30d'] = (profitable_trades_30d / stats['sell_num_30d']) * 100
        if stats['sell_num_7d'] > 0:
            stats['win_rate_7d'] = (profitable_trades_7d / stats['sell_num_7d']) * 100
        if stats['sell_num_1d'] > 0:
            stats['win_rate_1d'] = (profitable_trades_1d / stats['sell_num_1d']) * 100

        # 計算平均已實現利潤
        if stats['sell_num_30d'] > 0:
            stats['avg_realized_profit_30d'] = stats['pnl_30d'] / stats['sell_num_30d']
        if stats['sell_num_7d'] > 0:
            stats['avg_realized_profit_7d'] = stats['pnl_7d'] / stats['sell_num_7d']
        if stats['sell_num_1d'] > 0:
            stats['avg_realized_profit_1d'] = stats['pnl_1d'] / stats['sell_num_1d']

        # 計算盈虧百分比
        if stats['total_cost_30d'] > 0:
            stats['pnl_percentage_30d'] = (stats['pnl_30d'] / stats['total_cost_30d']) * 100
        if stats['total_cost_7d'] > 0:
            stats['pnl_percentage_7d'] = (stats['pnl_7d'] / stats['total_cost_7d']) * 100
        if stats['total_cost_1d'] > 0:
            stats['pnl_percentage_1d'] = (stats['pnl_1d'] / stats['total_cost_1d']) * 100

        sol_token_info = TokenUtils.get_sol_info('So11111111111111111111111111111111111111112')
        sol_usdt_price = sol_token_info.get("priceUsd", 126)

        # 轉換 transactions 格式，符合 calculate_statistics2 需求
        formatted_transactions = [
            {
                'token_mint': tx['token_address'],
                'transaction_type': tx['transaction_type'],
                'token_amount': tx['amount'],
                "timestamp": tx['transaction_time'],
                "value": tx.get('value', 0),
                "realized_profit": tx.get('realized_profit', 0)
            }
            for tx in transactions
        ]

        # 7. 獲取當前錢包餘額
        current_balance = 0
        current_balance_usd = 0
        try:
            if chain.upper() == 'BSC':
                # BSC 鏈使用不同的餘額查詢方法
                from okx_crawler import get_bsc_balance, get_bnb_price
                import asyncio
                
                # 獲取 BNB 餘額
                current_balance = get_bsc_balance(address)
                
                # 獲取 BNB 價格並計算 USD 價值
                bnb_price = await get_bnb_price()
                current_balance_usd = float(current_balance) * float(bnb_price)
                
                logger.info(f"[BSC餘額] {address}: {current_balance} BNB (${current_balance_usd:.2f} USD)")
            else:
                # SOLANA 鏈使用原有的方法
                balance_info = await TokenUtils.get_usd_balance(client, address)
                current_balance = balance_info['balance']['float']
                current_balance_usd = balance_info['balance_usd']
                logger.info(f"[SOLANA餘額] {address}: {current_balance} SOL (${current_balance_usd:.2f} USD)")
        except Exception as e:
            logger.warning(f"獲取錢包餘額失敗: {e}")
            # 使用預設值
            current_balance = 0
            current_balance_usd = 0

        # 計算每日PNL圖表（pnl_pic_1d, pnl_pic_7d, pnl_pic_30d）
        def calc_pnl_pic(transactions, days, tz_offset=8):
            now = datetime.now(timezone(timedelta(hours=tz_offset)))
            end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_of_range = end_of_day - timedelta(days=days)
            start_ts = int(start_of_range.timestamp())
            end_ts = int(end_of_day.timestamp())

            filtered = [
                tx for tx in transactions
                if start_ts <= tx['transaction_time'] < end_ts
            ]

            daily_pnl = {}
            for tx in filtered:
                tx_type = tx['transaction_type'].lower()
                if tx_type in ['sell', 'clean']:
                    date_str = datetime.fromtimestamp(tx['transaction_time'], timezone(timedelta(hours=tz_offset))).strftime('%Y-%m-%d')
                    daily_pnl[date_str] = daily_pnl.get(date_str, 0) + tx.get('realized_profit', 0)

            date_range = []
            current_date = end_of_day - timedelta(days=1)
            for _ in range(days):
                date_range.append(current_date.strftime('%Y-%m-%d'))
                current_date -= timedelta(days=1)

            filled_daily_pnl = [str(round(daily_pnl.get(date, 0), 2)) for date in date_range]
            return ",".join(filled_daily_pnl)

        pnl_pic_1d = calc_pnl_pic(transactions, 1)
        pnl_pic_7d = calc_pnl_pic(transactions, 7)
        pnl_pic_30d = calc_pnl_pic(transactions, 30)

        # --- Unrealized Profit 快取 ---
        token_price_cache = {}

        async def get_token_price_usd(session, token_address):
            now = time.time()
            # 檢查快取
            if token_address in token_price_cache:
                price, ts = token_price_cache[token_address]
                if now - ts < 3600:  # 1小時內
                    return price
            # 查詢資料庫
            sql = """
                SELECT price_usd FROM dex_query_v1.tokens
                WHERE address = :address
                LIMIT 1
            """
            result = await session.execute(text(sql), {"address": token_address})
            row = result.fetchone()
            price = float(row[0]) if row and row[0] is not None else 0
            token_price_cache[token_address] = (price, now)
            return price

        async def calc_unrealized_profit(transactions, session, days, tz_offset=8):
            now = datetime.now(timezone(timedelta(hours=tz_offset)))
            end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
            start_of_range = end_of_day - timedelta(days=days)
            start_ts = int(start_of_range.timestamp())
            end_ts = int(end_of_day.timestamp())

            # 統計每個代幣的剩餘持倉
            holdings = {}
            for tx in transactions:
                if not (start_ts <= tx['transaction_time'] < end_ts):
                    continue
                token = tx['token_address']
                tx_type = tx['transaction_type'].lower()
                amount = tx['amount']
                if token not in holdings:
                    holdings[token] = 0
                if tx_type in ['buy', 'build']:
                    holdings[token] += amount
                elif tx_type in ['sell', 'clean']:
                    holdings[token] -= amount

            # 計算未實現利潤
            unrealized_profit = 0
            for token, remain in holdings.items():
                if remain > 0:
                    price_usd = await get_token_price_usd(session, token)
                    unrealized_profit += remain * price_usd
            return unrealized_profit

        unrealized_profit_1d = await calc_unrealized_profit(transactions, session, 1)
        unrealized_profit_7d = await calc_unrealized_profit(transactions, session, 7)
        unrealized_profit_30d = await calc_unrealized_profit(transactions, session, 30)

        # --- 收益分布計算 ---
        def get_distribution(trades, days):
            now = int(time.time())
            since = now - days * 24 * 60 * 60
            sell_trades = [
                tx for tx in trades
                if tx['transaction_type'].lower() in ['sell', 'clean']
                and tx['transaction_time'] > since
                and 'realized_profit_percentage' in tx
                and tx['realized_profit_percentage'] is not None
            ]
            total = len(sell_trades)
            gt500 = sum(1 for tx in sell_trades if tx['realized_profit_percentage'] > 500)
            _200to500 = sum(1 for tx in sell_trades if 200 < tx['realized_profit_percentage'] <= 500)
            _0to200 = sum(1 for tx in sell_trades if 0 < tx['realized_profit_percentage'] <= 200)
            _0to50 = sum(1 for tx in sell_trades if 0 < tx['realized_profit_percentage'] <= 50)
            lt50 = sum(1 for tx in sell_trades if tx['realized_profit_percentage'] <= 50)
            # 百分比（先算原始比例）
            gt500_pct = gt500 / total if total else 0
            _200to500_pct = _200to500 / total if total else 0
            _0to200_pct = _0to200 / total if total else 0
            _0to50_pct = _0to50 / total if total else 0
            lt50_pct = lt50 / total if total else 0
            # 校正比例總和不超過 1
            pct_sum = gt500_pct + _200to500_pct + _0to200_pct + _0to50_pct + lt50_pct
            if pct_sum > 1.0001:
                gt500_pct /= pct_sum
                _200to500_pct /= pct_sum
                _0to200_pct /= pct_sum
                _0to50_pct /= pct_sum
                lt50_pct /= pct_sum
            # 轉為百分比格式（0~100）
            return {
                'distribution_gt500': gt500, 'distribution_200to500': _200to500, 'distribution_0to200': _0to200, 'distribution_0to50': _0to50, 'distribution_lt50': lt50,
                'distribution_gt500_percentage': gt500_pct * 100,
                'distribution_200to500_percentage': _200to500_pct * 100,
                'distribution_0to200_percentage': _0to200_pct * 100,
                'distribution_0to50_percentage': _0to50_pct * 100,
                'distribution_lt50_percentage': lt50_pct * 100,
            }

        dist_30d = get_distribution(transactions, 30)
        dist_7d = get_distribution(transactions, 7)
        chain_id = 501 if chain == "SOLANA" else 9006 if chain == "BSC" else None

        # 8. 更新錢包信息
        def safe_value(v):
            if v is None:
                return None  # 保持 None 值，讓數據庫處理
            # 對於 datetime 對象，直接返回，不要轉換為字符串
            if isinstance(v, datetime):
                return v
            return v
        
        # 初始化 update_values，確保在所有情況下都有定義
        update_values = {}
        
        try:
            await session.execute(text("SET search_path TO dex_query_v1;"))
            update_values = {k: safe_value(v) for k, v in {
                **stats,
                'wallet_address': address,
                'is_active': True,
                'is_smart_wallet': True,
                'update_time': datetime.now(),
                'balance': current_balance,
                'balance_usd': current_balance_usd,
                'token_list': ','.join(recent_tokens) if recent_tokens else None,
                'asset_multiple': (stats['pnl_percentage_30d'] / 100) + 1 if stats['pnl_percentage_30d'] else 0,
                'chain': chain,
                'chain_id': chain_id,
                'pnl_pic_1d': pnl_pic_1d,
                'pnl_pic_7d': pnl_pic_7d,
                'pnl_pic_30d': pnl_pic_30d,
                'unrealized_profit_1d': unrealized_profit_1d,
                'unrealized_profit_7d': unrealized_profit_7d,
                'unrealized_profit_30d': unrealized_profit_30d,
                # 30d 分布
                'distribution_gt500_30d': dist_30d['distribution_gt500'],
                'distribution_200to500_30d': dist_30d['distribution_200to500'],
                'distribution_0to200_30d': dist_30d['distribution_0to200'],
                'distribution_0to50_30d': dist_30d['distribution_0to50'],
                'distribution_lt50_30d': dist_30d['distribution_lt50'],
                'distribution_gt500_percentage_30d': dist_30d['distribution_gt500_percentage'],
                'distribution_200to500_percentage_30d': dist_30d['distribution_200to500_percentage'],
                'distribution_0to200_percentage_30d': dist_30d['distribution_0to200_percentage'],
                'distribution_0to50_percentage_30d': dist_30d['distribution_0to50_percentage'],
                'distribution_lt50_percentage_30d': dist_30d['distribution_lt50_percentage'],
                # 7d 分布
                'distribution_gt500_7d': dist_7d['distribution_gt500'],
                'distribution_200to500_7d': dist_7d['distribution_200to500'],
                'distribution_0to200_7d': dist_7d['distribution_0to200'],
                'distribution_0to50_7d': dist_7d['distribution_0to50'],
                'distribution_lt50_7d': dist_7d['distribution_lt50'],
                'distribution_gt500_percentage_7d': dist_7d['distribution_gt500_percentage'],
                'distribution_200to500_percentage_7d': dist_7d['distribution_200to500_percentage'],
                'distribution_0to200_percentage_7d': dist_7d['distribution_0to200_percentage'],
                'distribution_0to50_percentage_7d': dist_7d['distribution_0to50_percentage'],
                'distribution_lt50_percentage_7d': dist_7d['distribution_lt50_percentage'],
            }.items()}
            
            # --- Twitter 欄位優先用傳入參數覆蓋 ---
            if twitter_name:
                update_values['twitter_name'] = twitter_name
            if twitter_username:
                update_values['twitter_username'] = twitter_username
            if is_kol:
                update_values['tag'] = 'kol'
                update_values['is_active'] = True
                update_values['is_smart_wallet'] = True
                
            stmt = pg_insert(WalletSummary).values(**update_values)
            update_cols = {c: stmt.excluded[c] for c in update_values.keys() if c != 'id'}
            stmt = stmt.on_conflict_do_update(
                index_elements=['wallet_address'],
                set_=update_cols
            )
            await session.execute(stmt)
            await session.commit()
            logger.info(f"完成錢包 {address} 的 upsert 寫入")
            
        except Exception as e:
            print(f"[錯誤] 構建或寫入 update_values 失敗: {e}")
            print(f"stats: {stats}")
            print(f"recent_tokens: {recent_tokens}")
            print(f"pnl_pic_1d: {pnl_pic_1d}, pnl_pic_7d: {pnl_pic_7d}, pnl_pic_30d: {pnl_pic_30d}")
            print(f"unrealized_profit_1d: {unrealized_profit_1d}, unrealized_profit_7d: {unrealized_profit_7d}, unrealized_profit_30d: {unrealized_profit_30d}")
            return
        # --- 新增：推送到 API ---
        try:
            import importlib
            daily_update = importlib.import_module('daily_update_smart_money')
            wallet_to_api_dict = getattr(daily_update, 'wallet_to_api_dict')
            push_wallets_to_api = getattr(daily_update, 'push_wallets_to_api')
            wallet_obj = await session.execute(select(WalletSummary).where(WalletSummary.wallet_address == address))
            wallet_obj = wallet_obj.scalar_one_or_none()
            if wallet_obj:
                api_dict = wallet_to_api_dict(wallet_obj)
                # --- 修正: API dict 也做型別安全處理 ---
                api_dict = {k: (v.isoformat() if isinstance(v, datetime) else ("" if v is None else v)) for k, v in api_dict.items()}
                # 強制 network 欄位大寫
                if 'chain' in api_dict:
                    api_dict['chain'] = str(api_dict['chain']).upper()
                if api_dict:
                    await push_wallets_to_api([api_dict])
        except Exception as e:
            print(f"推送錢包 {address} 到 API 失敗: {e}")

        async def update_wallet_holding(session, wallet_address, chain, transactions):
            holdings = {}
            last_tx_time = {}
            total_buy = {}
            total_cost = {}
            realized_pnl = {}
            avg_price = {}
            cumulative_cost = {}
            cumulative_profit = {}

            for tx in transactions:
                token = tx['token_address']
                tx_type = tx['transaction_type'].lower()
                amount = tx['amount']
                value = tx.get('value', 0)
                realized_profit = tx.get('realized_profit', 0)
                ts = tx['transaction_time']

                if token not in holdings:
                    holdings[token] = 0
                    total_buy[token] = 0
                    total_cost[token] = 0
                    realized_pnl[token] = 0
                    avg_price[token] = 0
                    cumulative_cost[token] = 0
                    cumulative_profit[token] = 0
                    last_tx_time[token] = 0

                if tx_type in ['buy', 'build']:
                    holdings[token] += amount
                    total_buy[token] += amount
                    total_cost[token] += value
                    cumulative_cost[token] += value
                elif tx_type in ['sell', 'clean']:
                    # 防止持倉數量變為負值
                    holdings[token] = max(0, holdings[token] - amount)
                    avg_buy_price = total_cost[token] / total_buy[token] if total_buy[token] > 0 else 0
                    realized_pnl[token] += (value - avg_buy_price * amount)
                    cumulative_profit[token] += realized_profit
                    if total_buy[token] > 0:
                        total_cost[token] -= avg_buy_price * amount
                        total_buy[token] -= amount
                        # 防止總買入數量變為負值
                        if total_buy[token] < 0:
                            total_buy[token] = 0
                            total_cost[token] = 0
                        # 防止持倉數量變為負值
                        if holdings[token] < 0:
                            holdings[token] = 0
                last_tx_time[token] = max(last_tx_time[token], ts)

            holding_dicts = []
            now_dt = make_naive_time(datetime.now(timezone(timedelta(hours=8))))
            for token, remain in holdings.items():
                # 確保持倉數量不為負值
                remain = max(0, remain)
                
                if remain == 0 and cumulative_cost[token] == 0 and cumulative_profit[token] == 0:
                    continue
                price_usd = await get_token_price_usd(session, token)
                token_name, token_icon, supply, marketcap = None, None, 0, 0
                try:
                    sql = "SELECT supply, symbol, logo FROM dex_query_v1.tokens WHERE address = :address LIMIT 1"
                    result = await session.execute(text(sql), {"address": token})
                    row = result.fetchone()
                    if row:
                        supply = float(row[0]) if row[0] is not None else 0
                        token_name = row[1] or None
                        token_icon = row[2] or None
                        marketcap = price_usd * supply
                except:
                    pass

                avg_buy_price = total_cost[token] / total_buy[token] if total_buy[token] > 0 else 0
                value = remain * price_usd
                unrealized_profit = value - total_cost[token] if remain > 0 else 0
                is_cleared = remain == 0
                # 計算pnl_percentage，最小值不能小於-100
                cumulative_cost_value = cumulative_cost[token]
                if cumulative_cost_value > 0:
                    pnl_percentage = max((unrealized_profit / cumulative_cost_value) * 100, -100)
                else:
                    pnl_percentage = 0

                holding_dicts.append({
                    "wallet_address": wallet_address,
                    "token_address": token,
                    "token_icon": token_icon,
                    "token_name": token_name,
                    "chain": chain,
                    "chain_id": 501,
                    "amount": remain,
                    "value": value,
                    "value_usdt": value,
                    "unrealized_profits": unrealized_profit,
                    "pnl": realized_pnl[token],
                    "pnl_percentage": pnl_percentage,
                    "avg_price": avg_buy_price,
                    "marketcap": marketcap,
                    "is_cleared": is_cleared,
                    "cumulative_cost": cumulative_cost[token],
                    "cumulative_profit": cumulative_profit[token],
                    "last_transaction_time": last_tx_time[token],
                    "time": now_dt
                })

            if holding_dicts:
                logging.info(f"[寫入] update_wallet_holding 將寫入 {Holding.__tablename__}，wallet_address={wallet_address}，共{len(holding_dicts)}筆")
                # 移除所有dict中的id欄位，避免主鍵衝突
                for d in holding_dicts:
                    d.pop('id', None)
                
                # 分批處理，避免參數數量過多
                batch_size = 100  # 每批處理100條記錄
                total_batches = (len(holding_dicts) + batch_size - 1) // batch_size
                
                try:
                    await session.execute(text("SET search_path TO dex_query_v1;"))
                    for i in range(total_batches):
                        start_idx = i * batch_size
                        end_idx = min((i + 1) * batch_size, len(holding_dicts))
                        batch_data = holding_dicts[start_idx:end_idx]
                        logging.info(f"[寫入] wallet_holding 批次 {i+1}/{total_batches}，資料: {batch_data[0] if batch_data else None}")
                        
                        stmt = pg_insert(Holding).values(batch_data)
                        update_fields = {k: stmt.excluded[k] for k in batch_data[0] if k not in ('id', 'wallet_address', 'token_address', 'chain')}
                        stmt = stmt.on_conflict_do_update(
                            index_elements=['wallet_address', 'token_address', 'chain'],
                            set_=update_fields
                        )
                        await session.execute(stmt)
                    
                    await session.commit()
                    print(f"[SUCCESS] upsert {len(holding_dicts)} 筆 wallet_holding 資料 (分 {total_batches} 批處理)")
                except Exception as e:
                    await session.rollback()
                    print(f"[ERROR] wallet_holding upsert 失敗: {e}")
                    raise

        await update_wallet_holding(session, address, chain, transactions)

    except Exception as e:
        print(f"更新錢包 {address} 數據時出錯: {e}")
        print(traceback.format_exc())

async def analyze_swap_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, token_buy_data, chain):
    """
    優化後的 SWAP 交易分析函數
    """
    try:
        # 將常用的靜態數據移到函數外部作為常量
        STABLECOINS = frozenset([
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ])
        SOL_ADDRESS = "So11111111111111111111111111111111111111112"
        
        # 預先提取常用數據，減少字典查詢
        signature = transaction.get("signature")
        timestamp = transaction.get("timestamp")
        fee = transaction.get("fee", 0)
        source = transaction.get("source", "UNKNOWN")
        
        # 快速檢查必要條件
        events = transaction.get("events")
        if not events or "swap" not in events:
            return await analyze_special_transaction(transaction, address, async_session, 
                                                  wallet_balance_usdt, sol_usdt_price, client, chain)
        
        swap_event = events["swap"]
        
        # ===================== MEMECOIN對MEMECOIN交易處理 =====================
        # 首先檢測是否為memecoin對memecoin的交易
        token_transfers = transaction.get("tokenTransfers", [])
        
        # 檢查用戶是否賣出了代幣
        user_sold_tokens = False
        sold_token_mint = None
        sold_token_amount = 0
        
        token_inputs = swap_event.get("tokenInputs", [])
        for token_input in token_inputs:
            if token_input.get("userAccount") == address:
                user_sold_tokens = True
                sold_token_mint = token_input.get("mint", "")
                raw_amount = token_input.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    sold_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                break
        
        # 檢查用戶是否在同一交易中收到了其他非SOL/USDC代幣
        user_received_tokens = []
        for transfer in token_transfers:
            if transfer.get("toUserAccount") == address:
                token_mint = transfer.get("mint", "")
                # 跳過SOL和穩定幣
                if token_mint in STABLECOINS or token_mint == SOL_ADDRESS:
                    continue
                # 跳過賣出的同一種代幣（這可能是交易的一部分返回）
                if token_mint == sold_token_mint:
                    continue
                
                # 這是一個向用戶轉賬的非SOL/穩定幣代幣
                amount = 0
                raw_amount = transfer.get("rawTokenAmount", {})
                token_amount = transfer.get("tokenAmount", 0)
                
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                elif token_amount:
                    amount = float(token_amount)
                
                if amount > 0:
                    user_received_tokens.append({
                        "mint": token_mint,
                        "amount": amount,
                        "symbol": TokenUtils.get_token_info(token_mint).get('symbol', '')
                    })
        
        # 檢查中間SOL金額
        intermediate_sol_amount = 0.0
        if "nativeOutput" in swap_event and swap_event["nativeOutput"]:
            intermediate_sol_amount = float(swap_event["nativeOutput"]["amount"]) / (10 ** 9)
        
        # 檢測到memecoin對memecoin交易
        if user_sold_tokens and user_received_tokens and intermediate_sol_amount > 0:
            print(f"檢測到memecoin對memecoin交易: {signature}")
            print(f"賣出: {sold_token_amount} {sold_token_mint}")
            print(f"買入: {user_received_tokens[0]['amount']} {user_received_tokens[0]['mint']}")
            print(f"中間SOL金額: {intermediate_sol_amount}")
            
            # 處理賣出部分
            # 創建賣出交易數據
            swap_data_sell = {
                "swap_type": "SELL",
                "sold_token": sold_token_mint,
                "sold_amount": sold_token_amount,
                "bought_token": SOL_ADDRESS,  # 中間SOL
                "bought_amount": intermediate_sol_amount,
                "value": intermediate_sol_amount * sol_usdt_price,  # 確保設置 value
                "value_usd": intermediate_sol_amount * sol_usdt_price
            }
            
            token_data_sell = await _process_token_data(
                address, sold_token_mint, sold_token_amount, swap_data_sell, 
                async_session, chain, wallet_balance_usdt
            )
            
            if token_data_sell:
                result_sell = await _build_transaction_result(
                    sold_token_mint, token_data_sell, swap_data_sell, 
                    address, signature, timestamp,
                    chain, wallet_balance_usdt, client
                )
                await save_past_transaction(async_session, result_sell, address, signature, chain)
            
            # 處理買入部分
            if user_received_tokens:
                received_token = user_received_tokens[0]  # 取第一個收到的代幣
                swap_data_buy = {
                    "swap_type": "BUY",
                    "sold_token": SOL_ADDRESS,  # 使用中間SOL
                    "sold_amount": intermediate_sol_amount,
                    "bought_token": received_token["mint"],
                    "bought_amount": received_token["amount"],
                    "value": intermediate_sol_amount * sol_usdt_price,  # 確保設置 value
                    "value_usd": intermediate_sol_amount * sol_usdt_price
                }
                
                token_data_buy = await _process_token_data(
                    address, received_token["mint"], received_token["amount"], swap_data_buy, 
                    async_session, chain, wallet_balance_usdt
                )
                
                if token_data_buy:
                    result_buy = await _build_transaction_result(
                        received_token["mint"], token_data_buy, swap_data_buy, 
                        address, signature, timestamp,
                        chain, wallet_balance_usdt, client
                    )
                    await save_past_transaction(async_session, result_buy, address, signature, chain)
                    return result_buy  # 返回買入部分作為結果
            
            return result_sell  # 如果沒有買入部分，則返回賣出部分
        
        # ===================== 一般交易處理 =====================
        swap_data = {}
        
        # 檢測交易類型並處理
        if "tokenInputs" in swap_event and swap_event["tokenInputs"]:
            # 可能是賣出交易
            for token_input in swap_event["tokenInputs"]:
                if token_input.get("userAccount") == address:
                    # 用戶確實在賣出代幣
                    swap_data = _process_sell_swap(swap_event, sol_usdt_price, address)
                    break
        
        if not swap_data and "tokenOutputs" in swap_event and swap_event["tokenOutputs"]:
            # 可能是買入交易
            for token_output in swap_event["tokenOutputs"]:
                if token_output.get("userAccount") == address:
                    # 用戶確實在買入代幣
                    swap_data = _process_buy_swap(swap_event, sol_usdt_price, address)
                    break
        
        # 驗證交易數據
        if not swap_data or "swap_type" not in swap_data:
            return None
            
        # 確保 value 欄位存在
        if "value" not in swap_data:
            if "value_usd" in swap_data:
                swap_data["value"] = swap_data["value_usd"]
            else:
                # 計算默認值
                sol_amount = swap_data.get("sold_amount", 0) if swap_data["swap_type"] == "SELL" else swap_data.get("bought_amount", 0)
                swap_data["value"] = sol_amount * sol_usdt_price
        
        swap_type = swap_data["swap_type"]
        token_address = swap_data.get("sold_token" if swap_type == "SELL" else "bought_token")
        amount = swap_data.get("sold_amount" if swap_type == "SELL" else "bought_amount", 0)
        
        # 處理代幣數據
        token_data = await _process_token_data(
            address, token_address, amount, swap_data, 
            async_session, chain, wallet_balance_usdt
        )
        
        if not token_data:
            return None
        
        # 構建最終結果
        result = await _build_transaction_result(
            token_address, token_data, swap_data, address, signature, timestamp,
            chain, wallet_balance_usdt, client
        )
        
        await save_past_transaction(async_session, result, address, signature, chain)
        return result
        
    except Exception as e:
        print(f"分析或保存交易失敗: {str(e)}")
        traceback.print_exc()  # 打印完整的錯誤堆疊
        return None

async def _get_token_symbol(token_mint: str) -> str:
    """獲取代幣符號 (基於已知的mint地址)"""
    # 這裡可以擴展成一個完整的代幣符號映射表
    tokens = {
        "So11111111111111111111111111111111111111112": "SOL",
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": "USDC",
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": "USDT",
        # 可以添加更多代幣
    }
    
    # 嘗試從TokenUtils獲取符號
    token_info = TokenUtils.get_token_info(token_mint)
    if token_info and 'symbol' in token_info:
        return token_info['symbol']
    
    return tokens.get(token_mint, '')  # 如果未知，返回mint地址的前6個字符

async def _process_sell_transaction(signature, timestamp, source, token_mint, token_amount, token_symbol, 
                                   sol_amount, usdc_amount, address, async_session, wallet_balance_usdt, 
                                   sol_usdt_price, client, chain):
    """處理賣出交易邏輯"""
    try:
        # 計算交易價值 (統一為 USD)
        value = 0
        if sol_amount > 0:
            value = sol_amount * sol_usdt_price
        elif usdc_amount > 0:
            value = usdc_amount
        
        # 獲取該代幣的買入數據
        token_data = await token_buy_data_cache.get_token_data(
            address, token_mint, async_session, chain
        )
        
        # 使用 FIFO 方法計算盈虧
        avg_buy_price = token_data.get("avg_buy_price", 0)
        total_amount = token_data.get("total_amount", 0)
        
        # 計算賣出佔比和盈虧
        if total_amount > 0:
            sell_percentage = min((token_amount / total_amount) * 100, 100)
            sell_price = value / token_amount if token_amount > 0 else 0
            realized_profit = 0
            realized_profit_percentage = 0
            
            if avg_buy_price > 0:
                realized_profit = (sell_price - avg_buy_price) * token_amount
                realized_profit_percentage = max(((sell_price / avg_buy_price) - 1) * 100, -100)
        else:
            # 如果之前沒有買入記錄 (可能是空投或其他來源)
            sell_percentage = 100
            realized_profit = value  # 全部視為利潤
            realized_profit_percentage = 100
        
        # 更新剩餘代幣數量
        new_total_amount = max(0, total_amount - token_amount)
        new_total_cost = 0 if new_total_amount <= 0 else token_data.get("total_cost", 0) * (new_total_amount / total_amount)
        
        # 保存更新後的代幣數據
        tx_data = {
            "avg_buy_price": avg_buy_price,
            "token_address": token_mint,
            "total_amount": new_total_amount,
            "total_cost": new_total_cost
        }
        await save_wallet_buy_data(tx_data, address, async_session, chain)
        
        # 獲取代幣信息和供應量
        token_info = TokenUtils.get_token_info(token_mint)
        supply = await get_token_supply(client, token_mint) or 0
        price = value / token_amount if token_amount > 0 else 0
        marketcap = round(price * supply, 2)
        
        # 構建交易結果數據
        result = {
            "wallet_address": address,
            "wallet_balance": wallet_balance_usdt,
            "token_address": token_mint,
            "token_icon": token_info.get('url', ''),
            "token_name": token_info.get('symbol', token_symbol),
            "price": price,
            "amount": token_amount,
            "marketcap": marketcap,
            "value": value,
            "holding_percentage": sell_percentage,
            "chain": "SOLANA",
            "realized_profit": realized_profit,
            "realized_profit_percentage": realized_profit_percentage,
            "transaction_type": "sell",
            "transaction_time": timestamp,
            "time": datetime.now(timezone(timedelta(hours=8)))
        }
        
        # 保存交易記錄
        await save_past_transaction(async_session, result, address, signature, chain)
        return result
        
    except Exception as e:
        logging.error(f"處理賣出交易失敗: {e}\n{traceback.format_exc()}")
        return None

async def _process_buy_transaction(signature, timestamp, source, token_mint, token_amount, token_symbol, 
                                  sol_amount, usdc_amount, address, async_session, wallet_balance_usdt, 
                                  sol_usdt_price, client, chain):
    """處理買入交易邏輯"""
    try:
        # 計算交易價值 (統一為 USD)
        value = 0
        if sol_amount > 0:
            value = sol_amount * sol_usdt_price
        elif usdc_amount > 0:
            value = usdc_amount
        
        # 獲取該代幣的買入數據
        token_data = await token_buy_data_cache.get_token_data(
            address, token_mint, async_session, chain
        )
        
        # 更新代幣數據
        total_amount = token_data.get("total_amount", 0) + token_amount
        total_cost = token_data.get("total_cost", 0) + value
        avg_buy_price = total_cost / total_amount if total_amount > 0 else 0
        
        # 保存更新後的代幣數據
        tx_data = {
            "avg_buy_price": avg_buy_price,
            "token_address": token_mint,
            "total_amount": total_amount,
            "total_cost": total_cost
        }
        await save_wallet_buy_data(tx_data, address, async_session, chain)
        
        # 獲取代幣信息和供應量
        token_info = TokenUtils.get_token_info(token_mint)
        supply = await get_token_supply(client, token_mint) or 0
        price = value / token_amount if token_amount > 0 else 0
        marketcap = round(price * supply, 2)
        
        # 計算持倉百分比
        holding_percentage = min((value / (value + wallet_balance_usdt)) * 100, 100) if wallet_balance_usdt > 0 else 100
        
        # 構建交易結果數據
        result = {
            "wallet_address": address,
            "wallet_balance": wallet_balance_usdt,
            "token_address": token_mint,
            "token_icon": token_info.get('url', ''),
            "token_name": token_info.get('symbol', token_symbol),
            "price": price,
            "amount": token_amount,
            "marketcap": marketcap,
            "value": value,
            "holding_percentage": holding_percentage,
            "chain": "SOLANA",
            "realized_profit": 0,  # 買入交易沒有已實現利潤
            "realized_profit_percentage": 0,
            "transaction_type": "buy",
            "transaction_time": timestamp,
            "time": datetime.now(timezone(timedelta(hours=8)))
        }
        
        # 保存交易記錄
        await save_past_transaction(async_session, result, address, signature, chain)
        return result
        
    except Exception as e:
        logging.error(f"處理買入交易失敗: {e}\n{traceback.format_exc()}")
        return None

def _process_sell_swap(swap_event, sol_usdt_price, user_wallet):
    """
    處理賣出類型的 SWAP，支援更複雜的交易模式
    """
    result = {"swap_type": "SELL"}
    
    # 確保用戶實際上在賣出代幣
    user_token_input = None
    for token_input in swap_event.get("tokenInputs", []):
        if token_input.get("userAccount") == user_wallet:
            user_token_input = token_input
            break
    
    if not user_token_input:
        return {}  # 用戶沒有賣出任何代幣
    
    # 設置賣出代幣信息
    result["sold_token"] = user_token_input["mint"]
    result["sold_amount"] = float(user_token_input["rawTokenAmount"]["tokenAmount"]) / 10**user_token_input["rawTokenAmount"]["decimals"]
    
    # 檢查是否獲得了 SOL
    if "nativeOutput" in swap_event and swap_event["nativeOutput"]:
        result["bought_token"] = "So11111111111111111111111111111111111111112"
        result["bought_amount"] = float(swap_event["nativeOutput"]["amount"]) / 1e9
        result["value"] = result["bought_amount"]
        result["value_usd"] = result["bought_amount"] * sol_usdt_price
    
    # 檢查是否獲得了穩定幣 (USDC/USDT)
    elif "tokenOutputs" in swap_event and swap_event["tokenOutputs"]:
        for token_output in swap_event["tokenOutputs"]:
            if token_output.get("userAccount") == user_wallet:
                # 檢查是否為穩定幣
                stablecoins = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]
                if token_output["mint"] in stablecoins:
                    result["bought_token"] = token_output["mint"]
                    result["bought_amount"] = float(token_output["rawTokenAmount"]["tokenAmount"]) / 10**token_output["rawTokenAmount"]["decimals"]
                    result["value"] = result["bought_amount"]  # 穩定幣價值就是其數量
                    result["value_usd"] = result["bought_amount"]  # 穩定幣 1:1 對應美元
                    break
    
    return result

def _process_buy_swap(swap_event, sol_usdt_price, user_wallet):
    """
    處理買入類型的 SWAP，支援更複雜的交易模式
    """
    result = {"swap_type": "BUY"}
    
    # 確保用戶實際上在買入代幣
    user_token_output = None
    for token_output in swap_event.get("tokenOutputs", []):
        if token_output.get("userAccount") == user_wallet:
            # 排除穩定幣和原生代幣
            stablecoins = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]
            if token_output["mint"] not in stablecoins and token_output["mint"] != "So11111111111111111111111111111111111111112":
                user_token_output = token_output
                break
    
    if not user_token_output:
        return {}  # 用戶沒有買入任何非穩定幣/非原生代幣
    
    # 設置買入代幣信息
    result["bought_token"] = user_token_output["mint"]
    result["bought_amount"] = float(user_token_output["rawTokenAmount"]["tokenAmount"]) / 10**user_token_output["rawTokenAmount"]["decimals"]
    
    # 檢查是否使用了 SOL
    if "nativeInput" in swap_event and swap_event["nativeInput"]:
        result["sold_token"] = "So11111111111111111111111111111111111111112"
        result["sold_amount"] = float(swap_event["nativeInput"]["amount"]) / 1e9
        result["value"] = result["sold_amount"]
        result["value_usd"] = result["sold_amount"] * sol_usdt_price
    
    # 檢查是否使用了穩定幣 (USDC/USDT)
    elif "tokenInputs" in swap_event and swap_event["tokenInputs"]:
        for token_input in swap_event["tokenInputs"]:
            if token_input.get("userAccount") == user_wallet:
                # 檢查是否為穩定幣
                stablecoins = ["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"]
                if token_input["mint"] in stablecoins:
                    result["sold_token"] = token_input["mint"]
                    result["sold_amount"] = float(token_input["rawTokenAmount"]["tokenAmount"]) / 10**token_input["rawTokenAmount"]["decimals"]
                    result["value"] = result["sold_amount"]  # 穩定幣價值就是其數量
                    result["value_usd"] = result["sold_amount"]  # 穩定幣 1:1 對應美元
                    break
    
    return result

def _should_skip_transaction(swap_data, STABLECOINS, SOL_ADDRESS):
    """快速判斷是否需要跳過交易"""
    sold_token = swap_data["sold_token"]
    bought_token = swap_data["bought_token"]
    
    return (
        (sold_token in STABLECOINS and bought_token == SOL_ADDRESS) or
        (bought_token in STABLECOINS and sold_token == SOL_ADDRESS) or
        (bought_token != SOL_ADDRESS and sold_token != SOL_ADDRESS)
    )

class TokenBuyDataCache:
    def __init__(self, max_size=1000):
        self._cache = {}  # 緩存字典
        self._max_size = max_size  # 最大緩存大小

    async def get_token_data(self, wallet_address, token_address, session, chain):
        # 生成唯一緩存鍵
        cache_key = f"{wallet_address}_{token_address}_{chain}"
        
        # 如果已經在緩存中，直接返回
        if cache_key in self._cache:
            return self._cache[cache_key]
        
        # 從數據庫獲取數據
        token_data = await get_token_buy_data(wallet_address, token_address, session, chain)
        
        # 緩存已滿時，移除最早的條目
        if len(self._cache) >= self._max_size:
            self._cache.popitem()  
        
        # 將數據存入緩存
        self._cache[cache_key] = token_data
        return token_data

token_buy_data_cache = TokenBuyDataCache()

async def _process_token_data(address, token_address, amount, swap_data, async_session, chain, wallet_balance_usdt):
    """處理代幣相關數據"""
    token_data = await token_buy_data_cache.get_token_data(
        address, token_address, async_session, chain
    )
    
    if swap_data["swap_type"] == "SELL":
        token_data = _process_sell_token_data(token_data, amount, swap_data)
    else:
        token_data = _process_buy_token_data(token_data, amount, swap_data)
        
    # 構建要保存的交易數據
    tx_data = {
        "avg_buy_price": token_data.get("avg_buy_price", 0),
        "token_address": token_address,
        "total_amount": token_data["total_amount"],
        "total_cost": token_data["total_cost"]
    }

    # 保存到數據庫
    await save_wallet_buy_data(tx_data, address, async_session, chain)
    
    return token_data

def _process_sell_token_data(token_data, amount, swap_data):
    """處理賣出時的代幣數據"""
    if token_data["total_amount"] <= 0:
        token_data.update({
            "total_amount": 0,
            "total_cost": 0,
            "pnl": 0,
            "pnl_percentage": 0,
            "sell_percentage": 0
        })
        return token_data
        
    sell_percentage = min((amount / token_data["total_amount"]) * 100, 100)
    avg_buy_price = token_data["total_cost"] / token_data["total_amount"]
    sell_price = swap_data["value"] / amount
    total_amount = max(0, token_data["total_amount"] - amount)
    
    token_data.update({
        "pnl": (sell_price - avg_buy_price) * amount,
        "pnl_percentage": max(((sell_price / avg_buy_price) - 1) * 100, -100) if avg_buy_price > 0 else 0,
        "sell_percentage": sell_percentage,
        "total_amount": total_amount,
        "total_cost": 0 if total_amount <= amount else token_data["total_cost"]
    })
    
    return token_data

def _process_buy_token_data(token_data, amount, swap_data):
    """處理買入時的代幣數據"""
    token_data["total_amount"] += amount
    token_data["total_cost"] += swap_data["value"]
    token_data["avg_buy_price"] = token_data["total_cost"] / token_data["total_amount"]
    return token_data

async def _build_transaction_result(token_address, token_data, swap_data, address, signature, timestamp, chain, wallet_balance_usdt, client):
    """構建最終的交易結果"""
    # 獲取交易類型和數量
    transaction_type = swap_data.get("swap_type", "unknown")
    amount = swap_data.get("sold_amount", 0) if transaction_type == "SELL" else swap_data.get("bought_amount", 0)
    
    # 使用緩存獲取 token 信息和供應量
    token_info = TokenUtils.get_token_info(token_address)
    supply = await get_token_supply(client, token_address) or 0
    
    # 安全地獲取 value，如果不存在則計算默認值
    value = swap_data.get("value", 0)
    if value == 0 and "value_usd" in swap_data:
        value = swap_data["value_usd"]
        
    price = value / amount if value and amount else 0
    marketcap = round(price * supply, 2)

    # 安全地計算持倉百分比
    if transaction_type == "BUY":
        holding_percentage = min((value / (value + wallet_balance_usdt)) * 100, 100) if value and wallet_balance_usdt else 0
    else:
        holding_percentage = token_data.get("sell_percentage", 0)
    
    # 設置來源和目標代幣信息
    from_token_address = ""
    from_token_symbol = ""
    from_token_amount = 0
    dest_token_address = ""
    dest_token_symbol = ""
    dest_token_amount = 0
    
    # 根據交易類型設置來源和目標代幣信息
    if transaction_type == "BUY":
        # 買入交易: 從 sold_token 買入 bought_token
        from_token_address = swap_data.get("sold_token", "")
        from_token_symbol = _get_token_symbol_from_address(from_token_address)
        from_token_amount = swap_data.get("sold_amount", 0)
        
        dest_token_address = swap_data.get("bought_token", "")
        dest_token_symbol = _get_token_symbol_from_address(dest_token_address)
        dest_token_amount = swap_data.get("bought_amount", 0)
    else:  # SELL
        # 賣出交易: 從 sold_token 賣出獲得 bought_token
        from_token_address = swap_data.get("sold_token", "")
        from_token_symbol = _get_token_symbol_from_address(from_token_address)
        from_token_amount = swap_data.get("sold_amount", 0)
        
        dest_token_address = swap_data.get("bought_token", "")
        dest_token_symbol = _get_token_symbol_from_address(dest_token_address)
        dest_token_amount = swap_data.get("bought_amount", 0)

    return {
        "wallet_address": address,
        "wallet_balance": wallet_balance_usdt,
        "token_address": token_address,
        "token_icon": token_info.get('url', ''),
        "token_name": token_info.get('symbol', ''),
        "price": price,
        "amount": amount,
        "marketcap": marketcap,
        "value": value,
        "holding_percentage": holding_percentage,
        "chain": "SOLANA",
        "realized_profit": token_data.get("pnl", 0) if transaction_type == "SELL" else 0,
        "realized_profit_percentage": token_data.get("pnl_percentage", 0) if transaction_type == "SELL" else 0,
        "transaction_type": transaction_type.lower(),
        "transaction_time": timestamp,
        "time": datetime.now(timezone(timedelta(hours=8))),
        "from_token_address": from_token_address,
        "from_token_symbol": from_token_symbol,
        "from_token_amount": from_token_amount,
        "dest_token_address": dest_token_address,
        "dest_token_symbol": dest_token_symbol,
        "dest_token_amount": dest_token_amount
    }

# 輔助函數: 從地址獲取代幣符號
def _get_token_symbol_from_address(token_address):
    """根據代幣地址獲取符號"""
    if token_address == "So11111111111111111111111111111111111111112":
        return "SOL"
    elif token_address == USDC_MINT:
        return "USDC"
    elif token_address == USDT_MINT:
        return "USDT"
    else:
        # 嘗試從緩存或API獲取
        token_info = TokenUtils.get_token_info(token_address)
        return token_info.get('symbol', '')

async def analyze_special_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, chain):
    """
    分析一笔 SWAP 类型的交易，依据 tokenTransfers 判断是买入还是卖出，并保存到数据库。
    """
    try:
        # 提前獲取常用值，避免重複 dict lookup
        signature = transaction["signature"]
        timestamp = transaction["timestamp"]
        fee = transaction.get("fee", 0)
        token_transfers = transaction.get("tokenTransfers", [])
        
        if not token_transfers:
            return None
        
        # 初始化基礎結果
        result = {
            "signature": signature,
            "timestamp": timestamp,
            "swap_type": None,
            "sold_token": None,
            "sold_amount": None,
            "bought_token": None,
            "bought_amount": None,
            "fee": fee,
            "value": None,
            "pnl": 0,
            "pnl_percentage": 0
        }
        # 提前獲取 transfer 相關數據
        first_transfer = token_transfers[0]
        from_user = first_transfer["fromUserAccount"]
        to_user = first_transfer["toUserAccount"]
        token_amount = first_transfer["tokenAmount"]
        token_mint = first_transfer["mint"]

        # 提前判斷交易類型
        is_sell = from_user == address
        is_buy = to_user == address

        if not (is_sell or is_buy):
            return None
        # 提前獲取賬戶數據
        account_data = transaction.get("accountData", [])
        target_account = to_user if is_sell else from_user

        # 使用 dict comprehension 優化賬戶數據查找
        account_dict = {acc["account"]: acc for acc in account_data}
        target_account_data = account_dict.get(target_account)

        if target_account_data:
            native_balance_change = abs(target_account_data.get("nativeBalanceChange", 0))
            value = (native_balance_change / 1e9) * sol_usdt_price
        else:
            value = 0

        # 根據交易類型處理數據
        if is_sell:
            result.update({
                "swap_type": "SELL",
                "sold_token": token_mint,
                "sold_amount": token_amount,
                "value": value
            })
            
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            avg_buy_price = token_data.get("avg_buy_price", 0)
            sell_price = value / token_amount
            total_amount = max(0, token_data.get('total_amount', 0) - token_amount)
            
            if token_amount and avg_buy_price:
                result["pnl_percentage"] = max(((sell_price / avg_buy_price) - 1) * 100, -100) if avg_buy_price > 0 else 0
                result["pnl"] = (sell_price - avg_buy_price) * token_amount
            
            tx_data = {
                "avg_buy_price": avg_buy_price,
                "token_address": token_mint,
                "total_amount": total_amount,
                "total_cost": 0 if total_amount <= token_amount else token_data["total_cost"]
            }
            
        else:  # Buy case
            result.update({
                "swap_type": "BUY",
                "bought_token": token_mint,
                "bought_amount": token_amount,
                "value": value
            })
            
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            total_amount = token_data.get('total_amount', 0) + token_amount
            total_cost = token_data.get('total_cost', 0) + value
            
            tx_data = {
                "avg_buy_price": total_cost / total_amount if total_amount else 0,
                "token_address": token_mint,
                "total_amount": total_amount,
                "total_cost": total_cost
            }

        await save_wallet_buy_data(tx_data, address, async_session, chain)

        # 獲取 token 信息
        token_address = token_mint
        token_info = TokenUtils.get_token_info(token_address) or {"url": "", "symbol": "", "priceUsd": 0}
        
        # 計算相關數據
        amount = token_amount
        price = value / amount if amount else 0
        supply = await get_token_supply(client, token_address)
        marketcap = round(price * sol_usdt_price * (supply or 0), 2)

        # 計算持倉百分比
        holding_percentage = (
            min((value / value + wallet_balance_usdt) * 100, 100) if is_buy and value
            else (amount / token_data["total_amount"]) * 100 if not is_buy and token_data.get("total_amount")
            else 0
        )

        # 構建最終交易數據
        tx_data = {
            "wallet_address": address,
            "wallet_balance": wallet_balance_usdt,
            "token_address": token_address,
            "token_icon": token_info.get('url', ''),
            "token_name": token_info.get('symbol', ''),
            "price": price,
            "amount": amount,
            "marketcap": marketcap,
            "value": value,
            "holding_percentage": holding_percentage,
            "chain": "SOLANA",
            "realized_profit": result.get("pnl", 0)  if is_sell else value,
            "realized_profit_percentage": result.get("pnl_percentage", 0) if is_sell else 0,
            "transaction_type": result["swap_type"].lower(),
            "transaction_time": timestamp,
            "time": datetime.now(timezone(timedelta(hours=8)))
        }

        await save_past_transaction(async_session, tx_data, address, signature, chain)
        return tx_data

    except Exception as e:
        logger.error(f"處理交易失敗: {e}", exc_info=True)
        return None

async def analyze_event_transaction(transaction, address, async_session: AsyncSession, wallet_balance_usdt, sol_usdt_price, client, chain):
    """
    分析一筆 SWAP 類型的交易，依據交易的内容判斷是買入、賣出或memecoin對memecoin的交易，並保存到資料表。
    使用與WalletAnalyzer.process_transactions相同的邏輯，但針對單筆交易處理。
    """
    try:
        description = transaction.get("description", "").lower()
        # 如果不是SWAP交易，直接返回
        if transaction.get("type") != "SWAP":
            return None
        if "swap" not in description and "for" not in description:
            return None  # 快速跳过非交换交易

        if not transaction.get("events", {}).get("swap"):
            return await _analyze_swap_by_token_transfers(
                transaction, 
                address, 
                async_session, 
                wallet_balance_usdt, 
                sol_usdt_price, 
                client, 
                chain
            )

        # 定義常量
        STABLECOINS = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ]
        WSOL_MINT = "So11111111111111111111111111111111111111112"
        SUPPORTED_TOKENS = STABLECOINS + [WSOL_MINT]
        SOL_DECIMALS = 9

        # 提取基本交易信息
        signature = transaction.get("signature", "")
        timestamp = transaction.get("timestamp", 0)
        source = transaction.get("source", "UNKNOWN")
        
        # 提取 swap 事件數據
        events = transaction.get("events", {})
        swap_event = events.get("swap", {})
        
        if not swap_event:
            return None
        
        # 提取token輸入、輸出和SOL輸入輸出
        token_inputs = swap_event.get("tokenInputs", [])
        token_outputs = swap_event.get("tokenOutputs", [])
        native_output = swap_event.get("nativeOutput", {})
        native_input = swap_event.get("nativeInput", {})
        
        # 提取token轉賬信息用於檢測memecoin對memecoin交易
        token_transfers = transaction.get("tokenTransfers", [])
        
        # 檢查是否是memecoin對memecoin的交易
        user_sold_tokens = False
        sold_token_mint = None
        sold_token_amount = 0
        sold_token_symbol = None
        sold_token_icon = None
        
        for token_input in token_inputs:
            if token_input.get("userAccount") == address:
                user_sold_tokens = True
                sold_token_mint = token_input.get("mint", "")
                token_info = TokenUtils.get_token_info(sold_token_mint)
                sold_token_symbol = token_info.get('symbol', '')
                sold_token_icon = token_info.get('url', '')
                raw_amount = token_input.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    sold_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                break

        # 檢查用戶是否在同一交易中收到了其他非SOL/USDC代幣
        user_received_tokens = []
        for transfer in token_transfers:
            # 確保這是一個向用戶的轉賬
            if transfer.get("toUserAccount") == address:
                token_mint = transfer.get("mint", "")
                # 跳過SOL和穩定幣
                if token_mint in SUPPORTED_TOKENS:
                    continue
                # 跳過賣出的同一種代幣（這可能是交易的一部分返回）
                if token_mint == sold_token_mint:
                    continue
                
                # 獲取代幣信息
                token_info = TokenUtils.get_token_info(token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                # 處理金額
                raw_amount = None
                if "rawTokenAmount" in transfer:
                    raw_amount = transfer["rawTokenAmount"]
                else:
                    # 嘗試從tokenAmount字段獲取金額
                    token_amount = transfer.get("tokenAmount", 0)
                    if token_amount:
                        # 假設tokenAmount已經是一個浮點數
                        try:
                            token_amount = float(token_amount)
                            raw_amount = {"tokenAmount": token_amount, "decimals": 0}
                        except:
                            continue

                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                    user_received_tokens.append({
                        "mint": token_mint,
                        "symbol": token_symbol,
                        "icon": token_icon,
                        "amount": received_amount
                    })
        
        # 處理SOL的中間金額（用於計算交易價值）
        intermediate_sol_amount = 0.0
        if native_output and "amount" in native_output:
            intermediate_sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
        elif native_input and "amount" in native_input:
            intermediate_sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
        
        # 處理交易保存的結果列表
        processed_txs = []
        
        # 處理memecoin對memecoin的交易
        if user_sold_tokens and user_received_tokens and intermediate_sol_amount > 0:
            # 1. 首先處理賣出部分
            sell_tx_data = await process_token_transaction(
                signature=signature,
                timestamp=timestamp,
                tx_type="SELL",
                source=source,
                token_mint=sold_token_mint,
                token_amount=sold_token_amount,
                token_symbol=sold_token_symbol,
                token_icon=sold_token_icon,
                sol_amount=intermediate_sol_amount,
                usdc_amount=0,
                address=address,
                async_session=async_session,
                wallet_balance_usdt=wallet_balance_usdt,
                sol_usdt_price=sol_usdt_price,
                client=client,
                chain=chain
            )
            if sell_tx_data:
                processed_txs.append(sell_tx_data)
            
            # 2. 然後處理買入部分
            for received_token in user_received_tokens:
                buy_tx_data = await process_token_transaction(
                    signature=signature,  # 添加後綴以避免主鍵衝突
                    timestamp=timestamp,
                    tx_type="BUY",
                    source=source,
                    token_mint=received_token["mint"],
                    token_amount=received_token["amount"],
                    token_symbol=received_token["symbol"],
                    token_icon=received_token["icon"],
                    sol_amount=intermediate_sol_amount,
                    usdc_amount=0,
                    address=address,
                    async_session=async_session,
                    wallet_balance_usdt=wallet_balance_usdt,
                    sol_usdt_price=sol_usdt_price,
                    client=client,
                    chain=chain
                )
                if buy_tx_data:
                    processed_txs.append(buy_tx_data)
            
            # 返回處理結果
            return processed_txs if processed_txs else None
        
        # 以下處理常規買入賣出交易
        
        # 處理代幣輸入 (賣出)
        for token_input in token_inputs:
            # 確保這是目標錢包的交易
            if token_input.get("userAccount") != address:
                continue
            
            raw_amount = token_input.get("rawTokenAmount", {})
            token_mint = token_input.get("mint", "")
            token_info = TokenUtils.get_token_info(token_mint)
            token_symbol = token_info.get('symbol', '')
            token_icon = token_info.get('url', '')
            
            if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                
                sol_amount = 0.0
                usdc_amount = 0.0
                
                # 處理SOL輸出 (賣出代幣獲得SOL)
                if native_output and "amount" in native_output:
                    sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                
                # 處理USDC/USDT輸出 (檢查token_outputs是否包含穩定幣)
                else:
                    for output in token_outputs:
                        output_mint = output.get("mint", "")
                        if output_mint in STABLECOINS and output.get("userAccount") == address:
                            output_raw_amount = output.get("rawTokenAmount", {})
                            if output_raw_amount and "tokenAmount" in output_raw_amount and "decimals" in output_raw_amount:
                                usdc_amount = float(output_raw_amount["tokenAmount"]) / (10 ** output_raw_amount["decimals"])
                
                # 只處理有SOL或穩定幣輸出的交易
                if sol_amount > 0 or usdc_amount > 0:
                    tx_data = await process_token_transaction(
                        signature=signature,
                        timestamp=timestamp,
                        tx_type="SELL",
                        source=source,
                        token_mint=token_mint,
                        token_amount=token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=sol_amount,
                        usdc_amount=usdc_amount,
                        address=address,
                        async_session=async_session,
                        wallet_balance_usdt=wallet_balance_usdt,
                        sol_usdt_price=sol_usdt_price,
                        client=client,
                        chain=chain
                    )
                    if tx_data:
                        processed_txs.append(tx_data)
        
        # 處理代幣輸出 (買入)
        for token_output in token_outputs:
            # 確保這是目標錢包的交易
            if token_output.get("userAccount") != address:
                continue
            
            raw_amount = token_output.get("rawTokenAmount", {})
            token_mint = token_output.get("mint", "")
            token_info = TokenUtils.get_token_info(token_mint)
            token_symbol = token_info.get('symbol', '')
            token_icon = token_info.get('url', '')
            
            if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                
                # 處理三種情況：SOL輸入、穩定幣輸入、其他代幣輸入
                sol_amount = 0.0
                usdc_amount = 0.0
                
                # 處理SOL輸入 (用SOL買入代幣)
                if native_input and "amount" in native_input:
                    sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
                # 處理穩定幣輸入 (檢查token_inputs是否包含穩定幣)
                else:
                    for input_token in token_inputs:
                        input_mint = input_token.get("mint", "")
                        if input_mint in STABLECOINS and input_token.get("userAccount") == address:
                            input_raw_amount = input_token.get("rawTokenAmount", {})
                            if input_raw_amount and "tokenAmount" in input_raw_amount and "decimals" in input_raw_amount:
                                usdc_amount = float(input_raw_amount["tokenAmount"]) / (10 ** input_raw_amount["decimals"])
                
                # 只處理有SOL或穩定幣輸入的交易
                if sol_amount > 0 or usdc_amount > 0:
                    tx_data = await process_token_transaction(
                        signature=signature,
                        timestamp=timestamp,
                        tx_type="BUY",
                        source=source,
                        token_mint=token_mint,
                        token_amount=token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=sol_amount,
                        usdc_amount=usdc_amount,
                        address=address,
                        async_session=async_session,
                        wallet_balance_usdt=wallet_balance_usdt,
                        sol_usdt_price=sol_usdt_price,
                        client=client,
                        chain=chain
                    )
                    if tx_data:
                        processed_txs.append(tx_data)
        
        # 新增特殊情況處理 - 檢查是否成功處理了交易，如果沒有，嘗試特殊處理邏輯
        if not processed_txs and native_input and "amount" in native_input and native_input.get("account") == address:
            # 找到用戶實際收到的非SOL/USDC代幣
            for transfer in token_transfers:
                if transfer.get("toUserAccount") == address and transfer.get("mint") not in SUPPORTED_TOKENS:
                    token_mint = transfer.get("mint")
                    token_info = TokenUtils.get_token_info(token_mint)
                    token_symbol = token_info.get('symbol', '')
                    token_icon = token_info.get('url', '')
                    
                    received_amount = 0.0
                    if "tokenAmount" in transfer:
                        try:
                            received_amount = float(transfer["tokenAmount"])
                        except:
                            pass
                    elif "rawTokenAmount" in transfer:
                        raw_amount = transfer["rawTokenAmount"]
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                    
                    if received_amount > 0:
                        sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                        
                        tx_data = await process_token_transaction(
                            signature=signature,
                            timestamp=timestamp,
                            tx_type="BUY",
                            source=source,
                            token_mint=token_mint,
                            token_amount=received_amount,
                            token_symbol=token_symbol,
                            token_icon=token_icon,
                            sol_amount=sol_amount,
                            usdc_amount=0,
                            address=address,
                            async_session=async_session,
                            wallet_balance_usdt=wallet_balance_usdt,
                            sol_usdt_price=sol_usdt_price,
                            client=client,
                            chain=chain
                        )
                        if tx_data:
                            processed_txs.append(tx_data)
                        break  # 只處理第一個找到的轉入代幣
        
        # 返回處理結果
        return processed_txs[0] if processed_txs else None
    
    except Exception as e:
        logging.error(f"分析交易 {transaction.get('signature')} 失败: {str(e)}")
        logging.error(f"异常详细信息: {traceback.format_exc()}")
        return None

async def _analyze_swap_by_token_transfers(transaction, address, async_session, wallet_balance_usdt, sol_usdt_price, client, chain):
    """
    针对 events 为空的特殊 SWAP 交易的处理方法
    """
    try:
        # 定义常量
        STABLECOINS = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ]
        WSOL_MINT = "So11111111111111111111111111111111111111112"
        SOL_DECIMALS = 9

        signature = transaction.get("signature", "")
        timestamp = transaction.get("timestamp", 0)
        source = transaction.get("source", "UNKNOWN")

        # 提取 tokenTransfers 和 nativeTransfers
        token_transfers = transaction.get("tokenTransfers", [])
        native_transfers = transaction.get("nativeTransfers", [])
        account_data = transaction.get("accountData", [])
        
        # 处理特殊交易的代币转账
        processed_txs = []
        
        # 特别处理 PUMP_FUN 源的交易
        if source == "PUMP_FUN":
            # 处理 SOL 的支出情况
            sol_spent = 0
            for native_transfer in native_transfers:
                # 检查用户是否从此钱包发送 SOL
                if native_transfer.get("fromUserAccount") == address:
                    recipient = native_transfer.get("toUserAccount")
                    # 忽略计算预算和其他系统转账
                    if (recipient != "ComputeBudget111111111111111111111111111111" and 
                        recipient != address):
                        sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
            
            # 检查用户是否收到了代币
            token_received = False
            received_token_mint = None
            received_token_amount = 0
            
            for token_transfer in token_transfers:
                if token_transfer.get("toUserAccount") == address:
                    token_mint = token_transfer.get("mint", "")
                    # 跳过稳定币
                    if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                        continue
                    
                    # 提取代币数量
                    raw_amount = token_transfer.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        token_received = True
                        received_token_mint = token_mint
                        received_token_amount = token_amount
                        break
            
            # 如果没有在 tokenTransfers 中找到用户收到的代币，尝试从 accountData 中的 tokenBalanceChanges 寻找
            if not token_received:
                for account in account_data:
                    for token_balance in account.get("tokenBalanceChanges", []):
                        if token_balance.get("userAccount") == address:
                            raw_amount = token_balance.get("rawTokenAmount", {})
                            if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                # 只关注正向变化 (代币增加)
                                if amount_change > 0:
                                    token_mint = token_balance.get("mint")
                                    token_received = True
                                    received_token_mint = token_mint
                                    received_token_amount = amount_change
                                    break
            
            # 如果用户花费了 SOL 并且收到了代币，这是一笔买入交易
            if sol_spent > 0 and token_received:
                token_info = TokenUtils.get_token_info(received_token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                buy_tx_data = await process_token_transaction(
                    signature=signature,
                    timestamp=timestamp,
                    tx_type="BUY",
                    source=source,
                    token_mint=received_token_mint,
                    token_amount=received_token_amount,
                    token_symbol=token_symbol,
                    token_icon=token_icon,
                    sol_amount=sol_spent,
                    usdc_amount=0,
                    address=address,
                    async_session=async_session,
                    wallet_balance_usdt=wallet_balance_usdt,
                    sol_usdt_price=sol_usdt_price,
                    client=client,
                    chain=chain
                )
                
                if buy_tx_data:
                    processed_txs.append(buy_tx_data)
                    return buy_tx_data
        
        # 找出用户卖出和买入的代币
        sold_tokens = []
        bought_tokens = []
        
        for transfer in token_transfers:
            if transfer.get("fromUserAccount") == address:
                sold_tokens.append(transfer)
            elif transfer.get("toUserAccount") == address:
                bought_tokens.append(transfer)
        
        # 计算 SOL 变化
        sol_balance_change = 0
        for account in account_data:
            if account.get("account") == address:
                sol_balance_change = account.get("nativeBalanceChange", 0) / (10 ** SOL_DECIMALS)
                break
        
        # 确定交易类型和金额
        if sold_tokens and not bought_tokens:
            # 卖出代币获得 SOL
            for sold_token in sold_tokens:
                token_mint = sold_token.get("mint")
                
                # 排除稳定币和已知的其他币种
                if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                    continue
                
                # 查找代币信息
                token_info = TokenUtils.get_token_info(token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                # 获取代币数量（从 rawTokenAmount 或 tokenAmount）
                token_amount = 0
                raw_amount = sold_token.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                elif "tokenAmount" in sold_token:
                    token_amount = float(sold_token.get("tokenAmount", 0))
                else:
                    # 查找 accountData 中的详细信息获取 rawTokenAmount
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("mint") == token_mint and token_balance.get("userAccount") == address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                    # 使用绝对值，因为卖出时金额为负
                                    token_amount = abs(token_amount)
                                break
                
                # SOL 收入金额（正值表示收到 SOL）
                sol_amount = abs(sol_balance_change) if sol_balance_change > 0 else 0
                
                # 检查是否找到了有效的 token_amount 和 sol_amount
                if token_amount <= 0 or sol_amount <= 0:
                    continue
                
                # 创建卖出交易记录
                sell_tx_data = await process_token_transaction(
                    signature=signature,
                    timestamp=timestamp,
                    tx_type="SELL",
                    source=source,
                    token_mint=token_mint,
                    token_amount=token_amount,
                    token_symbol=token_symbol,
                    token_icon=token_icon,
                    sol_amount=sol_amount,
                    usdc_amount=0,
                    address=address,
                    async_session=async_session,
                    wallet_balance_usdt=wallet_balance_usdt,
                    sol_usdt_price=sol_usdt_price,
                    client=client,
                    chain=chain
                )
                
                if sell_tx_data:
                    processed_txs.append(sell_tx_data)
        
        elif bought_tokens and not sold_tokens:
            # 买入代币消耗 SOL
            for bought_token in bought_tokens:
                token_mint = bought_token.get("mint")
                
                # 排除稳定币和已知的其他币种
                if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                    continue
                
                # 查找代币信息
                token_info = TokenUtils.get_token_info(token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                # 获取代币数量（从 rawTokenAmount 或 tokenAmount）
                token_amount = 0
                raw_amount = bought_token.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                elif "tokenAmount" in bought_token:
                    token_amount = float(bought_token.get("tokenAmount", 0))
                else:
                    # 查找 accountData 中的详细信息获取 rawTokenAmount
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("mint") == token_mint and token_balance.get("userAccount") == address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                    # 买入时金额为正
                                    token_amount = abs(token_amount)
                                break
                
                # SOL 支出金额（负值表示支出 SOL）
                sol_amount = abs(sol_balance_change) if sol_balance_change < 0 else 0
                
                # 如果没有直接在 nativeBalanceChange 中找到 SOL 支出，尝试从 nativeTransfers 中查找
                if sol_amount == 0:
                    for native_transfer in native_transfers:
                        if native_transfer.get("fromUserAccount") == address and native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111":
                            sol_amount += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                
                # 检查是否找到了有效的 token_amount 和 sol_amount
                if token_amount <= 0 or sol_amount <= 0:
                    continue
                
                # 创建买入交易记录，使用原始 signature
                buy_tx_data = await process_token_transaction(
                    signature=signature,
                    timestamp=timestamp,
                    tx_type="BUY",
                    source=source,
                    token_mint=token_mint,
                    token_amount=token_amount,
                    token_symbol=token_symbol,
                    token_icon=token_icon,
                    sol_amount=sol_amount,
                    usdc_amount=0,
                    address=address,
                    async_session=async_session,
                    wallet_balance_usdt=wallet_balance_usdt,
                    sol_usdt_price=sol_usdt_price,
                    client=client,
                    chain=chain
                )
                
                if buy_tx_data:
                    processed_txs.append(buy_tx_data)
        
        elif sold_tokens and bought_tokens:
            # 代币对代币交易（memecoin 对 memecoin）
            # 先处理卖出
            for idx, sold_token in enumerate(sold_tokens):
                token_mint = sold_token.get("mint")
                
                # 排除稳定币和已知的其他币种
                if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                    continue
                
                token_info = TokenUtils.get_token_info(token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                # 获取代币数量
                token_amount = 0
                raw_amount = sold_token.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                elif "tokenAmount" in sold_token:
                    token_amount = float(sold_token.get("tokenAmount", 0))
                else:
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("mint") == token_mint and token_balance.get("userAccount") == address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount:
                                    token_amount = abs(float(raw_amount.get("tokenAmount", 0)) / (10 ** raw_amount.get("decimals", 0)))
                                    break
                
                # 估计中间 SOL 金额（从 nativeTransfers 中查找最大的非 ComputeBudget 转账）
                intermediate_sol_amount = 0
                for native_transfer in native_transfers:
                    # 尝试找出最大的非手续费 SOL 转账作为中间金额
                    if (native_transfer.get("amount", 0) > 10000000 and  # 0.01 SOL 以上
                        native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111"):
                        transfer_amount = float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                        if transfer_amount > intermediate_sol_amount:
                            intermediate_sol_amount = transfer_amount
                
                # 卖出交易的 SOL 金额设为估计的中间金额
                if token_amount > 0:
                    sell_tx_data = await process_token_transaction(
                        signature=signature,  # 添加后缀以避免主键冲突
                        timestamp=timestamp,
                        tx_type="SELL",
                        source=source,
                        token_mint=token_mint,
                        token_amount=token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=intermediate_sol_amount,
                        usdc_amount=0,
                        address=address,
                        async_session=async_session,
                        wallet_balance_usdt=wallet_balance_usdt,
                        sol_usdt_price=sol_usdt_price,
                        client=client,
                        chain=chain
                    )
                    
                    if sell_tx_data:
                        processed_txs.append(sell_tx_data)
            
            # 再处理买入
            for idx, bought_token in enumerate(bought_tokens):
                token_mint = bought_token.get("mint")
                
                # 排除稳定币和已知的其他币种
                if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                    continue
                
                token_info = TokenUtils.get_token_info(token_mint)
                token_symbol = token_info.get('symbol', '')
                token_icon = token_info.get('url', '')
                
                # 获取代币数量
                token_amount = 0
                raw_amount = bought_token.get("rawTokenAmount", {})
                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                elif "tokenAmount" in bought_token:
                    token_amount = float(bought_token.get("tokenAmount", 0))
                else:
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("mint") == token_mint and token_balance.get("userAccount") == address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount:
                                    token_amount = abs(float(raw_amount.get("tokenAmount", 0)) / (10 ** raw_amount.get("decimals", 0)))
                                    break
                
                # 使用与卖出相同的中间 SOL 金额
                intermediate_sol_amount = 0
                for native_transfer in native_transfers:
                    # 尝试找出最大的非手续费 SOL 转账作为中间金额
                    if (native_transfer.get("amount", 0) > 10000000 and  # 0.01 SOL 以上
                        native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111"):
                        transfer_amount = float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                        if transfer_amount > intermediate_sol_amount:
                            intermediate_sol_amount = transfer_amount
                
                # 买入交易的 SOL 金额设为估计的中间金额
                if token_amount > 0:
                    buy_tx_data = await process_token_transaction(
                        signature=signature,  # 添加后缀以避免主键冲突
                        timestamp=timestamp,
                        tx_type="BUY",
                        source=source,
                        token_mint=token_mint,
                        token_amount=token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=intermediate_sol_amount,
                        usdc_amount=0,
                        address=address,
                        async_session=async_session,
                        wallet_balance_usdt=wallet_balance_usdt,
                        sol_usdt_price=sol_usdt_price,
                        client=client,
                        chain=chain
                    )
                    
                    if buy_tx_data:
                        processed_txs.append(buy_tx_data)
        
        else:
            # 检查是否是通过 nativeTransfers 交易的
            # 找出用户支付的 SOL
            sol_spent = 0
            for transfer in native_transfers:
                if transfer.get("fromUserAccount") == address:
                    # 不包括给平台的小额费用，只计算主要交易金额
                    if transfer.get("amount", 0) > 10000000:  # 0.01 SOL 以上的交易
                        sol_spent += float(transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
            
            # 处理 accountData 中的 tokenBalanceChanges
            for account in account_data:
                for token_balance in account.get("tokenBalanceChanges", []):
                    if token_balance.get("userAccount") == address:
                        raw_amount = token_balance.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            token_mint = token_balance.get("mint")
                            
                            # 排除稳定币和已知的其他币种
                            if token_mint in STABLECOINS or token_mint == WSOL_MINT:
                                continue
                            
                            token_info = TokenUtils.get_token_info(token_mint)
                            token_symbol = token_info.get('symbol', '')
                            token_icon = token_info.get('url', '')
                            
                            if token_amount > 0:  # 买入
                                buy_tx_data = await process_token_transaction(
                                    signature=signature,  # 使用原始 signature
                                    timestamp=timestamp,
                                    tx_type="BUY",
                                    source=source,
                                    token_mint=token_mint,
                                    token_amount=token_amount,
                                    token_symbol=token_symbol,
                                    token_icon=token_icon,
                                    sol_amount=sol_spent,
                                    usdc_amount=0,
                                    address=address,
                                    async_session=async_session,
                                    wallet_balance_usdt=wallet_balance_usdt,
                                    sol_usdt_price=sol_usdt_price,
                                    client=client,
                                    chain=chain
                                )
                                
                                if buy_tx_data:
                                    processed_txs.append(buy_tx_data)
                            
                            elif token_amount < 0:  # 卖出
                                token_amount = abs(token_amount)
                                # 卖出获得的 SOL 是余额变化的绝对值
                                sol_received = abs(sol_balance_change) if sol_balance_change > 0 else 0
                                
                                sell_tx_data = await process_token_transaction(
                                    signature=signature,  # 使用原始 signature
                                    timestamp=timestamp,
                                    tx_type="SELL",
                                    source=source,
                                    token_mint=token_mint,
                                    token_amount=token_amount,
                                    token_symbol=token_symbol,
                                    token_icon=token_icon,
                                    sol_amount=sol_received,
                                    usdc_amount=0,
                                    address=address,
                                    async_session=async_session,
                                    wallet_balance_usdt=wallet_balance_usdt,
                                    sol_usdt_price=sol_usdt_price,
                                    client=client,
                                    chain=chain
                                )
                                
                                if sell_tx_data:
                                    processed_txs.append(sell_tx_data)
        
        return processed_txs[0] if processed_txs else None
    
    except Exception as e:
        logging.error(f"特殊 SWAP 交易分析失败: {str(e)}")
        logging.error(f"异常详细信息: {traceback.format_exc()}")
        return None

async def process_token_transaction(signature, timestamp, tx_type, source, token_mint, token_amount, token_symbol, token_icon,
                                   sol_amount, usdc_amount, address, async_session, wallet_balance_usdt, sol_usdt_price, client, chain,
                                   from_token_mint=None, from_token_symbol=None, from_token_amount=None,
                                   dest_token_mint=None, dest_token_symbol=None, dest_token_amount=None):
    """
    處理並保存一筆代幣交易到資料庫
    處理PnL計算和TokenBuyData更新邏輯
    增加詳細日志以調試問題
    """
    try:
        # 記錄關鍵輸入參數
        # logging.info(f"處理交易 {signature}: type={tx_type}, token={token_mint}({token_symbol}), " +
        #              f"amount={token_amount}, sol_amount={sol_amount}, usdc_amount={usdc_amount}")
        token_info = TokenUtils.get_token_info(token_mint)
        supply = token_info.get('supply', 0)
        # token_price = token_info.get('priceUsd', '')

        # 計算USD價值
        if sol_amount > 0:
            usd_amount = sol_amount * sol_usdt_price
            # logging.info(f"使用SOL計算USD: {sol_amount} SOL * {sol_usdt_price} = {usd_amount} USD")
        elif usdc_amount > 0:
            usd_amount = usdc_amount
            # logging.info(f"使用穩定幣: {usdc_amount} USD")
        else:
            usd_amount = 0
            # logging.warning(f"交易 {signature} 沒有SOL或穩定幣金額！")
        
        # supply = await get_token_supply(client, token_mint) or 0

        # 計算代幣價格
        token_price = usd_amount / token_amount if token_amount > 0 else 0
        value = token_price * token_amount
        # logging.info(f"計算代幣價格: {usd_amount} / {token_amount} = {token_price}")
        
        marketcap = token_price * supply
        
        # 定义常量
        WSOL_MINT = "So11111111111111111111111111111111111111112"
        STABLECOINS = [
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v",  # USDC
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"   # USDT
        ]
        
        # 確保token_symbol不為空
        if not token_symbol:
            token_symbol = token_info.get('symbol', '')
            if not token_symbol:
                token_symbol = token_mint[:6] + "..."  # 使用地址前6位作為符號
            # logging.info(f"更新代幣符號: {token_symbol}")
            
        # 處理默認的 from/dest token 數據
        if tx_type == "BUY":
            # 如果是買入，默認 from_token 是支付的代幣 (SOL 或穩定幣)
            if from_token_mint is None:
                from_token_mint = WSOL_MINT  # 默認為SOL
                for stablecoin in STABLECOINS:
                    if usdc_amount > 0:
                        from_token_mint = stablecoin
                        break
                        
            if from_token_symbol is None:
                if from_token_mint == WSOL_MINT:
                    from_token_symbol = "SOL"
                elif from_token_mint in STABLECOINS:
                    from_token_symbol = "USDC" if from_token_mint == STABLECOINS[0] else "USDT"
                else:
                    from_token_info = TokenUtils.get_token_info(from_token_mint)
                    from_token_symbol = from_token_info.get('symbol', '')
                    
            if from_token_amount is None:
                if from_token_mint == WSOL_MINT:
                    from_token_amount = sol_amount
                else:
                    from_token_amount = usdc_amount
                    
            # 目標代幣是買入的代幣
            if dest_token_mint is None:
                dest_token_mint = token_mint
                
            if dest_token_symbol is None:
                dest_token_symbol = token_symbol
                
            if dest_token_amount is None:
                dest_token_amount = token_amount
                
        else:  # tx_type == "SELL"
            # 如果是賣出，from_token 是賣出的代幣
            if from_token_mint is None:
                from_token_mint = token_mint
                
            if from_token_symbol is None:
                from_token_symbol = token_symbol
                
            if from_token_amount is None:
                from_token_amount = token_amount
                
            # 目標代幣是收款的代幣 (SOL 或穩定幣)
            if dest_token_mint is None:
                dest_token_mint = WSOL_MINT  # 默認為SOL
                for stablecoin in STABLECOINS:
                    if usdc_amount > 0:
                        dest_token_mint = stablecoin
                        break
                        
            if dest_token_symbol is None:
                if dest_token_mint == WSOL_MINT:
                    dest_token_symbol = "SOL"
                elif dest_token_mint in STABLECOINS:
                    dest_token_symbol = "USDC" if dest_token_mint == STABLECOINS[0] else "USDT"
                else:
                    dest_token_info = TokenUtils.get_token_info(dest_token_mint)
                    dest_token_symbol = dest_token_info.get('symbol', '')
                    
            if dest_token_amount is None:
                if dest_token_mint == WSOL_MINT:
                    dest_token_amount = sol_amount
                else:
                    dest_token_amount = usdc_amount
            
        if tx_type == "BUY":
            # 買入交易邏輯
            
            # 向資料庫獲取先前的代幣數據
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            
            old_total_amount = token_data.get("total_amount", 0)
            old_total_cost = token_data.get("total_cost", 0)
            
            # 計算新的總數量和總成本
            new_total_amount = old_total_amount + token_amount
            new_total_cost = old_total_cost + usd_amount
            
            # 計算新的平均買入價格
            if new_total_amount > 0:
                avg_buy_price = new_total_cost / new_total_amount
            else:
                avg_buy_price = 0
            
            # 更新 TokenBuyData
            token_buy_data = {
                "token_address": token_mint,
                "avg_buy_price": avg_buy_price,
                "total_amount": new_total_amount,
                "total_cost": new_total_cost
            }
            await save_wallet_buy_data(token_buy_data, address, async_session, chain)
            
            # 買入時：value / 錢包當前餘額 * 100 (不超過100%)
            if wallet_balance_usdt > 0:
                holding_percentage = min(100, (value / (value + wallet_balance_usdt)) * 100)
            else:
                holding_percentage = 100  # 如果錢包餘額為0，則設為100%
            
            # 確定支付代幣
            payment_token_address = from_token_mint
            
            # 構建要儲存的交易資料
            tx_data = {
                "wallet_address": address,
                "wallet_balance": wallet_balance_usdt,
                "token_address": token_mint,
                "token_name": token_symbol,
                "token_icon": token_icon,
                "price": token_price,
                "amount": token_amount,
                "marketcap": marketcap,
                "value": value,
                "chain": chain,
                "transaction_type": "buy",
                "transaction_time": timestamp,
                "time": datetime.now(timezone(timedelta(hours=8))),
                "signature": signature,
                "holding_percentage": holding_percentage,
                "realized_profit": 0,
                "realized_profit_percentage": 0,
                # 添加额外信息供API使用
                "sol_amount": sol_amount,
                "usdc_amount": usdc_amount,
                "sol_usdt_price": sol_usdt_price,
                "payment_token_address": payment_token_address,
                "source": source,
                # 新增六個欄位
                "from_token_address": from_token_mint,
                "from_token_symbol": from_token_symbol,
                "from_token_amount": from_token_amount,
                "dest_token_address": dest_token_mint,
                "dest_token_symbol": dest_token_symbol,
                "dest_token_amount": dest_token_amount
            }
            
            # 儲存交易
            await save_past_transaction(async_session, tx_data, address, signature, chain)
            # logging.info(f"成功保存買入交易: {signature}, SOL金額={sol_amount}")
            return tx_data
            
        else:  # tx_type == "SELL"
            # 賣出交易邏輯
            
            # 獲取代幣買入數據
            token_data = await token_buy_data_cache.get_token_data(
                address, token_mint, async_session, chain
            )
            
            if token_data and token_data.get("total_amount", 0) > 0:
                # 有買入記錄，計算盈虧
                avg_buy_price = token_data.get("avg_buy_price", 0)
                total_amount = token_data.get("total_amount", 0)
                total_cost = token_data.get("total_cost", 0)
                  
                # 計算賣出時的總持倉數量（當前餘額 + 賣出量）
                total_holdings = total_amount
                
                # 計算比例，確保不超過100%
                if total_holdings > 0:
                    holding_percentage = min(100, (token_amount / total_holdings) * 100)
                else:
                    holding_percentage = 100  # 如果無持倉，則設為100%
                
                # 計算實現的盈虧
                if avg_buy_price > 0:
                    realized_profit = (token_price - avg_buy_price) * token_amount
                    realized_profit_percentage = ((token_price / avg_buy_price) - 1) * 100
                    realized_profit_percentage = max(realized_profit_percentage, -100)  # 確保下限為 -100%
                else:
                    realized_profit = usd_amount  # 如果沒有買入記錄，全部視為利潤
                    realized_profit_percentage = 100
                
                # 更新剩餘代幣數量和成本
                new_total_amount = max(0, total_amount - token_amount)
                new_total_cost = 0
                if new_total_amount > 0 and total_amount > 0:
                    # 按比例減少總成本
                    remaining_ratio = new_total_amount / total_amount
                    new_total_cost = total_cost * remaining_ratio
                
                # 更新 TokenBuyData
                token_buy_data = {
                    "token_address": token_mint,
                    "avg_buy_price": avg_buy_price,  # 平均價格不變
                    "total_amount": new_total_amount,
                    "total_cost": new_total_cost
                }
                await save_wallet_buy_data(token_buy_data, address, async_session, chain)
                
            else:
                # 沒有買入記錄的情況（可能是空投等）
                holding_percentage = 100
                realized_profit = usd_amount  # 全部視為利潤
                realized_profit_percentage = 100
            
            # 確定收款代幣
            received_token_address = dest_token_mint
            
            # 重要：確保sol_amount不為0
            if sol_amount <= 0 and usd_amount > 0 and sol_usdt_price > 0:
                # 嘗試根據USD價值估算SOL數量
                estimated_sol = usd_amount / sol_usdt_price
                if estimated_sol > 0:
                    sol_amount = estimated_sol
                    # logging.info(f"卖出交易：估算SOL数量 {usd_amount} USD / {sol_usdt_price} = {sol_amount} SOL")
            
            # 構建要儲存的交易資料
            tx_data = {
                "wallet_address": address,
                "wallet_balance": wallet_balance_usdt,
                "token_address": token_mint,
                "token_name": token_symbol,
                "token_icon": token_icon,
                "price": token_price,
                "amount": token_amount,
                "marketcap": marketcap,
                "value": value,
                "chain": chain,
                "transaction_type": "sell",
                "transaction_time": timestamp,
                "time": datetime.now(timezone(timedelta(hours=8))),
                "signature": signature,
                "holding_percentage": holding_percentage,
                "realized_profit": realized_profit,
                "realized_profit_percentage": realized_profit_percentage,
                # 添加额外信息供API使用
                "sol_amount": sol_amount,
                "usdc_amount": usdc_amount,
                "sol_usdt_price": sol_usdt_price,
                "received_token_address": received_token_address,
                "source": source,
                # 新增六個欄位
                "from_token_address": from_token_mint,
                "from_token_symbol": from_token_symbol,
                "from_token_amount": from_token_amount,
                "dest_token_address": dest_token_mint,
                "dest_token_symbol": dest_token_symbol,
                "dest_token_amount": dest_token_amount
            }
            
            # 儲存交易
            await save_past_transaction(async_session, tx_data, address, signature, chain)
            # logging.info(f"成功保存卖出交易: {signature}, SOL金额={sol_amount}")
            return tx_data
    
    except Exception as e:
        logging.error(f"處理交易時發生錯誤: {str(e)}")
        logging.error(f"异常详细信息: {traceback.format_exc()}")
        return None

def now_utc8_str():
    return datetime.now(timezone(timedelta(hours=8))).strftime('%Y-%m-%d %H:%M:%S')

# 新增：分析結束後合併臨時表到主表的工具函數
async def merge_all_tmp_to_main(session):
    # 根據你的主鍵與欄位設計，這裡分別設置
    await merge_tmp_to_main(session, 'wallet_holding', ['wallet_address', 'token_address', 'chain'], [
        'token_icon', 'token_name', 'chain_id', 'amount', 'value', 'value_usdt', 'unrealized_profits', 'pnl', 'pnl_percentage', 'avg_price', 'marketcap', 'is_cleared', 'cumulative_cost', 'cumulative_profit', 'last_transaction_time', 'time'])
    await merge_tmp_to_main(session, 'wallet', ['wallet_address'], [
        'balance', 'balance_usd', 'chain', 'chain_id', 'tag', 'twitter_name', 'twitter_username', 'is_smart_wallet', 'wallet_type', 'asset_multiple', 'token_list', 'avg_cost_30d', 'avg_cost_7d', 'avg_cost_1d', 'total_transaction_num_30d', 'total_transaction_num_7d', 'total_transaction_num_1d', 'buy_num_30d', 'buy_num_7d', 'buy_num_1d', 'sell_num_30d', 'sell_num_7d', 'sell_num_1d', 'win_rate_30d', 'win_rate_7d', 'win_rate_1d', 'pnl_30d', 'pnl_7d', 'pnl_1d', 'pnl_percentage_30d', 'pnl_percentage_7d', 'pnl_percentage_1d', 'pnl_pic_30d', 'pnl_pic_7d', 'pnl_pic_1d', 'unrealized_profit_30d', 'unrealized_profit_7d', 'unrealized_profit_1d', 'total_cost_30d', 'total_cost_7d', 'total_cost_1d', 'avg_realized_profit_30d', 'avg_realized_profit_7d', 'avg_realized_profit_1d', 'distribution_gt500_30d', 'distribution_200to500_30d', 'distribution_0to200_30d', 'distribution_0to50_30d', 'distribution_lt50_30d', 'distribution_gt500_percentage_30d', 'distribution_200to500_percentage_30d', 'distribution_0to200_percentage_30d', 'distribution_0to50_percentage_30d', 'distribution_lt50_percentage_30d', 'distribution_gt500_7d', 'distribution_200to500_7d', 'distribution_0to200_7d', 'distribution_0to50_7d', 'distribution_lt50_7d', 'distribution_gt500_percentage_7d', 'distribution_200to500_percentage_7d', 'distribution_0to200_percentage_7d', 'distribution_0to50_percentage_7d', 'distribution_lt50_percentage_7d', 'update_time', 'last_transaction_time', 'is_active'])
    await merge_tmp_to_main(session, 'wallet_transaction', ['signature'], [
        'wallet_address', 'wallet_balance', 'token_address', 'token_icon', 'token_name', 'price', 'amount', 'marketcap', 'value', 'holding_percentage', 'chain', 'chain_id', 'realized_profit', 'realized_profit_percentage', 'transaction_type', 'transaction_time', 'time', 'from_token_address', 'from_token_symbol', 'from_token_amount', 'dest_token_address', 'dest_token_symbol', 'dest_token_amount'])
    await merge_tmp_to_main(session, 'wallet_buy_data', ['wallet_address', 'token_address'], [
        'total_amount', 'total_cost', 'avg_buy_price', 'position_opened_at', 'chain', 'chain_id', 'historical_total_buy_amount', 'historical_total_buy_cost', 'historical_total_sell_amount', 'historical_total_sell_value', 'historical_avg_buy_price', 'historical_avg_sell_price', 'last_active_position_closed_at', 'last_transaction_time', 'date', 'realized_profit', 'updated_at'])
    await merge_tmp_to_main(session, 'wallet_token_state', ['wallet_address', 'token_address'], [
        'chain', 'chain_id', 'current_amount', 'current_total_cost', 'current_avg_buy_price', 'position_opened_at', 'historical_buy_amount', 'historical_buy_cost', 'historical_sell_amount', 'historical_sell_value', 'historical_buy_count', 'historical_sell_count', 'last_transaction_time', 'historical_realized_pnl', 'updated_at'])