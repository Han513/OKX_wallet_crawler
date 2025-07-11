# -------------------------------------------------------------------------------------------------------------------
import logging
import base58
import aiohttp
import asyncio
import time
import json
import os
import requests
import pandas as pd
from collections import deque
from solders.pubkey import Pubkey
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Tuple, Optional, Deque
from models import save_past_transaction, get_token_buy_data, save_wallet_buy_data, write_wallet_data_to_db
from token_info import TokenUtils
from smart_wallet_filter import filter_smart_wallets, filter_smart_wallets2
from WalletHolding import calculate_remaining_tokens
from decimal import Decimal

# 定義常量
API_URL = "https://api.helius.xyz/v0/addresses/{}/transactions"
TARGET_WALLET = "5XkYg6pmmUXkD9F3A6ahGVnn7BrgTtrLSX8TB3fLRvMr"
LIMIT = 100
SOL_DECIMALS = 9
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
WSOL_MINT = "So11111111111111111111111111111111111111112"

# 穩定幣和原生代幣列表 (用於過濾交易)
STABLECOINS = [USDC_MINT, USDT_MINT]
NATIVE_TOKENS = [WSOL_MINT]
SUPPORTED_TOKENS = STABLECOINS + NATIVE_TOKENS

token_supply_cache = {}

class DecimalEncoder(json.JSONEncoder):
    """
    自定义JSON编码器，确保小数值和日期时间使用标准格式
    """
    def default(self, obj):
        if isinstance(obj, Decimal):
            # 直接转换 Decimal 为字符串
            return str(obj)
        if isinstance(obj, float):
            # 使用 Decimal 来确保不用科学计数法
            return str(Decimal(str(obj)))
        if isinstance(obj, datetime):
            # 将 datetime 转换为 ISO 格式字符串
            return obj.isoformat()
        return super(DecimalEncoder, self).default(obj)

class TokenData:
    """提供代幣相關的工具函數"""
    _token_cache = {
        WSOL_MINT: {"symbol": "WSOL", "icon": ""},
        USDC_MINT: {"symbol": "USDC", "icon": ""},
        USDT_MINT: {"symbol": "USDT", "icon": ""}
    }

    @classmethod
    def _get_token_symbol(cls, token_mint: str) -> Tuple[str, str]:
        """獲取代幣符號與圖標 (基於已知的 mint 地址)，未知代幣則查詢並快取"""

        # 如果代幣已經在快取中，直接返回
        if token_mint in cls._token_cache:
            token_info = cls._token_cache[token_mint]
            return token_info["symbol"], token_info["icon"]

        # 調用 API 獲取代幣資訊
        token_info = TokenUtils.get_token_info(token_mint)

        # 解析獲取的資料
        token_symbol = token_info.get("symbol", '')
        token_icon = token_info.get("url", "")

        # 儲存到快取，減少未來請求
        cls._token_cache[token_mint] = {"symbol": token_symbol, "icon": token_icon}

        return token_symbol, token_icon

def get_sol_price():
    """
    獲取SOL的即時價格（美元）
    """
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{WSOL_MINT}"
        response = requests.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                return float(data['pairs'][0].get('priceUsd', 0))
        return 141.91  # 默認SOL價格，如果API請求失敗
    except Exception as e:
        print(f"獲取SOL價格時出錯: {str(e)}")
        return 141.91  # 出錯時的默認價格
    
class TokenTransaction:
    """代表一個代幣交易，包含買入或賣出信息"""
    def __init__(self, signature: str, timestamp: int, tx_type: str, source: str, 
             token_mint: str, token_amount: float, token_symbol: str, token_icon: str,
             sol_amount: float = 0, usdc_amount: float = 0, from_token_mint: str = None,
             from_token_amount: float = 0, from_token_symbol: str = None,
             dest_token_mint: str = None, dest_token_amount: float = 0, dest_token_symbol: str = None):
        # 使用全局SOL價格轉換SOL金額為美元
        try:
            global wallet_analyzer
            if 'wallet_analyzer' in globals() and hasattr(wallet_analyzer, 'sol_price_usd'):
                self.sol_price_usd = wallet_analyzer.sol_price_usd
            else:
                self.sol_price_usd = 141.91  # 默認值
        except:
            self.sol_price_usd = 141.91
        
        self.signature = signature
        self.timestamp = timestamp
        self.date = datetime.fromtimestamp(timestamp)
        self.tx_type = tx_type  # BUY 或 SELL
        self.source = source  # 平台來源
        self.token_mint = token_mint
        self.token_amount = token_amount
        self.token_symbol = token_symbol
        self.token_icon = token_icon
        self.sol_amount = sol_amount  # SOL 的数量
        self.usdc_amount = usdc_amount  # USDC 金額 (如果是 USDC 交易)
        
        # 源幣種信息
        self.from_token_mint = from_token_mint
        self.from_token_amount = from_token_amount
        self.from_token_symbol = from_token_symbol
        
        # 新增：目標幣種信息
        self.dest_token_mint = dest_token_mint
        self.dest_token_amount = dest_token_amount
        self.dest_token_symbol = dest_token_symbol
        
        # 将 sol_amount 转换为美元金额 (sol_amount * sol_price_usd)
        self.sol_in_usd = self.sol_amount * self.sol_price_usd  # SOL 以美元表示
        
        self.token_price = self._calculate_price()
        
        # 计算美元金额
        self.usd_amount = self._calculate_usd_amount()
    
    # def _calculate_price(self) -> float:
    #     token_info = TokenUtils.get_token_info(self.token_mint)
    #     token_price = token_info.get('priceUsd', 0)
    #     if token_price == 0:
    #         """計算代幣價格 (以 SOL 或 USDC 計價)"""
    #         if self.token_amount <= 0:
    #             return 0
    #         if self.sol_amount > 0:
    #             return self.sol_in_usd / self.token_amount
    #         elif self.usdc_amount > 0:
    #             return self.usdc_amount / self.token_amount
    #         else:
    #             return 0
    #     else:
    #         return token_price

    def _calculate_price(self) -> float:
        """計算代幣價格 (以 SOL 或 USDC 計價)"""
        if self.token_amount <= 0:
            return 0
        if self.sol_amount > 0:
            return self.sol_in_usd / self.token_amount
        elif self.usdc_amount > 0:
            return self.usdc_amount / self.token_amount
        else:
            return 0
    
    def _calculate_usd_amount(self) -> float:
        """計算交易的美元金額"""
        if self.sol_in_usd > 0:
            return self.sol_in_usd  # 如果是 SOL 交易，就用 SOL 的美元价值
        elif self.usdc_amount > 0:
            return self.usdc_amount
        else:
            return 0
    
    def __str__(self) -> str:
        if self.sol_in_usd > 0:
            price_currency = "SOL"
            price_amount = self.sol_in_usd
            usd_equivalent = f"(≈ ${self.usd_amount:.2f})"
        elif self.usdc_amount > 0:
            price_currency = "USDC"
            price_amount = self.usdc_amount
            usd_equivalent = ""
        else:
            price_currency = "UNKNOWN"
            price_amount = 0
            usd_equivalent = ""
        
        # 添加完整交易路徑信息
        swap_path = ""
        if self.from_token_symbol and self.dest_token_symbol:
            swap_path = f" ({self.from_token_amount:.6f} {self.from_token_symbol} → {self.dest_token_amount:.6f} {self.dest_token_symbol})"
        
        return (f"[{self.date}] {self.tx_type} {self.token_amount:.6f} {self.token_symbol} "
                f"for {price_amount:.6f} {price_currency} {usd_equivalent}"
                f"{swap_path} "
                f"@ {self.token_price:.8f} {price_currency}/token "
                f"(Source: {self.source})")

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

class TokenLot:
    """表示一批購買的代幣，用於FIFO計算"""
    def __init__(self, token_amount: float, cost_sol: float = 0, cost_usdc: float = 0, timestamp: int = 0):
        self.token_amount = token_amount
        self.cost_sol = cost_sol  # 以SOL為單位的成本
        self.cost_usdc = cost_usdc  # 以USDC為單位的成本
        self.timestamp = timestamp
        self.unit_cost_sol = cost_sol / token_amount if token_amount > 0 and cost_sol > 0 else 0
        self.unit_cost_usdc = cost_usdc / token_amount if token_amount > 0 and cost_usdc > 0 else 0
    
    def __str__(self) -> str:
        if self.cost_sol > 0:
            return f"{self.token_amount:.6f} tokens @ {self.unit_cost_sol:.8f} SOL/token = {self.cost_sol:.6f} SOL"
        elif self.cost_usdc > 0:
            return f"{self.token_amount:.6f} tokens @ {self.unit_cost_usdc:.8f} USDC/token = {self.cost_usdc:.6f} USDC"
        else:
            return f"{self.token_amount:.6f} tokens (unknown cost)"


class TokenPortfolio:
    """管理特定代幣的交易組合，追蹤買入/賣出和計算盈虧"""
    def __init__(self, token_mint: str, token_symbol: str):
        self.token_mint = token_mint
        self.token_symbol = token_symbol
        self.transactions: List[TokenTransaction] = []
        self.total_bought = 0.0
        self.total_sold = 0.0
        self.total_cost_sol = 0.0
        self.total_cost_usdc = 0.0
        self.total_revenue_sol = 0.0
        self.total_revenue_usdc = 0.0
        self.avg_buy_price_sol = 0.0
        self.avg_buy_price_usdc = 0.0
        self.avg_sell_price_sol = 0.0
        self.avg_sell_price_usdc = 0.0
        self.remaining_tokens = 0.0
        self.weighted_avg_cost = 0.0  # 加權平均成本
        self.currency = "UNKNOWN"  # 主要使用的貨幣 (SOL 或 USDC)
        self.quick_trades_count = 0  # 1分鐘內完成同幣種買入及賣出交易的次數
        
        # FIFO處理
        self.token_lots: Deque[TokenLot] = deque()  # 使用雙端隊列存儲代幣批次
        self.realized_pnl_sol = 0.0
        self.realized_pnl_usdc = 0.0
    
    def add_transaction(self, tx: TokenTransaction):
        """添加交易並更新統計數據"""
        # 確保按時間順序添加交易
        self.transactions.append(tx)
        self.transactions.sort(key=lambda t: t.timestamp)
        
        # 清空先前的統計數據
        self.total_bought = 0.0
        self.total_sold = 0.0
        self.total_cost_sol = 0.0
        self.total_cost_usdc = 0.0
        self.total_revenue_sol = 0.0
        self.total_revenue_usdc = 0.0
        self.remaining_tokens = 0.0
        self.token_lots.clear()
        self.realized_pnl_sol = 0.0
        self.realized_pnl_usdc = 0.0
        
        # 按時間順序重新處理所有交易
        for tx_item in self.transactions:
            self._process_transaction(tx_item)
        
        # 計算平均價格和其他指標
        self._calculate_averages()
        
        # 檢查是否有1分鐘內的快速交易
        self._check_for_quick_trades()
    
    def _process_transaction(self, tx: TokenTransaction):
        """按FIFO處理單個交易"""
        if tx.tx_type == "BUY":
            # 買入交易: 添加新的代幣批次
            self.total_bought += tx.token_amount
            
            if tx.sol_amount > 0:
                # 使用原始SOL數量，不轉換為美元
                self.total_cost_sol += tx.sol_amount
                
                if self.currency == "UNKNOWN":
                    self.currency = "SOL"
                
                # 添加新批次，保存原始SOL數量和等值美元成本
                self.token_lots.append(TokenLot(
                    token_amount=tx.token_amount,
                    cost_sol=tx.sol_amount,  # 保存SOL數量
                    cost_usdc=tx.sol_amount * tx.sol_price_usd,  # 同時保存等值美元
                    timestamp=tx.timestamp
                ))
            
            elif tx.usdc_amount > 0:
                # USDC 交易保持不變
                self.total_cost_usdc += tx.usdc_amount
                
                if self.currency == "UNKNOWN":
                    self.currency = "USDC"
                
                self.token_lots.append(TokenLot(
                    token_amount=tx.token_amount,
                    cost_usdc=tx.usdc_amount,
                    cost_sol=tx.usdc_amount / tx.sol_price_usd if tx.sol_price_usd > 0 else 0,  # 換算為SOL等值
                    timestamp=tx.timestamp
                ))
        
        elif tx.tx_type == "SELL":
            # 賣出交易: 按FIFO原則從最早的批次開始賣出
            remaining_to_sell = tx.token_amount
            self.total_sold += tx.token_amount
            
            # 記錄收入
            if tx.sol_amount > 0:
                self.total_revenue_sol += tx.sol_amount
            elif tx.usdc_amount > 0:
                self.total_revenue_usdc += tx.usdc_amount
            
            # 新增：檢查賣出數量是否大於當前可用批次總量
            available_tokens = sum(lot.token_amount for lot in self.token_lots)
            if remaining_to_sell > available_tokens:
                
                # 方案1：按比例計算盈虧（適用於空投情況）
                if available_tokens > 0:
                    # 只計算與批次對應部分的盈虧
                    proportion = available_tokens / remaining_to_sell
                    
                    # 按比例分配收入
                    proportional_sol = tx.sol_amount * proportion if tx.sol_amount > 0 else 0
                    proportional_usdc = tx.usdc_amount * proportion if tx.usdc_amount > 0 else 0
                    
                    # 處理可用批次
                    to_sell_from_batches = available_tokens
                    remaining_to_sell = to_sell_from_batches  # 修改為只賣出可用數量
                    
                    # 記錄額外數量的收入（不計入盈虧）
                    extra_tokens = tx.token_amount - available_tokens
                    extra_sol = tx.sol_amount * (1 - proportion) if tx.sol_amount > 0 else 0
                    extra_usdc = tx.usdc_amount * (1 - proportion) if tx.usdc_amount > 0 else 0
                    
                else:
                    # 如果沒有批次，則所有收入都視為利潤
                    if tx.sol_amount > 0:
                        self.realized_pnl_sol += tx.sol_amount
                    elif tx.usdc_amount > 0:
                        self.realized_pnl_usdc += tx.usdc_amount
                    return  # 沒有批次可處理，直接返回
            
            while remaining_to_sell > 0 and self.token_lots:
                lot = self.token_lots[0]
                
                if lot.token_amount <= remaining_to_sell:
                    # 整個批次都被賣出
                    remaining_to_sell -= lot.token_amount
                    
                    # 計算這批次的盈虧 - 保持單位一致性
                    if tx.sol_amount > 0:
                        # 使用SOL比例計算本批次的收入 (SOL單位)
                        lot_revenue_sol = (lot.token_amount / tx.token_amount) * tx.sol_amount
                        
                        # 計算SOL單位的盈虧
                        self.realized_pnl_sol += lot_revenue_sol - lot.cost_sol
                        
                        # 也可以計算美元單位的盈虧
                        lot_revenue_usd = lot_revenue_sol * tx.sol_price_usd
                        lot_cost_usd = lot.cost_sol * tx.sol_price_usd
                        self.realized_pnl_usdc += lot_revenue_usd - lot_cost_usd
                    
                    elif tx.usdc_amount > 0:
                        # 使用USDC比例計算本批次的收入 (USDC單位)
                        lot_revenue_usdc = (lot.token_amount / tx.token_amount) * tx.usdc_amount
                        
                        # 計算USDC單位的盈虧
                        self.realized_pnl_usdc += lot_revenue_usdc - lot.cost_usdc
                        
                        # 也可以計算SOL單位的盈虧 (如果需要)
                        lot_revenue_sol = lot_revenue_usdc / tx.sol_price_usd if tx.sol_price_usd > 0 else 0
                        self.realized_pnl_sol += lot_revenue_sol - lot.cost_sol
                    
                    # 移除批次
                    self.token_lots.popleft()
                
                else:
                    # 只賣出批次的一部分
                    sell_ratio = remaining_to_sell / lot.token_amount
                    
                    if tx.sol_amount > 0:
                        # 按比例計算收入 (SOL單位)
                        lot_revenue_sol = (remaining_to_sell / tx.token_amount) * tx.sol_amount
                        
                        # 按比例計算成本 (SOL單位)
                        lot_cost_sol = sell_ratio * lot.cost_sol
                        
                        # 計算SOL單位的盈虧
                        self.realized_pnl_sol += lot_revenue_sol - lot_cost_sol
                        
                        # 計算美元單位的盈虧
                        lot_revenue_usd = lot_revenue_sol * tx.sol_price_usd
                        lot_cost_usd = lot_cost_sol * tx.sol_price_usd
                        self.realized_pnl_usdc += lot_revenue_usd - lot_cost_usd
                    
                    elif tx.usdc_amount > 0:
                        # 按比例計算收入 (USDC單位)
                        lot_revenue_usdc = (remaining_to_sell / tx.token_amount) * tx.usdc_amount
                        
                        # 按比例計算成本 (USDC單位)
                        lot_cost_usdc = sell_ratio * lot.cost_usdc
                        
                        # 計算USDC單位的盈虧
                        self.realized_pnl_usdc += lot_revenue_usdc - lot_cost_usdc
                        
                        # 計算SOL單位的盈虧 (如果需要)
                        lot_revenue_sol = lot_revenue_usdc / tx.sol_price_usd if tx.sol_price_usd > 0 else 0
                        lot_cost_sol = sell_ratio * lot.cost_sol
                        self.realized_pnl_sol += lot_revenue_sol - lot_cost_sol
                    
                    # 更新批次
                    lot.token_amount -= remaining_to_sell
                    
                    # 按比例調整成本
                    lot.cost_sol *= (1 - sell_ratio)
                    lot.cost_usdc *= (1 - sell_ratio)
                    
                    remaining_to_sell = 0
    
    def _check_for_quick_trades(self):
        """檢查是否有1分鐘內的買入-賣出交易對"""
        # 按時間戳排序交易
        sorted_txs = sorted(self.transactions, key=lambda t: t.timestamp)
        
        # 重置計數
        self.quick_trades_count = 0
        
        # 尋找買入和賣出組合
        for i in range(len(sorted_txs) - 1):
            tx1 = sorted_txs[i]
            
            for j in range(i + 1, len(sorted_txs)):
                tx2 = sorted_txs[j]
                
                # 檢查是否為買入後賣出
                is_buy_then_sell = tx1.tx_type == "BUY" and tx2.tx_type == "SELL"
                # 或賣出後買入
                is_sell_then_buy = tx1.tx_type == "SELL" and tx2.tx_type == "BUY"
                
                # 時間差小於60秒 (1分鐘)
                time_diff = abs(tx2.timestamp - tx1.timestamp)
                
                if (is_buy_then_sell or is_sell_then_buy) and time_diff <= 60:
                    self.quick_trades_count += 1
    
    def _calculate_averages(self):
        """計算平均買入/賣出價格和加權成本"""
        # 計算剩餘代幣和加權平均成本
        self.remaining_tokens = sum(lot.token_amount for lot in self.token_lots)
        
        if self.total_bought > 0:
            self.avg_buy_price_sol = self.total_cost_sol / self.total_bought if self.total_cost_sol > 0 else 0
            self.avg_buy_price_usdc = self.total_cost_usdc / self.total_bought if self.total_cost_usdc > 0 else 0
        
        if self.total_sold > 0:
            self.avg_sell_price_sol = self.total_revenue_sol / self.total_sold if self.total_revenue_sol > 0 else 0
            self.avg_sell_price_usdc = self.total_revenue_usdc / self.total_sold if self.total_revenue_usdc > 0 else 0
        
        # 計算加權平均成本
        if self.remaining_tokens > 0:
            if self.currency == "SOL":
                total_cost = sum(lot.cost_sol for lot in self.token_lots)
                self.weighted_avg_cost = total_cost / self.remaining_tokens if total_cost > 0 else 0
            elif self.currency == "USDC":
                total_cost = sum(lot.cost_usdc for lot in self.token_lots)
                self.weighted_avg_cost = total_cost / self.remaining_tokens if total_cost > 0 else 0
    
    def calculate_pnl(self) -> Tuple[float, float]:
        """計算已實現利潤/虧損 (PnL) 和百分比"""
        realized_pnl = 0.0
        
        if self.currency == "SOL":
            realized_pnl = self.realized_pnl_sol
        elif self.currency == "USDC":
            realized_pnl = self.realized_pnl_usdc
        
        # 計算百分比
        total_cost = 0
        if self.currency == "SOL":
            total_cost = self.total_cost_sol
        elif self.currency == "USDC":
            total_cost = self.total_cost_usdc
        
        realized_pnl_percentage = (realized_pnl / total_cost) * 100 if total_cost > 0 else 0
        
        return realized_pnl, realized_pnl_percentage
    
    def calculate_pnl_usd(self, sol_price_usd: float) -> Tuple[float, float]:
        """計算以美元計價的已實現利潤/虧損 (PnL) 和百分比"""
        realized_pnl, realized_pnl_percentage = self.calculate_pnl()
        
        if self.currency == "SOL":
            realized_pnl_usd = realized_pnl * sol_price_usd
        elif self.currency == "USDC":
            realized_pnl_usd = realized_pnl
        else:
            realized_pnl_usd = 0.0
        
        return realized_pnl_usd, realized_pnl_percentage
    
    def calculate_unrealized_pnl(self, current_price: float) -> Tuple[float, float]:
        """計算未實現利潤/虧損 (基於當前市場價格)"""
        if self.remaining_tokens <= 0 or current_price <= 0:
            return 0.0, 0.0
        
        current_value = self.remaining_tokens * current_price
        cost_basis = self.remaining_tokens * self.weighted_avg_cost
        
        unrealized_pnl = current_value - cost_basis
        unrealized_pnl_percentage = (unrealized_pnl / cost_basis) * 100 if cost_basis > 0 else 0.0
        
        return unrealized_pnl, unrealized_pnl_percentage
    
    def has_buy_transactions(self) -> bool:
        """檢查是否有買入交易"""
        return self.total_bought > 0
    
    def has_sell_transactions(self) -> bool:
        """檢查是否有賣出交易"""
        return self.total_sold > 0
    
    def is_only_sell(self) -> bool:
        """檢查是否只有賣出交易而沒有買入交易"""
        return self.has_sell_transactions() and not self.has_buy_transactions()
    
    def get_lots_summary(self) -> str:
        """獲取代幣批次的摘要信息"""
        if not self.token_lots:
            return "沒有剩餘代幣批次"
        
        output = []
        for i, lot in enumerate(self.token_lots):
            output.append(f"批次 {i+1}: {lot}")
        
        return "\n".join(output)
    
    def __str__(self) -> str:
        realized_pnl, realized_pnl_pct = self.calculate_pnl()
        
        output = [
            f"Token: {self.token_symbol} ({self.token_mint[:6]}...)",
            f"Currency: {self.currency}",
            f"Total Bought: {self.total_bought:.6f}",
            f"Total Sold: {self.total_sold:.6f}",
            f"Remaining Tokens: {self.remaining_tokens:.6f}",
        ]
        
        # 獲取當前SOL價格
        try:
            sol_price_usd = 141.91  # 默認值
            
            # 檢查全局變數中是否存在wallet_analyzer實例
            for var_name, var_val in globals().items():
                if isinstance(var_val, WalletAnalyzer) and hasattr(var_val, 'sol_price_usd'):
                    sol_price_usd = var_val.sol_price_usd
                    break
        except:
            sol_price_usd = 141.91  # 如果出錯，使用默認值
        
        if self.currency == "SOL":
            realized_pnl_usd = realized_pnl * sol_price_usd
            output.extend([
                f"Total Cost: {self.total_cost_sol:.6f} SOL (≈ ${self.total_cost_sol * sol_price_usd:.2f} USD)",
                f"Total Revenue: {self.total_revenue_sol:.6f} SOL (≈ ${self.total_revenue_sol * sol_price_usd:.2f} USD)",
                f"Avg Buy Price: {self.avg_buy_price_sol:.8f} SOL/token",
                f"Avg Sell Price: {self.avg_sell_price_sol:.8f} SOL/token",
                f"Weighted Avg Cost: {self.weighted_avg_cost:.8f} SOL/token",
                f"Realized PnL: {realized_pnl:.6f} SOL (≈ ${realized_pnl_usd:.2f} USD) ({realized_pnl_pct:.2f}%)"
            ])
        elif self.currency == "USDC":
            output.extend([
                f"Total Cost: {self.total_cost_usdc:.6f} USDC",
                f"Total Revenue: {self.total_revenue_usdc:.6f} USDC",
                f"Avg Buy Price: {self.avg_buy_price_usdc:.8f} USDC/token",
                f"Avg Sell Price: {self.avg_sell_price_usdc:.8f} USDC/token",
                f"Weighted Avg Cost: {self.weighted_avg_cost:.8f} USDC/token",
                f"Realized PnL: {realized_pnl:.6f} USDC ({realized_pnl_pct:.2f}%)"
            ])
        
        output.append(f"Transaction Count: {len(self.transactions)}")
        
        if self.quick_trades_count > 0:
            output.append(f"Quick Trades (< 1min): {self.quick_trades_count}")
        
        return "\n".join(output)

async def get_token_supply(client, mint: str) -> float:
    if mint in token_supply_cache:
        return token_supply_cache[mint]

    try:
        supply_data = await client.get_token_supply(Pubkey(base58.b58decode(mint)))
        supply = int(supply_data.value.amount) / (10 ** supply_data.value.decimals)
        token_supply_cache[mint] = supply
        return supply
    except Exception:
        return 1000000000

class WalletAnalyzer:
    """分析錢包的所有交易並追蹤代幣組合"""
    def __init__(self, wallet_address: str):
        self.wallet_address = wallet_address
        self.portfolios: Dict[str, TokenPortfolio] = {}  # 以代幣 mint 為鍵
        self.transactions: List[Dict[str, Any]] = []  # 原始交易數據
        self.processed_txs: List[TokenTransaction] = []  # 處理後的交易
        self.sol_price_usd = get_sol_price()  # 獲取SOL的美元價格
        self.only_sell_tokens = 0  # 只有賣出沒有買入的幣種數量
        self.total_quick_trades = 0  # 1分鐘內完成的快速交易總數
        # print(f"當前SOL價格: ${self.sol_price_usd:.2f} USD")

    async def save_transactions_to_db(self, client, async_session, chain="solana", wallet_balance_usdt=0):
        """將處理後的交易記錄儲存到資料庫，同時更新當前持倉和歷史數據"""

        # 首先，按時間順序對所有交易進行排序
        sorted_txs = sorted(self.processed_txs, key=lambda tx: tx.timestamp)
        
        # 建立一個字典來跟蹤每種代幣的即時持倉狀況
        current_holdings = {}
        
        # 1. 按時間順序處理所有交易，更新即時持倉狀況並保存每筆交易記錄
        for tx in sorted_txs:
            supply = await get_token_supply(client, tx.token_mint) or 1000000000
            token_mint = tx.token_mint
            
            # 初始化當前代幣的持倉記錄（如果不存在）
            if token_mint not in current_holdings:
                current_holdings[token_mint] = {
                    "total_amount": 0,  # 持有的總數量
                    "total_cost": 0,    # 總成本
                    "buys": [],         # 買入記錄列表(FIFO計算用)
                    "avg_buy_price": 0  # 平均買入價格
                }
            
            holding = current_holdings[token_mint]
            
            # 計算交易時的持有比例
            holding_percentage = 0
            tx_data = {
                "wallet_address": self.wallet_address,
                "wallet_balance": wallet_balance_usdt,
                "token_address": token_mint,
                "token_name": tx.token_symbol,
                "token_icon": getattr(tx, "token_icon", ""),
                "price": tx.token_price,
                "amount": tx.token_amount,
                "marketcap": tx.token_price * supply,
                "value": tx.usd_amount if tx.usd_amount > 0 else (tx.sol_amount * self.sol_price_usd if tx.sol_amount > 0 else tx.usdc_amount),
                "chain": chain,
                "transaction_type": tx.tx_type.lower(),
                "transaction_time": tx.timestamp,
                "time": datetime.now(timezone(timedelta(hours=8))),
                "signature": tx.signature,
                "from_token_address": tx.from_token_mint,
                "from_token_symbol": tx.from_token_symbol,
                "from_token_amount": tx.from_token_amount,
                "dest_token_address": tx.dest_token_mint,
                "dest_token_symbol": tx.dest_token_symbol,
                "dest_token_amount": tx.dest_token_amount
            }
            
            if tx.tx_type == "BUY":
                # 買入交易：計算佔錢包餘額的比例
                tx_value = tx_data["value"]
                if wallet_balance_usdt > 0:
                    # 這裡的計算更準確，因為是用交易發生時的錢包餘額
                    # 買入時：value / (value + 剩餘錢包餘額) * 100%
                    holding_percentage = min(100, (tx_value / (tx_value + wallet_balance_usdt)) * 100)
                else:
                    holding_percentage = 100
                
                # 更新買入代幣的持倉數據
                holding["buys"].append({
                    "amount": tx.token_amount,
                    "cost": tx_value,
                    "price": tx_value / tx.token_amount if tx.token_amount > 0 else 0
                })
                holding["total_amount"] += tx.token_amount
                holding["total_cost"] += tx_value
                
                # 更新平均買入價格
                if holding["total_amount"] > 0:
                    holding["avg_buy_price"] = holding["total_cost"] / holding["total_amount"]
                
                # 買入交易沒有已實現利潤
                realized_profit = 0
                realized_profit_percentage = 0
                
            else:  # SELL
                # 賣出交易：計算佔持有量的比例
                if holding["total_amount"] > 0:
                    # 賣出時：賣出數量 / (當前持有數量 + 賣出數量) * 100%
                    holding_percentage = min(100, (tx.token_amount / holding["total_amount"]) * 100)
                else:
                    # 如果沒有持倉記錄（可能是空投），設為100%
                    holding_percentage = 100
                
                # 計算賣出的代幣價值
                sell_value = tx_data["value"]
                
                # 計算已實現利潤（使用FIFO方法）
                realized_profit = 0
                cost_basis = 0
                remaining_to_sell = tx.token_amount
                
                if holding["buys"]:
                    # 使用FIFO方法計算賣出成本和利潤
                    while remaining_to_sell > 0 and holding["buys"]:
                        oldest_buy = holding["buys"][0]
                        
                        if oldest_buy["amount"] <= remaining_to_sell:
                            # 整個批次都被賣出
                            cost_basis += oldest_buy["cost"]
                            remaining_to_sell -= oldest_buy["amount"]
                            holding["buys"].pop(0)
                        else:
                            # 只賣出部分批次
                            sell_ratio = remaining_to_sell / oldest_buy["amount"]
                            cost_basis += oldest_buy["cost"] * sell_ratio
                            oldest_buy["amount"] -= remaining_to_sell
                            oldest_buy["cost"] *= (1 - sell_ratio)
                            remaining_to_sell = 0
                    
                    # 計算已實現利潤
                    if holding["avg_buy_price"] > 0:
                        sell_price = sell_value / tx.token_amount if tx.token_amount > 0 else 0
                        realized_profit = sell_value - cost_basis
                        realized_profit_percentage = ((sell_price / holding["avg_buy_price"]) - 1) * 100 if holding["avg_buy_price"] > 0 else 0
                        realized_profit_percentage = max(realized_profit_percentage, -100)  # 確保下限為-100%
                    else:
                        realized_profit = sell_value  # 如果沒有買入價格記錄，全部視為利潤
                        realized_profit_percentage = 100
                else:
                    # 如果沒有買入記錄（可能是空投），全部視為利潤
                    realized_profit = sell_value
                    realized_profit_percentage = 100
                
                # 更新持倉數據
                holding["total_amount"] = max(0, holding["total_amount"] - tx.token_amount)
                # 如果完全賣出，重置成本
                if holding["total_amount"] <= 0:
                    holding["total_cost"] = 0
                    holding["avg_buy_price"] = 0
                else:
                    # 重新計算總成本和平均價格（從剩餘的buys計算）
                    holding["total_cost"] = sum(buy["cost"] for buy in holding["buys"])
                    holding["avg_buy_price"] = holding["total_cost"] / holding["total_amount"] if holding["total_amount"] > 0 else 0

            # 設置交易的持有比例和已實現利潤
            tx_data["holding_percentage"] = holding_percentage
            tx_data["realized_profit"] = realized_profit if tx.tx_type == "SELL" else 0
            tx_data["realized_profit_percentage"] = realized_profit_percentage if tx.tx_type == "SELL" else 0
            
            # 儲存交易記錄
            await save_past_transaction(async_session, tx_data, self.wallet_address, tx.signature, chain)
        
        # 2. 直接從計算好的持倉數據更新 TokenBuyData 表格
        await self.update_token_buy_data_from_portfolios(async_session, chain)

    # async def save_transactions_to_db(self, client, async_session, chain="solana", wallet_balance_usdt=0):
    #     """將處理後的交易記錄儲存到資料庫"""
    #     # 按時間排序交易
    #     sorted_txs = sorted(self.processed_txs, key=lambda tx: tx.timestamp)
        
    #     # 1. 保存每筆交易
    #     for tx in sorted_txs:
    #         # 獲取代幣supply
    #         supply = await get_token_supply(client, tx.token_mint) or 0
    #         token_mint = tx.token_mint
            
    #         # 計算交易USD價值
    #         tx_value_usd = tx.usd_amount if tx.usd_amount > 0 else \
    #                     (tx.sol_amount * self.sol_price_usd if tx.sol_amount > 0 else tx.usdc_amount)
            
    #         # 獲取代幣統計數據
    #         token_stat = self.token_stats.get(token_mint, {})
            
    #         # 準備交易數據
    #         tx_data = {
    #             "wallet_address": self.wallet_address,
    #             "wallet_balance": wallet_balance_usdt,
    #             "token_address": token_mint,
    #             "token_name": tx.token_symbol,
    #             "token_icon": getattr(tx, "token_icon", ""),
    #             "price": tx.token_price,
    #             "amount": tx.token_amount,
    #             "marketcap": tx.token_price * supply,
    #             "value": tx_value_usd,
    #             "chain": chain,
    #             "transaction_type": tx.tx_type.lower(),
    #             "transaction_time": tx.timestamp,
    #             "time": datetime.now(timezone(timedelta(hours=8))),
    #             "signature": tx.signature
    #         }
            
    #         # 設置持有比例和已實現利潤
    #         if tx.tx_type == "BUY":
    #             # 買入交易：計算佔錢包餘額的比例
    #             if wallet_balance_usdt > 0:
    #                 holding_percentage = min(100, (tx_value_usd / (tx_value_usd + wallet_balance_usdt)) * 100)
    #             else:
    #                 holding_percentage = 100
                
    #             # 買入交易沒有實現利潤
    #             realized_profit = 0
    #             realized_profit_percentage = 0
    #         else:  # SELL
    #             # 賣出交易：計算佔持有量的比例
    #             current_holdings = token_stat.get('current_holdings', 0) + tx.token_amount
    #             if current_holdings > 0:
    #                 holding_percentage = min(100, (tx.token_amount / current_holdings) * 100)
    #             else:
    #                 holding_percentage = 100
                
    #             # 計算實現利潤
    #             # 這裡需要更細緻的處理，根據具體交易時刻的實現利潤
    #             # 簡化處理：從portfolio獲取該代幣的平均PNL
    #             portfolio = self.portfolios.get(token_mint)
    #             if portfolio and portfolio.has_buy_transactions():
    #                 avg_buy_price = portfolio.avg_buy_price_sol * self.sol_price_usd if portfolio.currency == "SOL" else portfolio.avg_buy_price_usdc
                    
    #                 if avg_buy_price > 0:
    #                     sell_price = tx_value_usd / tx.token_amount if tx.token_amount > 0 else 0
    #                     realized_profit = (sell_price - avg_buy_price) * tx.token_amount
    #                     realized_profit_percentage = ((sell_price / avg_buy_price) - 1) * 100
    #                     realized_profit_percentage = max(realized_profit_percentage, -100)  # 確保不低於-100%
    #                 else:
    #                     realized_profit = tx_value_usd  # 如果沒有買入記錄，全部視為利潤
    #                     realized_profit_percentage = 100
    #             else:
    #                 # 沒有買入記錄的賣出
    #                 realized_profit = tx_value_usd
    #                 realized_profit_percentage = 100
            
    #         tx_data["holding_percentage"] = holding_percentage
    #         tx_data["realized_profit"] = realized_profit if tx.tx_type == "SELL" else 0
    #         tx_data["realized_profit_percentage"] = realized_profit_percentage if tx.tx_type == "SELL" else 0
            
    #         # 儲存交易記錄
    #         await save_past_transaction(async_session, tx_data, self.wallet_address, tx.signature, chain)
        
    #     # 2. 保存代幣持倉和歷史數據
    #     await self.update_token_buy_data_from_portfolios(async_session, chain)

    # # 2. 修改 update_token_buy_data_from_portfolios 方法來正確處理 historical 數值
    async def update_token_buy_data_from_portfolios(self, async_session, chain="solana"):
        """直接從計算好的 Portfolio 數據更新 TokenBuyData 表格"""
        
        # 對每個 token portfolio 進行處理
        for token_mint, portfolio in self.portfolios.items():
            # 從資料庫獲取當前記錄
            token_data = await token_buy_data_cache.get_token_data(
                self.wallet_address, token_mint, async_session, chain
            )
            
            # 保留歷史數據，如果沒有則初始化
            historical_total_buy_amount = token_data.get("historical_total_buy_amount", 0)
            historical_total_buy_cost = token_data.get("historical_total_buy_cost", 0) 
            historical_total_sell_amount = token_data.get("historical_total_sell_amount", 0)
            historical_total_sell_value = token_data.get("historical_total_sell_value", 0)
            historical_avg_buy_price = token_data.get("historical_avg_buy_price", 0)
            historical_avg_sell_price = token_data.get("historical_avg_sell_price", 0)
            
            # 如果資料庫中的歷史數據為0，則用 portfolio 數據初始化
            if historical_total_buy_amount == 0 and portfolio.total_bought > 0:
                historical_total_buy_amount = portfolio.total_bought
                
                if portfolio.currency == "SOL":
                    historical_total_buy_cost = portfolio.total_cost_sol * self.sol_price_usd
                else:  # USDC
                    historical_total_buy_cost = portfolio.total_cost_usdc
                    
                # 計算歷史平均買入價格
                # historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0
                # historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0
            
            if historical_total_sell_amount == 0 and portfolio.total_sold > 0:
                historical_total_sell_amount = portfolio.total_sold
                
                if portfolio.currency == "SOL":
                    historical_total_sell_value = portfolio.total_revenue_sol * self.sol_price_usd
                else:  # USDC
                    historical_total_sell_value = portfolio.total_revenue_usdc
            
            # 如果是新一輪的買入（之前清倉後又重新建倉）
            # 檢查是否有新的買入交易導致歷史數據需要更新
            if portfolio.total_bought > historical_total_buy_amount:
                # 計算新增買入的數量和成本
                new_buy_amount = portfolio.total_bought - historical_total_buy_amount
                
                if portfolio.currency == "SOL":
                    new_buy_cost = (portfolio.total_cost_sol * self.sol_price_usd) - historical_total_buy_cost
                else:  # USDC
                    new_buy_cost = portfolio.total_cost_usdc - historical_total_buy_cost
                
                # 更新歷史數據
                historical_total_buy_amount = portfolio.total_bought
                
                if portfolio.currency == "SOL":
                    historical_total_buy_cost = portfolio.total_cost_sol * self.sol_price_usd
                else:  # USDC
                    historical_total_buy_cost = portfolio.total_cost_usdc
            
            # 如果是新一輪的賣出
            if portfolio.total_sold > historical_total_sell_amount:
                # 更新歷史賣出數據
                historical_total_sell_amount = portfolio.total_sold
                
                if portfolio.currency == "SOL":
                    historical_total_sell_value = portfolio.total_revenue_sol * self.sol_price_usd
                else:  # USDC
                    historical_total_sell_value = portfolio.total_revenue_usdc
            
            historical_avg_buy_price = historical_total_buy_cost / historical_total_buy_amount if historical_total_buy_amount > 0 else 0
            historical_avg_sell_price = historical_total_sell_value / historical_total_sell_amount if historical_total_sell_amount > 0 else 0
            
            # 準備更新數據
            token_buy_data = {
                "token_address": token_mint,
                
                # 當前持倉數據 - 使用 portfolio 直接計算的結果
                "total_amount": portfolio.remaining_tokens,
                
                # 根據幣種轉換貨幣單位 (全部轉為美元)
                "total_cost": portfolio.total_cost_sol * self.sol_price_usd if portfolio.currency == "SOL" else portfolio.total_cost_usdc,
                
                "avg_buy_price": portfolio.avg_buy_price_sol * self.sol_price_usd if portfolio.currency == "SOL" else portfolio.avg_buy_price_usdc,
                
                # 如果沒有倉位開始時間，使用第一筆買入交易的時間
                "position_opened_at": token_data.get("position_opened_at"),
                
                # 歷史累計數據 - 使用保留的數據或 portfolio 數據
                "historical_total_buy_amount": historical_total_buy_amount,
                "historical_total_buy_cost": historical_total_buy_cost,
                "historical_total_sell_amount": historical_total_sell_amount,
                "historical_total_sell_value": historical_total_sell_value,
                "historical_avg_buy_price": historical_avg_buy_price,
                "historical_avg_sell_price": historical_avg_sell_price,
                
                # 保留原有值
                "last_active_position_closed_at": token_data.get("last_active_position_closed_at")
            }
            
            # 如果代幣全部賣出但之前有持倉，更新關閉時間
            if portfolio.remaining_tokens == 0 and portfolio.total_sold > 0 and token_data.get("total_amount", 0) > 0:
                # 找到最後一筆賣出交易的時間
                last_sell_tx = None
                for tx in portfolio.transactions:
                    if tx.tx_type == "SELL":
                        if last_sell_tx is None or tx.timestamp > last_sell_tx.timestamp:
                            last_sell_tx = tx
                
                if last_sell_tx:
                    token_buy_data["last_active_position_closed_at"] = datetime.fromtimestamp(last_sell_tx.timestamp)
            
            # 如果之前沒有設置開倉時間，找到第一筆買入交易設置
            if not token_buy_data["position_opened_at"] and portfolio.remaining_tokens > 0:
                first_buy_tx = None
                for tx in portfolio.transactions:
                    if tx.tx_type == "BUY":
                        if first_buy_tx is None or tx.timestamp < first_buy_tx.timestamp:
                            first_buy_tx = tx
                
                if first_buy_tx:
                    token_buy_data["position_opened_at"] = datetime.fromtimestamp(first_buy_tx.timestamp)
            
            # 保存更新
            await save_wallet_buy_data(token_buy_data, self.wallet_address, async_session, chain)

    async def fetch_transactions(self, api_key: str, max_records: int = 2000):
        """從 Helius API 獲取交易"""
        try:
            self.transactions = await fetch_all_transactions(self.wallet_address, api_key, max_records)
            if self.transactions is None:
                self.transactions = []
                print(f"獲取 {self.wallet_address} 的交易記錄時返回了None")
            transaction_count = len(self.transactions)
            
            # 保存原始數據到文件
            # with open(f"{self.wallet_address}_raw_transactions.json", 'w', encoding='utf-8') as f:
            #     json.dump(self.transactions, f, indent=2, ensure_ascii=False)
            
            # 按時間排序交易
            self.transactions.sort(key=lambda tx: tx.get("timestamp", 0))
            
            print(f"獲取了 {len(self.transactions)} 筆交易記錄")
            return transaction_count
        except Exception as e:
            print(f"獲取交易記錄時出錯: {str(e)}")
            # 出錯時返回0，表示沒有獲取到有效交易
            self.transactions = []
            return 0

    # def process_transactions(self):
    #     """處理所有交易，提取買入/賣出信息"""
    #     # 確保交易按時間排序
    #     self.transactions.sort(key=lambda tx: tx.get("timestamp", 0))

    #     existing_holdings = {}
    #     unique_token_mints = set()
        
    #     for tx in self.transactions:
    #         # 跳過非 SWAP 類型的交易
    #         if tx.get("type") != "SWAP":
    #             continue
            
    #         # 提取基本交易信息
    #         signature = tx.get("signature", "")
    #         timestamp = tx.get("timestamp", 0)
    #         source = tx.get("source", "UNKNOWN")

    #         if tx.get("source") == "RAYDIUM":
    #             tx_obj = self.process_raydium_swap(tx)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 continue
            
    #         # 提取 swap 事件數據
    #         events = tx.get("events", {})
    #         swap_event = events.get("swap", {})
            
    #         # 提取 token 轉賬和原生 SOL 轉賬數據
    #         token_transfers = tx.get("tokenTransfers", [])
    #         native_transfers = tx.get("nativeTransfers", [])
    #         account_data = tx.get("accountData", [])
            
    #         # 檢查是否已處理此交易
    #         has_processed = False
    #         for existing_tx in self.processed_txs:
    #             if existing_tx.signature == signature:
    #                 has_processed = True
    #                 break
                    
    #         if has_processed:
    #             continue
                
    #         # ===================== 優先處理 PUMP_FUN 來源的交易 =====================
    #         if tx.get("source") == "PUMP_FUN":
    #             # 處理 SOL 的支出情況
    #             sol_spent = 0
    #             for native_transfer in native_transfers:
    #                 # 檢查用戶是否從此錢包發送 SOL
    #                 if native_transfer.get("fromUserAccount") == self.wallet_address:
    #                     recipient = native_transfer.get("toUserAccount")
    #                     # 忽略計算預算和其他系統轉賬
    #                     if (recipient != "ComputeBudget111111111111111111111111111111" and 
    #                         recipient != self.wallet_address):
    #                         sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                
    #             # 檢查用戶是否收到了代幣
    #             token_received = False
    #             received_token_mint = None
    #             received_token_amount = 0
                
    #             for token_transfer in token_transfers:
    #                 if token_transfer.get("toUserAccount") == self.wallet_address:
    #                     token_mint = token_transfer.get("mint", "")
    #                     # 跳過穩定幣
    #                     if token_mint in SUPPORTED_TOKENS:
    #                         continue
                        
    #                     # 提取代幣數量
    #                     raw_amount = token_transfer.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         token_received = True
    #                         received_token_mint = token_mint
    #                         received_token_amount = token_amount
    #                         break
                
    #             # 如果沒有在 tokenTransfers 中找到用戶收到的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
    #             if not token_received:
    #                 for account in account_data:
    #                     for token_balance in account.get("tokenBalanceChanges", []):
    #                         if token_balance.get("userAccount") == self.wallet_address:
    #                             raw_amount = token_balance.get("rawTokenAmount", {})
    #                             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                                 amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                                 # 只關注正向變化 (代幣增加)
    #                                 if amount_change > 0:
    #                                     token_mint = token_balance.get("mint")
    #                                     token_received = True
    #                                     received_token_mint = token_mint
    #                                     received_token_amount = amount_change
    #                                     break
                
    #             # 如果用戶花費了 SOL 並且收到了代幣，這是一筆買入交易
    #             if sol_spent > 0 and token_received:
    #                 token_symbol, token_icon = self._get_token_symbol(received_token_mint)
                    
    #                 tx_obj = TokenTransaction(
    #                     signature=signature,
    #                     timestamp=timestamp,
    #                     tx_type="BUY",
    #                     source=source,
    #                     token_mint=received_token_mint,
    #                     token_amount=received_token_amount,
    #                     token_symbol=token_symbol,
    #                     token_icon=token_icon,
    #                     sol_amount=sol_spent,
    #                     usdc_amount=0
    #                 )
                    
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 continue  # 跳到下一個交易
                
    #             # 檢查用戶是否賣出了代幣並獲得了 SOL
    #             token_sold = False
    #             sold_token_mint = None
    #             sold_token_amount = 0
    #             sol_received = 0
                
    #             # 檢查 SOL 餘額變化
    #             for account in account_data:
    #                 if account.get("account") == self.wallet_address:
    #                     bal_change = account.get("nativeBalanceChange", 0)
    #                     if bal_change > 0:  # 正值表示獲得 SOL
    #                         sol_received = bal_change / (10 ** SOL_DECIMALS)
                
    #             # 檢查用戶是否發送了代幣
    #             for token_transfer in token_transfers:
    #                 if token_transfer.get("fromUserAccount") == self.wallet_address:
    #                     token_mint = token_transfer.get("mint", "")
    #                     # 跳過穩定幣
    #                     if token_mint in SUPPORTED_TOKENS:
    #                         continue
                        
    #                     # 提取代幣數量
    #                     raw_amount = token_transfer.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         token_sold = True
    #                         sold_token_mint = token_mint
    #                         sold_token_amount = token_amount
    #                         break
    #                     elif "tokenAmount" in token_transfer:
    #                         token_amount = float(token_transfer.get("tokenAmount", 0))
    #                         token_sold = True
    #                         sold_token_mint = token_mint
    #                         sold_token_amount = token_amount
    #                         break
                
    #             # 如果沒有在 tokenTransfers 中找到用戶發送的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
    #             if not token_sold:
    #                 for account in account_data:
    #                     for token_balance in account.get("tokenBalanceChanges", []):
    #                         if token_balance.get("userAccount") == self.wallet_address:
    #                             raw_amount = token_balance.get("rawTokenAmount", {})
    #                             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                                 # 只關注負向變化 (代幣減少)
    #                                 if token_amount < 0:
    #                                     token_mint = token_balance.get("mint")
    #                                     # 跳過穩定幣
    #                                     if token_mint in SUPPORTED_TOKENS:
    #                                         continue
    #                                     token_sold = True
    #                                     sold_token_mint = token_mint
    #                                     sold_token_amount = abs(token_amount)
    #                                     break
                
    #             # 如果用戶賣出了代幣並收到了 SOL，這是一筆賣出交易
    #             if token_sold and sol_received > 0:
    #                 token_symbol, token_icon = self._get_token_symbol(sold_token_mint)
                    
    #                 tx_obj = TokenTransaction(
    #                     signature=signature,
    #                     timestamp=timestamp,
    #                     tx_type="SELL",
    #                     source=source,
    #                     token_mint=sold_token_mint,
    #                     token_amount=sold_token_amount,
    #                     token_symbol=token_symbol,
    #                     token_icon=token_icon,
    #                     sol_amount=sol_received,
    #                     usdc_amount=0
    #                 )
                    
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 continue  # 跳到下一個交易
            
    #         # ===================== 核心交易類型檢測邏輯 =====================
            
    #         # 1. 檢查 tokenTransfers 中的買入/賣出模式
    #         # 檢查是否是用戶用 SOL 購買代幣的情況（從 tokenTransfers）
    #         sol_spent = 0
    #         user_sent_sol = False
                
    #         # 從 nativeTransfers 收集 SOL 支出信息
    #         for native_transfer in native_transfers:
    #             if native_transfer.get("fromUserAccount") == self.wallet_address:
    #                 recipient = native_transfer.get("toUserAccount")
    #                 # 忽略可能的手續費或其他小額轉賬
    #                 if recipient != "ComputeBudget111111111111111111111111111111":
    #                     sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
    #                     user_sent_sol = True
            
    #         # 檢查用戶是否收到了代幣
    #         token_received = False
    #         received_token_mint = None
    #         received_token_amount = 0
            
    #         for token_transfer in token_transfers:
    #             if token_transfer.get("toUserAccount") == self.wallet_address:
    #                 token_mint = token_transfer.get("mint", "")
    #                 # 跳過穩定幣
    #                 if token_mint in SUPPORTED_TOKENS:
    #                     continue
                    
    #                 # 提取代幣數量
    #                 raw_amount = token_transfer.get("rawTokenAmount", {})
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                     token_received = True
    #                     received_token_mint = token_mint
    #                     received_token_amount = token_amount
    #                     break
            
    #         # 如果用戶發送了 SOL 並收到了代幣，這是一筆購買交易
    #         if user_sent_sol and token_received and sol_spent > 0 and received_token_amount > 0:

    #             token_symbol, token_icon = self._get_token_symbol(received_token_mint)
                
    #             tx_obj = TokenTransaction(
    #                 signature=signature,
    #                 timestamp=timestamp,
    #                 tx_type="BUY",
    #                 source=source,
    #                 token_mint=received_token_mint,
    #                 token_amount=received_token_amount,
    #                 token_symbol=token_symbol,
    #                 token_icon=token_icon,
    #                 sol_amount=sol_spent,
    #                 usdc_amount=0
    #             )
                
    #             self.processed_txs.append(tx_obj)
    #             self._add_to_portfolio(tx_obj)
    #             continue  # 跳到下一個交易
            
    #         # 2. 檢查用戶是否賣出代幣獲得 SOL
    #         sol_received = 0
    #         user_received_sol = False
            
    #         for native_transfer in native_transfers:
    #             if native_transfer.get("toUserAccount") == self.wallet_address:
    #                 sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
    #                 user_received_sol = True
            
    #         # 檢查用戶是否發送了代幣
    #         token_sent = False
    #         sent_token_mint = None
    #         sent_token_amount = 0
            
    #         for token_transfer in token_transfers:
    #             if token_transfer.get("fromUserAccount") == self.wallet_address:
    #                 token_mint = token_transfer.get("mint", "")
    #                 # 跳過穩定幣
    #                 if token_mint in SUPPORTED_TOKENS:
    #                     continue
                    
    #                 # 提取代幣數量
    #                 raw_amount = token_transfer.get("rawTokenAmount", {})
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                     token_sent = True
    #                     sent_token_mint = token_mint
    #                     sent_token_amount = token_amount
    #                     break
            
    #         # 如果用戶發送了代幣並收到了 SOL，這是一筆賣出交易
    #         if token_sent and user_received_sol and sent_token_amount > 0 and sol_received > 0:
    #             token_symbol, token_icon = self._get_token_symbol(sent_token_mint)
                
    #             tx_obj = TokenTransaction(
    #                 signature=signature,
    #                 timestamp=timestamp,
    #                 tx_type="SELL",
    #                 source=source,
    #                 token_mint=sent_token_mint,
    #                 token_amount=sent_token_amount,
    #                 token_symbol=token_symbol,
    #                 token_icon=token_icon,
    #                 sol_amount=sol_received,
    #                 usdc_amount=0
    #             )
                
    #             self.processed_txs.append(tx_obj)
    #             self._add_to_portfolio(tx_obj)
    #             continue  # 跳到下一個交易
            
    #         # 3. 如果上面的檢測都沒有找到買賣模式，嘗試使用 events.swap 數據
    #         if swap_event:
    #             # 處理 memecoin 對 memecoin 交易
    #             token_inputs = swap_event.get("tokenInputs", [])
    #             token_outputs = swap_event.get("tokenOutputs", [])
    #             native_output = swap_event.get("nativeOutput", {})
    #             native_input = swap_event.get("nativeInput", {})
                
    #             # 檢查是否是 memecoin 對 memecoin 的交易
    #             user_sold_tokens = False
    #             sold_token_mint = None
    #             sold_token_amount = 0
    #             sold_token_symbol = None
    #             sold_token_icon = None
                
    #             for token_input in token_inputs:
    #                 if token_input.get("userAccount") == self.wallet_address:
    #                     user_sold_tokens = True
    #                     sold_token_mint = token_input.get("mint", "")
    #                     sold_token_symbol, sold_token_icon = self._get_token_symbol(sold_token_mint)
    #                     raw_amount = token_input.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         sold_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                     break
                
    #             # 檢查用戶是否收到了非穩定幣代幣
    #             user_received_tokens = []
                
    #             for transfer in token_transfers:
    #                 if transfer.get("toUserAccount") == self.wallet_address:
    #                     token_mint = transfer.get("mint", "")
    #                     # 跳過 SOL 和穩定幣
    #                     if token_mint in SUPPORTED_TOKENS:
    #                         continue
    #                     # 跳過賣出的同一種代幣
    #                     if token_mint == sold_token_mint:
    #                         continue
                        
    #                     # 提取代幣數量
    #                     raw_amount = None
    #                     if "rawTokenAmount" in transfer:
    #                         raw_amount = transfer["rawTokenAmount"]
    #                     else:
    #                         token_amount = transfer.get("tokenAmount", 0)
    #                         if token_amount:
    #                             try:
    #                                 token_amount = float(token_amount)
    #                                 raw_amount = {"tokenAmount": token_amount, "decimals": 0}
    #                             except:
    #                                 continue
                        
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         token_symbol, token_icon = self._get_token_symbol(token_mint)
    #                         received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         user_received_tokens.append({
    #                             "mint": token_mint,
    #                             "symbol": token_symbol,
    #                             "icon": token_icon,
    #                             "amount": received_amount
    #                         })
                
    #             # 處理 SOL 的中間金額
    #             intermediate_sol_amount = 0.0
    #             if native_output and "amount" in native_output:
    #                 intermediate_sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
    #             elif native_input and "amount" in native_input:
    #                 intermediate_sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
    #             # 處理 memecoin 對 memecoin 的交易
    #             if user_sold_tokens and user_received_tokens and intermediate_sol_amount > 0:
    #                 # 先處理賣出部分
    #                 tx_obj_sell = TokenTransaction(
    #                     signature=signature,
    #                     timestamp=timestamp,
    #                     tx_type="SELL",
    #                     source=source,
    #                     token_mint=sold_token_mint,
    #                     token_amount=sold_token_amount,
    #                     token_symbol=sold_token_symbol,
    #                     token_icon=sold_token_icon,
    #                     sol_amount=intermediate_sol_amount,
    #                     usdc_amount=0,
    #                 )
                    
    #                 self.processed_txs.append(tx_obj_sell)
    #                 self._add_to_portfolio(tx_obj_sell)
                    
    #                 # 再處理買入部分
    #                 for received_token in user_received_tokens:
    #                     tx_obj_buy = TokenTransaction(
    #                         signature=signature + "_BUY",  # 添加後綴以區分
    #                         timestamp=timestamp,
    #                         tx_type="BUY",
    #                         source=source,
    #                         token_mint=received_token["mint"],
    #                         token_amount=received_token["amount"],
    #                         token_symbol=received_token["symbol"],
    #                         token_icon=received_token["icon"],
    #                         sol_amount=intermediate_sol_amount,
    #                         usdc_amount=0
    #                     )
                        
    #                     self.processed_txs.append(tx_obj_buy)
    #                     self._add_to_portfolio(tx_obj_buy)
                    
    #                 continue  # 跳到下一個交易
                
    #             # 處理常規的買入和賣出交易
    #             # 過濾非穩定幣或原生代幣的交易
    #             has_supported_token = False
                
    #             # 檢查輸入中是否有支持的代幣
    #             for token_input in token_inputs:
    #                 if token_input.get("mint") in SUPPORTED_TOKENS:
    #                     has_supported_token = True
    #                     break
                
    #             # 檢查輸出中是否有支持的代幣
    #             if not has_supported_token:
    #                 for token_output in token_outputs:
    #                     if token_output.get("mint") in SUPPORTED_TOKENS:
    #                         has_supported_token = True
    #                         break
                
    #             # 檢查是否涉及原生 SOL
    #             if (native_input and "amount" in native_input) or (native_output and "amount" in native_output):
    #                 has_supported_token = True
                
    #             # 如果不涉及支持的代幣，跳過此交易
    #             if not has_supported_token:
    #                 continue
                
    #             # 處理代幣輸入 (賣出)
    #             for token_input in token_inputs:
    #                 # 確保這是目標錢包的交易
    #                 if token_input.get("userAccount") != self.wallet_address:
    #                     continue
                    
    #                 raw_amount = token_input.get("rawTokenAmount", {})
    #                 token_mint = token_input.get("mint", "")
    #                 token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        
    #                     sol_amount = 0.0
    #                     usdc_amount = 0.0
                        
    #                     # 處理 SOL 輸出 (賣出代幣獲得 SOL)
    #                     if native_output and "amount" in native_output:
    #                         sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                        
    #                     # 處理 USDC/USDT 輸出
    #                     else:
    #                         for output in token_outputs:
    #                             output_mint = output.get("mint", "")
    #                             if output_mint in STABLECOINS and output.get("userAccount") == self.wallet_address:
    #                                 output_raw_amount = output.get("rawTokenAmount", {})
    #                                 if output_raw_amount and "tokenAmount" in output_raw_amount and "decimals" in output_raw_amount:
    #                                     usdc_amount = float(output_raw_amount["tokenAmount"]) / (10 ** output_raw_amount["decimals"])
                        
    #                     # 只處理有 SOL 或穩定幣輸出的交易
    #                     if sol_amount > 0 or usdc_amount > 0:
    #                         tx_obj = TokenTransaction(
    #                             signature=signature,
    #                             timestamp=timestamp,
    #                             tx_type="SELL",
    #                             source=source,
    #                             token_mint=token_mint,
    #                             token_amount=token_amount,
    #                             token_symbol=token_symbol,
    #                             token_icon=token_icon,
    #                             sol_amount=sol_amount,
    #                             usdc_amount=usdc_amount
    #                         )
                            
    #                         self.processed_txs.append(tx_obj)
    #                         self._add_to_portfolio(tx_obj)
                
    #             # 處理代幣輸出 (買入)
    #             for token_output in token_outputs:
    #                 # 確保這是目標錢包的交易
    #                 if token_output.get("userAccount") != self.wallet_address:
    #                     continue
                    
    #                 raw_amount = token_output.get("rawTokenAmount", {})
    #                 token_mint = token_output.get("mint", "")
    #                 token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        
    #                     # 處理三種情況：SOL 輸入、穩定幣輸入、其他代幣輸入
    #                     sol_amount = 0.0
    #                     usdc_amount = 0.0
                        
    #                     # 處理 SOL 輸入 (用 SOL 買入代幣)
    #                     if native_input and "amount" in native_input:
    #                         sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                        
    #                     # 處理穩定幣輸入
    #                     else:
    #                         for input_token in token_inputs:
    #                             input_mint = input_token.get("mint", "")
    #                             if input_mint in STABLECOINS and input_token.get("userAccount") == self.wallet_address:
    #                                 input_raw_amount = input_token.get("rawTokenAmount", {})
    #                                 if input_raw_amount and "tokenAmount" in input_raw_amount and "decimals" in input_raw_amount:
    #                                     usdc_amount = float(input_raw_amount["tokenAmount"]) / (10 ** input_raw_amount["decimals"])
                        
    #                     # 只處理有 SOL 或穩定幣輸入的交易
    #                     if sol_amount > 0 or usdc_amount > 0:
    #                         tx_obj = TokenTransaction(
    #                             signature=signature,
    #                             timestamp=timestamp,
    #                             tx_type="BUY",
    #                             source=source,
    #                             token_mint=token_mint,
    #                             token_amount=token_amount,
    #                             token_symbol=token_symbol,
    #                             token_icon=token_icon,
    #                             sol_amount=sol_amount,
    #                             usdc_amount=usdc_amount
    #                         )
                            
    #                         self.processed_txs.append(tx_obj)
    #                         self._add_to_portfolio(tx_obj)
            
    #         # 4. 強化特殊情況處理 - 用於處理 nativeTransfers 和 tokenTransfers
    #         # 檢查是否有交易但未處理
    #         has_processed_tx = False
    #         for tx_obj in self.processed_txs:
    #             if tx_obj.signature == signature:
    #                 has_processed_tx = True
    #                 break
            
    #         if not has_processed_tx and native_transfers:
    #             # 檢查是否有 SOL 轉出並且有代幣轉入
    #             sol_spent = 0
    #             for native_transfer in native_transfers:
    #                 if (native_transfer.get("fromUserAccount") == self.wallet_address and
    #                     native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111"):
    #                     sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                
    #             # 如果有 SOL 支出，檢查是否有代幣轉入
    #             if sol_spent > 0:
    #                 for token_transfer in token_transfers:
    #                     if token_transfer.get("toUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
    #                         token_mint = token_transfer.get("mint")
    #                         token_symbol, token_icon = self._get_token_symbol(token_mint)
                            
    #                         token_amount = 0
    #                         raw_amount = token_transfer.get("rawTokenAmount", {})
    #                         if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                             token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            
    #                         if token_amount > 0:
    #                             tx_obj = TokenTransaction(
    #                                 signature=signature,
    #                                 timestamp=timestamp,
    #                                 tx_type="BUY",
    #                                 source=source,
    #                                 token_mint=token_mint,
    #                                 token_amount=token_amount,
    #                                 token_symbol=token_symbol,
    #                                 token_icon=token_icon,
    #                                 sol_amount=sol_spent,
    #                                 usdc_amount=0
    #                             )
                                
    #                             self.processed_txs.append(tx_obj)
    #                             self._add_to_portfolio(tx_obj)
    #                             break  # 只處理第一個找到的轉入代幣
                    
    #                 # 如果沒有在 tokenTransfers 中找到代幣轉入，檢查 accountData
    #                 if has_processed_tx == False:
    #                     for account in account_data:
    #                         for token_balance in account.get("tokenBalanceChanges", []):
    #                             if token_balance.get("userAccount") == self.wallet_address:
    #                                 raw_amount = token_balance.get("rawTokenAmount", {})
    #                                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                                     amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                                     # 只關注正向變化 (代幣增加)
    #                                     if amount_change > 0:
    #                                         token_mint = token_balance.get("mint")
    #                                         # 跳過穩定幣
    #                                         if token_mint in SUPPORTED_TOKENS:
    #                                             continue
                                            
    #                                         token_symbol, token_icon = self._get_token_symbol(token_mint)
                                            
    #                                         tx_obj = TokenTransaction(
    #                                             signature=signature,
    #                                             timestamp=timestamp,
    #                                             tx_type="BUY",
    #                                             source=source,
    #                                             token_mint=token_mint,
    #                                             token_amount=amount_change,
    #                                             token_symbol=token_symbol,
    #                                             token_icon=token_icon,
    #                                             sol_amount=sol_spent,
    #                                             usdc_amount=0
    #                                         )
                                            
    #                                         self.processed_txs.append(tx_obj)
    #                                         self._add_to_portfolio(tx_obj)
    #                                         break  # 只處理第一個找到的代幣增加
                
    #             # 檢查是否有 SOL 轉入並且有代幣轉出 (賣出交易)
    #             if not has_processed_tx:
    #                 sol_received = 0
    #                 for native_transfer in native_transfers:
    #                     if native_transfer.get("toUserAccount") == self.wallet_address:
    #                         sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                    
    #                 # 檢查用戶是否發送了代幣
    #                 if sol_received > 0:
    #                     for token_transfer in token_transfers:
    #                         if token_transfer.get("fromUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
    #                             token_mint = token_transfer.get("mint")
    #                             token_symbol, token_icon = self._get_token_symbol(token_mint)
                                
    #                             token_amount = 0
    #                             raw_amount = token_transfer.get("rawTokenAmount", {})
    #                             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                
    #                             if token_amount > 0:
    #                                 tx_obj = TokenTransaction(
    #                                     signature=signature,
    #                                     timestamp=timestamp,
    #                                     tx_type="SELL",
    #                                     source=source,
    #                                     token_mint=token_mint,
    #                                     token_amount=token_amount,
    #                                     token_symbol=token_symbol,
    #                                     token_icon=token_icon,
    #                                     sol_amount=sol_received,
    #                                     usdc_amount=0
    #                                 )
                                    
    #                                 self.processed_txs.append(tx_obj)
    #                                 self._add_to_portfolio(tx_obj)
    #                                 break  # 只處理第一個找到的代幣增加

    def process_transactions(self):
        """處理所有交易，提取買入/賣出信息"""
        # 確保交易按時間排序
        self.transactions.sort(key=lambda tx: tx.get("timestamp", 0))

        existing_holdings = {}
        unique_token_mints = set()
        
        for tx in self.transactions:
            # 跳過非 SWAP 類型的交易
            if tx.get("type") != "SWAP":
                continue
            
            # 提取基本交易信息
            signature = tx.get("signature", "")
            timestamp = tx.get("timestamp", 0)
            source = tx.get("source", "UNKNOWN")
            description = tx.get("description", "").lower()

            has_sol_keywords = ("sol" in description or 
                       "so11111111111111111111111111111111111111112" in description)
    
            # 檢查穩定幣關鍵字和合約地址
            has_stablecoin_keywords = any(keyword in description.lower() for keyword in 
                                        ["usdc", "usdt", "usdh", "usdc-sol", 
                                        USDC_MINT.lower(), USDT_MINT.lower()])

            # 如果既沒有 SOL 也沒有穩定幣關鍵字，則跳過這筆交易
            if not (has_sol_keywords or has_stablecoin_keywords):
                # 可選：記錄被跳過的 memecoin 對 memecoin 交易
                # logging.info(f"跳過 memecoin 對 memecoin 交易: {description}")
                continue

            # 檢查是否已處理此交易
            has_processed = False
            for existing_tx in self.processed_txs:
                if existing_tx.signature == signature:
                    has_processed = True
                    break
                    
            if has_processed:
                continue

            # 提取 swap 事件數據
            events = tx.get("events", {})
            swap_event = events.get("swap", {})
            
            # 記錄swap事件數據，便於調試複雜交易
            # if swap_event:
                # logging.info(f"Swap事件概要: {json.dumps(swap_event, cls=DecimalEncoder, default=str)[:500]}...")
            
            # 提取 token 轉賬和原生 SOL 轉賬數據
            token_transfers = tx.get("tokenTransfers", [])
            native_transfers = tx.get("nativeTransfers", [])
            account_data = tx.get("accountData", [])
            
            if source == "JUPITER":
                # 從描述中判斷交易類型
                tx_type_from_description = None
                extracted_sol_amount = None
                extracted_stablecoin_amount = None
                
                if description:
                    if "swapped" in description and "sol" in description.split("for")[0].lower():
                        # 買入交易: "swapped X SOL for Y TOKEN"
                        tx_type_from_description = "BUY"
                        
                        # 嘗試從描述中提取SOL數量
                        try:
                            import re
                            sol_buy_pattern = r"swapped\s+([\d\.]+)\s+sol\s+for"
                            sol_buy_match = re.search(sol_buy_pattern, description, re.IGNORECASE)
                            if sol_buy_match:
                                extracted_sol_amount = float(sol_buy_match.group(1))
                                # logging.info(f"從描述中提取SOL買入量: {extracted_sol_amount}")
                        except Exception as e:
                            logging.warning(f"從描述中提取SOL失敗: {e}")
                    
                    elif "for" in description and "sol" in description.split("for")[1].lower():
                        # 賣出交易: "swapped X TOKEN for Y SOL"
                        tx_type_from_description = "SELL"
                        
                        # 嘗試從描述中提取SOL數量
                        try:
                            import re
                            sol_sell_pattern = r"for\s+([\d\.]+)\s+sol"
                            sol_sell_match = re.search(sol_sell_pattern, description, re.IGNORECASE)
                            if sol_sell_match:
                                extracted_sol_amount = float(sol_sell_match.group(1))
                                # logging.info(f"從描述中提取SOL賣出量: {extracted_sol_amount}")
                        except Exception as e:
                            logging.warning(f"從描述中提取SOL失敗: {e}")

                    elif any(stable in description.split("for")[0].lower() for stable in ["usdc", "usdt", "usdh"]):
                        tx_type_from_description = "BUY"
                        
                        # 嘗試從描述中提取穩定幣數量和類型
                        try:
                            import re
                            # 匹配穩定幣金額和類型
                            stablecoin_pattern = r"swapped\s+([\d\.]+)\s+(usdc|usdt|usdh)"
                            stablecoin_match = re.search(stablecoin_pattern, description, re.IGNORECASE)
                            if stablecoin_match:
                                extracted_stablecoin_amount = float(stablecoin_match.group(1))
                                extracted_stablecoin_type = stablecoin_match.group(2).upper()
                                # logging.info(f"從描述中提取{extracted_stablecoin_type}買入量: {extracted_stablecoin_amount}")
                        except Exception as e:
                            logging.warning(f"從描述中提取穩定幣失敗: {e}")
                    
                    # 賣出交易: "swapped X TOKEN for Y USDC/USDT"
                    elif "for" in description and any(stable in description.split("for")[1].lower() for stable in ["usdc", "usdt", "usdh"]):
                        tx_type_from_description = "SELL"
                        
                        # 嘗試從描述中提取穩定幣數量和類型
                        try:
                            import re
                            # 匹配穩定幣金額和類型
                            stablecoin_pattern = r"for\s+([\d\.]+)\s+(usdc|usdt|usdh)"
                            stablecoin_match = re.search(stablecoin_pattern, description, re.IGNORECASE)
                            if stablecoin_match:
                                extracted_stablecoin_amount = float(stablecoin_match.group(1))
                                extracted_stablecoin_type = stablecoin_match.group(2).upper()
                                # logging.info(f"從描述中提取{extracted_stablecoin_type}賣出量: {extracted_stablecoin_amount}")
                        except Exception as e:
                            logging.warning(f"從描述中提取穩定幣失敗: {e}")
                
                # 從swap事件中判斷交易類型
                tx_type_from_swap = None
                sol_from_swap = 0
                usdc_from_swap = 0
                
                if swap_event:
                    # 檢查nativeInput (用戶買入代幣)
                    if "nativeInput" in swap_event and swap_event["nativeInput"]:
                        native_input = swap_event["nativeInput"]
                        if native_input.get("account") == self.wallet_address and "amount" in native_input:
                            sol_from_swap = float(native_input["amount"]) / (10 ** 9)
                            tx_type_from_swap = "BUY"
                            # logging.info(f"從swap事件確認買入交易: 用戶支付了 {sol_from_swap} SOL")
                    
                    # 檢查nativeOutput (用戶賣出代幣)
                    elif "nativeOutput" in swap_event and swap_event["nativeOutput"]:
                        native_output = swap_event["nativeOutput"]
                        if native_output.get("account") == self.wallet_address and "amount" in native_output:
                            sol_from_swap = float(native_output["amount"]) / (10 ** 9)
                            tx_type_from_swap = "SELL"
                            # logging.info(f"從swap事件確認賣出交易: 用戶收到了 {sol_from_swap} SOL")
                
                # 確定交易類型 (優先級: 描述 > swap事件)
                jupiter_tx_type = tx_type_from_description or tx_type_from_swap
                
                # 如果能確定交易類型，處理交易
                if jupiter_tx_type:
                    # logging.info(f"Jupiter交易類型: {jupiter_tx_type}")
                    
                    if jupiter_tx_type == "BUY":
                        # 處理買入交易
                        # 確定SOL金額
                        sol_amount = extracted_sol_amount if extracted_sol_amount is not None else sol_from_swap
                        usdc_amount = extracted_stablecoin_amount if extracted_stablecoin_amount is not None else usdc_from_swap

                        from_token_mint = "So11111111111111111111111111111111111111112"  # SOL 的 mint 地址
                        from_token_amount = sol_amount
                        from_token_symbol = "SOL"
                        
                        # 確定接收的代幣
                        received_token = None
                        
                        # 從tokenTransfers尋找接收的代幣
                        for transfer in token_transfers:
                            if transfer.get("toUserAccount") == self.wallet_address:
                                token_mint = transfer.get("mint", "")
                                # 跳過SOL和穩定幣
                                if token_mint in SUPPORTED_TOKENS:
                                    continue
                                
                                # 提取代幣數量
                                token_amount = 0
                                if "rawTokenAmount" in transfer:
                                    raw_amount = transfer.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                elif "tokenAmount" in transfer:
                                    token_amount = float(transfer.get("tokenAmount", 0))
                                
                                if token_amount > 0:
                                    token_symbol, token_icon = self._get_token_symbol(token_mint)
                                    # logging.info(f"用戶接收代幣: {token_symbol}, 數量: {token_amount}")
                                    
                                    # 創建買入交易記錄
                                    tx_obj = TokenTransaction(
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
                                        from_token_mint=from_token_mint,
                                        from_token_amount=from_token_amount,
                                        from_token_symbol=from_token_symbol,
                                        dest_token_mint=token_mint,
                                        dest_token_amount=token_amount,
                                        dest_token_symbol=token_symbol
                                    )
                                    
                                    self.processed_txs.append(tx_obj)
                                    self._add_to_portfolio(tx_obj)
                                    received_token = token_mint
                                    break
                        
                        # 如果沒有在tokenTransfers中找到，嘗試從swap_event的tokenOutputs中尋找
                        if not received_token and "tokenOutputs" in swap_event:
                            for output in swap_event["tokenOutputs"]:
                                if output.get("userAccount") == self.wallet_address:
                                    token_mint = output.get("mint", "")
                                    # 跳過SOL和穩定幣
                                    if token_mint in SUPPORTED_TOKENS:
                                        continue
                                    
                                    # 提取代幣數量
                                    raw_amount = output.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                        
                                        if token_amount > 0:
                                            token_symbol, token_icon = self._get_token_symbol(token_mint)
                                            # logging.info(f"用戶從swap.tokenOutputs接收代幣: {token_symbol}, 數量: {token_amount}")
                                            
                                            # 創建買入交易記錄
                                            tx_obj = TokenTransaction(
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
                                                from_token_mint=from_token_mint,
                                                from_token_amount=from_token_amount,
                                                from_token_symbol=from_token_symbol,
                                                dest_token_mint=token_mint,
                                                dest_token_amount=token_amount,
                                                dest_token_symbol=token_symbol
                                            )
                                            
                                            self.processed_txs.append(tx_obj)
                                            self._add_to_portfolio(tx_obj)
                                            received_token = token_mint
                                            break
                    
                    elif jupiter_tx_type == "SELL":
                        # 處理賣出交易
                        # 確定SOL金額
                        sol_amount = extracted_sol_amount if extracted_sol_amount is not None else sol_from_swap
                        usdc_amount = extracted_stablecoin_amount if extracted_stablecoin_amount is not None else usdc_from_swap
                        
                        # 確定發送的代幣
                        sent_token = None
                        
                        # 從tokenTransfers尋找發送的代幣
                        for transfer in token_transfers:
                            if transfer.get("fromUserAccount") == self.wallet_address:
                                token_mint = transfer.get("mint", "")
                                # 跳過SOL和穩定幣
                                if token_mint in SUPPORTED_TOKENS:
                                    continue
                                
                                # 提取代幣數量
                                token_amount = 0
                                if "rawTokenAmount" in transfer:
                                    raw_amount = transfer.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                elif "tokenAmount" in transfer:
                                    token_amount = float(transfer.get("tokenAmount", 0))
                                
                                if token_amount > 0:
                                    token_symbol, token_icon = self._get_token_symbol(token_mint)
                                    # logging.info(f"用戶發送代幣: {token_symbol}, 數量: {token_amount}")
                                    
                                    # 創建賣出交易記錄
                                    tx_obj = TokenTransaction(
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
                                        from_token_mint=token_mint,
                                        from_token_amount=token_amount,
                                        from_token_symbol=token_symbol,
                                        dest_token_mint=token_mint,
                                        dest_token_amount=token_amount,
                                        dest_token_symbol=token_symbol
                                    )
                                    
                                    self.processed_txs.append(tx_obj)
                                    self._add_to_portfolio(tx_obj)
                                    sent_token = token_mint
                                    break
                        
                        # 如果沒有在tokenTransfers中找到，嘗試從innerSwaps中尋找
                        if not sent_token and "innerSwaps" in swap_event:
                            for inner_swap in swap_event["innerSwaps"]:
                                if "tokenInputs" in inner_swap:
                                    for token_input in inner_swap["tokenInputs"]:
                                        if token_input.get("fromUserAccount") == self.wallet_address:
                                            token_mint = token_input.get("mint", "")
                                            # 跳過SOL和穩定幣
                                            if token_mint in SUPPORTED_TOKENS:
                                                continue
                                            
                                            token_amount = token_input.get("tokenAmount", 0)
                                            if token_amount > 0:
                                                token_symbol, token_icon = self._get_token_symbol(token_mint)
                                                # logging.info(f"用戶從innerSwaps發送代幣: {token_symbol}, 數量: {token_amount}")
                                                
                                                # 創建賣出交易記錄
                                                tx_obj = TokenTransaction(
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
                                                    from_token_mint=token_mint,
                                                    from_token_amount=token_amount,
                                                    from_token_symbol=token_symbol,
                                                    dest_token_mint=token_mint,
                                                    dest_token_amount=token_amount,
                                                    dest_token_symbol=token_symbol
                                                )
                                                
                                                self.processed_txs.append(tx_obj)
                                                self._add_to_portfolio(tx_obj)
                                                sent_token = token_mint
                                                break
                                    if sent_token:
                                        break
                        
                        # 如果仍然沒有找到發送的代幣，嘗試從accountData中的tokenBalanceChanges尋找
                        if not sent_token:
                            for account in account_data:
                                for token_balance in account.get("tokenBalanceChanges", []):
                                    if token_balance.get("userAccount") == self.wallet_address:
                                        raw_amount = token_balance.get("rawTokenAmount", {})
                                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                            token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                            
                                            # 只關注負向變化 (代幣減少)
                                            if token_amount < 0:
                                                token_mint = token_balance.get("mint")
                                                # 跳過SOL和穩定幣
                                                if token_mint in SUPPORTED_TOKENS:
                                                    continue
                                                
                                                token_symbol, token_icon = self._get_token_symbol(token_mint)
                                                token_amount = abs(token_amount)
                                                # logging.info(f"用戶從tokenBalanceChanges發送代幣: {token_symbol}, 數量: {token_amount}")
                                                
                                                # 創建賣出交易記錄
                                                tx_obj = TokenTransaction(
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
                                                    from_token_mint=token_mint,  # 發送的代幣是源代幣
                                                    from_token_amount=token_amount,
                                                    from_token_symbol=token_symbol,
                                                    dest_token_mint="So11111111111111111111111111111111111111112",  # 賣出時獲得的是SOL
                                                    dest_token_amount=sol_amount,
                                                    dest_token_symbol="SOL"
                                                )
                                                
                                                self.processed_txs.append(tx_obj)
                                                self._add_to_portfolio(tx_obj)
                                                sent_token = token_mint
                                                break
                                    if sent_token:
                                        break
                
                # 檢查是否已處理此交易
                has_processed = False
                for existing_tx in self.processed_txs:
                    if existing_tx.signature == signature:
                        has_processed = True
                        break
                
                if has_processed:
                    continue
            
            if tx.get("source") == "RAYDIUM":
                tx_obj = self.process_raydium_swap(tx)
                if tx_obj:
                    self.processed_txs.append(tx_obj)
                    self._add_to_portfolio(tx_obj)
                    continue
            
            if tx.get("source") == "PUMP_FUN":
                # 處理 SOL 的支出情況
                sol_spent = 0
                for native_transfer in native_transfers:
                    # 檢查用戶是否從此錢包發送 SOL
                    if native_transfer.get("fromUserAccount") == self.wallet_address:
                        recipient = native_transfer.get("toUserAccount")
                        # 忽略計算預算和其他系統轉賬
                        if (recipient != "ComputeBudget111111111111111111111111111111" and 
                            recipient != self.wallet_address):
                            sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                
                # 檢查用戶是否收到了代幣
                token_received = False
                received_token_mint = None
                received_token_amount = 0
                
                for token_transfer in token_transfers:
                    if token_transfer.get("toUserAccount") == self.wallet_address:
                        token_mint = token_transfer.get("mint", "")
                        # 跳過穩定幣
                        if token_mint in SUPPORTED_TOKENS:
                            continue
                        
                        # 提取代幣數量
                        raw_amount = token_transfer.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            token_received = True
                            received_token_mint = token_mint
                            received_token_amount = token_amount
                            break
                
                # 如果沒有在 tokenTransfers 中找到用戶收到的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
                if not token_received:
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("userAccount") == self.wallet_address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                    # 只關注正向變化 (代幣增加)
                                    if amount_change > 0:
                                        token_mint = token_balance.get("mint")
                                        token_received = True
                                        received_token_mint = token_mint
                                        received_token_amount = amount_change
                                        break
                
                # 如果用戶花費了 SOL 並且收到了代幣，這是一筆買入交易
                if sol_spent > 0 and token_received:
                    token_symbol, token_icon = self._get_token_symbol(received_token_mint)
                    
                    tx_obj = TokenTransaction(
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
                        from_token_mint="So11111111111111111111111111111111111111112",
                        from_token_amount=sol_spent,
                        from_token_symbol="SOL",
                        dest_token_mint=received_token_mint,
                        dest_token_amount=received_token_amount,
                        dest_token_symbol=token_symbol
                    )
                    
                    self.processed_txs.append(tx_obj)
                    self._add_to_portfolio(tx_obj)
                    continue  # 跳到下一個交易
                
                # 檢查用戶是否賣出了代幣並獲得了 SOL
                token_sold = False
                sold_token_mint = None
                sold_token_amount = 0
                sol_received = 0
                net_sol_change = 0  # 新增：計算淨 SOL 變化
                
                # 檢查 SOL 餘額變化 - 先取得總體變化
                for account in account_data:
                    if account.get("account") == self.wallet_address:
                        net_sol_change = account.get("nativeBalanceChange", 0)
                        if net_sol_change > 0:  # 正值表示淨獲得 SOL
                            sol_received = net_sol_change / (10 ** SOL_DECIMALS)
                
                # 檢查用戶是否發送了代幣
                for token_transfer in token_transfers:
                    if token_transfer.get("fromUserAccount") == self.wallet_address:
                        token_mint = token_transfer.get("mint", "")
                        # 跳過穩定幣
                        if token_mint in SUPPORTED_TOKENS:
                            continue
                        
                        # 提取代幣數量
                        raw_amount = token_transfer.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            token_sold = True
                            sold_token_mint = token_mint
                            sold_token_amount = token_amount
                            break
                        elif "tokenAmount" in token_transfer:
                            token_amount = float(token_transfer.get("tokenAmount", 0))
                            token_sold = True
                            sold_token_mint = token_mint
                            sold_token_amount = token_amount
                            break
                
                # 如果沒有在 tokenTransfers 中找到用戶發送的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
                if not token_sold:
                    for account in account_data:
                        for token_balance in account.get("tokenBalanceChanges", []):
                            if token_balance.get("userAccount") == self.wallet_address:
                                raw_amount = token_balance.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                    # 只關注負向變化 (代幣減少)
                                    if token_amount < 0:
                                        token_mint = token_balance.get("mint")
                                        # 跳過穩定幣
                                        if token_mint in SUPPORTED_TOKENS:
                                            continue
                                        token_sold = True
                                        sold_token_mint = token_mint
                                        sold_token_amount = abs(token_amount)
                                        break
                
                # 修改判斷條件: 如果用戶賣出了代幣且SOL餘額有增加(即使是通過複雜的交易結構)，都視為賣出交易
                if token_sold and net_sol_change > 0:
                    token_symbol, token_icon = self._get_token_symbol(sold_token_mint)
                    
                    # 這裡可以添加調試日誌
                    # print(f"檢測到PUMP_FUN賣出交易: {signature}")
                    # print(f"賣出代幣: {token_symbol}, 數量: {sold_token_amount}")
                    # print(f"SOL餘額淨變化: {sol_received} SOL")
                    
                    tx_obj = TokenTransaction(
                        signature=signature,
                        timestamp=timestamp,
                        tx_type="SELL",
                        source=source,
                        token_mint=sold_token_mint,
                        token_amount=sold_token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=sol_received,  # 使用淨SOL變化作為獲得的SOL金額
                        usdc_amount=0,
                        from_token_mint=sold_token_mint,
                        from_token_amount=sold_token_amount,
                        from_token_symbol=token_symbol,
                        dest_token_mint="So11111111111111111111111111111111111111112",
                        dest_token_amount=sol_received,
                        dest_token_symbol="SOL"
                    )
                    
                    self.processed_txs.append(tx_obj)
                    self._add_to_portfolio(tx_obj)
                    continue  # 跳到下一個交易
            
            # ===================== 核心交易類型檢測邏輯 =====================
            
            # 1. 檢查 tokenTransfers 中的買入/賣出模式
            # 檢查是否是用戶用 SOL 購買代幣的情況（從 tokenTransfers）
            sol_spent = 0
            user_sent_sol = False
                
            # 從 nativeTransfers 收集 SOL 支出信息
            for native_transfer in native_transfers:
                if native_transfer.get("fromUserAccount") == self.wallet_address:
                    recipient = native_transfer.get("toUserAccount")
                    # 忽略可能的手續費或其他小額轉賬
                    if recipient != "ComputeBudget111111111111111111111111111111":
                        sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                        user_sent_sol = True
            
            # 檢查用戶是否收到了代幣
            token_received = False
            received_token_mint = None
            received_token_amount = 0
            
            for token_transfer in token_transfers:
                if token_transfer.get("toUserAccount") == self.wallet_address:
                    token_mint = token_transfer.get("mint", "")
                    # 跳過穩定幣
                    if token_mint in SUPPORTED_TOKENS:
                        continue
                    
                    # 提取代幣數量
                    raw_amount = token_transfer.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        token_received = True
                        received_token_mint = token_mint
                        received_token_amount = token_amount
                        break
            
            # 如果用戶發送了 SOL 並收到了代幣，這是一筆購買交易
            if user_sent_sol and token_received and sol_spent > 0 and received_token_amount > 0:
                token_symbol, token_icon = self._get_token_symbol(received_token_mint)
                
                tx_obj = TokenTransaction(
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
                    from_token_mint="So11111111111111111111111111111111111111112",
                    from_token_amount=sol_spent,
                    from_token_symbol="SOL",
                    dest_token_mint=received_token_mint,
                    dest_token_amount=received_token_amount,
                    dest_token_symbol=token_symbol
                )
                
                self.processed_txs.append(tx_obj)
                self._add_to_portfolio(tx_obj)
                continue  # 跳到下一個交易
            
            # 2. 檢查用戶是否賣出代幣獲得 SOL
            sol_received = 0
            user_received_sol = False
            
            for native_transfer in native_transfers:
                if native_transfer.get("toUserAccount") == self.wallet_address:
                    sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                    user_received_sol = True
            
            # 檢查用戶是否發送了代幣
            token_sent = False
            sent_token_mint = None
            sent_token_amount = 0
            
            for token_transfer in token_transfers:
                if token_transfer.get("fromUserAccount") == self.wallet_address:
                    token_mint = token_transfer.get("mint", "")
                    # 跳過穩定幣
                    if token_mint in SUPPORTED_TOKENS:
                        continue
                    
                    # 提取代幣數量
                    raw_amount = token_transfer.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        token_sent = True
                        sent_token_mint = token_mint
                        sent_token_amount = token_amount
                        break
            
            # 如果用戶發送了代幣並收到了 SOL，這是一筆賣出交易
            if token_sent and user_received_sol and sent_token_amount > 0 and sol_received > 0:
                token_symbol, token_icon = self._get_token_symbol(sent_token_mint)
                
                tx_obj = TokenTransaction(
                    signature=signature,
                    timestamp=timestamp,
                    tx_type="SELL",
                    source=source,
                    token_mint=sent_token_mint,
                    token_amount=sent_token_amount,
                    token_symbol=token_symbol,
                    token_icon=token_icon,
                    sol_amount=sol_received,
                    usdc_amount=0,
                    from_token_mint=sent_token_mint,
                    from_token_amount=sent_token_amount,
                    from_token_symbol=token_symbol,
                    dest_token_mint="So11111111111111111111111111111111111111112",
                    dest_token_amount=sol_received,
                    dest_token_symbol="SOL"
                )
                
                self.processed_txs.append(tx_obj)
                self._add_to_portfolio(tx_obj)
                continue  # 跳到下一個交易
            
            # 3. 如果上面的檢測都沒有找到買賣模式，嘗試使用 events.swap 數據
            if swap_event:
                # 處理 memecoin 對 memecoin 交易
                token_inputs = swap_event.get("tokenInputs", [])
                token_outputs = swap_event.get("tokenOutputs", [])
                native_output = swap_event.get("nativeOutput", {})
                native_input = swap_event.get("nativeInput", {})
                
                # 檢查是否是 memecoin 對 memecoin 的交易
                user_sold_tokens = False
                from_token_mint = None
                from_token_amount = 0
                from_token_symbol = None
                from_token_icon = None
                sold_token_mint = None
                sold_token_amount = 0
                sold_token_symbol = None
                sold_token_icon = None
                
                for token_input in token_inputs:
                    if token_input.get("userAccount") == self.wallet_address:
                        user_sold_tokens = True
                        from_token_mint = token_input.get("mint", "")
                        from_token_symbol, from_token_icon = self._get_token_symbol(from_token_mint)
                        raw_amount = token_input.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            from_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        break

                for token_output in token_outputs:
                    if token_output.get("userAccount") == self.wallet_address:
                        user_sold_tokens = True
                        sold_token_mint = token_output.get("mint", "")
                        sold_token_symbol, sold_token_icon = self._get_token_symbol(sold_token_mint)
                        raw_amount = token_output.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            sold_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        break
                
                # 檢查用戶是否收到了非穩定幣代幣
                user_received_tokens = []
                
                for transfer in token_transfers:
                    if transfer.get("toUserAccount") == self.wallet_address:
                        token_mint = transfer.get("mint", "")
                        # 跳過 SOL 和穩定幣
                        if token_mint in SUPPORTED_TOKENS:
                            continue
                        # 跳過賣出的同一種代幣
                        if token_mint == sold_token_mint:
                            continue
                        
                        # 提取代幣數量
                        raw_amount = None
                        if "rawTokenAmount" in transfer:
                            raw_amount = transfer["rawTokenAmount"]
                        else:
                            token_amount = transfer.get("tokenAmount", 0)
                            if token_amount:
                                try:
                                    token_amount = float(token_amount)
                                    raw_amount = {"tokenAmount": token_amount, "decimals": 0}
                                except:
                                    continue
                        
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            token_symbol, token_icon = self._get_token_symbol(token_mint)
                            received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            user_received_tokens.append({
                                "mint": token_mint,
                                "symbol": token_symbol,
                                "icon": token_icon,
                                "amount": received_amount
                            })
                
                # 處理 SOL 的中間金額
                intermediate_sol_amount = 0.0
                if native_output and "amount" in native_output:
                    intermediate_sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                elif native_input and "amount" in native_input:
                    intermediate_sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
                # 處理 memecoin 對 memecoin 的交易
                if user_sold_tokens and user_received_tokens and intermediate_sol_amount > 0:
                    # 从tokenTransfers中获取准确的代币数量
                    accurate_sold_amount = 0
                    for transfer in token_transfers:
                        if (transfer.get("fromUserAccount") == self.wallet_address and 
                            transfer.get("mint") == sold_token_mint):
                            if "tokenAmount" in transfer:
                                accurate_sold_amount = float(transfer.get("tokenAmount", 0))
                            elif "rawTokenAmount" in transfer:
                                raw_amount = transfer.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    accurate_sold_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            break
                    
                    # 如果tokenTransfers没有找到，尝试从accountData中查找
                    if accurate_sold_amount == 0:
                        for account in account_data:
                            for token_balance in account.get("tokenBalanceChanges", []):
                                if (token_balance.get("userAccount") == self.wallet_address and
                                    token_balance.get("mint") == sold_token_mint):
                                    raw_amount = token_balance.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                        if amount_change < 0:  # 负值表示代币减少（卖出）
                                            accurate_sold_amount = abs(amount_change)
                                            break
                    
                    # 如果找到了准确的数量，使用它
                    if accurate_sold_amount > 0:
                        sold_token_amount = accurate_sold_amount
                    
                    # 创建卖出交易记录，使用准确的数量
                    tx_obj_sell = TokenTransaction(
                        signature=signature,
                        timestamp=timestamp,
                        tx_type="SELL",
                        source=source,
                        token_mint=sold_token_mint,
                        token_amount=sold_token_amount,  # 使用修正后的数量
                        token_symbol=sold_token_symbol,
                        token_icon=sold_token_icon,
                        sol_amount=intermediate_sol_amount,
                        usdc_amount=0,
                        from_token_mint=from_token_mint,
                        from_token_amount=from_token_amount,
                        from_token_symbol=from_token_symbol,
                        dest_token_mint=token_mint,
                        dest_token_amount=token_amount,
                        dest_token_symbol=token_symbol
                    )
                    
                    self.processed_txs.append(tx_obj_sell)
                    self._add_to_portfolio(tx_obj_sell)
                    
                    # 再處理買入部分
                    for received_token in user_received_tokens:
                        # 从tokenTransfers中获取准确的接收代币数量
                        accurate_received_amount = 0
                        received_token_mint = received_token["mint"]
                        
                        for transfer in token_transfers:
                            if (transfer.get("toUserAccount") == self.wallet_address and 
                                transfer.get("mint") == received_token_mint):
                                if "tokenAmount" in transfer:
                                    accurate_received_amount = float(transfer.get("tokenAmount", 0))
                                elif "rawTokenAmount" in transfer:
                                    raw_amount = transfer.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        accurate_received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                break
                        
                        # 如果tokenTransfers没有找到，尝试从accountData中查找
                        if accurate_received_amount == 0:
                            for account in account_data:
                                for token_balance in account.get("tokenBalanceChanges", []):
                                    if (token_balance.get("userAccount") == self.wallet_address and
                                        token_balance.get("mint") == received_token_mint):
                                        raw_amount = token_balance.get("rawTokenAmount", {})
                                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                            amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                            if amount_change > 0:  # 正值表示代币增加（买入）
                                                accurate_received_amount = amount_change
                                                break
                        
                        # 如果找到了准确的数量，使用它，否则使用previously检测到的数量
                        token_amount = accurate_received_amount if accurate_received_amount > 0 else received_token["amount"]
                        
                        # 创建买入交易记录
                        tx_obj_buy = TokenTransaction(
                            signature=signature,  # 添加后缀以区分
                            timestamp=timestamp,
                            tx_type="BUY",
                            source=source,
                            token_mint=received_token_mint,
                            token_amount=token_amount,  # 使用修正后的数量
                            token_symbol=received_token["symbol"],
                            token_icon=received_token["icon"],
                            sol_amount=intermediate_sol_amount,  # 注意：在memecoin对memecoin交易中，这只是中间值
                            usdc_amount=0,
                            from_token_mint=sold_token_mint,
                            from_token_amount=sold_token_amount,
                            from_token_symbol=sold_token_symbol,
                            # from_token_mint="So11111111111111111111111111111111111111112", # 中間SOL
                            # from_token_amount=intermediate_sol_amount,
                            # from_token_symbol="SOL",
                            dest_token_mint=received_token_mint,
                            dest_token_amount=token_amount,
                            dest_token_symbol=received_token["symbol"]
                        )
                        
                        self.processed_txs.append(tx_obj_buy)
                        self._add_to_portfolio(tx_obj_buy)
                        
                        # 记录日志，帮助调试
                        logging.info(f"处理memecoin对memecoin买入部分：接收代币={received_token['symbol']}, 数量={token_amount}")
                    
                    continue  # 跳到下一個交易
                if signature == "2dZd2L2Bpff42dFuRuQiDUWsJ2YaCgnNhD5LxjQRNNYAjnvSz2MnnRPZ8BukxHyz12EePSTZw1FoEebVGc2V9gH8":
                    logging.info(f"正在处理特殊交易: {description}")
                    logging.info(f"中间SOL金额: {intermediate_sol_amount}")
                    logging.info(f"卖出代币数量: {sold_token_amount}")
                    for token in user_received_tokens:
                        logging.info(f"买入代币: {token['symbol']}, 数量: {token['amount']}")
                # 處理常規的買入和賣出交易
                # 過濾非穩定幣或原生代幣的交易
                has_supported_token = False
                
                # 檢查輸入中是否有支持的代幣
                for token_input in token_inputs:
                    if token_input.get("mint") in SUPPORTED_TOKENS:
                        has_supported_token = True
                        break
                
                # 檢查輸出中是否有支持的代幣
                if not has_supported_token:
                    for token_output in token_outputs:
                        if token_output.get("mint") in SUPPORTED_TOKENS:
                            has_supported_token = True
                            break
                
                # 檢查是否涉及原生 SOL
                if (native_input and "amount" in native_input) or (native_output and "amount" in native_output):
                    has_supported_token = True
                
                # 如果不涉及支持的代幣，跳過此交易
                if not has_supported_token:
                    continue
                
                # 處理代幣輸入 (賣出)
                for token_input in token_inputs:
                    # 確保這是目標錢包的交易
                    if token_input.get("userAccount") != self.wallet_address:
                        continue
                    
                    raw_amount = token_input.get("rawTokenAmount", {})
                    token_mint = token_input.get("mint", "")
                    token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        
                        sol_amount = 0.0
                        usdc_amount = 0.0
                        dest_token_mint = ""
                        dest_token_amount = 0.0
                        dest_token_symbol = ""
                        
                        # 處理 SOL 輸出 (賣出代幣獲得 SOL)
                        if native_output and "amount" in native_output:
                            sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                            dest_token_mint = "So11111111111111111111111111111111111111112"  # SOL 的 mint 地址
                            dest_token_amount = sol_amount
                            dest_token_symbol = "SOL"
                        
                        # 處理 USDC/USDT 輸出
                        else:
                            for output in token_outputs:
                                output_mint = output.get("mint", "")
                                if output_mint in STABLECOINS and output.get("userAccount") == self.wallet_address:
                                    output_raw_amount = output.get("rawTokenAmount", {})
                                    if output_raw_amount and "tokenAmount" in output_raw_amount and "decimals" in output_raw_amount:
                                        usdc_amount = float(output_raw_amount["tokenAmount"]) / (10 ** output_raw_amount["decimals"])
                                        # 根據穩定幣類型設置目標代幣信息
                                        dest_token_mint = output_mint
                                        dest_token_amount = usdc_amount
                                        # 根據 mint 地址確定穩定幣類型
                                        if output_mint == USDC_MINT:
                                            dest_token_symbol = "USDC"
                                        elif output_mint == USDT_MINT:
                                            dest_token_symbol = "USDT"
                                        else:
                                            dest_token_symbol = "USDC"  # 默認
                        
                        # 只處理有 SOL 或穩定幣輸出的交易
                        if sol_amount > 0 or usdc_amount > 0:
                            tx_obj = TokenTransaction(
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
                                from_token_mint=token_mint,
                                from_token_amount=token_amount,
                                from_token_symbol=token_symbol,
                                dest_token_mint=dest_token_mint,
                                dest_token_amount=dest_token_amount,
                                dest_token_symbol=dest_token_symbol
                            )
                            
                            self.processed_txs.append(tx_obj)
                            self._add_to_portfolio(tx_obj)
                
                # 處理代幣輸出 (買入)
                for token_output in token_outputs:
                    # 確保這是目標錢包的交易
                    if token_output.get("userAccount") != self.wallet_address:
                        continue
                    
                    raw_amount = token_output.get("rawTokenAmount", {})
                    token_mint = token_output.get("mint", "")
                    token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        
                        # 處理三種情況：SOL 輸入、穩定幣輸入、其他代幣輸入
                        sol_amount = 0.0
                        usdc_amount = 0.0
                        from_token_mint = ""
                        from_token_amount = 0.0
                        from_token_symbol = ""
                        
                        # 處理 SOL 輸入 (用 SOL 買入代幣)
                        if native_input and "amount" in native_input:
                            sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                            from_token_mint = "So11111111111111111111111111111111111111112"  # SOL的mint地址
                            from_token_amount = sol_amount
                            from_token_symbol = "SOL"
                        
                        # 處理穩定幣輸入
                        else:
                            for input_token in token_inputs:
                                input_mint = input_token.get("mint", "")
                                if input_mint in STABLECOINS and input_token.get("userAccount") == self.wallet_address:
                                    input_raw_amount = input_token.get("rawTokenAmount", {})
                                    if input_raw_amount and "tokenAmount" in input_raw_amount and "decimals" in input_raw_amount:
                                        usdc_amount = float(input_raw_amount["tokenAmount"]) / (10 ** input_raw_amount["decimals"])
                                        from_token_mint = input_mint
                                        from_token_amount = usdc_amount
                                        # 根據 mint 地址確定穩定幣類型
                                        if input_mint == USDC_MINT:
                                            from_token_symbol = "USDC"
                                        elif input_mint == USDT_MINT:
                                            from_token_symbol = "USDT"
                                        else:
                                            from_token_symbol = "USDC"  # 默認
                        
                        # 只處理有 SOL 或穩定幣輸入的交易
                        if sol_amount > 0 or usdc_amount > 0:
                            tx_obj = TokenTransaction(
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
                                from_token_mint=from_token_mint,
                                from_token_amount=from_token_amount,
                                from_token_symbol=from_token_symbol,
                                dest_token_mint=token_mint,
                                dest_token_amount=token_amount,
                                dest_token_symbol=token_symbol
                            )
                            
                            self.processed_txs.append(tx_obj)
                            self._add_to_portfolio(tx_obj)
            
            # 4. 強化特殊情況處理 - 用於處理 nativeTransfers 和 tokenTransfers
            # 檢查是否有交易但未處理
            has_processed_tx = False
            for tx_obj in self.processed_txs:
                if tx_obj.signature == signature:
                    has_processed_tx = True
                    break

            if not has_processed_tx and native_transfers:
                # 檢查是否有 SOL 轉出並且有代幣轉入
                sol_spent = 0
                for native_transfer in native_transfers:
                    if (native_transfer.get("fromUserAccount") == self.wallet_address and
                        native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111"):
                        sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                
                # 如果有 SOL 支出，檢查是否有代幣轉入
                if sol_spent > 0:
                    for token_transfer in token_transfers:
                        if token_transfer.get("toUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
                            token_mint = token_transfer.get("mint")
                            token_symbol, token_icon = self._get_token_symbol(token_mint)
                            
                            token_amount = 0
                            raw_amount = token_transfer.get("rawTokenAmount", {})
                            if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            
                            if token_amount > 0:
                                tx_obj = TokenTransaction(
                                    signature=signature,
                                    timestamp=timestamp,
                                    tx_type="BUY",
                                    source=source,
                                    token_mint=token_mint,
                                    token_amount=token_amount,
                                    token_symbol=token_symbol,
                                    token_icon=token_icon,
                                    sol_amount=sol_spent,
                                    usdc_amount=0,
                                    from_token_mint="So11111111111111111111111111111111111111112",  # SOL 的 mint 地址
                                    from_token_amount=sol_spent,
                                    from_token_symbol="SOL",
                                    dest_token_mint=token_mint,
                                    dest_token_amount=token_amount,
                                    dest_token_symbol=token_symbol
                                )
                                
                                self.processed_txs.append(tx_obj)
                                self._add_to_portfolio(tx_obj)
                                break  # 只處理第一個找到的轉入代幣
                    
                    # 如果沒有在 tokenTransfers 中找到代幣轉入，檢查 accountData
                    if has_processed_tx == False:
                        for account in account_data:
                            for token_balance in account.get("tokenBalanceChanges", []):
                                if token_balance.get("userAccount") == self.wallet_address:
                                    raw_amount = token_balance.get("rawTokenAmount", {})
                                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                        amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                        # 只關注正向變化 (代幣增加)
                                        if amount_change > 0:
                                            token_mint = token_balance.get("mint")
                                            # 跳過穩定幣
                                            if token_mint in SUPPORTED_TOKENS:
                                                continue
                                            
                                            token_symbol, token_icon = self._get_token_symbol(token_mint)
                                            
                                            tx_obj = TokenTransaction(
                                                signature=signature,
                                                timestamp=timestamp,
                                                tx_type="BUY",
                                                source=source,
                                                token_mint=token_mint,
                                                token_amount=amount_change,
                                                token_symbol=token_symbol,
                                                token_icon=token_icon,
                                                sol_amount=sol_spent,
                                                usdc_amount=0,
                                                from_token_mint="So11111111111111111111111111111111111111112",  # SOL 的 mint 地址
                                                from_token_amount=sol_spent,
                                                from_token_symbol="SOL",
                                                dest_token_mint=token_mint,
                                                dest_token_amount=amount_change,
                                                dest_token_symbol=token_symbol
                                            )
                                            
                                            self.processed_txs.append(tx_obj)
                                            self._add_to_portfolio(tx_obj)
                                            break  # 只處理第一個找到的代幣增加
                
                # 檢查是否有 SOL 轉入並且有代幣轉出 (賣出交易)
                if not has_processed_tx:
                    sol_received = 0
                    for native_transfer in native_transfers:
                        if native_transfer.get("toUserAccount") == self.wallet_address:
                            sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
                    
                    # 檢查用戶是否發送了代幣
                    if sol_received > 0:
                        for token_transfer in token_transfers:
                            if token_transfer.get("fromUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
                                token_mint = token_transfer.get("mint")
                                token_symbol, token_icon = self._get_token_symbol(token_mint)
                                
                                token_amount = 0
                                raw_amount = token_transfer.get("rawTokenAmount", {})
                                if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                    token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                elif "tokenAmount" in token_transfer:
                                    token_amount = float(token_transfer.get("tokenAmount", 0))
                                
                                if token_amount > 0:
                                    tx_obj = TokenTransaction(
                                        signature=signature,
                                        timestamp=timestamp,
                                        tx_type="SELL",
                                        source=source,
                                        token_mint=token_mint,
                                        token_amount=token_amount,
                                        token_symbol=token_symbol,
                                        token_icon=token_icon,
                                        sol_amount=sol_received,
                                        usdc_amount=0,
                                        from_token_mint=token_mint,  # 賣出交易，源幣種就是被賣出的代幣
                                        from_token_amount=token_amount,
                                        from_token_symbol=token_symbol,
                                        dest_token_mint="So11111111111111111111111111111111111111112",  # SOL 的 mint 地址
                                        dest_token_amount=sol_received,
                                        dest_token_symbol="SOL"
                                    )
                                    
                                    self.processed_txs.append(tx_obj)
                                    self._add_to_portfolio(tx_obj)
                                    break  # 只處理第一個找到的代幣增加

# --------------------------------------------------------------------------------------------------------------------
    # def process_transactions(self):
    #     """處理所有交易，提取買入/賣出信息並統一計算指標"""
    #     # 確保交易按時間排序
    #     self.transactions.sort(key=lambda tx: tx.get("timestamp", 0))
        
    #     # 重置處理結果
    #     self.processed_txs = []
    #     self.portfolios = {}
        
    #     # 初始化統計數據字典
    #     self.token_stats = {}
        
    #     for tx in self.transactions:
    #         # 跳過非 SWAP 類型的交易
    #         if tx.get("type") != "SWAP":
    #             continue
            
    #         # 提取基本交易信息
    #         signature = tx.get("signature", "")
    #         timestamp = tx.get("timestamp", 0)
    #         source = tx.get("source", "UNKNOWN")

    #         # 檢查是否已處理此交易
    #         has_processed = False
    #         for existing_tx in self.processed_txs:
    #             if existing_tx.signature == signature:
    #                 has_processed = True
    #                 break
                    
    #         if has_processed:
    #             continue

    #         # 處理不同來源的交易
    #         tx_obj = None
            
    #         # 處理 RAYDIUM 來源的交易
    #         if tx.get("source") == "RAYDIUM":
    #             tx_obj = self.process_raydium_swap(tx)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 self._update_token_stats(tx_obj)  # 更新統計數據
    #                 continue
            
    #         # 提取交易數據
    #         events = tx.get("events", {})
    #         swap_event = events.get("swap", {})
    #         token_transfers = tx.get("tokenTransfers", [])
    #         native_transfers = tx.get("nativeTransfers", [])
    #         account_data = tx.get("accountData", [])
            
    #         # ===================== 優先處理 PUMP_FUN 來源的交易 =====================
    #         if tx.get("source") == "PUMP_FUN":
    #             # 處理買入交易
    #             tx_obj = self._process_pump_fun_buy(tx, signature, timestamp, source, native_transfers, token_transfers, account_data)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 self._update_token_stats(tx_obj)
    #                 continue
                    
    #             # 處理賣出交易
    #             tx_obj = self._process_pump_fun_sell(tx, signature, timestamp, source, native_transfers, token_transfers, account_data)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 self._update_token_stats(tx_obj)
    #                 continue
            
    #         # ===================== 核心交易類型檢測邏輯 =====================
    #         # 1. 檢查 tokenTransfers 中的買入模式
    #         tx_obj = self._process_standard_buy(tx, signature, timestamp, source, native_transfers, token_transfers)
    #         if tx_obj:
    #             self.processed_txs.append(tx_obj)
    #             self._add_to_portfolio(tx_obj)
    #             self._update_token_stats(tx_obj)
    #             continue
                
    #         # 2. 檢查用戶是否賣出代幣獲得 SOL
    #         tx_obj = self._process_standard_sell(tx, signature, timestamp, source, native_transfers, token_transfers)
    #         if tx_obj:
    #             self.processed_txs.append(tx_obj)
    #             self._add_to_portfolio(tx_obj)
    #             self._update_token_stats(tx_obj)
    #             continue
                
    #         # 3. 如果上面的檢測都沒有找到買賣模式，嘗試使用 events.swap 數據
    #         if swap_event:
    #             # 處理 memecoin 對 memecoin 交易
    #             memecoin_swap_results = self._process_memecoin_swap(tx, signature, timestamp, source, swap_event, token_transfers)
    #             if memecoin_swap_results:
    #                 for tx_obj in memecoin_swap_results:
    #                     self.processed_txs.append(tx_obj)
    #                     self._add_to_portfolio(tx_obj)
    #                     self._update_token_stats(tx_obj)
    #                 continue
                    
    #             # 處理常規的買入和賣出交易
    #             swap_results = self._process_standard_swap(tx, signature, timestamp, source, swap_event, token_transfers)
    #             if swap_results:
    #                 for tx_obj in swap_results:
    #                     self.processed_txs.append(tx_obj)
    #                     self._add_to_portfolio(tx_obj)
    #                     self._update_token_stats(tx_obj)
    #                 continue
                    
    #         # 4. 強化特殊情況處理 - 用於處理 nativeTransfers 和 tokenTransfers
    #         has_processed_tx = False
    #         for tx_obj in self.processed_txs:
    #             if tx_obj.signature == signature:
    #                 has_processed_tx = True
    #                 break
            
    #         if not has_processed_tx and native_transfers:
    #             # 處理特殊買入情況
    #             tx_obj = self._process_special_buy(tx, signature, timestamp, source, native_transfers, token_transfers, account_data)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 self._update_token_stats(tx_obj)
    #                 continue
                    
    #             # 處理特殊賣出情況
    #             tx_obj = self._process_special_sell(tx, signature, timestamp, source, native_transfers, token_transfers)
    #             if tx_obj:
    #                 self.processed_txs.append(tx_obj)
    #                 self._add_to_portfolio(tx_obj)
    #                 self._update_token_stats(tx_obj)
    #                 continue
        
    #     # 計算總體指標
    #     self._calculate_overall_metrics()
        
    #     print(f"處理了 {len(self.processed_txs)} 筆代幣交易")

    # def _update_token_stats(self, tx):
    #     """更新代幣統計數據，用於後續計算聰明錢指標"""
    #     token_mint = tx.token_mint
        
    #     # 初始化代幣統計資料
    #     if token_mint not in self.token_stats:
    #         self.token_stats[token_mint] = {
    #             'total_buy_amount': 0,           # 總買入量
    #             'total_buy_cost_usd': 0,         # 總買入成本(USD)
    #             'total_sell_amount': 0,          # 總賣出量
    #             'total_sell_value_usd': 0,       # 總賣出價值(USD)
    #             'current_holdings': 0,           # 當前持有量
    #             'current_cost_basis': 0,         # 當前成本基礎
    #             'buy_lots': [],                  # 買入批次(FIFO計算用)
    #             'realized_pnl_usd': 0,           # 已實現PNL(USD)
    #             'buy_count': 0,                  # 買入次數
    #             'sell_count': 0,                 # 賣出次數
    #             'avg_buy_price': 0,              # 平均買入價格
    #             'first_buy_time': None,          # 首次買入時間
    #             'last_sell_time': None           # 最後賣出時間
    #         }
            
    #     stats = self.token_stats[token_mint]
        
    #     # 計算交易的USD價值
    #     tx_value_usd = tx.usd_amount if tx.usd_amount > 0 else \
    #                 (tx.sol_amount * self.sol_price_usd if tx.sol_amount > 0 else tx.usdc_amount)
        
    #     if tx.tx_type == "BUY":
    #         # 記錄買入批次(用於FIFO計算)
    #         stats['buy_lots'].append({
    #             'amount': tx.token_amount,
    #             'cost': tx_value_usd,
    #             'price': tx_value_usd / tx.token_amount if tx.token_amount > 0 else 0,
    #             'time': tx.timestamp
    #         })
            
    #         # 更新統計數據
    #         stats['buy_count'] += 1
    #         stats['total_buy_amount'] += tx.token_amount
    #         stats['total_buy_cost_usd'] += tx_value_usd
    #         stats['current_holdings'] += tx.token_amount
    #         stats['current_cost_basis'] += tx_value_usd
            
    #         # 更新平均買入價格
    #         stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings'] if stats['current_holdings'] > 0 else 0
            
    #         # 記錄首次買入時間
    #         if stats['first_buy_time'] is None or tx.timestamp < stats['first_buy_time']:
    #             stats['first_buy_time'] = tx.timestamp
            
    #     elif tx.tx_type == "SELL":
    #         # 確保有足夠的買入記錄
    #         sell_amount = min(tx.token_amount, stats['current_holdings'])
            
    #         if sell_amount > 0 and stats['buy_lots']:
    #             # 使用FIFO計算已實現利潤
    #             remaining_to_sell = sell_amount
    #             cost_basis = 0
                
    #             # 計算賣出的成本基礎
    #             temp_buy_lots = stats['buy_lots'].copy()
                
    #             while remaining_to_sell > 0 and temp_buy_lots:
    #                 oldest_lot = temp_buy_lots[0]
                    
    #                 if oldest_lot['amount'] <= remaining_to_sell:
    #                     # 整個批次都被賣出
    #                     cost_basis += oldest_lot['cost']
    #                     remaining_to_sell -= oldest_lot['amount']
    #                     temp_buy_lots.pop(0)
    #                 else:
    #                     # 部分批次被賣出
    #                     sell_ratio = remaining_to_sell / oldest_lot['amount']
    #                     partial_cost = oldest_lot['cost'] * sell_ratio
    #                     cost_basis += partial_cost
                        
    #                     # 更新原批次
    #                     oldest_lot['amount'] -= remaining_to_sell
    #                     oldest_lot['cost'] -= partial_cost
    #                     remaining_to_sell = 0
                
    #             # 計算賣出價值
    #             sell_price_per_token = tx_value_usd / tx.token_amount if tx.token_amount > 0 else 0
    #             sell_value_usd = sell_amount * sell_price_per_token
                
    #             # 計算已實現PNL
    #             realized_pnl = sell_value_usd - cost_basis
    #             stats['realized_pnl_usd'] += realized_pnl
                
    #             # 更新統計數據
    #             stats['sell_count'] += 1
    #             stats['total_sell_amount'] += sell_amount
    #             stats['total_sell_value_usd'] += sell_value_usd
    #             stats['current_holdings'] -= sell_amount
                
    #             # 實際更新買入批次
    #             stats['buy_lots'] = temp_buy_lots
                
    #             # 重新計算當前成本基礎
    #             stats['current_cost_basis'] = sum(lot['cost'] for lot in stats['buy_lots'])
                
    #             # 如果完全清倉，重置平均買入價格
    #             if stats['current_holdings'] <= 0:
    #                 stats['current_cost_basis'] = 0
    #                 stats['avg_buy_price'] = 0
    #             else:
    #                 # 更新平均買入價格
    #                 stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings']
    #         else:
    #             # 外部轉入或無買入記錄的賣出，只記錄統計不計入PNL
    #             stats['sell_count'] += 1
    #             stats['total_sell_amount'] += tx.token_amount
    #             stats['total_sell_value_usd'] += tx_value_usd
            
    #         # 記錄最後賣出時間
    #         stats['last_sell_time'] = tx.timestamp

    # def _calculate_overall_metrics(self):
    #     """計算聰明錢篩選所需的總體指標"""
    #     # 計算總體指標
    #     self.total_buy = sum(stats['buy_count'] for stats in self.token_stats.values())
    #     self.total_sell = sum(stats['sell_count'] for stats in self.token_stats.values())
    #     self.total_cost = sum(stats['total_buy_cost_usd'] for stats in self.token_stats.values())
    #     self.total_sell_value = sum(stats['total_sell_value_usd'] for stats in self.token_stats.values())
    #     self.total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in self.token_stats.values())
        
    #     # 計算勝率：只考慮有買入記錄的代幣
    #     tokens_with_buys = [token for token, stats in self.token_stats.items() if stats['total_buy_amount'] > 0]
    #     self.profitable_tokens = sum(1 for token in tokens_with_buys if self.token_stats[token]['realized_pnl_usd'] > 0)
    #     self.total_tokens = len(tokens_with_buys)
        
    #     # 計算PNL百分比：已實現PNL / 總賣出價值
    #     self.pnl_percentage = (self.total_realized_pnl / self.total_sell_value) * 100 if self.total_sell_value > 0 else 0
    #     # 確保不低於-100%
    #     self.pnl_percentage = max(self.pnl_percentage, -100)
        
    #     # 勝率：盈利代幣數量/總代幣數量
    #     self.win_rate = (self.profitable_tokens / self.total_tokens) * 100 if self.total_tokens > 0 else 0
        
    #     # 資產槓桿
    #     self.asset_multiple = self.pnl_percentage / 100
        
    #     # 計算其他指標
    #     self.only_sell_tokens = sum(1 for p in self.portfolios.values() if p.is_only_sell())
    #     self.only_sell_percentage = (self.only_sell_tokens / len(self.portfolios)) * 100 if self.portfolios else 0
        
    #     self.total_quick_trades = sum(p.quick_trades_count for p in self.portfolios.values())
    #     self.quick_trades_percentage = (self.total_quick_trades / len(self.processed_txs)) * 100 if self.processed_txs else 0

    # def _process_pump_fun_buy(self, tx, signature, timestamp, source, native_transfers, token_transfers, account_data):
    #     """處理 PUMP_FUN 來源的買入交易"""
    #     # 處理 SOL 的支出情況
    #     sol_spent = 0
    #     for native_transfer in native_transfers:
    #         # 檢查用戶是否從此錢包發送 SOL
    #         if native_transfer.get("fromUserAccount") == self.wallet_address:
    #             recipient = native_transfer.get("toUserAccount")
    #             # 忽略計算預算和其他系統轉賬
    #             if (recipient != "ComputeBudget111111111111111111111111111111" and 
    #                 recipient != self.wallet_address):
    #                 sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
        
    #     # 檢查用戶是否收到了代幣
    #     token_received = False
    #     received_token_mint = None
    #     received_token_amount = 0
        
    #     for token_transfer in token_transfers:
    #         if token_transfer.get("toUserAccount") == self.wallet_address:
    #             token_mint = token_transfer.get("mint", "")
    #             # 跳過穩定幣
    #             if token_mint in SUPPORTED_TOKENS:
    #                 continue
                
    #             # 提取代幣數量
    #             raw_amount = token_transfer.get("rawTokenAmount", {})
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                 token_received = True
    #                 received_token_mint = token_mint
    #                 received_token_amount = token_amount
    #                 break
        
    #     # 如果沒有在 tokenTransfers 中找到用戶收到的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
    #     if not token_received:
    #         for account in account_data:
    #             for token_balance in account.get("tokenBalanceChanges", []):
    #                 if token_balance.get("userAccount") == self.wallet_address:
    #                     raw_amount = token_balance.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         # 只關注正向變化 (代幣增加)
    #                         if amount_change > 0:
    #                             token_mint = token_balance.get("mint")
    #                             token_received = True
    #                             received_token_mint = token_mint
    #                             received_token_amount = amount_change
    #                             break
        
    #     # 如果用戶花費了 SOL 並且收到了代幣，這是一筆買入交易
    #     if sol_spent > 0 and token_received:
    #         token_symbol, token_icon = self._get_token_symbol(received_token_mint)
            
    #         return TokenTransaction(
    #             signature=signature,
    #             timestamp=timestamp,
    #             tx_type="BUY",
    #             source=source,
    #             token_mint=received_token_mint,
    #             token_amount=received_token_amount,
    #             token_symbol=token_symbol,
    #             token_icon=token_icon,
    #             sol_amount=sol_spent,
    #             usdc_amount=0
    #         )
        
    #     return None

    # def _process_pump_fun_sell(self, tx, signature, timestamp, source, native_transfers, token_transfers, account_data):
    #     """處理 PUMP_FUN 來源的賣出交易"""
    #     token_sold = False
    #     sold_token_mint = None
    #     sold_token_amount = 0
    #     sol_received = 0
        
    #     # 檢查 SOL 餘額變化
    #     for account in account_data:
    #         if account.get("account") == self.wallet_address:
    #             bal_change = account.get("nativeBalanceChange", 0)
    #             if bal_change > 0:  # 正值表示獲得 SOL
    #                 sol_received = bal_change / (10 ** SOL_DECIMALS)
        
    #     # 檢查用戶是否發送了代幣
    #     for token_transfer in token_transfers:
    #         if token_transfer.get("fromUserAccount") == self.wallet_address:
    #             token_mint = token_transfer.get("mint", "")
    #             # 跳過穩定幣
    #             if token_mint in SUPPORTED_TOKENS:
    #                 continue
                
    #             # 提取代幣數量
    #             raw_amount = token_transfer.get("rawTokenAmount", {})
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                 token_sold = True
    #                 sold_token_mint = token_mint
    #                 sold_token_amount = token_amount
    #                 break
    #             elif "tokenAmount" in token_transfer:
    #                 token_amount = float(token_transfer.get("tokenAmount", 0))
    #                 token_sold = True
    #                 sold_token_mint = token_mint
    #                 sold_token_amount = token_amount
    #                 break
        
    #     # 如果沒有在 tokenTransfers 中找到用戶發送的代幣，嘗試從 accountData 中的 tokenBalanceChanges 尋找
    #     if not token_sold:
    #         for account in account_data:
    #             for token_balance in account.get("tokenBalanceChanges", []):
    #                 if token_balance.get("userAccount") == self.wallet_address:
    #                     raw_amount = token_balance.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         # 只關注負向變化 (代幣減少)
    #                         if token_amount < 0:
    #                             token_mint = token_balance.get("mint")
    #                             # 跳過穩定幣
    #                             if token_mint in SUPPORTED_TOKENS:
    #                                 continue
    #                             token_sold = True
    #                             sold_token_mint = token_mint
    #                             sold_token_amount = abs(token_amount)
    #                             break
        
    #     # 如果用戶賣出了代幣並收到了 SOL，這是一筆賣出交易
    #     if token_sold and sol_received > 0:
    #         token_symbol, token_icon = self._get_token_symbol(sold_token_mint)
            
    #         return TokenTransaction(
    #             signature=signature,
    #             timestamp=timestamp,
    #             tx_type="SELL",
    #             source=source,
    #             token_mint=sold_token_mint,
    #             token_amount=sold_token_amount,
    #             token_symbol=token_symbol,
    #             token_icon=token_icon,
    #             sol_amount=sol_received,
    #             usdc_amount=0
    #         )
        
    #     return None

    # def _process_standard_buy(self, tx, signature, timestamp, source, native_transfers, token_transfers):
    #     """處理標準的買入交易"""
    #     sol_spent = 0
    #     user_sent_sol = False
            
    #     # 從 nativeTransfers 收集 SOL 支出信息
    #     for native_transfer in native_transfers:
    #         if native_transfer.get("fromUserAccount") == self.wallet_address:
    #             recipient = native_transfer.get("toUserAccount")
    #             # 忽略可能的手續費或其他小額轉賬
    #             if recipient != "ComputeBudget111111111111111111111111111111":
    #                 sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
    #                 user_sent_sol = True
        
    #     # 檢查用戶是否收到了代幣
    #     token_received = False
    #     received_token_mint = None
    #     received_token_amount = 0
        
    #     for token_transfer in token_transfers:
    #         if token_transfer.get("toUserAccount") == self.wallet_address:
    #             token_mint = token_transfer.get("mint", "")
    #             # 跳過穩定幣
    #             if token_mint in SUPPORTED_TOKENS:
    #                 continue
                
    #             # 提取代幣數量
    #             raw_amount = token_transfer.get("rawTokenAmount", {})
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                 token_received = True
    #                 received_token_mint = token_mint
    #                 received_token_amount = token_amount
    #                 break
        
    #     # 如果用戶發送了 SOL 並收到了代幣，這是一筆購買交易
    #     if user_sent_sol and token_received and sol_spent > 0 and received_token_amount > 0:
    #         token_symbol, token_icon = self._get_token_symbol(received_token_mint)
            
    #         return TokenTransaction(
    #             signature=signature,
    #             timestamp=timestamp,
    #             tx_type="BUY",
    #             source=source,
    #             token_mint=received_token_mint,
    #             token_amount=received_token_amount,
    #             token_symbol=token_symbol,
    #             token_icon=token_icon,
    #             sol_amount=sol_spent,
    #             usdc_amount=0
    #         )
        
    #     return None

    # def _process_standard_sell(self, tx, signature, timestamp, source, native_transfers, token_transfers):
    #     """處理標準的賣出交易"""
    #     sol_received = 0
    #     user_received_sol = False
        
    #     for native_transfer in native_transfers:
    #         if native_transfer.get("toUserAccount") == self.wallet_address:
    #             sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
    #             user_received_sol = True
        
    #     # 檢查用戶是否發送了代幣
    #     token_sent = False
    #     sent_token_mint = None
    #     sent_token_amount = 0
        
    #     for token_transfer in token_transfers:
    #         if token_transfer.get("fromUserAccount") == self.wallet_address:
    #             token_mint = token_transfer.get("mint", "")
    #             # 跳過穩定幣
    #             if token_mint in SUPPORTED_TOKENS:
    #                 continue
                
    #             # 提取代幣數量
    #             raw_amount = token_transfer.get("rawTokenAmount", {})
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                 token_sent = True
    #                 sent_token_mint = token_mint
    #                 sent_token_amount = token_amount
    #                 break
        
    #     # 如果用戶發送了代幣並收到了 SOL，這是一筆賣出交易
    #     if token_sent and user_received_sol and sent_token_amount > 0 and sol_received > 0:
    #         token_symbol, token_icon = self._get_token_symbol(sent_token_mint)
            
    #         return TokenTransaction(
    #             signature=signature,
    #             timestamp=timestamp,
    #             tx_type="SELL",
    #             source=source,
    #             token_mint=sent_token_mint,
    #             token_amount=sent_token_amount,
    #             token_symbol=token_symbol,
    #             token_icon=token_icon,
    #             sol_amount=sol_received,
    #             usdc_amount=0
    #         )
        
    #     return None

    # def _process_memecoin_swap(self, tx, signature, timestamp, source, swap_event, token_transfers):
    #     """處理 memecoin 對 memecoin 的交易"""
    #     token_inputs = swap_event.get("tokenInputs", [])
    #     token_outputs = swap_event.get("tokenOutputs", [])
    #     native_output = swap_event.get("nativeOutput", {})
    #     native_input = swap_event.get("nativeInput", {})
        
    #     # 檢查是否是 memecoin 對 memecoin 的交易
    #     user_sold_tokens = False
    #     sold_token_mint = None
    #     sold_token_amount = 0
    #     sold_token_symbol = None
    #     sold_token_icon = None
        
    #     for token_input in token_inputs:
    #         if token_input.get("userAccount") == self.wallet_address:
    #             user_sold_tokens = True
    #             sold_token_mint = token_input.get("mint", "")
    #             sold_token_symbol, sold_token_icon = self._get_token_symbol(sold_token_mint)
    #             raw_amount = token_input.get("rawTokenAmount", {})
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 sold_token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #             break
        
    #     # 檢查用戶是否收到了非穩定幣代幣
    #     user_received_tokens = []
        
    #     for transfer in token_transfers:
    #         if transfer.get("toUserAccount") == self.wallet_address:
    #             token_mint = transfer.get("mint", "")
    #             # 跳過 SOL 和穩定幣
    #             if token_mint in SUPPORTED_TOKENS:
    #                 continue
    #             # 跳過賣出的同一種代幣
    #             if token_mint == sold_token_mint:
    #                 continue
                
    #             # 提取代幣數量
    #             raw_amount = None
    #             if "rawTokenAmount" in transfer:
    #                 raw_amount = transfer["rawTokenAmount"]
    #             else:
    #                 token_amount = transfer.get("tokenAmount", 0)
    #                 if token_amount:
    #                     try:
    #                         token_amount = float(token_amount)
    #                         raw_amount = {"tokenAmount": token_amount, "decimals": 0}
    #                     except:
    #                         continue
                
    #             if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                 token_symbol, token_icon = self._get_token_symbol(token_mint)
    #                 received_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                 user_received_tokens.append({
    #                     "mint": token_mint,
    #                     "symbol": token_symbol,
    #                     "icon": token_icon,
    #                     "amount": received_amount
    #                 })
        
    #     # 處理 SOL 的中間金額
    #     intermediate_sol_amount = 0.0
    #     if native_output and "amount" in native_output:
    #         intermediate_sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
    #     elif native_input and "amount" in native_input:
    #         intermediate_sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
        
    #     # 處理 memecoin 對 memecoin 的交易
    #     if user_sold_tokens and user_received_tokens and intermediate_sol_amount > 0:
    #         result_txs = []
            
    #         # 先處理賣出部分
    #         tx_obj_sell = TokenTransaction(
    #             signature=signature,
    #             timestamp=timestamp,
    #             tx_type="SELL",
    #             source=source,
    #             token_mint=sold_token_mint,
    #             token_amount=sold_token_amount,
    #             token_symbol=sold_token_symbol,
    #             token_icon=sold_token_icon,
    #             sol_amount=intermediate_sol_amount,
    #             usdc_amount=0,
    #         )
    #         result_txs.append(tx_obj_sell)
            
    #         # 再處理買入部分
    #         for received_token in user_received_tokens:
    #             tx_obj_buy = TokenTransaction(
    #                 signature=signature + "_BUY",  # 添加後綴以區分
    #                 timestamp=timestamp,
    #                 tx_type="BUY",
    #                 source=source,
    #                 token_mint=received_token["mint"],
    #                 token_amount=received_token["amount"],
    #                 token_symbol=received_token["symbol"],
    #                 token_icon=received_token["icon"],
    #                 sol_amount=intermediate_sol_amount,
    #                 usdc_amount=0
    #             )
                
    #             result_txs.append(tx_obj_buy)
            
    #         return result_txs
        
    #     return None
    
    # def _process_standard_swap(self, tx, signature, timestamp, source, swap_event, token_transfers):
    #     """處理標準的 swap 交易"""
    #     result_txs = []
    #     token_inputs = swap_event.get("tokenInputs", [])
    #     token_outputs = swap_event.get("tokenOutputs", [])
    #     native_output = swap_event.get("nativeOutput", {})
    #     native_input = swap_event.get("nativeInput", {})
        
    #     # 過濾非穩定幣或原生代幣的交易
    #     has_supported_token = False
        
    #     # 檢查輸入中是否有支持的代幣
    #     for token_input in token_inputs:
    #         if token_input.get("mint") in SUPPORTED_TOKENS:
    #             has_supported_token = True
    #             break
        
    #     # 檢查輸出中是否有支持的代幣
    #     if not has_supported_token:
    #         for token_output in token_outputs:
    #             if token_output.get("mint") in SUPPORTED_TOKENS:
    #                 has_supported_token = True
    #                 break
        
    #     # 檢查是否涉及原生 SOL
    #     if (native_input and "amount" in native_input) or (native_output and "amount" in native_output):
    #         has_supported_token = True
        
    #     # 如果不涉及支持的代幣，跳過此交易
    #     if not has_supported_token:
    #         return None
        
    #     # 處理代幣輸入 (賣出)
    #     for token_input in token_inputs:
    #         # 確保這是目標錢包的交易
    #         if token_input.get("userAccount") != self.wallet_address:
    #             continue
            
    #         raw_amount = token_input.get("rawTokenAmount", {})
    #         token_mint = token_input.get("mint", "")
    #         token_symbol, token_icon = self._get_token_symbol(token_mint)
            
    #         if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #             token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                
    #             sol_amount = 0.0
    #             usdc_amount = 0.0
                
    #             # 處理 SOL 輸出 (賣出代幣獲得 SOL)
    #             if native_output and "amount" in native_output:
    #                 sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                
    #             # 處理 USDC/USDT 輸出
    #             else:
    #                 for output in token_outputs:
    #                     output_mint = output.get("mint", "")
    #                     if output_mint in STABLECOINS and output.get("userAccount") == self.wallet_address:
    #                         output_raw_amount = output.get("rawTokenAmount", {})
    #                         if output_raw_amount and "tokenAmount" in output_raw_amount and "decimals" in output_raw_amount:
    #                             usdc_amount = float(output_raw_amount["tokenAmount"]) / (10 ** output_raw_amount["decimals"])
                
    #             # 只處理有 SOL 或穩定幣輸出的交易
    #             if sol_amount > 0 or usdc_amount > 0:
    #                 tx_obj = TokenTransaction(
    #                     signature=signature,
    #                     timestamp=timestamp,
    #                     tx_type="SELL",
    #                     source=source,
    #                     token_mint=token_mint,
    #                     token_amount=token_amount,
    #                     token_symbol=token_symbol,
    #                     token_icon=token_icon,
    #                     sol_amount=sol_amount,
    #                     usdc_amount=usdc_amount
    #                 )
                    
    #                 result_txs.append(tx_obj)
        
    #     # 處理代幣輸出 (買入)
    #     for token_output in token_outputs:
    #         # 確保這是目標錢包的交易
    #         if token_output.get("userAccount") != self.wallet_address:
    #             continue
            
    #         raw_amount = token_output.get("rawTokenAmount", {})
    #         token_mint = token_output.get("mint", "")
    #         token_symbol, token_icon = self._get_token_symbol(token_mint)
            
    #         if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #             token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                
    #             # 處理三種情況：SOL 輸入、穩定幣輸入、其他代幣輸入
    #             sol_amount = 0.0
    #             usdc_amount = 0.0
                
    #             # 處理 SOL 輸入 (用 SOL 買入代幣)
    #             if native_input and "amount" in native_input:
    #                 sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
    #             # 處理穩定幣輸入
    #             else:
    #                 for input_token in token_inputs:
    #                     input_mint = input_token.get("mint", "")
    #                     if input_mint in STABLECOINS and input_token.get("userAccount") == self.wallet_address:
    #                         input_raw_amount = input_token.get("rawTokenAmount", {})
    #                         if input_raw_amount and "tokenAmount" in input_raw_amount and "decimals" in input_raw_amount:
    #                             usdc_amount = float(input_raw_amount["tokenAmount"]) / (10 ** input_raw_amount["decimals"])
                
    #             # 只處理有 SOL 或穩定幣輸入的交易
    #             if sol_amount > 0 or usdc_amount > 0:
    #                 tx_obj = TokenTransaction(
    #                     signature=signature,
    #                     timestamp=timestamp,
    #                     tx_type="BUY",
    #                     source=source,
    #                     token_mint=token_mint,
    #                     token_amount=token_amount,
    #                     token_symbol=token_symbol,
    #                     token_icon=token_icon,
    #                     sol_amount=sol_amount,
    #                     usdc_amount=usdc_amount
    #                 )
                    
    #                 result_txs.append(tx_obj)
        
    #     return result_txs if result_txs else None

    # def _process_special_buy(self, tx, signature, timestamp, source, native_transfers, token_transfers, account_data):
    #     """處理特殊情況下的買入交易"""
    #     # 檢查是否有 SOL 轉出並且有代幣轉入
    #     sol_spent = 0
    #     for native_transfer in native_transfers:
    #         if (native_transfer.get("fromUserAccount") == self.wallet_address and
    #             native_transfer.get("toUserAccount") != "ComputeBudget111111111111111111111111111111"):
    #             sol_spent += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
        
    #     # 如果有 SOL 支出，檢查是否有代幣轉入
    #     if sol_spent > 0:
    #         for token_transfer in token_transfers:
    #             if token_transfer.get("toUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
    #                 token_mint = token_transfer.get("mint")
    #                 token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
    #                 token_amount = 0
    #                 raw_amount = token_transfer.get("rawTokenAmount", {})
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                    
    #                 if token_amount > 0:
    #                     return TokenTransaction(
    #                         signature=signature,
    #                         timestamp=timestamp,
    #                         tx_type="BUY",
    #                         source=source,
    #                         token_mint=token_mint,
    #                         token_amount=token_amount,
    #                         token_symbol=token_symbol,
    #                         token_icon=token_icon,
    #                         sol_amount=sol_spent,
    #                         usdc_amount=0
    #                     )
            
    #         # 如果沒有在 tokenTransfers 中找到代幣轉入，檢查 accountData
    #         for account in account_data:
    #             for token_balance in account.get("tokenBalanceChanges", []):
    #                 if token_balance.get("userAccount") == self.wallet_address:
    #                     raw_amount = token_balance.get("rawTokenAmount", {})
    #                     if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                         amount_change = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
    #                         # 只關注正向變化 (代幣增加)
    #                         if amount_change > 0:
    #                             token_mint = token_balance.get("mint")
    #                             # 跳過穩定幣
    #                             if token_mint in SUPPORTED_TOKENS:
    #                                 continue
                                
    #                             token_symbol, token_icon = self._get_token_symbol(token_mint)
                                
    #                             return TokenTransaction(
    #                                 signature=signature,
    #                                 timestamp=timestamp,
    #                                 tx_type="BUY",
    #                                 source=source,
    #                                 token_mint=token_mint,
    #                                 token_amount=amount_change,
    #                                 token_symbol=token_symbol,
    #                                 token_icon=token_icon,
    #                                 sol_amount=sol_spent,
    #                                 usdc_amount=0
    #                             )
        
    #     return None

    # def _process_special_sell(self, tx, signature, timestamp, source, native_transfers, token_transfers):
    #     """處理特殊情況下的賣出交易"""
    #     sol_received = 0
    #     for native_transfer in native_transfers:
    #         if native_transfer.get("toUserAccount") == self.wallet_address:
    #             sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)
        
    #     # 檢查用戶是否發送了代幣
    #     if sol_received > 0:
    #         for token_transfer in token_transfers:
    #             if token_transfer.get("fromUserAccount") == self.wallet_address and token_transfer.get("mint") not in SUPPORTED_TOKENS:
    #                 token_mint = token_transfer.get("mint")
    #                 token_symbol, token_icon = self._get_token_symbol(token_mint)
                    
    #                 token_amount = 0
    #                 raw_amount = token_transfer.get("rawTokenAmount", {})
    #                 if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
    #                     token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                    
    #                 if token_amount > 0:
    #                     return TokenTransaction(
    #                         signature=signature,
    #                         timestamp=timestamp,
    #                         tx_type="SELL",
    #                         source=source,
    #                         token_mint=token_mint,
    #                         token_amount=token_amount,
    #                         token_symbol=token_symbol,
    #                         token_icon=token_icon,
    #                         sol_amount=sol_received,
    #                         usdc_amount=0
    #                     )
        
    #     return None
    
# # -----------------------------------------------------------------------------------------------------------------------

    # def process_raydium_swap(self, tx):
    #     """處理 Raydium 的 swap 交易"""
    #     swap_event = tx.get("events", {}).get("swap", {})
    #     timestamp = tx.get("timestamp", 0)
    #     signature = tx.get("signature", "")
    #     source = tx.get("source", "UNKNOWN")
        
    #     # 處理 SOL -> Token 買入
    #     if swap_event.get("nativeInput"):
    #         native_input = swap_event["nativeInput"]
    #         if native_input.get("account") == self.wallet_address:
    #             sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
    #             # 檢查代幣輸出
    #             for token_output in swap_event.get("tokenOutputs", []):
    #                 if token_output.get("mint") not in SUPPORTED_TOKENS:  # 跳過穩定幣
    #                     token_amount = float(token_output["rawTokenAmount"]["tokenAmount"]) / (10 ** token_output["rawTokenAmount"]["decimals"])
    #                     token_mint = token_output["mint"]
    #                     token_symbol, token_icon = self._get_token_symbol(token_mint)
                        
    #                     return TokenTransaction(
    #                         signature=signature,
    #                         timestamp=timestamp,
    #                         tx_type="BUY",
    #                         source=source,
    #                         token_mint=token_mint,
    #                         token_amount=token_amount,
    #                         token_symbol=token_symbol,
    #                         token_icon=token_icon,
    #                         sol_amount=sol_amount,
    #                         usdc_amount=0
    #                     )
        
    #     # 處理 Token -> SOL 賣出
    #     elif swap_event.get("nativeOutput"):
    #         native_output = swap_event["nativeOutput"]
    #         if native_output.get("account") == self.wallet_address:
    #             sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                
    #             # 檢查代幣輸入
    #             for token_input in swap_event.get("tokenInputs", []):
    #                 if (token_input.get("userAccount") == self.wallet_address and 
    #                     token_input.get("mint") not in SUPPORTED_TOKENS):
    #                     token_amount = float(token_input["rawTokenAmount"]["tokenAmount"]) / (10 ** token_input["rawTokenAmount"]["decimals"])
    #                     token_mint = token_input["mint"]
    #                     token_symbol, token_icon = self._get_token_symbol(token_mint)
                        
    #                     return TokenTransaction(
    #                         signature=signature,
    #                         timestamp=timestamp,
    #                         tx_type="SELL",
    #                         source=source,
    #                         token_mint=token_mint,
    #                         token_amount=token_amount,
    #                         token_symbol=token_symbol,
    #                         token_icon=token_icon,
    #                         sol_amount=sol_amount,
    #                         usdc_amount=0
    #                     )
        
    #     return None

    def process_raydium_swap(self, tx):
        """處理 Raydium 的 swap 交易"""
        swap_event = tx.get("events", {}).get("swap", {})
        timestamp = tx.get("timestamp", 0)
        signature = tx.get("signature", "")
        source = tx.get("source", "UNKNOWN")
        description = tx.get("description", "").lower()
        token_transfers = tx.get("tokenTransfers", [])
        account_data = tx.get("accountData", [])
        
        # logging.info(f"處理 Raydium 交易: {signature}")
        # if description:
        #     logging.info(f"交易描述: {description}")
        
        # 從描述解析交易信息
        stable_amount = 0
        stable_coin_type = "USDC"  # 默認穩定幣類型
        
        if description and "swapped" in description:
            # 嘗試從描述中提取信息
            try:
                import re
                # 買入交易 - 匹配 "swapped X SOL for Y TOKEN" 格式
                if "sol for" in description.lower():
                    sol_pattern = r"swapped\s+([\d\.]+)\s+sol\s+for\s+([\d\.]+)\s+(\w+)"
                    match = re.search(sol_pattern, description, re.IGNORECASE)
                    if match:
                        sol_amount = float(match.group(1))
                        token_amount = float(match.group(2))
                        token_symbol = match.group(3)
                        
                        # 設置源幣種為 SOL
                        from_token_mint = "So11111111111111111111111111111111111111112"
                        from_token_amount = sol_amount
                        from_token_symbol = "SOL"
                        
                        # 查找對應的 token mint
                        for output in swap_event.get("tokenOutputs", []):
                            token_mint = output.get("mint", "")
                            user_acct = output.get("userAccount", "")
                            
                            if user_acct == self.wallet_address and token_mint not in SUPPORTED_TOKENS:
                                token_symbol_found, token_icon = self._get_token_symbol(token_mint)
                                
                                return TokenTransaction(
                                    signature=signature,
                                    timestamp=timestamp,
                                    tx_type="BUY",
                                    source=source,
                                    token_mint=token_mint,
                                    token_amount=token_amount,
                                    token_symbol=token_symbol_found,
                                    token_icon=token_icon,
                                    sol_amount=sol_amount,
                                    usdc_amount=0,
                                    from_token_mint=from_token_mint,
                                    from_token_amount=from_token_amount,
                                    from_token_symbol=from_token_symbol,
                                    dest_token_mint=token_mint,
                                    dest_token_amount=token_amount,
                                    dest_token_symbol=token_symbol_found
                                )
                
                # 賣出交易 - 匹配 "swapped X TOKEN for Y SOL" 格式
                elif "for sol" in description.lower() or " for " in description.lower() and " sol" in description.lower().split(" for ")[1]:
                    token_pattern = r"swapped\s+([\d\.]+)\s+(\w+)\s+for\s+([\d\.]+)\s+sol"
                    match = re.search(token_pattern, description, re.IGNORECASE)

                    dest_token_mint = "So11111111111111111111111111111111111111112"
                    dest_token_symbol = "SOL"
                    if match:
                        token_amount = float(match.group(1))
                        token_symbol_from_desc = match.group(2)
                        sol_amount = float(match.group(3))
                        
                        token_symbol, token_icon = self._get_token_symbol(token_symbol_from_desc)
                        is_token_mint_isaddress = None

                        if not token_symbol:
                            # 從 tokenInputs 中尋找
                            for token_input in swap_event.get("tokenInputs", []):
                                if token_input.get("userAccount") == self.wallet_address:
                                    token_symbol_from_desc = token_input.get("mint", "")
                                    is_token_mint_isaddress = token_symbol_from_desc
                                    break
                            
                            # 如果沒找到，從 tokenTransfers 中尋找
                            if not is_token_mint_isaddress:
                                for transfer in token_transfers:
                                    if transfer.get("fromUserAccount") == self.wallet_address and transfer.get("mint") not in SUPPORTED_TOKENS:
                                        token_symbol_from_desc = transfer.get("mint")
                                        break
                            
                        return TokenTransaction(
                            signature=signature,
                            timestamp=timestamp,
                            tx_type="SELL",
                            source=source,
                            token_mint=token_symbol_from_desc,
                            token_amount=token_amount,
                            token_symbol=token_symbol,
                            token_icon=token_icon,
                            sol_amount=sol_amount,
                            usdc_amount=0,
                            from_token_mint=token_symbol_from_desc,
                            from_token_amount=token_amount,
                            from_token_symbol=token_symbol,
                            dest_token_mint=dest_token_mint,
                            dest_token_amount=sol_amount,
                            dest_token_symbol=dest_token_symbol
                        )
                
                # 買入交易 - 穩定幣交易，如 "swapped X USDC for Y TOKEN" 或 "swapped X USDT for Y TOKEN"
                for stable in ["usdc", "usdt", "usdh", "usdc-sol"]:
                    if f"{stable} for" in description.lower() or USDC_MINT.lower() in description.lower() or USDT_MINT.lower() in description.lower():
                        stable_pattern = r"swapped\s+([\d\.]+)\s+" + stable + r"\s+for\s+([\d\.]+)\s+(\w+)"
                        match = re.search(stable_pattern, description, re.IGNORECASE)
                        if match:
                            stable_amount = float(match.group(1))
                            token_amount = float(match.group(2))
                            token_symbol = match.group(3)
                            stable_coin_type = stable.upper()
                            
                            # 設置源幣種為相應的穩定幣
                            if stable_coin_type == "USDC" or stable_coin_type == USDC_MINT.upper():
                                from_token_mint = USDC_MINT
                            elif stable_coin_type == "USDT" or stable_coin_type == USDT_MINT.upper():
                                from_token_mint = USDT_MINT
                            else:
                                from_token_mint = USDC_MINT  # 默認使用 USDC
                            
                            from_token_amount = stable_amount
                            from_token_symbol = stable_coin_type
                            
                            # 查找對應的 token mint
                            for output in swap_event.get("tokenOutputs", []):
                                token_mint = output.get("mint", "")
                                user_acct = output.get("userAccount", "")
                                
                                if user_acct == self.wallet_address and token_mint not in SUPPORTED_TOKENS:
                                    token_symbol_found, token_icon = self._get_token_symbol(token_mint)
                                    
                                    return TokenTransaction(
                                        signature=signature,
                                        timestamp=timestamp,
                                        tx_type="BUY",
                                        source=source,
                                        token_mint=token_mint,
                                        token_amount=token_amount,
                                        token_symbol=token_symbol_found,
                                        token_icon=token_icon,
                                        sol_amount=0,
                                        usdc_amount=stable_amount,
                                        from_token_mint=from_token_mint,
                                        from_token_amount=from_token_amount,
                                        from_token_symbol=from_token_symbol,
                                        dest_token_mint=token_mint,
                                        dest_token_amount=token_amount,
                                        dest_token_symbol=token_symbol_found
                                    )
                
                # 賣出交易 - 穩定幣交易，如 "swapped X TOKEN for Y USDC" 或 "swapped X TOKEN for Y USDT"
                for stable in ["usdc", "usdt", "usdh", "usdc-sol"]:
                    if " for " in description.lower() and f" {stable}" in description.lower().split(" for ")[1]:
                        stable_pattern = r"swapped\s+([\d\.]+)\s+(\w+)\s+for\s+([\d\.]+)\s+" + stable
                        match = re.search(stable_pattern, description, re.IGNORECASE)
                        if match:
                            token_amount = float(match.group(1))
                            token_symbol_from_desc = match.group(2)
                            stable_amount = float(match.group(3))
                            stable_coin_type = stable.upper()

                            if stable_coin_type == "USDC" or stable_coin_type == USDC_MINT.upper():
                                dest_token_mint = USDC_MINT
                            elif stable_coin_type == "USDT" or stable_coin_type == USDT_MINT.upper():
                                dest_token_mint = USDT_MINT
                            else:
                                dest_token_mint = USDC_MINT  # 默認使用 USDC
                            
                            token_symbol, token_icon = self._get_token_symbol(token_symbol_from_desc)
                            is_token_mint_isaddress = None
                            
                            # 從 tokenInputs 中尋找
                            if not token_symbol:
                                for token_input in swap_event.get("tokenInputs", []):
                                    if token_input.get("userAccount") == self.wallet_address:
                                        token_symbol_from_desc = token_input.get("mint", "")
                                        is_token_mint_isaddress = token_symbol_from_desc
                                        break
                                
                                # 如果沒找到，從 tokenTransfers 中尋找
                                if not is_token_mint_isaddress:
                                    for transfer in token_transfers:
                                        if transfer.get("fromUserAccount") == self.wallet_address and transfer.get("mint") not in SUPPORTED_TOKENS:
                                            token_symbol_from_desc = transfer.get("mint")
                                            break
                                
                            return TokenTransaction(
                                signature=signature,
                                timestamp=timestamp,
                                tx_type="SELL",
                                source=source,
                                token_mint=token_symbol_from_desc,
                                token_amount=token_amount,
                                token_symbol=token_symbol,
                                token_icon=token_icon,
                                sol_amount=0,
                                usdc_amount=stable_amount,
                                from_token_mint=token_symbol_from_desc,
                                from_token_amount=token_amount,
                                from_token_symbol=token_symbol,
                                dest_token_mint=dest_token_mint,
                                dest_token_amount=stable_amount,
                                dest_token_symbol=stable_coin_type
                            )
            
            except Exception as e:
                logging.warning(f"從描述中提取交易信息失敗: {e}")
        
        # 檢查帳戶數據中的穩定幣轉移
        stable_input = 0
        stable_mint = None
        for account in account_data:
            token_changes = account.get("tokenBalanceChanges", [])
            for token_change in token_changes:
                mint = token_change.get("mint")
                user_account = token_change.get("userAccount")
                
                if user_account == self.wallet_address and mint in STABLECOINS:
                    raw_amount = token_change.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        if amount < 0:  # 負值表示支出
                            stable_input = abs(amount)
                            stable_mint = mint
                            # 根據穩定幣mint判斷幣種
                            if mint == USDC_MINT:
                                stable_coin_type = "USDC"
                            elif mint == USDT_MINT:
                                stable_coin_type = "USDT"
                            # logging.info(f"偵測到 {stable_coin_type} 支出: {stable_input}")
        
        # 處理 SOL -> Token 買入
        if swap_event.get("nativeInput"):
            native_input = swap_event["nativeInput"]
            if native_input.get("account") == self.wallet_address:
                sol_amount = float(native_input["amount"]) / (10 ** SOL_DECIMALS)
                
                # 檢查代幣輸出
                for token_output in swap_event.get("tokenOutputs", []):
                    if token_output.get("userAccount") == self.wallet_address and token_output.get("mint") not in SUPPORTED_TOKENS:
                        token_amount = float(token_output["rawTokenAmount"]["tokenAmount"]) / (10 ** token_output["rawTokenAmount"]["decimals"])
                        token_mint = token_output["mint"]
                        token_symbol, token_icon = self._get_token_symbol(token_mint)
                        
                        return TokenTransaction(
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
                            from_token_mint="So11111111111111111111111111111111111111112",
                            from_token_amount=sol_amount,
                            from_token_symbol="SOL",
                            dest_token_mint=token_mint,
                            dest_token_amount=token_amount,
                            dest_token_symbol=token_symbol
                        )
        
        # 處理 Token -> SOL 賣出
        elif swap_event.get("nativeOutput"):
            native_output = swap_event["nativeOutput"]
            if native_output.get("account") == self.wallet_address:
                sol_amount = float(native_output["amount"]) / (10 ** SOL_DECIMALS)
                
                # 檢查代幣輸入
                for token_input in swap_event.get("tokenInputs", []):
                    if (token_input.get("userAccount") == self.wallet_address and 
                        token_input.get("mint") not in SUPPORTED_TOKENS):
                        token_amount = float(token_input["rawTokenAmount"]["tokenAmount"]) / (10 ** token_input["rawTokenAmount"]["decimals"])
                        token_mint = token_input["mint"]
                        token_symbol, token_icon = self._get_token_symbol(token_mint)
                        
                        return TokenTransaction(
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
                            from_token_mint=token_mint,
                            from_token_amount=token_amount,
                            from_token_symbol=token_symbol,
                            dest_token_mint="So11111111111111111111111111111111111111112",  # SOL的mint地址
                            dest_token_amount=sol_amount,
                            dest_token_symbol="SOL"
                        )
        
        # 處理 穩定幣 -> Token 買入
        if stable_input > 0:
            # 檢查是否接收了某種代幣
            received_token = None
            received_amount = 0
            received_mint = None
            
            # 從 tokenTransfers 尋找接收的代幣
            for transfer in token_transfers:
                if transfer.get("toUserAccount") == self.wallet_address:
                    token_mint = transfer.get("mint", "")
                    if token_mint not in SUPPORTED_TOKENS:
                        raw_amount = transfer.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            if token_amount > 0:
                                token_symbol, token_icon = self._get_token_symbol(token_mint)
                                received_token = token_symbol
                                received_amount = token_amount
                                received_mint = token_mint
            
            # 從 accountData 尋找接收的代幣
            if not received_token:
                for account in account_data:
                    token_changes = account.get("tokenBalanceChanges", [])
                    for token_change in token_changes:
                        mint = token_change.get("mint")
                        user_account = token_change.get("userAccount")
                        
                        if user_account == self.wallet_address and mint not in SUPPORTED_TOKENS:
                            raw_amount = token_change.get("rawTokenAmount", {})
                            if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                                amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                                if amount > 0:  # 正值表示收到
                                    token_symbol, token_icon = self._get_token_symbol(mint)
                                    received_token = token_symbol
                                    received_amount = amount
                                    received_mint = mint
            
            if received_token and received_amount > 0:
                # logging.info(f"解析為 {stable_coin_type} 買入交易: 支付 {stable_input} {stable_coin_type}, 獲得 {received_amount} {received_token}")

                stable_symbol = "USDC"
                stable_token_mint = USDC_MINT
                if stable_mint == USDT_MINT:
                    stable_symbol = "USDT"
                    stable_token_mint = USDT_MINT

                return TokenTransaction(
                    signature=signature,
                    timestamp=timestamp,
                    tx_type="BUY",
                    source=source,
                    token_mint=received_mint,
                    token_amount=received_amount,
                    token_symbol=received_token,
                    token_icon=token_icon,
                    sol_amount=0,
                    usdc_amount=stable_input,
                    from_token_mint=stable_token_mint,
                    from_token_amount=stable_input,
                    from_token_symbol=stable_symbol,
                    dest_token_mint=received_mint,
                    dest_token_amount=received_amount,
                    dest_token_symbol=received_token
                )
        
        # 檢查 tokenInputs 和 tokenOutputs 中是否有穩定幣交易
        stable_from_inputs = None
        token_from_outputs = None
        
        for token_input in swap_event.get("tokenInputs", []):
            if token_input.get("userAccount") == self.wallet_address:
                mint = token_input.get("mint")
                if mint in STABLECOINS:
                    raw_amount = token_input.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                        stable_from_inputs = {
                            "mint": mint,
                            "amount": amount
                        }
                        # 確定穩定幣類型
                        if mint == USDC_MINT:
                            stable_coin_type = "USDC"
                        elif mint == USDT_MINT:
                            stable_coin_type = "USDT"
        
        # 如果找到穩定幣輸入，查找代幣輸出
        if stable_from_inputs:
            for token_output in swap_event.get("tokenOutputs", []):
                if token_output.get("userAccount") == self.wallet_address:
                    mint = token_output.get("mint")
                    if mint not in SUPPORTED_TOKENS:
                        raw_amount = token_output.get("rawTokenAmount", {})
                        if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                            amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                            token_symbol, token_icon = self._get_token_symbol(mint)
                            
                            # logging.info(f"從 tokenInputs/tokenOutputs 解析為 {stable_coin_type} 買入交易: 支付 {stable_from_inputs['amount']} {stable_coin_type}, 獲得 {amount} {token_symbol}")
                            
                            return TokenTransaction(
                                signature=signature,
                                timestamp=timestamp,
                                tx_type="BUY",
                                source=source,
                                token_mint=mint,
                                token_amount=amount,
                                token_symbol=token_symbol,
                                token_icon=token_icon,
                                sol_amount=0,
                                usdc_amount=stable_from_inputs["amount"]
                            )

        # 从tokenTransfers寻找发送的代币，特别关注直接转账数量
        sol_received = 0
        for native_transfer in tx.get("nativeTransfers", []):
            if native_transfer.get("toUserAccount") == self.wallet_address:
                sol_received += float(native_transfer.get("amount", 0)) / (10 ** SOL_DECIMALS)

        for transfer in token_transfers:
            if transfer.get("fromUserAccount") == self.wallet_address:
                token_mint = transfer.get("mint", "")
                # 跳过SOL和稳定币
                if token_mint in SUPPORTED_TOKENS:
                    continue
                
                # 获取原始转账数量
                token_amount = 0
                if "tokenAmount" in transfer:
                    token_amount = float(transfer.get("tokenAmount", 0))
                elif "rawTokenAmount" in transfer:
                    raw_amount = transfer.get("rawTokenAmount", {})
                    if raw_amount and "tokenAmount" in raw_amount and "decimals" in raw_amount:
                        token_amount = float(raw_amount["tokenAmount"]) / (10 ** raw_amount["decimals"])
                
                if token_amount > 0:
                    token_symbol, token_icon = self._get_token_symbol(token_mint)
                    # 记录详细日志，帮助调试
                    # logging.info(f"直接从tokenTransfers发现发送代币: {token_symbol}, 数量: {token_amount}")
                    
                    # 创建卖出交易记录
                    return TokenTransaction(
                        signature=signature,
                        timestamp=timestamp,
                        tx_type="SELL",
                        source=source,
                        token_mint=token_mint,
                        token_amount=token_amount,
                        token_symbol=token_symbol,
                        token_icon=token_icon,
                        sol_amount=sol_received,  # 使用计算得到的SOL金额
                        usdc_amount=0,
                        from_token_mint=token_mint,
                        from_token_amount=token_amount,
                        from_token_symbol=token_symbol,
                        dest_token_mint="So11111111111111111111111111111111111111112",  # SOL的mint地址
                        dest_token_amount=sol_received,
                        dest_token_symbol="SOL"
                    )
        
        return None

# -----------------------------------------------------------------------------------------------------------------------
    def _get_token_symbol(self, token_mint: str) -> Tuple[str, str]:
        return TokenData._get_token_symbol(token_mint)
    
    def _add_to_portfolio(self, tx: TokenTransaction):
        """將交易添加到相應的代幣組合中"""
        if tx.token_mint not in self.portfolios:
            self.portfolios[tx.token_mint] = TokenPortfolio(tx.token_mint, tx.token_symbol)
        
        self.portfolios[tx.token_mint].add_transaction(tx)
    
    # def generate_summary(self) -> str:
    #     """生成所有代幣的交易摘要"""
    #     if not self.portfolios:
    #         return "沒有找到任何代幣交易"
        
    #     output = [f"錢包地址: {self.wallet_address}"]
    #     output.append(f"當前SOL價格: ${self.sol_price_usd:.2f} USD")
    #     output.append(f"總交易筆數: {len(self.processed_txs)}")
    #     output.append(f"代幣種類數: {len(self.portfolios)}")
        
    #     # 統計只有賣出沒有買入的代幣
    #     self.only_sell_tokens = sum(1 for p in self.portfolios.values() if p.is_only_sell())
    #     only_sell_percentage = (self.only_sell_tokens / len(self.portfolios)) * 100 if self.portfolios else 0
    #     output.append(f"只賣出沒買入的幣種數: {self.only_sell_tokens} ({only_sell_percentage:.2f}%)")
        
    #     # 統計1分鐘內完成的快速交易
    #     self.total_quick_trades = sum(p.quick_trades_count for p in self.portfolios.values())
    #     quick_trades_percentage = (self.total_quick_trades / len(self.processed_txs)) * 100 if self.processed_txs else 0
    #     output.append(f"1分鐘內完成的快速交易: {self.total_quick_trades} ({quick_trades_percentage:.2f}%)")
        
    #     output.append("\n=== 代幣交易摘要 ===")
        
    #     # 按代幣組合排序，先按貨幣 (SOL/USDC)，再按已實現盈虧降序
    #     sorted_portfolios = []
    #     for mint, portfolio in self.portfolios.items():
    #         pnl_usd, _ = portfolio.calculate_pnl_usd(self.sol_price_usd)
    #         sorted_portfolios.append((portfolio.currency, pnl_usd, portfolio))
        
    #     sorted_portfolios.sort(key=lambda x: (x[0] == "UNKNOWN", -x[1]))
        
    #     # for _, _, portfolio in sorted_portfolios:
    #     #     output.append("\n" + str(portfolio))
            
    #     #     # 添加前3筆交易作為示例
    #     #     if portfolio.transactions:
    #     #         output.append("\n前3筆交易:")
    #     #         for i, tx in enumerate(sorted(portfolio.transactions, key=lambda t: t.timestamp)[:3]):
    #     #             output.append(f"{i+1}. {tx}")
            
    #     #     # 添加代幣批次信息
    #     #     if portfolio.token_lots:
    #     #         output.append("\n剩餘代幣批次:")
    #     #         output.append(portfolio.get_lots_summary())
        
    #     # 添加總盈虧統計 (以美元為單位)
    #     total_pnl_usd = 0.0
        
    #     for mint, portfolio in self.portfolios.items():
    #         pnl_usd, _ = portfolio.calculate_pnl_usd(self.sol_price_usd)
    #         total_pnl_usd += pnl_usd
        
    #     output.append("\n=== 總盈虧統計 (美元) ===")
    #     output.append(f"總實現盈虧: ${total_pnl_usd:.2f} USD")
        
    #     return "\n".join(output)

    async def generate_summary(self) -> str:
        """生成所有代幣的交易摘要"""
        if not self.portfolios:
            return "沒有找到任何代幣交易"
        
        output = [f"錢包地址: {self.wallet_address}"]
        output.append(f"當前SOL價格: ${self.sol_price_usd:.2f} USD")
        output.append(f"總交易筆數: {len(self.processed_txs)}")
        output.append(f"代幣種類數: {len(self.portfolios)}")
        
        # 统计只卖出没有买入的代币
        self.only_sell_tokens = sum(1 for p in self.portfolios.values() if p.is_only_sell())
        only_sell_percentage = (self.only_sell_tokens / len(self.portfolios)) * 100 if self.portfolios else 0
        output.append(f"只賣出沒買入的幣種數: {self.only_sell_tokens} ({only_sell_percentage:.2f}%)")
        
        # 统计1分钟内完成的快速交易
        self.total_quick_trades = sum(p.quick_trades_count for p in self.portfolios.values())
        quick_trades_percentage = (self.total_quick_trades / len(self.processed_txs)) * 100 if self.processed_txs else 0
        output.append(f"1分鐘內完成的快速交易: {self.total_quick_trades} ({quick_trades_percentage:.2f}%)")
        
        return "\n".join(output)

    async def filter_smart_money(self, async_session, client, wallet_type) -> str:
        if not self.portfolios:
            return False
        
        self.only_sell_tokens = sum(1 for p in self.portfolios.values() if p.is_only_sell())
        only_sell_percentage = (self.only_sell_tokens / len(self.portfolios)) * 100 if self.portfolios else 0
        
        self.total_quick_trades = sum(p.quick_trades_count for p in self.portfolios.values())
        quick_trades_percentage = (self.total_quick_trades / len(self.processed_txs)) * 100 if self.processed_txs else 0
        print(f"token num: {len(self.portfolios)}")
        print(f"only sell percentage: {only_sell_percentage}")
        print(f"quick trade percentage: {quick_trades_percentage}")

        if len(self.portfolios) > 15 and only_sell_percentage < 50 and quick_trades_percentage < 50:
            # 初始化 wallet_transactions 字典
            wallet_transactions = {
                self.wallet_address: []
            }
            
            # 遍歷每個代幣組合
            for token_mint, portfolio in self.portfolios.items():
                # 遍歷該代幣的每筆交易
                for tx in portfolio.transactions:
                    # 確保每筆交易都有完整的結構
                    transaction_info = {
                        'timestamp': tx.timestamp,  # 添加時間戳
                        'token_mint': token_mint,
                        'token_amount': tx.token_amount,
                        'transaction_type': tx.tx_type,
                        'sol_amount': tx.sol_amount,
                        'usdc_amount': tx.usdc_amount,
                        'sol_price_usd': tx.sol_price_usd
                    }
                    
                    # 添加到 wallet_transactions
                    wallet_transactions[self.wallet_address].append(transaction_info)
            
            is_smart_wallet = await filter_smart_wallets(
                wallet_transactions, 
                self.sol_price_usd, 
                async_session, 
                client, 
                "SOLANA", 
                wallet_type
            )
            
            if is_smart_wallet:
                await calculate_remaining_tokens(
                    wallet_transactions, 
                    self.wallet_address, 
                    async_session, 
                    "SOLANA",
                    client
                )
            
            return is_smart_wallet
        else:
            return False

    # async def filter_smart_money(self, async_session, client, wallet_type) -> bool:
    #     """判斷是否為聰明錢包"""
    #     if not self.portfolios:
    #         return False
        
    #     # 已在 process_transactions 中計算好的指標
    #     print(f"token num: {len(self.portfolios)}")
    #     print(f"only sell percentage: {self.only_sell_percentage}")
    #     print(f"quick trade percentage: {self.quick_trades_percentage}")

    #     # 初步篩選
    #     if len(self.portfolios) > 15 and self.only_sell_percentage < 50 and self.quick_trades_percentage < 50:
    #         # 準備用於計算統計的交易數據 (已從 token_stats 中整理好)
    #         stats_1d = await self.calculate_statistics_for_period(1)
    #         stats_7d = await self.calculate_statistics_for_period(7)
    #         stats_30d = await self.calculate_statistics_for_period(30)
            
    #         # 獲取錢包餘額
    #         sol_balance_data = await TokenUtils.get_usd_balance(client, self.wallet_address)
            
    #         # 取得最近交易的代幣
    #         recent_tokens = await self._get_recent_tokens(3)
            
    #         # 構建錢包數據
    #         wallet_data = {
    #             "wallet_address": self.wallet_address,
    #             "balance": round(sol_balance_data["balance"]["float"], 3),
    #             "balance_usd": round(sol_balance_data["balance_usd"], 2),
    #             "chain": "SOLANA",
    #             "tag": "",
    #             "is_smart_wallet": True,
    #             "wallet_type": wallet_type,
    #             "asset_multiple": float(stats_30d["asset_multiple"]),
    #             "token_list": recent_tokens,
    #             "stats_1d": stats_1d,
    #             "stats_7d": stats_7d,
    #             "stats_30d": stats_30d,
    #             "last_transaction_time": max(tx.timestamp for tx in self.processed_txs) if self.processed_txs else 0
    #         }
            
    #         # 寫入數據庫
    #         await write_wallet_data_to_db(async_session, wallet_data, "SOLANA")
            
    #         # 最終篩選條件
    #         if (
    #             stats_7d.get("total_cost", 0) > 0 and
    #             stats_30d.get("pnl", 0) > 0 and
    #             stats_30d.get("win_rate", 0) > 30 and
    #             stats_30d.get("win_rate", 0) != 100 and
    #             float(stats_30d.get("asset_multiple", 0)) > 0.3 and
    #             stats_30d.get("total_transaction_num", 0) < 2000 and
    #             sol_balance_data["balance_usd"] > 0
    #         ):
    #             # 構建交易格式以符合 calculate_remaining_tokens 函數
    #             wallet_transactions = {
    #                 self.wallet_address: [
    #                     {
    #                         'timestamp': tx.timestamp,
    #                         'token_mint': tx.token_mint,
    #                         'token_amount': tx.token_amount,
    #                         'transaction_type': tx.tx_type.lower(),
    #                         'sol_amount': tx.sol_amount,
    #                         'usdc_amount': tx.usdc_amount,
    #                         'sol_price_usd': tx.sol_price_usd
    #                     }
    #                     for tx in self.processed_txs
    #                 ]
    #             }
                
    #             # 保存餘額數據
    #             await calculate_remaining_tokens(
    #                 wallet_transactions, 
    #                 self.wallet_address, 
    #                 async_session, 
    #                 "SOLANA",
    #                 client
    #             )
    #             return True
        
    #     return False

    async def calculate_statistics_for_period(self, days):
        """計算指定時間範圍內的統計指標，利用已計算好的token_stats"""
        # 獲取當前時間
        now = datetime.now(timezone(timedelta(hours=8)))
        start_of_range = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=days)
        start_timestamp = int(start_of_range.timestamp())
        
        # 過濾指定時間範圍內的交易
        filtered_txs = [tx for tx in self.processed_txs if tx.timestamp >= start_timestamp]
        
        # 按token分組
        token_period_stats = {}
        
        # 利用 token_stats 中已有的數據計算時間範圍內的指標
        for tx in filtered_txs:
            token_mint = tx.token_mint
            
            # 初始化該token在此時間段的統計數據
            if token_mint not in token_period_stats:
                token_period_stats[token_mint] = {
                    'buy_count': 0,
                    'sell_count': 0,
                    'total_buy_amount': 0,
                    'total_buy_cost_usd': 0,
                    'total_sell_amount': 0,
                    'total_sell_value_usd': 0,
                    'realized_pnl_usd': 0
                }
            
            # 計算交易的USD價值
            tx_value_usd = tx.usd_amount if tx.usd_amount > 0 else \
                        (tx.sol_amount * self.sol_price_usd if tx.sol_amount > 0 else tx.usdc_amount)
            
            stats = token_period_stats[token_mint]
            original_stats = self.token_stats.get(token_mint, {})
            
            if tx.tx_type == "BUY":
                stats['buy_count'] += 1
                stats['total_buy_amount'] += tx.token_amount
                stats['total_buy_cost_usd'] += tx_value_usd
            elif tx.tx_type == "SELL":
                stats['sell_count'] += 1
                stats['total_sell_amount'] += tx.token_amount
                stats['total_sell_value_usd'] += tx_value_usd
                
                # 計算這筆賣出的PNL
                # 注意：這裡簡化處理，實際上應該追蹤每筆交易的PNL
                if tx.token_amount > 0 and original_stats.get('avg_buy_price', 0) > 0:
                    sell_price = tx_value_usd / tx.token_amount
                    buy_price = original_stats.get('avg_buy_price', 0)
                    pnl = (sell_price - buy_price) * tx.token_amount
                    stats['realized_pnl_usd'] += pnl
        
        # 總計數據
        total_buy = sum(stats['buy_count'] for stats in token_period_stats.values())
        total_sell = sum(stats['sell_count'] for stats in token_period_stats.values())
        total_cost = sum(stats['total_buy_cost_usd'] for stats in token_period_stats.values())
        total_sell_value = sum(stats['total_sell_value_usd'] for stats in token_period_stats.values())
        total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in token_period_stats.values())
        
        # 計算勝率等指標
        tokens_with_buys = [token for token, stats in token_period_stats.items() if stats['total_buy_amount'] > 0]
        profitable_tokens = sum(1 for token in tokens_with_buys if token_period_stats[token]['realized_pnl_usd'] > 0)
        total_tokens = len(tokens_with_buys)
        
        # PNL相關指標
        pnl = total_realized_pnl
        pnl_percentage = (pnl / total_sell_value) * 100 if total_sell_value > 0 else 0
        pnl_percentage = max(pnl_percentage, -100)  # 確保不低於-100%
        
        # 勝率
        win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0
        
        # 資產槓桿
        asset_multiple = pnl_percentage / 100
        
        # 返回統計結果
        return {
            "asset_multiple": round(asset_multiple, 2),
            "total_buy": total_buy,
            "total_sell": total_sell,
            "total_transaction_num": total_buy + total_sell,
            "total_cost": total_cost,
            "average_cost": total_cost / total_buy if total_buy > 0 else 0,
            "total_profit": total_sell_value,
            "pnl": pnl,
            "pnl_percentage": round(pnl_percentage, 2),
            "win_rate": round(win_rate, 2),
            "avg_realized_profit": round(pnl / total_tokens if total_tokens > 0 else 0, 2),
            "daily_pnl_chart": ",".join([f"0.00" for _ in range(days)])  # 這部分需要更細緻的時間處理
        }

    # async def _get_recent_tokens(self, count=3):
    #     """獲取最近交易的代幣清單"""
    #     # 按最後交易時間排序
    #     token_last_time = {}
    #     for token_mint, stats in self.token_stats.items():
    #         last_time = stats.get('last_sell_time', 0) or stats.get('first_buy_time', 0) or 0
    #         token_last_time[token_mint] = last_time
        
    #     # 取最新的幾個代幣
    #     sorted_tokens = sorted(token_last_time.items(), key=lambda x: x[1], reverse=True)
    #     recent_tokens = []
        
    #     for token, _ in sorted_tokens[:count]:
    #         token_info = TokenUtils.get_token_info(token)
    #         if isinstance(token_info, dict):
    #             symbol = token_info.get('symbol')
    #             icon = token_info.get('url')
    #             if symbol and icon:
    #                 recent_tokens.append({"address": token, "symbol": symbol, "logo": icon})
        
    #     return recent_tokens

    async def _get_recent_tokens(self, count=3):
        """獲取最近交易的代幣清單"""
        # 按最後交易時間排序
        token_last_time = {}
        for token_mint, stats in self.token_stats.items():
            last_time = stats.get('last_sell_time', 0) or stats.get('first_buy_time', 0) or 0
            token_last_time[token_mint] = last_time
        
        # 取最新的幾個代幣
        sorted_tokens = sorted(token_last_time.items(), key=lambda x: x[1], reverse=True)
        
        # 直接取最新的 count 個 token 地址
        recent_tokens = [token for token, _ in sorted_tokens[:count]]
        
        # 將 token 地址用逗號分隔
        return ','.join(recent_tokens) if recent_tokens else None

    async def filter_smart_money_user_import(self, async_session, client, wallet_type) -> str:
        if not self.portfolios:
            return "沒有找到任何代幣交易"
        
        # 初始化 wallet_transactions 字典
        wallet_transactions = {
            self.wallet_address: []
        }
        
        # 遍歷每個代幣組合
        for token_mint, portfolio in self.portfolios.items():
            # 遍歷該代幣的每筆交易
            for tx in portfolio.transactions:
                # 確保每筆交易都有完整的結構
                transaction_info = {
                    'timestamp': tx.timestamp,  # 添加時間戳
                    'token_mint': token_mint,
                    'token_amount': tx.token_amount,
                    'transaction_type': tx.tx_type,
                    'sol_amount': tx.sol_amount,
                    'usdc_amount': tx.usdc_amount,
                    'sol_price_usd': tx.sol_price_usd
                }
                
                # 添加到 wallet_transactions
                wallet_transactions[self.wallet_address].append(transaction_info)
        
        await filter_smart_wallets2(
            wallet_transactions, 
            self.sol_price_usd, 
            async_session, 
            client, 
            "SOLANA", 
            wallet_type
        )
        await calculate_remaining_tokens(
            wallet_transactions, 
            self.wallet_address, 
            async_session, 
            "SOLANA",
            client
        )

    def export_to_csv(self, filename: str = None):
        """將交易數據導出為CSV文件"""
        if not filename:
            filename = f"{self.wallet_address}_transactions.csv"
        
        data = []
        for tx in self.processed_txs:
            data.append({
                "signature": tx.signature,
                "timestamp": tx.timestamp,
                "date": tx.date,
                "type": tx.tx_type,
                "source": tx.source,
                "token_mint": tx.token_mint,
                "token_symbol": tx.token_symbol,
                "token_amount": tx.token_amount,
                "sol_amount": tx.sol_amount,
                "usdc_amount": tx.usdc_amount,
                "token_price": tx.token_price,
                "usd_amount": tx.usd_amount,
                "price_currency": "SOL" if tx.sol_amount > 0 else "USDC" if tx.usdc_amount > 0 else "UNKNOWN"
            })
        
        df = pd.DataFrame(data)
        df.to_csv(filename, index=False)
        print(f"已導出交易數據到 {filename}")
        
        # 導出代幣組合數據
        portfolio_data = []
        for mint, portfolio in self.portfolios.items():
            realized_pnl, realized_pnl_pct = portfolio.calculate_pnl()
            realized_pnl_usd, _ = portfolio.calculate_pnl_usd(self.sol_price_usd)
            
            portfolio_data.append({
                "token_mint": portfolio.token_mint,
                "token_symbol": portfolio.token_symbol,
                "currency": portfolio.currency,
                "total_bought": portfolio.total_bought,
                "total_sold": portfolio.total_sold,
                "remaining_tokens": portfolio.remaining_tokens,
                "total_cost_sol": portfolio.total_cost_sol,
                "total_cost_usdc": portfolio.total_cost_usdc,
                "total_revenue_sol": portfolio.total_revenue_sol,
                "total_revenue_usdc": portfolio.total_revenue_usdc,
                "avg_buy_price_sol": portfolio.avg_buy_price_sol,
                "avg_buy_price_usdc": portfolio.avg_buy_price_usdc,
                "avg_sell_price_sol": portfolio.avg_sell_price_sol,
                "avg_sell_price_usdc": portfolio.avg_sell_price_usdc,
                "weighted_avg_cost": portfolio.weighted_avg_cost,
                "realized_pnl": realized_pnl,
                "realized_pnl_usd": realized_pnl_usd,
                "realized_pnl_percentage": realized_pnl_pct,
                "transaction_count": len(portfolio.transactions),
                "quick_trades_count": portfolio.quick_trades_count,
                "only_sell": portfolio.is_only_sell()
            })
        
        portfolio_df = pd.DataFrame(portfolio_data)
        portfolio_filename = f"{self.wallet_address}_portfolios.csv"
        portfolio_df.to_csv(portfolio_filename, index=False)
        print(f"已導出代幣組合數據到 {portfolio_filename}")
    
    def load_transactions_from_file(self, filename: str):
        """從JSON文件加載交易數據"""
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                self.transactions = json.load(f)
            print(f"從文件 {filename} 加載了 {len(self.transactions)} 筆交易記錄")
            
            # 按時間排序交易
            self.transactions.sort(key=lambda tx: tx.get("timestamp", 0))
            
            return True
        except Exception as e:
            print(f"加載文件時出錯: {str(e)}")
            return False


async def fetch_transactions(address, api_key, before=None, session=None):
    """
    發送單次請求以獲取交易數據。
    直接返回 API 響應，讓上層函數處理錯誤情況。
    """
    params = {"api-key": api_key, "type": "SWAP", "limit": LIMIT}
    if before:
        params["before"] = before
        # print(f"使用 before 參數: {before}")  # 添加日誌

    try:
        async with session.get(API_URL.format(address), params=params) as response:
            data = await response.json()
            return data
    except Exception as e:
        print(f"請求過程中發生錯誤: {str(e)}")
        return None


async def fetch_all_transactions(address, api_key, max_records=2000):
    """
    使用異步批次請求獲取所有相關交易數據。
    改進了錯誤處理和分頁邏輯。
    """
    transactions = []
    total_fetched = 0
    before_signature = None
    thirty_days_ago = datetime.now() - timedelta(days=30)
    
    async with aiohttp.ClientSession() as session:
        while total_fetched < max_records:
            try:
                # print(f"\n開始新的請求，before_signature: {before_signature}")  # 添加日誌
                data = await fetch_transactions(address, api_key, before_signature, session)
                
                if data is None:  # 請求出錯
                    print("請求失敗，退出循環")
                    break
                
                # 處理 API 返回的錯誤信息
                if isinstance(data, dict) and 'error' in data:
                    error_msg = data['error']
                    # print(f"收到 API 錯誤信息: {error_msg}")
                    
                    if "before` parameter set to" in error_msg:
                        # 提取新的 signature
                        import re
                        match = re.search(r'before` parameter set to ([^.]+)', error_msg)
                        if match:
                            before_signature = match.group(1)
                            # print(f"提取到新的 before signature: {before_signature}")
                            continue
                    print("無法處理的錯誤信息，退出循環")
                    break
                
                # 處理正常的數據響應
                if isinstance(data, list):
                    # print(f"成功獲取數據，筆數: {len(data)}")
                    
                    if not data:  # 空列表
                        break
                        
                    valid_transactions = 0
                    for txn in data:
                        timestamp = txn.get('timestamp')
                        if timestamp is None:
                            continue
                            
                        tx_time = datetime.fromtimestamp(timestamp)
                        if tx_time < thirty_days_ago:
                            print("超出時間範圍，結束查詢")
                            return transactions
                        
                        transactions.append(txn)
                        valid_transactions += 1
                    
                    # print(f"本批次有效交易數: {valid_transactions}")
                    total_fetched += len(data)
                    
                    if data:
                        before_signature = data[-1].get("signature")
                        # print(f"更新 before_signature: {before_signature}")
                else:
                    print(f"未預期的數據格式: {type(data)}")
                    break
                
            except Exception as e:
                print(f"處理數據時發生錯誤: {str(e)}")
                print(f"錯誤詳情：", e.__class__.__name__)
                break
    
    print(f"\n查詢完成，總共獲取交易數量: {len(transactions)}")
    return transactions


async def main():
    # 設置API金鑰和錢包地址
    api_key = "a8aa1dc1-feed-4bfa-91b5-daf2c0eedc08"  # 替換為您的API金鑰
    
    start_time = time.time()  # 開始計時
    
    # 創建錢包分析器
    global wallet_analyzer  # 使其成為全局變數，以便在TokenPortfolio.__str__中訪問
    wallet_analyzer = WalletAnalyzer(TARGET_WALLET)
    
    # 嘗試從文件加載交易數據，如果不存在則從 API 獲取
    filename = f"{TARGET_WALLET}_raw_transactions.json"
    if os.path.exists(filename):
        if wallet_analyzer.load_transactions_from_file(filename):
            print(f"成功從文件加載交易數據")
        else:
            # 如果加載失敗，則從 API 獲取
            await wallet_analyzer.fetch_transactions(api_key)
    else:
        # 獲取交易數據
        await wallet_analyzer.fetch_transactions(api_key)
    
    # 處理交易數據
    wallet_analyzer.process_transactions()
    
    # 生成並輸出摘要
    summary = await wallet_analyzer.generate_summary()
    print(summary)
    
    # 導出CSV文件
    # wallet_analyzer.export_to_csv()
    
    end_time = time.time()  # 結束計時
    elapsed_time = end_time - start_time
    print(f"\n分析完成，耗時: {elapsed_time:.2f} 秒")

# 如果直接運行此腳本
if __name__ == "__main__":
    asyncio.run(main())