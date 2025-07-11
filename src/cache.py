import os
import time
import requests
import json
import asyncio
import asyncpg
import logging
from dotenv import load_dotenv
from redis import asyncio as aioredis
from decimal import Decimal, getcontext, ROUND_DOWN
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import select, and_, text, func, case, distinct
from sqlalchemy.ext.asyncio import AsyncSession
from datetime import datetime, timedelta, timezone
from sqlalchemy.sql import func, case
from apscheduler.schedulers.asyncio import AsyncIOScheduler

load_dotenv()
time_windows = [1, 12, 24, 24*3, 24*7]
getcontext().prec = 28  
backend_api = os.getenv("backend_API")

# 設置日誌
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('redis_cache')

def get_redis_url_from_env():
    host = os.getenv('REDIS_HOST_SWAP', 'localhost')
    port = os.getenv('REDIS_PORT_SWAP', '6379')
    db = os.getenv('REDIS_DB_SWAP', '0')
    password = os.getenv('REDIS_PASSWORD', '')
    if password:
        return f'redis://:{password}@{host}:{port}/{db}'
    else:
        return f'redis://{host}:{port}/{db}'

def generate_cache_key(prefix: str, **kwargs) -> str:
    """生成緩存鍵"""
    sorted_items = sorted(kwargs.items())
    key_parts = [str(k) + ":" + str(v) for k, v in sorted_items]
    return f"{prefix}:{':'.join(key_parts)}"

# 使用更嚴格的 format_price
def format_price(value):
    """
    將數值格式化為字符串，避免科學計數法問題，更強健地處理各種輸入
    """
    try:
        # 導入需要的模組
        from decimal import Decimal, ROUND_DOWN, InvalidOperation, getcontext
        import re
        
        # 設置更高的精度來處理非常小的數字
        getcontext().prec = 28
        
        # 如果是 None，直接返回 "0"
        if value is None:
            return "0"
        
        # 處理可能是列表或其他非字符串/數字類型的情況
        if isinstance(value, (list, dict, tuple)):
            return "0"
            
        # 檢測零值的特殊情況
        try:
            if float(value) == 0:
                return "0"
        except (ValueError, TypeError):
            pass
            
        # 將值轉換為字符串
        try:
            str_value = str(value).lower().strip()
        except:
            return "0"
            
        # 如果是空字符串，返回 "0"
        if not str_value:
            return "0"
        
        # 檢查字符串是否可能是有效的數字格式
        # 有效數字格式：可能有負號，數字，可能有小數點，數字，可能有科學記數法
        valid_number_pattern = r'^-?\d*\.?\d*(?:e[-+]?\d+)?$'
        if not re.match(valid_number_pattern, str_value):
            return "0"  # 如果不是有效的數字格式，返回 "0"
        
        # 轉換科學記數法為浮點數
        try:
            float_value = float(str_value)
            if float_value == 0:
                return "0"
        except:
            return "0"
        
        # 非常小的數字使用特殊處理
        absolute_value = abs(float_value)
        
        # 根據數值大小使用不同的精度
        if absolute_value < 1e-12:  # 極小值
            return "{:.15f}".format(float_value).rstrip('0').rstrip('.') or "0"
        elif absolute_value < 1e-9:  # 非常小的值
            return "{:.12f}".format(float_value).rstrip('0').rstrip('.') or "0"
        elif absolute_value < 1e-6:  # 小值
            return "{:.9f}".format(float_value).rstrip('0').rstrip('.') or "0"
        elif absolute_value < 0.1:  # 中小值
            return "{:.9f}".format(float_value).rstrip('0').rstrip('.') or "0"
        elif absolute_value < 1:  # 接近1的值
            return "{:.9f}".format(float_value).rstrip('0').rstrip('.') or "0"
        else:  # 大於等於1的值
            return "{:.6f}".format(float_value).rstrip('0').rstrip('.') or "0"
        
    except Exception as e:
        print(f"Error in format_price: {e}, value: {type(value)}, {value}")
        return "0"

# 內存緩存類，使用Python字典實現
class MemoryCache:
    def __init__(self, max_size=1000, cleanup_interval=300):
        self.cache = {}
        self.expiry_times = {}
        self.access_times = {}
        self.max_size = max_size
        self.cleanup_interval = cleanup_interval
        self.lock = asyncio.Lock()
        self.cleanup_task = None
        self.logger = logging.getLogger('memory_cache')

    async def start_cleanup_task(self):
        """异步启动清理任务"""
        if self.cleanup_task is None:
            self.cleanup_task = asyncio.create_task(self._cleanup_loop())
            self.logger.info("内存缓存清理任务已启动")

    async def _cleanup_loop(self):
        """定期清理过期项"""
        while True:
            await asyncio.sleep(self.cleanup_interval)
            await self.cleanup_expired()
    
    async def cleanup_expired(self):
        """清理過期的緩存項目"""
        try:
            now = time.time()
            expired_keys = []
            
            async with self.lock:
                # 找出過期的鍵
                for key, expiry_time in self.expiry_times.items():
                    if expiry_time <= now:
                        expired_keys.append(key)
                
                # 刪除過期的項目
                for key in expired_keys:
                    self.cache.pop(key, None)
                    self.expiry_times.pop(key, None)
                    self.access_times.pop(key, None)
                
                # 如果緩存仍然過大，則刪除最早訪問的項目
                if len(self.cache) > self.max_size:
                    # 按訪問時間排序
                    sorted_keys = sorted(self.access_times.items(), key=lambda x: x[1])
                    # 計算需要刪除的項目數量
                    to_remove = len(self.cache) - self.max_size
                    # 刪除最早訪問的項目
                    for i in range(to_remove):
                        if i < len(sorted_keys):
                            key = sorted_keys[i][0]
                            self.cache.pop(key, None)
                            self.expiry_times.pop(key, None)
                            self.access_times.pop(key, None)
            
            self.logger.debug(f"緩存清理完成，刪除了 {len(expired_keys)} 個過期項目，當前緩存大小: {len(self.cache)}")
        except Exception as e:
            self.logger.error(f"緩存清理出錯: {e}")
    
    async def set(self, key: str, value: Any, expire_seconds: int = 60):
        """設置緩存項目"""
        async with self.lock:
            # 計算過期時間
            expiry_time = time.time() + expire_seconds
            # 更新緩存
            self.cache[key] = value
            self.expiry_times[key] = expiry_time
            self.access_times[key] = time.time()
            
            # 如果緩存大小超過限制，觸發清理
            if len(self.cache) > self.max_size:
                await self.cleanup_expired()
    
    async def get(self, key: str) -> Optional[Any]:
        """獲取緩存項目"""
        async with self.lock:
            # 檢查鍵是否存在且未過期
            if key in self.cache and key in self.expiry_times:
                if time.time() <= self.expiry_times[key]:
                    # 更新訪問時間
                    self.access_times[key] = time.time()
                    return self.cache[key]
                else:
                    # 如果已過期，刪除項目
                    self.cache.pop(key, None)
                    self.expiry_times.pop(key, None)
                    self.access_times.pop(key, None)
            
            return None

    async def get_json(self, key: str) -> Optional[Dict]:
        """獲取 JSON 格式的緩存"""
        data = await self.get(key)
        if data is None:
            return None
        
        try:
            if isinstance(data, str):
                return json.loads(data)
            return data  # 假設已經是解析過的JSON對象
        except json.JSONDecodeError as e:
            self.logger.error(f"JSON解析錯誤: {e}, key: {key}")
            return None

    async def set_json(self, key: str, value: Dict, expire_seconds: int = 3600):
        """設置 JSON 格式的緩存"""
        # 直接存儲對象，無需序列化
        await self.set(key, value, expire_seconds)

# 混合緩存類，結合內存緩存和Redis緩存
class HybridCache:
    def __init__(self, redis_url=None, memory_cache_size=1000):
        if redis_url is None:
            redis_url = get_redis_url_from_env()
        self.redis = RedisCache(redis_url)
        self.memory_cache = MemoryCache(max_size=memory_cache_size)
        self.logger = logging.getLogger('hybrid_cache')
        self._initialized = False  # 初始化標記

    async def start(self):
        """启动缓存服务"""
        if self._initialized:
            self.logger.info("混合緩存服務已在運行中")
            return
            
        await self.memory_cache.start_cleanup_task()
        self.redis.start()
        self._initialized = True  # 設置標記
        self.logger.info("混合缓存服务已启动")

    def stop(self):
        """停止缓存服务"""
        self.redis.stop()
        self.logger.info("混合缓存服务已停止")
    
    # 1. 交易查詢緩存方法
    async def generate_transaction_cache_key(self, chain, wallet_addresses=None, token_address=None, 
                                            name=None, fetch_all=False, transaction_type=None, min_value=None):
        """為交易查詢生成緩存鍵"""
        parts = [f"tx_query:{chain.lower()}"]
        
        if wallet_addresses:
            # 對列表排序以確保相同的內容產生相同的緩存鍵
            sorted_addresses = sorted(wallet_addresses)
            parts.append(f"wallets:{','.join(sorted_addresses)}")
        
        if token_address:
            parts.append(f"token:{token_address}")
        
        if name:
            parts.append(f"name:{name}")
        
        if fetch_all:
            parts.append("fetch_all:true")
        
        if transaction_type:
            parts.append(f"type:{transaction_type}")
        
        if min_value is not None:
            parts.append(f"min_value:{min_value}")
        
        return ":".join(parts)
    
    async def get_transaction_cache(self, cache_key):
        """從混合緩存獲取交易數據，先查內存再查Redis"""
        # 先查內存緩存
        memory_result = await self.memory_cache.get(cache_key)
        if memory_result:
            self.logger.debug(f"從內存緩存獲取交易數據: {cache_key}")
            return memory_result, "memory"
        
        # 如果內存沒有，查詢Redis
        redis_result = await self.redis.get_json(f"query_cache:{cache_key}")
        if redis_result:
            # 更新到內存緩存
            await self.memory_cache.set(cache_key, redis_result, 30)  # 內存緩存保存30秒
            self.logger.debug(f"從Redis緩存獲取交易數據: {cache_key}")
            return redis_result, "redis"
        
        return None, None
    
    async def set_transaction_cache(self, cache_key, data, expire_seconds=60):
        """設置交易數據緩存，同時更新內存和Redis緩存"""
        # 更新內存緩存（較短過期時間）
        await self.memory_cache.set(cache_key, data, min(30, expire_seconds))
        
        # 更新Redis緩存
        await self.redis.set_json(f"query_cache:{cache_key}", data, expire_seconds)
    
    # 2. 獲取通用緩存數據
    async def get_cached_data(self, chain):
        """獲取通用緩存數據，先查內存再查Redis"""
        memory_key = f"common:{chain.lower()}"
        
        # 先查內存緩存
        memory_result = await self.memory_cache.get(memory_key)
        if memory_result:
            self.logger.debug(f"從內存緩存獲取通用數據: {memory_key}")
            return memory_result, "memory"
        
        # 如果內存沒有，查詢Redis
        redis_result, is_cache_hit = await self.redis.get_cached_data(chain)
        if redis_result:
            # 更新到內存緩存
            await self.memory_cache.set(memory_key, redis_result, 30)  # 內存緩存保存30秒
            self.logger.debug(f"從Redis緩存獲取通用數據: {memory_key}")
            return redis_result, is_cache_hit
        
        return None, False
    
    # 3. 錢包持倉數據緩存方法
    async def generate_position_cache_key(self, chain, wallet_address, pnl_sort):
        """為錢包持倉數據生成緩存鍵"""
        return f"position:{chain.lower()}:{wallet_address}:{pnl_sort}"
    
    async def get_position_cache(self, chain, wallet_address, pnl_sort):
        """獲取緩存的錢包持倉數據，先查內存再查Redis"""
        cache_key = await self.generate_position_cache_key(chain, wallet_address, pnl_sort)
        
        # 先查內存緩存
        memory_result = await self.memory_cache.get(cache_key)
        if memory_result:
            self.logger.debug(f"從內存緩存獲取數據: {cache_key}")
            return memory_result, "memory"
        
        # 如果內存沒有，查詢Redis
        redis_result = await self.redis.get_json(f"query_cache:{cache_key}")
        if redis_result:
            # 更新到內存緩存
            await self.memory_cache.set(cache_key, redis_result, 30)  # 內存緩存時間較短
            self.logger.debug(f"從Redis緩存獲取數據: {cache_key}")
            return redis_result, "redis"
        
        return None, None
    
    async def set_position_cache(self, chain, wallet_address, pnl_sort, data, expire_seconds=60):
        """設置錢包持倉數據緩存，同時更新內存和Redis緩存"""
        cache_key = await self.generate_position_cache_key(chain, wallet_address, pnl_sort)
        
        # 更新內存緩存（較短過期時間）
        await self.memory_cache.set(cache_key, data, min(30, expire_seconds))
        
        # 更新Redis緩存
        await self.redis.set_json(f"query_cache:{cache_key}", data, expire_seconds)
    
    # 4. 代幣趨勢數據緩存方法
    async def generate_token_trend_cache_key(self, chain, token_addresses, time_range):
        """為代幣趨勢數據生成緩存鍵"""
        # 將token_addresses排序以確保相同內容產生相同的鍵
        sorted_token_addresses = sorted(token_addresses)
        tokens_str = ','.join(sorted_token_addresses)
        return f"tokentrend:{chain.lower()}:{tokens_str}:{time_range}"
    
    async def get_token_trend_cache(self, chain, token_addresses, time_range):
        """獲取緩存的代幣趨勢數據，先查內存再查Redis"""
        cache_key = await self.generate_token_trend_cache_key(chain, token_addresses, time_range)
        
        # 先查內存緩存
        memory_result = await self.memory_cache.get(cache_key)
        if memory_result:
            self.logger.debug(f"從內存緩存獲取數據: {cache_key}")
            return memory_result, "memory"
        
        # 如果內存沒有，查詢Redis
        redis_result = await self.redis.get_json(f"query_cache:{cache_key}")
        if redis_result:
            # 更新到內存緩存
            await self.memory_cache.set(cache_key, redis_result, 60)  # 內存緩存時間較短
            self.logger.debug(f"從Redis緩存獲取數據: {cache_key}")
            return redis_result, "redis"
        
        return None, None
    
    async def set_token_trend_cache(self, chain, token_addresses, time_range, data, expire_seconds=120):
        """設置代幣趨勢數據緩存，同時更新內存和Redis緩存"""
        cache_key = await self.generate_token_trend_cache_key(chain, token_addresses, time_range)
        
        # 更新內存緩存（較短過期時間）
        await self.memory_cache.set(cache_key, data, min(60, expire_seconds))
        
        # 更新Redis緩存
        await self.redis.set_json(f"query_cache:{cache_key}", data, expire_seconds)
    
    # 5. 多鏈代幣趨勢數據緩存方法
    async def generate_token_trend_allchain_cache_key(self, chains, token_addresses, time_range):
        """為多鏈代幣趨勢數據生成緩存鍵"""
        # 將chains和token_addresses排序以確保相同內容產生相同的鍵
        sorted_chains = sorted(chains)
        sorted_token_addresses = sorted(token_addresses)
        chains_str = ','.join(sorted_chains)
        tokens_str = ','.join(sorted_token_addresses)
        return f"tokentrend_allchain:{chains_str}:{tokens_str}:{time_range}"
    
    async def get_token_trend_allchain_cache(self, chains, token_addresses, time_range):
        """獲取緩存的多鏈代幣趨勢數據，先查內存再查Redis"""
        cache_key = await self.generate_token_trend_allchain_cache_key(chains, token_addresses, time_range)
        
        # 先查內存緩存
        memory_result = await self.memory_cache.get(cache_key)
        if memory_result:
            self.logger.debug(f"從內存緩存獲取數據: {cache_key}")
            return memory_result, "memory"
        
        # 如果內存沒有，查詢Redis
        redis_result = await self.redis.get_json(f"query_cache:{cache_key}")
        if redis_result:
            # 更新到內存緩存
            await self.memory_cache.set(cache_key, redis_result, 90)  # 內存緩存時間較短
            self.logger.debug(f"從Redis緩存獲取數據: {cache_key}")
            return redis_result, "redis"
        
        return None, None
    
    async def set_token_trend_allchain_cache(self, chains, token_addresses, time_range, data, expire_seconds=180):
        """設置多鏈代幣趨勢數據緩存，同時更新內存和Redis緩存"""
        cache_key = await self.generate_token_trend_allchain_cache_key(chains, token_addresses, time_range)
        
        # 更新內存緩存（較短過期時間）
        await self.memory_cache.set(cache_key, data, min(90, expire_seconds))
        
        # 更新Redis緩存
        await self.redis.set_json(f"query_cache:{cache_key}", data, expire_seconds)
    
    # 6. 其他通用方法
    async def get_cache_stats(self):
        """獲取混合緩存的統計信息"""
        memory_stats = await self.memory_cache.get_stats()
        return {
            "memory_cache": memory_stats,
            "redis_cache": "統計信息無法直接獲取" # Redis無法直接獲取統計信息
        }
    
    async def batch_get_transaction_cache(self, chain: str, wallet_addresses: List[str], transaction_type: str = None):
        """批量獲取多個錢包的交易緩存"""
        cache_keys = []
        for wallet in wallet_addresses:
            cache_key = await self.generate_transaction_cache_key(
                chain=chain,
                wallet_addresses=[wallet],
                transaction_type=transaction_type
            )
            cache_keys.append(cache_key)
        
        # 批量查詢內存緩存
        memory_results = {}
        for key in cache_keys:
            memory_result = await self.memory_cache.get(key)
            if memory_result:
                memory_results[key] = memory_result
        
        # 對於內存未命中的鍵，批量查詢Redis
        missing_keys = [k for k in cache_keys if k not in memory_results]
        if missing_keys:
            redis_keys = [f"query_cache:{k}" for k in missing_keys]
            redis_results = await self.redis.batch_get_json(redis_keys)
            
            # 處理Redis結果並更新內存緩存
            for i, key in enumerate(missing_keys):
                redis_key = redis_keys[i]
                if redis_key in redis_results and redis_results[redis_key]:
                    # 更新到內存緩存
                    await self.memory_cache.set(key, redis_results[redis_key], 30)
                    memory_results[key] = redis_results[redis_key]
        
        return memory_results

# 創建一個新的預熱任務調度器
class CacheWarmer:
    _instance = None  # 單例模式
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(CacheWarmer, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, sessions, refresh_interval=600):
        if self._initialized:
            return
            
        self.sessions = sessions
        self.refresh_interval = refresh_interval
        self.scheduler = AsyncIOScheduler()
        self._running_tasks = {}
        self._initialized = True  # 標記初始化完成
    
    def start(self):
        """啟動預熱調度器"""
        for chain in self.sessions.keys():
            self.scheduler.add_job(
                self._warm_chain_cache,
                'interval',
                seconds=self.refresh_interval,
                args=[chain],
                id=f"warm_cache_{chain}",
                replace_existing=True
            )
        self.scheduler.start()
        logging.info("緩存預熱調度器已啟動")
    
    def stop(self):
        self.scheduler.shutdown()
        logging.info("緩存預熱調度器已停止")
    
    async def _warm_chain_cache(self, chain):
        """預熱特定鏈的緩存"""
        if chain in self._running_tasks and not self._running_tasks[chain].done():
            logging.debug(f"{chain} 預熱任務已在運行，跳過")
            return
        
        self._running_tasks[chain] = asyncio.create_task(self._execute_chain_warming(chain))
    
    async def _execute_chain_warming(self, chain):
        start_time = time.time()
        logging.info(f"開始預熱 {chain} 鏈緩存...")
        
        cache_key = generate_cache_key("smart_money_wallet_count", chain=chain.lower())
        try:
            session_factory = self.sessions.get(chain)
            async with session_factory() as session:
                # 優先查詢 1 小時數據
                top_tokens = await fetch_adaptive_top_tokens(session, chain, 3600, 30)
                used_window = "1h"
                
                # 如果無數據，回退到歷史數據
                if not top_tokens or len(top_tokens) == 0:
                    top_tokens = await fetch_historical_top_tokens(session, chain, 30)
                    used_window = "all_time"
                
                # 更新緩存
                if top_tokens:
                    now = int(datetime.now(timezone.utc).timestamp())
                    cache_data = {
                        "data": top_tokens,
                        "timestamp": now,
                        "time_window": used_window
                    }
                    await cache_service.set_json(cache_key, cache_data)  # 無 TTL
                    logging.info(f"預熱 {chain} 完成，使用 {used_window} 窗口，耗時: {time.time() - start_time:.2f}秒，項目數: {len(top_tokens)}")
                else:
                    logging.warning(f"預熱 {chain} 未找到數據")
        except Exception as e:
            logging.error(f"預熱 {chain} 失敗: {e}", exc_info=True)
            await session.rollback()  # 顯式回滾

class RedisCache:
    def __init__(self, redis_url: str = None):
        if redis_url is None:
            redis_url = get_redis_url_from_env()
        logging.info(f"Redis URL: {redis_url}")
        self.redis = aioredis.from_url(redis_url, encoding="utf-8", decode_responses=True)
        self._stop_flag = True
        self._update_task = None
        self._top_tokens_task = None
        self._filtered_tokens_cache = None
        self._filtered_tokens_last_update = 0
        self._chain_locks = {}
        self._initialized = False
        logging.getLogger('redis_cache').setLevel(logging.DEBUG)
        
    def get_chain_lock(self, chain):
        """取得特定鏈的鎖，如果不存在則創建"""
        if chain not in self._chain_locks:
            self._chain_locks[chain] = asyncio.Lock()
        return self._chain_locks[chain]

    def start(self):
        """啟動所有緩存更新任務"""
        self._stop_flag = False
        
        # 啟動交易緩存更新任務
        if self._update_task is None:
            self._update_task = asyncio.create_task(self._update_loop())
            logger.info("交易緩存更新任務已啟動")
        
        # 啟動熱門代幣緩存更新任務
        if self._top_tokens_task is None:
            self._top_tokens_task = asyncio.create_task(self._top_tokens_update_loop())
            logger.info("熱門代幣緩存更新任務已啟動")

    def stop(self):
        """停止所有緩存更新任務"""
        self._stop_flag = True
        
        # 停止交易緩存更新任務
        if self._update_task:
            self._update_task.cancel()
            self._update_task = None
            logger.info("交易緩存更新任務已停止")
        
        # 停止熱門代幣緩存更新任務
        if self._top_tokens_task:
            self._top_tokens_task.cancel()
            self._top_tokens_task = None
            logger.info("熱門代幣緩存更新任務已停止")

    # async def _update_loop(self):
    #     """定期更新緩存的循環"""
    #     while not self._stop_flag:
    #         try:
    #             start_time = datetime.now()
    #             # logger.info("開始更新所有鏈的緩存...")
                
    #             await self._update_cache()
                
    #             end_time = datetime.now()
    #             execution_time = (end_time - start_time).total_seconds()
    #             # logger.info(f"緩存更新完成，耗時: {execution_time:.2f} 秒")
                
    #             # 確保最短2分鐘的間隔，考慮執行時間
    #             sleep_time = max(120 - execution_time, 10)  # 至少休息10秒
    #             await asyncio.sleep(sleep_time)
    #         except Exception as e:
    #             logger.error(f"緩存更新錯誤: {e}", exc_info=True)
    #             await asyncio.sleep(30)  # 發生錯誤時等待30秒後重試

    async def _update_loop(self):
        """定期更新緩存的循環"""
        while not self._stop_flag:
            try:
                start_time = datetime.now()
                
                from models import sessions  # 避免循環導入
                
                # 預先獲取要過濾的代幣列表
                filtered_token_addresses = await self.load_filtered_token_addresses()
                available_chains = list(sessions.keys())
                
                # 先更新各鏈緩存
                update_tasks = [
                    self._update_chain_cache(chain, sessions, filtered_token_addresses)
                    for chain in available_chains
                ]
                
                # 等待所有鏈的更新完成
                await asyncio.gather(*update_tasks)
                
                # 更新全鏈緩存
                await self._update_all_chains_cache(sessions)
                
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                # logger.info(f"緩存更新完成，耗時: {execution_time:.2f} 秒")
                
                # 確保最短2分鐘的間隔，考慮執行時間
                sleep_time = max(120 - execution_time, 10)  # 至少休息10秒
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logger.error(f"緩存更新錯誤: {e}", exc_info=True)
                await asyncio.sleep(30)  # 發生錯誤時等待30秒後重試

    async def _top_tokens_update_loop(self):
        """定期更新熱門代幣緩存的循環"""
        while not self._stop_flag:
            try:
                start_time = datetime.now()
                logger.info("開始更新熱門代幣緩存...")
                
                await self._update_top_tokens_cache()
                
                end_time = datetime.now()
                execution_time = (end_time - start_time).total_seconds()
                logger.info(f"熱門代幣緩存更新完成，耗時: {execution_time:.2f} 秒")
                
                # 每小時更新一次，確保最短間隔
                sleep_time = max(3600 - execution_time, 60)  # 至少休息1分鐘
                await asyncio.sleep(sleep_time)
            except Exception as e:
                logger.error(f"熱門代幣緩存更新錯誤: {e}", exc_info=True)
                await asyncio.sleep(60)  # 發生錯誤時等待1分鐘後重試

    async def _update_chain_cache(self, chain, sessions, filtered_token_addresses):
        """更新單個鏈的緩存"""
        from models import Transaction, WalletSummary, TokenState, debug_wallet_query  # 改用 TokenState
        chain_lock = self.get_chain_lock(chain)
        
        async with chain_lock:
            try:
                logger.info(f"開始更新 {chain} 鏈的緩存...")
                
                session_factory = sessions.get(chain.upper())
                if not session_factory:
                    logger.error(f"找不到 {chain} 鏈的會話工廠")
                    return

                smart_wallet_count = await debug_wallet_query(session_factory, chain)
                if smart_wallet_count == 0:
                    logger.error(f"{chain} 鏈確實沒有智能錢包記錄")
                    return
                    
                async with session_factory() as session:
                    try:
                        # schema = chain.lower()
                        schema = 'dex_query_v1'
                        Transaction.__table__.schema = schema
                        WalletSummary.__table__.schema = schema
                        TokenState.__table__.schema = schema  # 更新模型 schema
                        
                        raw_query = f"SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true"
                        result = await session.execute(text(raw_query))
                        smart_wallet_addresses = [row[0] for row in result]
                        
                        if not smart_wallet_addresses:
                            logger.error(f"{chain} 鏈沒有智能錢包")
                            return

                        TZ_UTC8 = timezone(timedelta(hours=8))
                        now = datetime.now(TZ_UTC8)
                        now_timestamp = int(now.timestamp())

                        all_transactions = []
                        # --- 修正 filtered_tokens_sql ---
                        if filtered_token_addresses:
                            filtered_tokens_sql = "array[" + ",".join([f"'{x}'" for x in set(filtered_token_addresses)]) + "]"
                        else:
                            filtered_tokens_sql = "ARRAY[]::text[]"
                        tx_query = f"""
                        SELECT t.*
                        FROM {schema}.wallet_transaction t
                        WHERE t.wallet_address IN (
                            SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
                        )
                        AND t.chain = '{chain}'
                        AND NOT EXISTS (
                            SELECT 1 FROM unnest({filtered_tokens_sql}) AS excluded_tokens(excluded_token)
                            WHERE excluded_token = t.token_address
                        )
                        ORDER BY transaction_time DESC 
                        LIMIT 5000
                        """
                        # --- 修改結束 ---
                        try:
                            chunk_result = await session.execute(text(tx_query))

                            for row in chunk_result:
                                tx = Transaction()
                                for key, value in row._mapping.items():
                                    setattr(tx, key, value)
                                all_transactions.append(tx)
                        except Exception as e:
                            logger.error(f"執行交易查詢時出錯: {e}")
                            return
                        
                        if not all_transactions:
                            logger.info(f"{chain} 鏈沒有符合條件的交易數據")
                            return
                        
                        wallet_addresses_from_txs = {tx.wallet_address for tx in all_transactions}
                        token_addresses = {tx.token_address for tx in all_transactions}
                        holdings_dict = await self._get_holdings(session, chain, wallet_addresses_from_txs, token_addresses)

                        one_hour_ago_timestamp = now_timestamp - 3600
                        stats_data = await self._get_stats(session, chain, smart_wallet_addresses, one_hour_ago_timestamp, token_addresses)

                        # 準備緩存數據並過濾
                        cache_data = []
                        filtered_count = 0
                        for tx in sorted(all_transactions, key=lambda tx: tx.transaction_time, reverse=True):
                            try:
                                stats = stats_data.get(tx.token_address, {'wallet_count': 0, 'buy_count': 0, 'sell_count': 0})
                                holding = holdings_dict.get((tx.wallet_address, tx.token_address))
                                
                                # 改用可能返回 None 的函數
                                cache_item = self._prepare_cache_item(tx, stats, holding)
                                
                                # 只有當 cache_item 不是 None 時才添加
                                if cache_item:
                                    cache_data.append(cache_item)
                                    
                                    # 達到90條就停止
                                    if len(cache_data) >= 90:
                                        break
                                else:
                                    filtered_count += 1
                            except Exception as e:
                                logger.error(f"處理交易記錄 {tx.signature} 時出錯: {e}", exc_info=True)
                                filtered_count += 1
                                continue
                    finally:
                        # 確保顯式關閉會話
                        await session.close()

                    if len(cache_data) < 90:
                        logger.warning(f"{chain} 鏈過濾後有效交易數不足 90 條，實際數量: {len(cache_data)}, "
                                    f"過濾掉 {filtered_count} 條無效記錄")

                    if cache_data:
                        newest_tx_time = cache_data[0].get('transaction_time', 0)
                        current_time = int(datetime.now().timestamp())
                        time_diff = current_time - newest_tx_time
                        
                        logger.info(f"{chain} 新數據準備完成，最新交易時間: {newest_tx_time} "
                                f"({datetime.fromtimestamp(newest_tx_time)}), 距現在: {time_diff} 秒")
                        
                        if time_diff > 600:
                            logger.warning(f"{chain} 準備寫入的數據疑似過舊！最新交易時間距今 {time_diff} 秒")

                    cache_key = f"transactions:{chain.lower()}"
                    await self.redis.set(cache_key, json.dumps(cache_data))
                    await self.redis.set(f"{cache_key}:last_update", str(now_timestamp))
                    
                    logger.info(f"{chain} 鏈緩存更新完成，存入 {len(cache_data)} 條有效交易，過濾掉 {filtered_count} 條")
            except asyncpg.exceptions.TooManyConnectionsError as e:
                logger.error(f"連線數過多，無法連接到 {chain} 鏈: {e}")
                # 可能等待一段時間後重試
                await asyncio.sleep(5)
                # 或者放棄此次更新
                return
            except Exception as e:
                logger.error(f"更新 {chain} 鏈的緩存時發生錯誤: {e}", exc_info=True)

    # 在 RedisCache 類中添加
    async def _update_all_chains_cache(self, sessions):
        """更新全鏈緩存，合併所有鏈的最新交易數據"""
        try:
            logger.info("開始更新全鏈緩存...")
            start_time = time.time()
            
            # 預取過濾代幣列表
            filtered_token_addresses = await self.load_filtered_token_addresses()
            available_chains = list(sessions.keys())
            
            # 從每條鏈獲取數據
            all_chains_data = []
            
            for chain in available_chains:
                try:
                    # 優先從緩存獲取
                    cache_key = f"transactions:{chain.lower()}"
                    chain_data = await self.get(cache_key)
                    
                    if chain_data:
                        chain_transactions = json.loads(chain_data)
                        
                        # 確保每筆交易都有 chain 字段
                        for tx in chain_transactions:
                            if 'chain' not in tx:
                                tx['chain'] = chain
                        
                        all_chains_data.extend(chain_transactions)
                        logger.info(f"從緩存獲取 {chain} 鏈數據: {len(chain_transactions)} 條")
                    else:
                        logger.warning(f"{chain} 鏈緩存不存在，跳過")
                except Exception as e:
                    logger.error(f"處理 {chain} 鏈數據時出錯: {str(e)}")
                    continue
            
            # 按交易時間排序
            all_chains_data.sort(key=lambda x: x.get("transaction_time", 0), reverse=True)
            
            # 只保留最新的90條
            top_transactions = all_chains_data[:90]
            
            # 儲存到緩存
            now = int(datetime.now(timezone.utc).timestamp())
            all_chains_cache_key = "transactions:all"
            
            await self.set(all_chains_cache_key, json.dumps(top_transactions))
            await self.set(f"{all_chains_cache_key}:last_update", str(now))
            
            # 記錄執行時間
            execution_time = time.time() - start_time
            logger.info(f"全鏈緩存更新完成，共 {len(top_transactions)} 筆交易，耗時: {execution_time:.2f}秒")
            
            # 計算各鏈佔比
            chain_counts = {}
            for tx in top_transactions:
                chain = tx.get('chain')
                if chain:
                    chain_counts[chain] = chain_counts.get(chain, 0) + 1
            
            logger.info(f"全鏈數據組成: {chain_counts}")
            
        except Exception as e:
            logger.error(f"更新全鏈緩存時出錯: {str(e)}", exc_info=True)

    async def _get_holdings(self, session, chain, wallet_addresses, token_addresses):
        """獲取持倉數據"""
        from models import TokenState  # 改用 TokenState
        start_time = datetime.now()
        holding_query = (
            select(TokenState)
            .where(
                and_(
                    TokenState.wallet_address.in_(wallet_addresses),
                    TokenState.token_address.in_(token_addresses)
                )
            )
        )
        # async with self.get_chain_lock(chain):
        await session.execute(text("SET search_path TO dex_query_v1;"))
        holding_result = await session.execute(holding_query)
        holdings = holding_result.scalars().all()
        holdings_dict = {
            (holding.wallet_address, holding.token_address): holding
            for holding in holdings
        }
        end_time = datetime.now()
        return holdings_dict

    async def _get_stats(self, session, chain, smart_wallet_addresses, one_hour_ago_timestamp, token_addresses):
        """獲取統計數據"""
        from models import Transaction  # 避免循環導入
        
        start_time = datetime.now()
        stats_query = (
            select(
                Transaction.token_address,
                func.count(distinct(Transaction.wallet_address)).label('wallet_count'),
                func.sum(case((Transaction.transaction_type == 'buy', 1), else_=0)).label('buy_count'),
                func.sum(case((Transaction.transaction_type == 'sell', 1), else_=0)).label('sell_count')
            )
            .where(
                and_(
                    Transaction.chain == chain,
                    Transaction.wallet_address.in_(smart_wallet_addresses),
                    Transaction.transaction_time >= one_hour_ago_timestamp,
                    Transaction.token_address.in_(token_addresses)
                )
            )
            .group_by(Transaction.token_address)
        )

        stats_result = await session.execute(stats_query)

        stats_data = {
            row.token_address: {
                'wallet_count': row.wallet_count,
                'buy_count': row.buy_count,
                'sell_count': row.sell_count
            }
            for row in stats_result
        }
        
        end_time = datetime.now()
        # logger.info(f"統計查詢耗時: {(end_time - start_time).total_seconds():.2f}秒")
        
        return stats_data

    def _prepare_cache_item(self, tx, stats, holding):
        """根據交易記錄與持倉 (TokenState) 組裝快取項目"""
        try:
            # --- 直接從 TokenState 讀取欄位 ---
            avg_buy_price = float(getattr(holding, 'current_avg_buy_price', 0) or 0)
            total_cost = float(getattr(holding, 'current_total_cost', 0) or 0)
            total_amount = float(getattr(holding, 'current_amount', 0) or 0)

            # 已實現 PnL 與百分比 (使用歷史買入成本作為基準)
            pnl = float(getattr(holding, 'historical_realized_pnl', 0) or 0)
            historical_buy_cost = float(getattr(holding, 'historical_buy_cost', 0) or 0)
            pnl_percentage = (pnl / historical_buy_cost * 100) if historical_buy_cost > 0 else 0

            # 風險控制: 過大數值視為異常歸零
            if abs(pnl) > 1_000_000 or abs(pnl_percentage) > 100_000:
                logger.warning(
                    f"異常PNL檢測 (reset to 0) - wallet: {tx.wallet_address}, token: {tx.token_address}, "
                    f"pnl: {pnl}, pnl_pct: {pnl_percentage}"
                )
                pnl = 0
                pnl_percentage = 0

            # 錢包餘額 (可能為 Decimal)
            wallet_balance = 0
            if hasattr(tx, 'wallet_balance'):
                wb = tx.wallet_balance
                wallet_balance = float(wb) if isinstance(wb, Decimal) else wb

            token_address = tx.token_address
            if hasattr(tx, 'chain') and tx.chain != 'SOLANA' and token_address:
                token_address = token_address.lower()

        except Exception as e:
            logger.error(f"_prepare_cache_item 發生錯誤: {e}", exc_info=True)
            avg_buy_price = 0
            total_cost = 0
            total_amount = 0
            pnl = 0
            pnl_percentage = 0
            wallet_balance = 0
            token_address = tx.token_address

        return {
            "wallet_address": tx.wallet_address,
            "wallet_balance": format_price(wallet_balance),
            "signature": tx.signature,
            "token_address": token_address,
            "token_icon": tx.token_icon,
            "token_name": tx.token_name,
            "price": format_price(tx.price),
            "amount": format_price(tx.amount),
            "marketcap": format_price(tx.marketcap),
            "value": format_price(tx.value),
            "holding_percentage": format_price(tx.holding_percentage),
            "chain": tx.chain,
            "realized_profit": format_price(tx.realized_profit),
            "realized_profit_percentage": format_price(tx.realized_profit_percentage),
            "transaction_type": tx.transaction_type,
            "wallet_count_last_hour": stats.get('wallet_count', 0),
            "buy_count_last_hour": stats.get('buy_count', 0),
            "sell_count_last_hour": stats.get('sell_count', 0),
            "transaction_time": tx.transaction_time,
            "time": tx.time.isoformat() if tx.time else None,
            "from_token_address": tx.from_token_address,
            "from_token_symbol": tx.from_token_symbol,
            "from_token_amount": format_price(tx.from_token_amount),
            "dest_token_address": tx.dest_token_address,
            "dest_token_symbol": tx.dest_token_symbol,
            "dest_token_amount": format_price(tx.dest_token_amount),

            # --- 新增持倉欄位 (TokenState) ---
            "avg_price": format_price(avg_buy_price),
            "total_cost": format_price(total_cost),
            "total_amount": format_price(total_amount),
            "total_pnl": format_price(pnl),
            "total_pnl_percentage": format_price(pnl_percentage)
        }

    # async def _update_top_tokens_cache(self):
    #     """更新所有鏈的熱門代幣緩存"""
    #     from models import sessions  # 避免循環導入
        
    #     # 獲取過濾代幣列表（緩存）
    #     filtered_token_addresses = await self.load_filtered_token_addresses()
    #     available_chains = list(sessions.keys())
        
    #     # 為每個鏈和時間窗口創建任務
    #     update_tasks = []
    #     for chain in available_chains:
    #         for time_window in time_windows:  # time_windows 是 [1, 12, 24, 24*3, 24*7]
    #             task = self._update_chain_time_window_tokens(
    #                 chain, time_window, sessions, filtered_token_addresses
    #             )
    #             update_tasks.append(task)
        
    #     # 並行執行所有任務
    #     await asyncio.gather(*update_tasks)

    async def _update_top_tokens_cache(self):
        """更新所有鏈的熱門代幣緩存，改為串行執行"""
        from models import sessions
        
        filtered_token_addresses = await self.load_filtered_token_addresses()
        available_chains = list(sessions.keys())
        
        # 先按鏈然後按時間窗口執行
        for chain in available_chains:
            for time_window in time_windows:
                try:
                    self._updating_top_tokens = True
                    await self._update_chain_time_window_tokens(
                        chain, time_window, sessions, filtered_token_addresses
                    )
                    # 每次查詢完後短暫等待，讓連接有時間釋放
                    await asyncio.sleep(0.2)
                except Exception as e:
                    logger.error(f"更新 {chain} {time_window}h 熱門代幣緩存失敗: {e}")
                finally:
                    self._updating_top_tokens = False

    async def _update_chain_time_window_tokens(self, chain, time_window, sessions, filtered_token_addresses):
        """更新特定鏈和時間窗口的熱門代幣緩存"""
        try:
            chain_lock = self.get_chain_lock(f"{chain}_tokens_{time_window}")
            
            async with chain_lock:
                session_factory = sessions.get(chain.upper())
                if not session_factory:
                    return
                
                async with session_factory() as session:
                    # 使用fetch_adaptive_top_tokens函數
                    top_tokens = await fetch_adaptive_top_tokens(
                        session, 
                        chain.upper(),
                        time_window=3600,
                        filtered_token_addresses=filtered_token_addresses
                    )
                    
                    # 生成緩存鍵
                    cache_key = generate_cache_key(
                        "top_tokens",
                        chain=chain.lower(),
                        time_window=time_window
                    )
                    
                    # 更新緩存
                    await self.set_json(cache_key, top_tokens, 3600)
                    # logger.info(f"已更新 {chain} 鏈 {time_window}小時 熱門代幣緩存")
        
        except Exception as e:
            logger.error(f"更新 {chain} 鏈 {time_window}小時 熱門代幣緩存時出錯: {e}", exc_info=True)

    async def load_filtered_token_addresses(self):
        """
        從本地文件或 API 加載要過濾的代幣地址列表，並緩存結果
        """
        current_time = int(datetime.now().timestamp())
        
        # 如果緩存存在且未過期（1小時有效期），直接返回緩存
        if (self._filtered_tokens_cache is not None and 
            current_time - self._filtered_tokens_last_update < 3600):
            return self._filtered_tokens_cache
            
        # 首先嘗試從本地文件讀取
        try:
            with open('filtered_tokens.json', 'r') as f:
                self._filtered_tokens_cache = json.load(f)
                self._filtered_tokens_last_update = current_time
                return self._filtered_tokens_cache
        except FileNotFoundError:
            # 如果本地文件不存在，則調用 API
            try:
                # response = requests.get("http://127.0.0.1:5007/internal/smart_token_filter_list?brand=BYD")
                # response = requests.get("http://172.25.183.205:4200/internal/smart_token_filter_list?brand=BYD")
                response = requests.get(f"{backend_api}/internal/internal/smart_token_filter_list?brand=BYD")
                
                if response.status_code == 200:
                    data = response.json()
                    
                    # 將 API 返回的數據保存到本地文件，以便下次直接讀取
                    with open('filtered_tokens.json', 'w') as f:
                        json.dump(data['data'], f)
                    
                    self._filtered_tokens_cache = data['data']
                    self._filtered_tokens_last_update = current_time
                    return self._filtered_tokens_cache
                else:
                    logger.error(f"獲取過濾代幣列表失敗：HTTP {response.status_code}")
                    return []
            except Exception as e:
                logger.error(f"調用 API 失敗: {e}")
                return []

    # async def get_cached_data(self, chain: str) -> Optional[Tuple[List[Dict], bool]]:
    #     """
    #     獲取緩存的交易數據，總是返回緩存數據，不檢查過期
    #     """
    #     try:
    #         cache_key = f"transactions:{chain.lower()}"
    #         data = await self.redis.get(cache_key)
    #         if not data:
    #             logger.warning(f"{chain} 鏈的緩存數據不存在")
    #             return None, False
            
    #         # 解析並排序數據
    #         cached_data = json.loads(data)
    #         sorted_data = sorted(
    #             cached_data,
    #             key=lambda x: x.get('transaction_time', 0),
    #             reverse=True
    #         )
    #         # 只返回最新的30條數據，並標記為命中緩存
    #         return sorted_data[:90], True
                    
    #     except Exception as e:
    #         logger.error(f"獲取 {chain} 鏈的緩存數據時發生錯誤: {e}")
    #         return None, False
    async def get_cached_data(self, chain: str) -> Optional[Tuple[List[Dict], bool]]:
        """獲取緩存的交易數據，總是返回緩存數據，不檢查過期"""
        try:
            # 處理全鏈請求
            if chain.lower() == 'all':
                cache_key = "transactions:all"
            else:
                cache_key = f"transactions:{chain.lower()}"
                
            data = await self.redis.get(cache_key)
            if not data:
                logger.warning(f"{chain} 鏈的緩存數據不存在")
                return None, False
            
            # 解析並排序數據
            cached_data = json.loads(data)
            sorted_data = sorted(
                cached_data,
                key=lambda x: x.get('transaction_time', 0),
                reverse=True
            )
            # 返回數據，並標記為命中緩存
            return sorted_data, True
                    
        except Exception as e:
            logger.error(f"獲取 {chain} 鏈的緩存數據時發生錯誤: {e}")
            return None, False
        
    async def generate_query_cache_key(self, chain, wallet_addresses=None, token_address=None, name=None, fetch_all=False, transaction_type=None, min_value=None):
        """生成查詢緩存鍵"""
        parts = [f"tx_query:{chain.lower()}"]
        
        if wallet_addresses:
            # 對列表排序以確保相同的內容產生相同的緩存鍵
            sorted_addresses = sorted(wallet_addresses)
            parts.append(f"wallets:{','.join(sorted_addresses)}")
        
        if token_address:
            parts.append(f"token:{token_address}")
        
        if name:
            parts.append(f"name:{name}")
        
        if fetch_all:
            parts.append("fetch_all:true")
        
        if transaction_type:
            parts.append(f"type:{transaction_type}")
        
        if min_value is not None:
            parts.append(f"min_value:{min_value}")
        
        return ":".join(parts)
    
    async def check_query_cache(self, cache_key):
        """檢查查詢緩存"""
        try:
            # 使用 self 來訪問 Redis 或其他緩存方法
            cached_data = await self.get_json(f"query_cache:{cache_key}")
            return cached_data
        except Exception as e:
            logger.error(f"檢查查詢緩存時出錯: {e}")
            return None
        
    async def update_query_cache(self, cache_key, data, expire_seconds=60):
        """更新查詢緩存 (默認1分鐘)"""
        try:
            # 使用 self 來訪問 Redis 或其他緩存方法
            await self.set_json(f"query_cache:{cache_key}", data, expire_seconds)
        except Exception as e:
            logger.error(f"更新查詢緩存時出錯: {e}")

    async def set(self, key: str, value: str, expire: int = None):
        if expire:
            await self.redis.set(key, value, ex=expire)
        else:
            await self.redis.set(key, value)  # 無 TTL

    async def get(self, key: str) -> Optional[str]:
        """獲取緩存"""
        return await self.redis.get(key)

    async def get_json(self, key: str) -> Optional[Dict]:
        """獲取 JSON 格式的緩存"""
        data = await self.get(key)
        return json.loads(data) if data else None

    async def set_json(self, key: str, value: Dict, expire: int = None):  # 默認無 TTL
        await self.set(key, json.dumps(value), expire)

    async def batch_get_json(self, keys: List[str]) -> Dict[str, Any]:
        """批量獲取多個緩存項目"""
        if not keys:
            return {}
            
        pipe = self.redis.pipeline()
        for key in keys:
            pipe.get(key)
        
        results = await pipe.execute()
        
        return {
            key: json.loads(result) if result else None
            for key, result in zip(keys, results)
        }

async def fetch_adaptive_top_tokens(
    session: AsyncSession, 
    chain: str,
    time_window: int = None,
    limit: int = 30,
    filtered_token_addresses: List[str] = None
) -> List[Dict[str, Any]]:
    """獲取特定時間窗口的熱門代幣，針對不同鏈適配 token_address 大小寫處理"""
    start_time = time.time()
    
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'        
        # 如果未提供過濾列表，則獲取
        if filtered_token_addresses is None:
            filtered_token_addresses = await cache_service.load_filtered_token_addresses()
        
        now = datetime.now(timezone.utc)
        now_timestamp = int(now.timestamp())
        
        # 判斷是否為 Solana 鏈
        is_solana = chain.upper() == "SOLANA"
        
        # 使用提供的時間窗口
        if time_window:
            time_ago_timestamp = now_timestamp - time_window
            
            # 根據不同鏈選擇不同的 SQL 查詢
            if is_solana:
                # Solana 鏈 - 保留原始大小寫
                token_stats_query = f"""
                WITH smart_wallets AS (
                    SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
                ),
                filtered_tokens AS (
                    SELECT UNNEST(ARRAY[{','.join(map(lambda x: f"'{x}'", filtered_token_addresses))}]) AS wallet_address
                ),
                token_aggregates AS (
                    SELECT 
                        t.token_address,
                        COUNT(DISTINCT t.wallet_address) as unique_wallets,
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN t.transaction_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
                        COUNT(t.id) as total_transactions,
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN t.value ELSE 0 END) as buy_value,
                        SUM(CASE WHEN t.transaction_type = 'sell' THEN t.value ELSE 0 END) as sell_value
                    FROM 
                        {schema}.wallet_transaction t
                    WHERE 
                        t.chain = '{chain}'
                        AND t.transaction_time >= {time_ago_timestamp}
                        AND t.transaction_time <= {now_timestamp}
                        AND t.wallet_address IN (SELECT wallet_address FROM smart_wallets)
                        AND NOT EXISTS (
                            SELECT 1 FROM filtered_tokens ft
                            WHERE ft.wallet_address = t.token_address
                        )
                    GROUP BY 
                        t.token_address
                    HAVING 
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) > 0
                ),
                latest_token_info AS (
                    SELECT DISTINCT ON (t.token_address)
                        t.token_address,
                        t.token_name,
                        COALESCE(t.token_icon, '') as token_icon,
                        t.price as latest_price,
                        t.marketcap as latest_marketcap,
                        t.transaction_time as latest_time
                    FROM 
                        {schema}.wallet_transaction t
                    WHERE 
                        t.token_address IN (SELECT token_address FROM token_aggregates)
                    ORDER BY 
                        t.token_address, t.transaction_time DESC
                )
                SELECT 
                    a.token_address,
                    COALESCE(i.token_name, 'Unknown Token') as token_name,
                    i.token_icon,
                    a.unique_wallets,
                    a.buy_count,
                    a.sell_count,
                    a.total_transactions,
                    a.buy_value,
                    a.sell_value,
                    i.latest_price,
                    i.latest_marketcap,
                    i.latest_time
                FROM 
                    token_aggregates a
                JOIN 
                    latest_token_info i ON a.token_address = i.token_address
                ORDER BY 
                    a.buy_count DESC
                LIMIT {limit}
                """
            else:
                # 非 Solana 鏈 - 統一轉為小寫
                token_stats_query = f"""
                WITH smart_wallets AS (
                    SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
                ),
                filtered_tokens AS (
                    SELECT UNNEST(ARRAY[{','.join(map(lambda x: f"'{x.lower()}'", filtered_token_addresses))}]) AS wallet_address
                ),
                token_aggregates AS (
                    SELECT 
                        LOWER(t.token_address) as token_address,
                        COUNT(DISTINCT t.wallet_address) as unique_wallets,
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
                        SUM(CASE WHEN t.transaction_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
                        COUNT(t.id) as total_transactions,
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN t.value ELSE 0 END) as buy_value,
                        SUM(CASE WHEN t.transaction_type = 'sell' THEN t.value ELSE 0 END) as sell_value
                    FROM 
                        {schema}.wallet_transaction t
                    WHERE 
                        t.chain = '{chain}'
                        AND t.transaction_time >= {time_ago_timestamp}
                        AND t.transaction_time <= {now_timestamp}
                        AND t.wallet_address IN (SELECT wallet_address FROM smart_wallets)
                        AND NOT EXISTS (
                            SELECT 1 FROM filtered_tokens ft
                            WHERE ft.wallet_address = LOWER(t.token_address)
                        )
                    GROUP BY 
                        LOWER(t.token_address)
                    HAVING 
                        SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) > 0
                ),
                latest_token_info AS (
                    SELECT DISTINCT ON (LOWER(t.token_address))
                        LOWER(t.token_address) as token_address,
                        t.token_name,
                        COALESCE(t.token_icon, '') as token_icon,
                        t.price as latest_price,
                        t.marketcap as latest_marketcap,
                        t.transaction_time as latest_time
                    FROM 
                        {schema}.wallet_transaction t
                    WHERE 
                        LOWER(t.token_address) IN (SELECT token_address FROM token_aggregates)
                    ORDER BY 
                        LOWER(t.token_address), t.transaction_time DESC
                )
                SELECT 
                    a.token_address,
                    COALESCE(i.token_name, 'Unknown Token') as token_name,
                    i.token_icon,
                    a.unique_wallets,
                    a.buy_count,
                    a.sell_count,
                    a.total_transactions,
                    a.buy_value,
                    a.sell_value,
                    i.latest_price,
                    i.latest_marketcap,
                    i.latest_time
                FROM 
                    token_aggregates a
                JOIN 
                    latest_token_info i ON a.token_address = i.token_address
                ORDER BY 
                    a.buy_count DESC
                LIMIT {limit}
                """
            
            result = await session.execute(text(token_stats_query))
            tokens = result.all()
            
            # 格式化結果
            time_window_hours = time_window // 3600  # 轉換為小時
            enriched_tokens = []
            
            for token in tokens:
                # 不再需要統一轉換大小寫，直接使用查詢結果
                token_address = token.token_address
                
                enriched_tokens.append({
                    "token_address": token_address,
                    "token_name": token.token_name,
                    "token_icon": token.token_icon,
                    "unique_wallets": token.unique_wallets,
                    "buy_count": token.buy_count,
                    "sell_count": token.sell_count,
                    "total_transactions": token.total_transactions,
                    "buy_value": format_price(token.buy_value),
                    "sell_value": format_price(token.sell_value),
                    "current_price": format_price(token.latest_price),
                    "marketcap": format_price(token.latest_marketcap),
                    "time_window_hours": str(time_window_hours),
                    "timestamp": now_timestamp,
                    "latest_tx_time": token.latest_time
                })
            
            execution_time = time.time() - start_time
            logging.info(f"{chain} {time_window_hours}小時查詢耗時: {execution_time:.2f}秒, 結果數: {len(enriched_tokens)}")
            
            return enriched_tokens
        
        # 缺少必要參數時返回空列表
        logging.warning("缺少必要的時間窗口參數")
        return []
    
    except Exception as e:
        execution_time = time.time() - start_time
        logging.error(f"查詢 {chain} 熱門代幣時出錯: {e}, 耗時: {execution_time:.2f}秒", exc_info=True)
        return []

async def fetch_historical_top_tokens(
    session: AsyncSession, 
    chain: str, 
    limit: int = 30
) -> List[Dict[str, Any]]:
    """
    获取历史上的热门代币（最后的备用方案），使用優化的單一SQL查詢
    """
    start_time = time.time()
    
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'        
        # 获取过滤代币列表
        filtered_token_addresses = await cache_service.load_filtered_token_addresses()
        
        # 判斷是否為 Solana 鏈
        is_solana = chain.upper() == "SOLANA"
        
        # 優化 SQL 查詢 - 針對 Solana 鏈和其他鏈使用不同的邏輯
        if is_solana:
            # Solana 鏈 - 不對 token_address 進行小寫轉換，保留原始大小寫
            historical_query = f"""
            WITH smart_wallets AS (
                SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
            ),
            filtered_tokens AS (
                SELECT UNNEST(ARRAY[{','.join(map(lambda x: f"'{x}'", filtered_token_addresses))}]) AS wallet_address
            ),
            token_aggregates AS (
                SELECT 
                    token_address,
                    COUNT(DISTINCT t.wallet_address) as unique_wallets,
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
                    SUM(CASE WHEN t.transaction_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
                    COUNT(t.id) as total_transactions,
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN t.value ELSE 0 END) as buy_value,
                    SUM(CASE WHEN t.transaction_type = 'sell' THEN t.value ELSE 0 END) as sell_value
                FROM 
                    {schema}.wallet_transaction t
                WHERE 
                    t.chain = '{chain}'
                    AND t.wallet_address IN (SELECT wallet_address FROM smart_wallets)
                    AND NOT EXISTS (
                        SELECT 1 FROM filtered_tokens ft
                        WHERE ft.wallet_address = t.token_address
                    )
                GROUP BY 
                    t.token_address
                HAVING 
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) > 0
            ),
            latest_token_info AS (
                SELECT DISTINCT ON (t.token_address)
                    t.token_address,
                    t.token_name,
                    COALESCE(t.token_icon, '') as token_icon,
                    t.price as latest_price,
                    t.marketcap as latest_marketcap,
                    t.transaction_time as latest_tx_time
                FROM 
                    {schema}.wallet_transaction t
                WHERE 
                    t.token_address IN (SELECT token_address FROM token_aggregates)
                ORDER BY 
                    t.token_address, t.transaction_time DESC
            )
            SELECT 
                a.token_address,
                COALESCE(i.token_name, 'Unknown Token') as token_name,
                i.token_icon,
                a.unique_wallets,
                a.buy_count,
                a.sell_count,
                a.total_transactions,
                a.buy_value,
                a.sell_value,
                i.latest_price,
                i.latest_marketcap,
                i.latest_tx_time
            FROM 
                token_aggregates a
            JOIN 
                latest_token_info i ON a.token_address = i.token_address
            ORDER BY 
                a.buy_count DESC
            LIMIT {limit}
            """
        else:
            # 非 Solana 鏈 - 對 token_address 進行小寫轉換
            historical_query = f"""
            WITH smart_wallets AS (
                SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
            ),
            filtered_tokens AS (
                SELECT UNNEST(ARRAY[{','.join(map(lambda x: f"'{x}'", filtered_token_addresses))}]) AS wallet_address
            ),
            token_aggregates AS (
                SELECT 
                    LOWER(token_address) as token_address,
                    COUNT(DISTINCT t.wallet_address) as unique_wallets,
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) as buy_count,
                    SUM(CASE WHEN t.transaction_type = 'sell' THEN 1 ELSE 0 END) as sell_count,
                    COUNT(t.id) as total_transactions,
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN t.value ELSE 0 END) as buy_value,
                    SUM(CASE WHEN t.transaction_type = 'sell' THEN t.value ELSE 0 END) as sell_value
                FROM 
                    {schema}.wallet_transaction t
                WHERE 
                    t.chain = '{chain}'
                    AND t.wallet_address IN (SELECT wallet_address FROM smart_wallets)
                    AND NOT EXISTS (
                        SELECT 1 FROM filtered_tokens ft
                        WHERE ft.wallet_address = LOWER(t.token_address)
                    )
                GROUP BY 
                    LOWER(t.token_address)
                HAVING 
                    SUM(CASE WHEN t.transaction_type = 'buy' THEN 1 ELSE 0 END) > 0
            ),
            latest_token_info AS (
                SELECT DISTINCT ON (LOWER(t.token_address))
                    LOWER(t.token_address) as token_address,
                    t.token_name,
                    COALESCE(t.token_icon, '') as token_icon,
                    t.price as latest_price,
                    t.marketcap as latest_marketcap,
                    t.transaction_time as latest_tx_time
                FROM 
                    {schema}.wallet_transaction t
                WHERE 
                    LOWER(t.token_address) IN (SELECT token_address FROM token_aggregates)
                ORDER BY 
                    LOWER(t.token_address), t.transaction_time DESC
            )
            SELECT 
                a.token_address,
                COALESCE(i.token_name, 'Unknown Token') as token_name,
                i.token_icon,
                a.unique_wallets,
                a.buy_count,
                a.sell_count,
                a.total_transactions,
                a.buy_value,
                a.sell_value,
                i.latest_price,
                i.latest_marketcap,
                i.latest_tx_time
            FROM 
                token_aggregates a
            JOIN 
                latest_token_info i ON a.token_address = i.token_address
            ORDER BY 
                a.buy_count DESC
            LIMIT {limit}
            """
        
        try:
            result = await session.execute(text(historical_query))
        except Exception as e:
            logging.error(f"查詢出錯: {e}")
            await session.rollback()  # 關鍵：回滾失敗的交易
            return []
        tokens = result.all()
        
        # 格式化结果
        now_timestamp = int(datetime.now(timezone.utc).timestamp())
        enriched_tokens = []
        
        for token in tokens:
            # 不再需要在這裡處理大小寫，因為已在 SQL 中處理
            enriched_tokens.append({
                "token_address": token.token_address,  # 直接使用查詢結果，不做大小寫轉換
                "token_name": token.token_name,
                "token_icon": token.token_icon,
                "unique_wallets": token.unique_wallets,
                "buy_count": token.buy_count,
                "sell_count": token.sell_count,
                "total_transactions": token.total_transactions,
                "buy_value": format_price(token.buy_value),
                "sell_value": format_price(token.sell_value),
                "current_price": format_price(token.latest_price),
                "marketcap": format_price(token.latest_marketcap),
                "time_window_hours": "all_time",  # 表示这是历史数据
                "timestamp": now_timestamp,
                "latest_tx_time": token.latest_tx_time
            })
        
        execution_time = time.time() - start_time
        logging.info(f"{chain} 历史查询耗时: {execution_time:.2f}秒, 结果数: {len(enriched_tokens)}")
        
        return enriched_tokens
    
    except Exception as e:
        execution_time = time.time() - start_time
        logging.error(f"查询 {chain} 历史热门代币时出错: {e}, 耗时: {execution_time:.2f}秒", exc_info=True)
        return []

# 創建緩存服務單例
cache_service = None
hybrid_cache_service = None

def init_cache_services():
    global cache_service, hybrid_cache_service
    if cache_service is None:
        cache_service = RedisCache()
    if hybrid_cache_service is None:
        hybrid_cache_service = HybridCache()
    return cache_service, hybrid_cache_service

# 為了向後兼容，仍然初始化但不啟動
init_cache_services()