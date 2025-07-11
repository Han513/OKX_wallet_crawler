import os
import re
import json
import traceback
import asyncio
import logging
from flask import Flask
from dotenv import load_dotenv
from typing import Dict, List, Optional, Set, Union
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, declarative_base
from sqlalchemy import Column, Integer, String, Float, DateTime, Boolean, Text, Index, BIGINT, select, update, text, and_, or_, func, distinct, case, delete
from sqlalchemy.dialects.postgresql import insert
from datetime import datetime, timedelta, timezone
from sqlalchemy.ext.declarative import as_declarative, declared_attr
from cache import RedisCache, generate_cache_key
from config import *
from loguru_logger import logger
from asyncio import gather
from decimal import Decimal, InvalidOperation, ROUND_DOWN
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.exc import IntegrityError

load_dotenv()
# 初始化資料庫
Base = declarative_base()
DATABASES = {
    "SOLANA": DATABASE_URI_SWAP_SOL,
    "ETH": DATABASE_URI_SWAP_ETH,
    "BASE": DATABASE_URI_SWAP_BASE,
    "BSC": DATABASE_URI_SWAP_BSC,
    "TRON": DATABASE_URI_SWAP_TRON,
}

logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)

# 为每条链初始化 engine 和 sessionmaker
engines = {
    chain: create_async_engine(
        db_uri, 
        echo=False, 
        future=True, 
        pool_size=10, 
        max_overflow=10, 
        pool_timeout=30, 
        pool_recycle=1800,
        pool_pre_ping=True
    )
    for chain, db_uri in DATABASES.items()
}

# 创建 sessionmaker 映射
sessions = {
    chain: sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    for chain, engine in engines.items()
}

TZ_UTC8 = timezone(timedelta(hours=8))

USE_TMP_TABLE = os.getenv("USE_TMP_TABLE", "false").lower() == "true"
logging.basicConfig(level=logging.INFO)
logging.info(f"[啟動] USE_TMP_TABLE={USE_TMP_TABLE}")
if USE_TMP_TABLE:
    logging.info("[啟動] ORM將寫入所有 *_tmp 臨時表")
else:
    logging.info("[啟動] ORM將寫入正式表")

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

def get_utc8_time():
    """获取 UTC+8 当前时间"""
    return datetime.now(TZ_UTC8).replace(tzinfo=None)

def make_naive_time(dt):
    """将时间转换为无时区的格式"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    # 添加一个动态 schema 属性
    __table_args__ = {}
    
    @classmethod
    def with_schema(cls, schema: str):
        cls.__table_args__ = {"schema": schema}
        for table in Base.metadata.tables.values():
            if table.name == cls.__tablename__:
                table.schema = schema
        return cls

# 基本錢包資訊表
class WalletSummary(Base):
    """
    整合的錢包數據表
    """
    __tablename__ = 'wallet_tmp' if USE_TMP_TABLE else 'wallet'
    logging.info(f"[啟動] WalletSummary.__tablename__ = {__tablename__}")
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, comment='ID')
    wallet_address = Column(String(100), nullable=False, unique=True, comment='錢包地址')
    balance = Column(Float, nullable=True, comment='錢包餘額')
    balance_usd = Column(Float, nullable=True, comment='錢包餘額 (USD)')
    chain = Column(String(50), nullable=False, comment='區塊鏈類型')
    chain_id = Column(Integer, nullable=False, comment='區塊鏈ID')
    tag = Column(String(50), nullable=True, comment='標籤')
    twitter_name = Column(String(50), nullable=True, comment='X名稱')
    twitter_username = Column(String(50), nullable=True, comment='X用戶名')
    is_smart_wallet = Column(Boolean, nullable=True, comment='是否為聰明錢包')
    wallet_type = Column(Integer, nullable=True, comment='0:一般聰明錢，1:pump聰明錢，2:moonshot聰明錢')
    asset_multiple = Column(Float, nullable=True, comment='資產翻倍數(到小數第1位)')
    token_list = Column(Text, nullable=True, comment='用户最近交易的三种代币信息')
    # token_list = Column(JSONB, nullable=True, comment='用户最近交易的三种代币信息 (格式: {"SOL": "icon_url", "USDC": "icon_url"})')

    # 交易數據
    avg_cost_30d = Column(Float, nullable=True, comment='30日平均成本')
    avg_cost_7d = Column(Float, nullable=True, comment='7日平均成本')
    avg_cost_1d = Column(Float, nullable=True, comment='1日平均成本')
    total_transaction_num_30d = Column(Integer, nullable=True, comment='30日總交易次數')
    total_transaction_num_7d = Column(Integer, nullable=True, comment='7日總交易次數')
    total_transaction_num_1d = Column(Integer, nullable=True, comment='1日總交易次數')
    buy_num_30d = Column(Integer, nullable=True, comment='30日買入次數')
    buy_num_7d = Column(Integer, nullable=True, comment='7日買入次數')
    buy_num_1d = Column(Integer, nullable=True, comment='1日買入次數')
    sell_num_30d = Column(Integer, nullable=True, comment='30日賣出次數')
    sell_num_7d = Column(Integer, nullable=True, comment='7日賣出次數')
    sell_num_1d = Column(Integer, nullable=True, comment='1日賣出次數')
    win_rate_30d = Column(Float, nullable=True, comment='30日勝率')
    win_rate_7d = Column(Float, nullable=True, comment='7日勝率')
    win_rate_1d = Column(Float, nullable=True, comment='1日勝率')

    # 盈虧數據
    pnl_30d = Column(Float, nullable=True, comment='30日盈虧')
    pnl_7d = Column(Float, nullable=True, comment='7日盈虧')
    pnl_1d = Column(Float, nullable=True, comment='1日盈虧')
    pnl_percentage_30d = Column(Float, nullable=True, comment='30日盈虧百分比')
    pnl_percentage_7d = Column(Float, nullable=True, comment='7日盈虧百分比')
    pnl_percentage_1d = Column(Float, nullable=True, comment='1日盈虧百分比')
    pnl_pic_30d = Column(String(512), nullable=True, comment='30日每日盈虧圖')
    pnl_pic_7d = Column(String(512), nullable=True, comment='7日每日盈虧圖')
    pnl_pic_1d = Column(String(512), nullable=True, comment='1日每日盈虧圖')
    unrealized_profit_30d = Column(Float, nullable=True, comment='30日未實現利潤')
    unrealized_profit_7d = Column(Float, nullable=True, comment='7日未實現利潤')
    unrealized_profit_1d = Column(Float, nullable=True, comment='1日未實現利潤')
    total_cost_30d = Column(Float, nullable=True, comment='30日總成本')
    total_cost_7d = Column(Float, nullable=True, comment='7日總成本')
    total_cost_1d = Column(Float, nullable=True, comment='1日總成本')
    avg_realized_profit_30d = Column(Float, nullable=True, comment='30日平均已實現利潤')
    avg_realized_profit_7d = Column(Float, nullable=True, comment='7日平均已實現利潤')
    avg_realized_profit_1d = Column(Float, nullable=True, comment='1日平均已實現利潤')

    # 收益分布數據
    distribution_gt500_30d = Column(Integer, nullable=True, comment='30日收益分布 >500% 的次數')
    distribution_200to500_30d = Column(Integer, nullable=True, comment='30日收益分布 200%-500% 的次數')
    distribution_0to200_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-200% 的次數')
    distribution_0to50_30d = Column(Integer, nullable=True, comment='30日收益分布 0%-50% 的次數')
    distribution_lt50_30d = Column(Integer, nullable=True, comment='30日收益分布 <50% 的次數')
    distribution_gt500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 >500% 的比例')
    distribution_200to500_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_30d = Column(Float, nullable=True, comment='30日收益分布 <50% 的比例')

    distribution_gt500_7d = Column(Integer, nullable=True, comment='7日收益分布 >500% 的次數')
    distribution_200to500_7d = Column(Integer, nullable=True, comment='7日收益分布 200%-500% 的次數')
    distribution_0to200_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-200% 的次數')
    distribution_0to50_7d = Column(Integer, nullable=True, comment='7日收益分布 0%-50% 的次數')
    distribution_lt50_7d = Column(Integer, nullable=True, comment='7日收益分布 <50% 的次數')
    distribution_gt500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 >500% 的比例')
    distribution_200to500_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 200%-500% 的比例')
    distribution_0to200_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-200% 的比例')
    distribution_0to50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 0%-50% 的比例')
    distribution_lt50_percentage_7d = Column(Float, nullable=True, comment='7日收益分布 <-50% 的比例')

    # 更新時間和最後交易時間
    update_time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    last_transaction_time = Column(Integer, nullable=True, comment='最後活躍時間')
    is_active = Column(Boolean, nullable=True, comment='是否還是聰明錢')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
class Transaction(Base):
    __tablename__ = 'wallet_transaction_tmp' if USE_TMP_TABLE else 'wallet_transaction'
    logging.info(f"[啟動] Transaction.__tablename__ = {__tablename__}")
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="聰明錢錢包地址")
    wallet_balance = Column(Float, nullable=False, comment="聰明錢錢包餘額(SOL+U)")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    token_icon = Column(Text, nullable=True, comment="代幣圖片網址")
    token_name = Column(String(100), nullable=True, comment="代幣名稱")
    price = Column(Float, nullable=True, comment="價格")
    amount = Column(Float, nullable=False, comment="數量")
    marketcap = Column(Float, nullable=True, comment="市值")
    value = Column(Float, nullable=True, comment="價值")
    holding_percentage = Column(Float, nullable=True, comment="倉位百分比")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    chain_id = Column(Integer, nullable=False, comment="區塊鏈ID")
    realized_profit = Column(Float, nullable=True, comment="已實現利潤")
    realized_profit_percentage = Column(Float, nullable=True, comment="已實現利潤百分比")
    transaction_type = Column(String(10), nullable=False, comment="事件 (buy, sell)")
    transaction_time = Column(BIGINT, nullable=False, comment="交易時間")
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')
    signature = Column(String(100), nullable=False, unique=True, comment="交易簽名")
    from_token_address = Column(String(100), nullable=False, comment="from token 地址")
    from_token_symbol = Column(String(100), nullable=True, comment="from token 名稱")
    from_token_amount = Column(Float, nullable=False, comment="from token 數量")
    dest_token_address = Column(String(100), nullable=False, comment="dest token 地址")
    dest_token_symbol = Column(String(100), nullable=True, comment="dest token 名稱")
    dest_token_amount = Column(Float, nullable=False, comment="dest token 數量")

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

class Holding(Base):
    __tablename__ = 'wallet_holding_tmp' if USE_TMP_TABLE else 'wallet_holding'
    logging.info(f"[啟動] Holding.__tablename__ = {__tablename__}")
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, autoincrement=True)
    wallet_address = Column(String(255), nullable=False)  # 添加长度限制
    token_address = Column(String(255), nullable=False)  # 添加长度限制
    token_icon = Column(String(255), nullable=True)  # 添加长度限制
    token_name = Column(String(255), nullable=True)  # 添加长度限制
    chain = Column(String(50), nullable=False, default='Unknown')  # 添加长度限制
    chain_id = Column(Integer, nullable=False, comment='區塊鏈ID')
    amount = Column(Float, nullable=False, default=0.0)
    value = Column(Float, nullable=False, default=0.0)
    value_usdt = Column(Float, nullable=False, default=0.0)
    unrealized_profits = Column(Float, nullable=False, default=0.0)
    pnl = Column(Float, nullable=False, default=0.0)
    pnl_percentage = Column(Float, nullable=False, default=0.0)
    avg_price = Column(Float, nullable=False, default=0.0)
    marketcap = Column(Float, nullable=False, default=0.0)
    is_cleared = Column(Boolean, nullable=False, default=False)
    cumulative_cost = Column(Float, nullable=False, default=0.0)
    cumulative_profit = Column(Float, nullable=False, default=0.0)
    last_transaction_time = Column(Integer, nullable=True, comment='最後活躍時間')
    time = Column(DateTime, nullable=False, default=get_utc8_time, comment='更新時間')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
    
# class TokenBuyData(Base):
#     __tablename__ = 'wallet_buy_data'

#     id = Column(Integer, primary_key=True)
#     wallet_address = Column(String(100), nullable=False, comment="錢包地址")
#     token_address = Column(String(100), nullable=False, comment="代幣地址")
#     total_amount = Column(Float, nullable=False, default=0.0, comment="代幣總數量")
#     total_cost = Column(Float, nullable=False, default=0.0, comment="代幣總成本")
#     avg_buy_price = Column(Float, nullable=False, default=0.0, comment="平均買入價格")
#     updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")

class TokenBuyData(Base):
    __tablename__ = 'wallet_buy_data_tmp' if USE_TMP_TABLE else 'wallet_buy_data'
    logging.info(f"[啟動] TokenBuyData.__tablename__ = {__tablename__}")
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    total_amount = Column(Float, nullable=False, default=0.0, comment="當前持有代幣數量")
    total_cost = Column(Float, nullable=False, default=0.0, comment="當前持倉總成本")
    avg_buy_price = Column(Float, nullable=False, default=0.0, comment="當前持倉平均買入價格")
    position_opened_at = Column(Integer, nullable=True, comment="當前倉位開始時間")
    chain = Column(String(50), nullable=False, comment="區塊鏈")
    chain_id = Column(Integer, nullable=False, comment="區塊鏈ID")
    historical_total_buy_amount = Column(Float, nullable=False, default=0.0, comment="歷史總買入數量")
    historical_total_buy_cost = Column(Float, nullable=False, default=0.0, comment="歷史總買入成本")
    historical_total_sell_amount = Column(Float, nullable=False, default=0.0, comment="歷史總賣出數量")
    historical_total_sell_value = Column(Float, nullable=False, default=0.0, comment="歷史總賣出價值")
    historical_avg_buy_price = Column(Float, nullable=False, default=0.0, comment="歷史平均買入價格")
    historical_avg_sell_price = Column(Float, nullable=False, default=0.0, comment="歷史平均賣出價格")
    last_active_position_closed_at = Column(Integer, nullable=True, comment="上一個活躍倉位關閉時間")
    last_transaction_time = Column(Integer, nullable=False, default=0.0, comment="最後活躍時間")
    date = Column(DateTime, nullable=False, comment="日期")
    realized_profit = Column(Float, default=0.0, comment="已實現利潤")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")
    
    # 索引優化查詢性能
    __table_args__ = (
        Index('idx_wallet_token', 'wallet_address', 'token_address', unique=True),
    )

class TokenState(Base):
    __tablename__ = 'wallet_token_state_tmp' if USE_TMP_TABLE else 'wallet_token_state'
    logging.info(f"[啟動] TokenState.__tablename__ = {__tablename__}")
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True)
    wallet_address = Column(String(100), nullable=False, comment="錢包地址")
    token_address = Column(String(100), nullable=False, comment="代幣地址")
    chain = Column(String(50), nullable=False, default='Unknown')
    chain_id = Column(Integer, nullable=False, comment='區塊鏈ID')
    current_amount = Column(Float, nullable=False, default=0.0, comment="當前持有代幣數量")
    current_total_cost = Column(Float, nullable=False, default=0.0, comment="當前持倉總成本")
    current_avg_buy_price = Column(Float, nullable=False, default=0.0, comment="當前持倉平均買入價格")
    position_opened_at = Column(Integer, nullable=True, comment="當前倉位開始時間")
    historical_buy_amount = Column(Float, nullable=False, default=0.0, comment="歷史總買入數量")
    historical_buy_cost = Column(Float, nullable=False, default=0.0, comment="歷史總買入成本")
    historical_sell_amount = Column(Float, nullable=False, default=0.0, comment="歷史總賣出數量")
    historical_sell_value = Column(Float, nullable=False, default=0.0, comment="歷史總賣出價值")
    historical_buy_count = Column(Integer, nullable=False, default=0.0, comment="歷史買入次數")
    historical_sell_count = Column(Integer, nullable=False, default=0.0, comment="歷史賣出次數")
    last_transaction_time = Column(Integer, nullable=False, default=0.0, comment="最後活躍時間")
    historical_realized_pnl = Column(Float, default=0.0, comment="已實現利潤")
    updated_at = Column(DateTime, nullable=False, default=get_utc8_time, comment="最後更新時間")
    
    # 索引優化查詢性能
    __table_args__ = (
        Index('idx_wallet_token', 'wallet_address', 'token_address', unique=True),
    )

class ErrorLog(Base):
    """
    錯誤訊息記錄表
    """
    __tablename__ = 'error_logs'
    __table_args__ = {'schema': 'dex_query_v1'}

    id = Column(Integer, primary_key=True, autoincrement=True, comment='ID')
    timestamp = Column(DateTime, nullable=False, default=get_utc8_time, comment='時間')
    module_name = Column(String(100), nullable=True, comment='檔案名稱')
    function_name = Column(String(100), nullable=True, comment='函數名稱')
    error_message = Column(Text, nullable=False, comment='錯誤訊息')

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}

# -------------------------------------------------------------------------------------------------------------------

async def debug_wallet_query(session_factory, chain):
    """調試智能錢包查詢問題"""
    async with session_factory() as session:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        
        # 直接使用原始SQL查詢
        raw_query = f"SELECT count(*) FROM {schema}.wallet WHERE is_smart_wallet = true"
        result = await session.execute(text(raw_query))
        count = result.scalar()
        # print(f"使用原始SQL查詢到 {count} 個智能錢包")
        
        if count > 0:
            # 查看幾個智能錢包的數據
            sample_query = f"""
            SELECT wallet_address, is_smart_wallet, chain 
            FROM {schema}.wallet 
            WHERE is_smart_wallet = true 
            LIMIT 5
            """
            result = await session.execute(text(sample_query))
        
        # 使用SQLAlchemy查詢
        WalletSummary.with_schema(schema)
        sqla_query = select(func.count()).select_from(WalletSummary).where(
            WalletSummary.is_smart_wallet == True
        )
        result = await session.execute(sqla_query)
        sqla_count = result.scalar()        
        
        return count

async def initialize_database():
    """
    初始化多个区块链的数据库，并为每个链创建对应的 Schema。
    """
    try:
        await alter_token_buy_data_tables()
        # 遍历 engines 字典，为每个区块链的数据库执行初始化
        for chain, engine in engines.items():
            schema_name = chain.lower()  # 使用链名作为 Schema 名称（全小写）
            async with engine.begin() as conn:
                # 创建 Schema（如果不存在）
                print(f"正在检查或创建 Schema: {schema_name}...")
                await conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))

                # 将所有表映射到对应的 Schema
                for table in Base.metadata.tables.values():
                    table.schema = schema_name  # 为每张表设置 Schema

                # 创建表
                print(f"正在初始化 {schema_name} Schema 中的表...")
                await conn.run_sync(Base.metadata.create_all)
                print(f"{schema_name} Schema 初始化完成。")
                
                # 执行数据迁移
                await migrate_token_buy_data(conn, schema_name)
    except Exception as e:
        logging.error(f"数据库初始化失败: {e}")
        raise

async def write_wallet_data_to_db(session, wallet_data, chain):
    """
    将钱包数据写入或更新 WalletSummary 表
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        WalletSummary.with_schema(schema)
        # 查詢是否已經存在相同的 wallet_address
        existing_wallet = await session.execute(
            select(WalletSummary).filter(WalletSummary.wallet_address == wallet_data["wallet_address"])
        )
        existing_wallet = existing_wallet.scalars().first()
        if existing_wallet:
            # 如果已經存在，就更新資料
            if wallet_data.get("balance") is not None:
                existing_wallet.balance = wallet_data.get("balance", 0)
            if wallet_data.get("balance_usd") is not None:
                existing_wallet.balance_usd = wallet_data.get("balance_usd", 0)
            if wallet_data.get("chain") is not None:
                existing_wallet.chain = wallet_data.get("chain", "Solana")
            if wallet_data.get("twitter_name") is not None:
                val = wallet_data.get("twitter_name")
                existing_wallet.twitter_name = str(val) if val is not None else None
            if wallet_data.get("twitter_username") is not None:
                val = wallet_data.get("twitter_username")
                existing_wallet.twitter_username = str(val) if val is not None else None
            if wallet_data.get("wallet_type") is not None:
                existing_wallet.wallet_type = wallet_data.get("wallet_type", 0)
            if wallet_data.get("asset_multiple") is not None:
                existing_wallet.asset_multiple = float(wallet_data.get("asset_multiple", 0) or 0)
            if wallet_data.get("token_list") is not None:
                existing_wallet.token_list = wallet_data.get("token_list", False)
            if wallet_data["stats_30d"].get("average_cost") is not None:
                existing_wallet.avg_cost_30d = wallet_data["stats_30d"].get("average_cost", 0)
            if wallet_data["stats_7d"].get("average_cost") is not None:
                existing_wallet.avg_cost_7d = wallet_data["stats_7d"].get("average_cost", 0)
            if wallet_data["stats_1d"].get("average_cost") is not None:
                existing_wallet.avg_cost_1d = wallet_data["stats_1d"].get("average_cost", 0)
            if wallet_data["stats_30d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_30d = wallet_data["stats_30d"].get("total_transaction_num", 0)
            if wallet_data["stats_7d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_7d = wallet_data["stats_7d"].get("total_transaction_num", 0)
            if wallet_data["stats_1d"].get("total_transaction_num") is not None:
                existing_wallet.total_transaction_num_1d = wallet_data["stats_1d"].get("total_transaction_num", 0)
            if wallet_data["stats_30d"].get("total_buy") is not None:
                existing_wallet.buy_num_30d = wallet_data["stats_30d"].get("total_buy", 0)
            if wallet_data["stats_7d"].get("total_buy") is not None:
                existing_wallet.buy_num_7d = wallet_data["stats_7d"].get("total_buy", 0)
            if wallet_data["stats_1d"].get("total_buy") is not None:
                existing_wallet.buy_num_1d = wallet_data["stats_1d"].get("total_buy", 0)
            if wallet_data["stats_30d"].get("total_sell") is not None:
                existing_wallet.sell_num_30d = wallet_data["stats_30d"].get("total_sell", 0)
            if wallet_data["stats_7d"].get("total_sell") is not None:
                existing_wallet.sell_num_7d = wallet_data["stats_7d"].get("total_sell", 0)
            if wallet_data["stats_1d"].get("total_sell") is not None:
                existing_wallet.sell_num_1d = wallet_data["stats_1d"].get("total_sell", 0)
            if wallet_data["stats_30d"].get("win_rate") is not None:
                existing_wallet.win_rate_30d = wallet_data["stats_30d"].get("win_rate", 0)
            if wallet_data["stats_7d"].get("win_rate") is not None:
                existing_wallet.win_rate_7d = wallet_data["stats_7d"].get("win_rate", 0)
            if wallet_data["stats_1d"].get("win_rate") is not None:
                existing_wallet.win_rate_1d = wallet_data["stats_1d"].get("win_rate", 0)
            if wallet_data["stats_30d"].get("pnl") is not None:
                existing_wallet.pnl_30d = wallet_data["stats_30d"].get("pnl", 0)
            if wallet_data["stats_7d"].get("pnl") is not None:
                existing_wallet.pnl_7d = wallet_data["stats_7d"].get("pnl", 0)
            if wallet_data["stats_1d"].get("pnl") is not None:
                existing_wallet.pnl_1d = wallet_data["stats_1d"].get("pnl", 0)
            if wallet_data["stats_30d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_30d = wallet_data["stats_30d"].get("pnl_percentage", 0)
            if wallet_data["stats_7d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_7d = wallet_data["stats_7d"].get("pnl_percentage", 0)
            if wallet_data["stats_1d"].get("pnl_percentage") is not None:
                existing_wallet.pnl_percentage_1d = wallet_data["stats_1d"].get("pnl_percentage", 0)
            if wallet_data["stats_30d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_30d = wallet_data["stats_30d"].get("daily_pnl_chart", "")
            if wallet_data["stats_7d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_7d = wallet_data["stats_7d"].get("daily_pnl_chart", "")
            if wallet_data["stats_1d"].get("daily_pnl_chart") is not None:
                existing_wallet.pnl_pic_1d = wallet_data["stats_1d"].get("daily_pnl_chart", "")
            if wallet_data["stats_30d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_30d = wallet_data["stats_30d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_7d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_7d = wallet_data["stats_7d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_1d"].get("total_unrealized_profit") is not None:
                existing_wallet.unrealized_profit_1d = wallet_data["stats_1d"].get("total_unrealized_profit", 0)
            if wallet_data["stats_30d"].get("total_cost") is not None:
                existing_wallet.total_cost_30d = wallet_data["stats_30d"].get("total_cost", 0)
            if wallet_data["stats_7d"].get("total_cost") is not None:
                existing_wallet.total_cost_7d = wallet_data["stats_7d"].get("total_cost", 0)
            if wallet_data["stats_1d"].get("total_cost") is not None:
                existing_wallet.total_cost_1d = wallet_data["stats_1d"].get("total_cost", 0)
            if wallet_data["stats_30d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_30d = wallet_data["stats_30d"].get("avg_realized_profit", 0)
            if wallet_data["stats_7d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_7d = wallet_data["stats_7d"].get("avg_realized_profit", 0)
            if wallet_data["stats_1d"].get("avg_realized_profit") is not None:
                existing_wallet.avg_realized_profit_1d = wallet_data["stats_1d"].get("avg_realized_profit", 0)
            if wallet_data["stats_30d"].get("distribution_gt500") is not None:
                existing_wallet.distribution_gt500_30d = wallet_data["stats_30d"].get("distribution_gt500", 0)
            if wallet_data["stats_30d"].get("distribution_200to500") is not None:
                existing_wallet.distribution_200to500_30d = wallet_data["stats_30d"].get("distribution_200to500", 0)
            if wallet_data["stats_30d"].get("distribution_0to200") is not None:
                existing_wallet.distribution_0to200_30d = wallet_data["stats_30d"].get("distribution_0to200", 0)
            if wallet_data["stats_30d"].get("distribution_0to50") is not None:
                existing_wallet.distribution_0to50_30d = wallet_data["stats_30d"].get("distribution_0to50", 0)
            if wallet_data["stats_30d"].get("distribution_lt50") is not None:
                existing_wallet.distribution_lt50_30d = wallet_data["stats_30d"].get("distribution_lt50", 0)
            if wallet_data["stats_30d"].get("distribution_gt500_percentage") is not None:
                existing_wallet.distribution_gt500_percentage_30d = wallet_data["stats_30d"].get("distribution_gt500_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_200to500_percentage") is not None:
                existing_wallet.distribution_200to500_percentage_30d = wallet_data["stats_30d"].get("distribution_200to500_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_0to200_percentage") is not None:
                existing_wallet.distribution_0to200_percentage_30d = wallet_data["stats_30d"].get("distribution_0to200_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_0to50_percentage") is not None:
                existing_wallet.distribution_0to50_percentage_30d = wallet_data["stats_30d"].get("distribution_0to50_percentage", 0.0)
            if wallet_data["stats_30d"].get("distribution_lt50_percentage") is not None:
                existing_wallet.distribution_lt50_percentage_30d = wallet_data["stats_30d"].get("distribution_lt50_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_gt500") is not None:
                existing_wallet.distribution_gt500_7d = wallet_data["stats_7d"].get("distribution_gt500", 0)
            if wallet_data["stats_7d"].get("distribution_200to500") is not None:
                existing_wallet.distribution_200to500_7d = wallet_data["stats_7d"].get("distribution_200to500", 0)
            if wallet_data["stats_7d"].get("distribution_0to200") is not None:
                existing_wallet.distribution_0to200_7d = wallet_data["stats_7d"].get("distribution_0to200", 0)
            if wallet_data["stats_7d"].get("distribution_0to50") is not None:
                existing_wallet.distribution_0to50_7d = wallet_data["stats_7d"].get("distribution_0to50", 0)
            if wallet_data["stats_7d"].get("distribution_lt50") is not None:
                existing_wallet.distribution_lt50_7d = wallet_data["stats_7d"].get("distribution_lt50", 0)
            if wallet_data["stats_7d"].get("distribution_gt500_percentage") is not None:
                existing_wallet.distribution_gt500_percentage_7d = wallet_data["stats_7d"].get("distribution_gt500_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_200to500_percentage") is not None:
                existing_wallet.distribution_200to500_percentage_7d = wallet_data["stats_7d"].get("distribution_200to500_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_0to200_percentage") is not None:
                existing_wallet.distribution_0to200_percentage_7d = wallet_data["stats_7d"].get("distribution_0to200_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_0to50_percentage") is not None:
                existing_wallet.distribution_0to50_percentage_7d = wallet_data["stats_7d"].get("distribution_0to50_percentage", 0.0)
            if wallet_data["stats_7d"].get("distribution_lt50_percentage") is not None:
                existing_wallet.distribution_lt50_percentage_7d = wallet_data["stats_7d"].get("distribution_lt50_percentage", 0.0)
            if wallet_data.get("last_transaction_time") is not None:
                existing_wallet.last_transaction_time = wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp()))

            existing_wallet.update_time = get_utc8_time()

            await session.commit()
            print(f"Successfully updated wallet: {wallet_data['wallet_address']}")
        else:
            # 如果不存在，就插入新資料
            wallet_summary = WalletSummary(
                wallet_address=wallet_data["wallet_address"],
                balance=wallet_data.get("balance", 0),
                balance_usd=wallet_data.get("balance_usd", 0),
                chain=wallet_data.get("chain", "Solana"),
                twitter_name=wallet_data.get("twitter_name", None),
                twitter_username=wallet_data.get("twitter_username", None),
                is_smart_wallet=wallet_data.get("is_smart_wallet", False),
                wallet_type=wallet_data.get("wallet_type", 0),
                asset_multiple=wallet_data.get("asset_multiple", 0),
                token_list=wallet_data.get("token_list", False),
                avg_cost_30d=wallet_data["stats_30d"].get("average_cost", 0),
                avg_cost_7d=wallet_data["stats_7d"].get("average_cost", 0),
                avg_cost_1d=wallet_data["stats_1d"].get("average_cost", 0),
                total_transaction_num_30d = wallet_data["stats_30d"].get("total_transaction_num", 0),
                total_transaction_num_7d = wallet_data["stats_7d"].get("total_transaction_num", 0),
                total_transaction_num_1d = wallet_data["stats_1d"].get("total_transaction_num", 0),
                buy_num_30d=wallet_data["stats_30d"].get("total_buy", 0),
                buy_num_7d=wallet_data["stats_7d"].get("total_buy", 0),
                buy_num_1d=wallet_data["stats_1d"].get("total_buy", 0),
                sell_num_30d=wallet_data["stats_30d"].get("total_sell", 0),
                sell_num_7d=wallet_data["stats_7d"].get("total_sell", 0),
                sell_num_1d=wallet_data["stats_1d"].get("total_sell", 0),
                win_rate_30d=wallet_data["stats_30d"].get("win_rate", 0),
                win_rate_7d=wallet_data["stats_7d"].get("win_rate", 0),
                win_rate_1d=wallet_data["stats_1d"].get("win_rate", 0),
                pnl_30d=wallet_data["stats_30d"].get("pnl", 0),
                pnl_7d=wallet_data["stats_7d"].get("pnl", 0),
                pnl_1d=wallet_data["stats_1d"].get("pnl", 0),
                pnl_percentage_30d=wallet_data["stats_30d"].get("pnl_percentage", 0),
                pnl_percentage_7d=wallet_data["stats_7d"].get("pnl_percentage", 0),
                pnl_percentage_1d=wallet_data["stats_1d"].get("pnl_percentage", 0),
                pnl_pic_30d=wallet_data["stats_30d"].get("daily_pnl_chart", ""),
                pnl_pic_7d=wallet_data["stats_7d"].get("daily_pnl_chart", ""),
                pnl_pic_1d=wallet_data["stats_1d"].get("daily_pnl_chart", ""),
                unrealized_profit_30d=wallet_data["stats_30d"].get("total_unrealized_profit", 0),
                unrealized_profit_7d=wallet_data["stats_7d"].get("total_unrealized_profit", 0),
                unrealized_profit_1d=wallet_data["stats_1d"].get("total_unrealized_profit", 0),
                total_cost_30d=wallet_data["stats_30d"].get("total_cost", 0),
                total_cost_7d=wallet_data["stats_7d"].get("total_cost", 0),
                total_cost_1d=wallet_data["stats_1d"].get("total_cost", 0),
                avg_realized_profit_30d=wallet_data["stats_30d"].get("avg_realized_profit", 0),
                avg_realized_profit_7d=wallet_data["stats_7d"].get("avg_realized_profit", 0),
                avg_realized_profit_1d=wallet_data["stats_1d"].get("avg_realized_profit", 0),
                distribution_gt500_30d=wallet_data["stats_30d"].get("distribution_gt500", 0),
                distribution_200to500_30d=wallet_data["stats_30d"].get("distribution_200to500", 0),
                distribution_0to200_30d=wallet_data["stats_30d"].get("distribution_0to200", 0),
                distribution_0to50_30d=wallet_data["stats_30d"].get("distribution_0to50", 0),
                distribution_lt50_30d=wallet_data["stats_30d"].get("distribution_lt50", 0),
                distribution_gt500_percentage_30d=wallet_data["stats_30d"].get("distribution_gt500_percentage", 0.0),
                distribution_200to500_percentage_30d=wallet_data["stats_30d"].get("distribution_200to500_percentage", 0.0),
                distribution_0to200_percentage_30d=wallet_data["stats_30d"].get("distribution_0to200_percentage", 0.0),
                distribution_0to50_percentage_30d=wallet_data["stats_30d"].get("distribution_0to50_percentage", 0.0),
                distribution_lt50_percentage_30d=wallet_data["stats_30d"].get("distribution_lt50_percentage", 0.0),
                distribution_gt500_7d=wallet_data["stats_7d"].get("distribution_gt500", 0),
                distribution_200to500_7d=wallet_data["stats_7d"].get("distribution_200to500", 0),
                distribution_0to200_7d=wallet_data["stats_7d"].get("distribution_0to200", 0),
                distribution_0to50_7d=wallet_data["stats_7d"].get("distribution_0to50", 0),
                distribution_lt50_7d=wallet_data["stats_7d"].get("distribution_lt50", 0),
                distribution_gt500_percentage_7d=wallet_data["stats_7d"].get("distribution_gt500_percentage", 0.0),
                distribution_200to500_percentage_7d=wallet_data["stats_7d"].get("distribution_200to500_percentage", 0.0),
                distribution_0to200_percentage_7d=wallet_data["stats_7d"].get("distribution_0to200_percentage", 0.0),
                distribution_0to50_percentage_7d=wallet_data["stats_7d"].get("distribution_0to50_percentage", 0.0),
                distribution_lt50_percentage_7d=wallet_data["stats_7d"].get("distribution_lt50_percentage", 0.0),
                update_time=get_utc8_time(),
                last_transaction_time=wallet_data.get("last_transaction_time", int(datetime.now(timezone.utc).timestamp())),
                is_active=True,
            )
            session.add(wallet_summary)
            await session.commit()
            print(f"Successfully added wallet: {wallet_data['wallet_address']}")
        return True
    except Exception as e:
        await session.rollback()
        print(e)
        await log_error(
            session,
            str(e),
            "models",
            "write_wallet_data_to_db",
            f"Failed to save wallet {wallet_data['wallet_address']}"
        )
        return False
    
async def get_wallets_address_by_chain(chain, session):
    """
    根據指定的鏈類型 (chain) 從 WalletSummary 資料表中查詢數據，
    並回傳符合條件的所有地址，且該地址的 is_smart_wallet 為 False。
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        WalletSummary.with_schema(schema)
        result = await session.execute(
            select(WalletSummary.wallet_address)
            .where(WalletSummary.chain == chain)
            .where(WalletSummary.is_smart_wallet == False)  # 加入 is_smart_wallet 為 False 的條件
        )
        addresses = result.scalars().all()  # 只取出 wallet_address 欄位的所有符合條件的地址
        return addresses
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "get_wallets_by_chain",
            f"查詢所有聰明錢列表失敗，原因 {e}"
        )
        return []
    
async def deactivate_wallets(session, addresses):
    """
    根據提供的地址列表，將 WalletSummary 中符合的錢包地址的 is_active 欄位設置為 False。
    
    :param session: 資料庫會話
    :param addresses: 一個包含要更新的地址的列表
    :return: 更新成功的錢包數量
    """
    try:
        # 更新符合條件的錢包地址的 is_active 為 False
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.wallet_address.in_(addresses))  # 篩選出符合的錢包地址
            .values(is_active=False)  # 將 is_active 設為 False
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "deactivate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0
    
async def activate_wallets(session, addresses):
    try:
        # 更新符合條件的錢包地址的 is_active 為 False
        result = await session.execute(
            update(WalletSummary)
            .where(WalletSummary.wallet_address.in_(addresses))
            .values(is_active=True)
        )
        await session.commit()  # 提交交易
        
        return result.rowcount  # 返回更新的行數，即更新成功的錢包數量
    except Exception as e:
        # 日誌記錄錯誤
        await log_error(
            session,
            str(e),
            "models",
            "activate_wallets",
            f"更新錢包 is_active 欄位為 False 失敗，原因 {e}"
        )
        return 0  # 若更新失敗，返回 0
    
async def get_active_wallets(session, chain):
        
        # schema = chain.lower()
        schema = 'dex_query_v1'
        WalletSummary.with_schema(schema)
        # async with self.async_session() as session:
        result = await session.execute(
            select(WalletSummary.wallet_address).where(WalletSummary.is_active == True)
        )
        return [row[0] for row in result]

async def get_smart_wallets(session, chain):
    """
    获取 WalletSummary 表中 is_smart_wallet 为 True 的钱包地址
    """
    # schema = chain.lower()
    schema = 'dex_query_v1'
    WalletSummary.with_schema(schema)
    result = await session.execute(
        select(WalletSummary.wallet_address).where(WalletSummary.is_smart_wallet == True)
    )
    return [row[0] for row in result]

async def get_active_or_smart_wallets(session, chain):
    """
    获取 WalletSummary 表中 is_active 或 is_smart_wallet 为 True 的钱包地址
    """
    # schema = chain.lower()
    schema = 'dex_query_v1'
    WalletSummary.with_schema(schema)
    result = await session.execute(
        select(WalletSummary.wallet_address).where(
            (WalletSummary.is_active == True) | (WalletSummary.is_smart_wallet == True)
        )
    )
    return [row[0] for row in result]

async def save_transaction(self, tx_data: dict, wallet_address: str, signature: str):
    """Save transaction record to the database"""
    async with self.async_session() as session:
        try:
            # 創建 Transaction 實例並填充所有字段
            transaction = Transaction(
                wallet_address=wallet_address,
                token_address=tx_data['token_address'],
                token_icon=tx_data.get('token_icon', ''),  # 如果缺少默認為空
                token_name=tx_data.get('token_name', ''),  # 如果缺少默認為空
                price=tx_data.get('price', 0),
                amount=tx_data.get('amount', 0),
                marketcap=tx_data.get('marketcap', 0),
                value=tx_data.get('value', 0),
                holding_percentage=tx_data.get('holding_percentage', 0),
                chain=tx_data.get('chain', 'Unknown'),
                realized_profit=tx_data.get('realized_profit', None),  
                realized_profit_percentage=tx_data.get('realized_profit_percentage', None),
                transaction_type=tx_data.get('transaction_type', 'unknown'),
                transaction_time=tx_data.get('transaction_time'),
                time=tx_data.get('time', get_utc8_time()),  # 當前時間作為備用
                signature=signature
            )

            # 將交易記錄添加到數據庫
            session.add(transaction)
            await session.commit()
            
            # 更新 WalletSummary 表中的最後交易時間
            await session.execute(
                update(WalletSummary)
                .where(WalletSummary.wallet_address == wallet_address)
                .values(
                    last_transaction_time=tx_data['transaction_time']  # 正確傳遞字段值
                )
            )
            await session.commit()
            
        except Exception as e:
            await session.rollback()
            await log_error(
            session,
            str(e),
            "models",
            "save_transaction",
            f"Failed to save transaction {wallet_address}"
        )

def remove_emoji(text):
    emoji_pattern = re.compile(
        "["
        "\U0001F600-\U0001F64F"  # 表情符号
        "\U0001F300-\U0001F5FF"  # 符号和图片字符
        "\U0001F680-\U0001F6FF"  # 运输和地图符号
        "\U0001F1E0-\U0001F1FF"  # 国旗
        "]+",
        flags=re.UNICODE,
    )
    return emoji_pattern.sub(r"", text)

def convert_to_decimal(value: Union[float, str, None]) -> Union[Decimal, None]:
    """
    將數值轉換為 Decimal 類型，確保不會出現科學符號
    
    Args:
        value: 要轉換的數值，可以是浮點數、字符串或 None
        
    Returns:
        轉換後的 Decimal 數值，如果輸入為 None 則返回 None
    """
    if value is None:
        return None
    try:
        # 如果是字符串且包含科學符號，先轉換為浮點數
        if isinstance(value, str) and ('e' in value.lower() or 'E' in value):
            value = float(value)
        # 轉換為 Decimal 並使用字符串形式以保持精確度
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation):
        return Decimal('0')

# async def save_past_transaction(async_session: AsyncSession, tx_data: dict, wallet_address: str, signature: str, chain: str):
#     """Save or update transaction record to the database for different blockchain schemas"""
#     try:
#         schema = chain.lower()
#         Transaction.with_schema(schema)
#         WalletSummary.with_schema(schema)

#         # 檢查交易是否存在
#         existing_transaction = await async_session.execute(
#             select(Transaction).where(Transaction.signature == signature)
#         )
#         transaction = existing_transaction.scalar()

#         tx_data["token_name"] = remove_emoji(tx_data.get('token_name', ''))
#         tx_data["transaction_time"] = make_naive_time(tx_data.get("transaction_time"))
#         tx_data["time"] = make_naive_time(tx_data.get("time", get_utc8_time()))
#         tx_data["price"] = convert_to_decimal(tx_data.get('price', 0))
#         tx_data["amount"] = convert_to_decimal(tx_data.get('amount', 0))
#         tx_data["marketcap"] = convert_to_decimal(tx_data.get('marketcap', 0))
#         tx_data["value"] = convert_to_decimal(tx_data.get('value', 0))
#         tx_data["holding_percentage"] = convert_to_decimal(tx_data.get('holding_percentage', 0))
#         tx_data["realized_profit"] = convert_to_decimal(tx_data.get('realized_profit'))
#         tx_data["realized_profit_percentage"] = convert_to_decimal(tx_data.get('realized_profit_percentage'))

#         if transaction:
#             # 更新現有記錄
#             await async_session.execute(
#                 update(Transaction)
#                 .where(Transaction.signature == signature)
#                 .values(
#                     wallet_address=wallet_address,
#                     wallet_balance=tx_data.get('wallet_balance', ''),
#                     token_address=tx_data['token_address'],
#                     token_icon=tx_data.get('token_icon', ''),
#                     token_name=tx_data.get('token_name', ''),
#                     price=tx_data['price'],
#                     amount=tx_data['amount'],
#                     marketcap=tx_data['marketcap'],
#                     value=tx_data['value'],
#                     holding_percentage=tx_data['holding_percentage'],
#                     chain=tx_data.get('chain', 'Unknown'),
#                     realized_profit=tx_data['realized_profit'],
#                     realized_profit_percentage=tx_data['realized_profit_percentage'],
#                     transaction_type=tx_data.get('transaction_type', 'unknown'),
#                     transaction_time=tx_data['transaction_time'],
#                     time=tx_data.get("time", datetime.utcnow()),
#                 )
#             )
#         else:
#             # 插入新交易
#             new_transaction = Transaction(
#                 wallet_address=wallet_address,
#                 wallet_balance=tx_data.get('wallet_balance', ''),
#                 token_address=tx_data['token_address'],
#                 token_icon=tx_data.get('token_icon', ''),
#                 token_name=tx_data.get('token_name', ''),
#                 price=tx_data['price'],
#                 amount=tx_data['amount'],
#                 marketcap=tx_data['marketcap'],
#                 value=tx_data['value'],
#                 holding_percentage=tx_data['holding_percentage'],
#                 chain=tx_data.get('chain', 'Unknown'),
#                 realized_profit=tx_data['realized_profit'],
#                 realized_profit_percentage=tx_data['realized_profit_percentage'],
#                 transaction_type=tx_data.get('transaction_type', 'unknown'),
#                 transaction_time=tx_data['transaction_time'],
#                 time=tx_data.get("time", datetime.utcnow()),
#                 signature=signature
#             )
#             async_session.add(new_transaction)

#         await async_session.commit()

#     except IntegrityError as e:
#         print(f"存储交易记录失败: {e}")
#         await async_session.rollback()
#     except Exception as e:
#         print(f"未知错误: {e}")
#         await async_session.rollback()

async def save_past_transaction(async_session: AsyncSession, tx_data: dict, wallet_address: str, signature: str, chain: str):
    """Save or update transaction record to the database for different blockchain schemas"""
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)

        # 檢查交易是否存在
        existing_transaction = await async_session.execute(
            select(Transaction).where(Transaction.signature == signature)
        )
        transaction = existing_transaction.scalar()

        tx_data["token_name"] = remove_emoji(tx_data.get('token_name', ''))
        tx_data["transaction_time"] = make_naive_time(tx_data.get("transaction_time"))
        tx_data["time"] = make_naive_time(tx_data.get("time", get_utc8_time()))
        tx_data["price"] = convert_to_decimal(tx_data.get('price', 0))
        tx_data["amount"] = convert_to_decimal(tx_data.get('amount', 0))
        tx_data["marketcap"] = convert_to_decimal(tx_data.get('marketcap', 0))
        tx_data["value"] = convert_to_decimal(tx_data.get('value', 0))
        tx_data["holding_percentage"] = convert_to_decimal(tx_data.get('holding_percentage', 0))
        tx_data["realized_profit"] = convert_to_decimal(tx_data.get('realized_profit'))
        tx_data["realized_profit_percentage"] = convert_to_decimal(tx_data.get('realized_profit_percentage'))
        
        # 處理新增的六個欄位
        tx_data["from_token_amount"] = convert_to_decimal(tx_data.get('from_token_amount', 0))
        tx_data["dest_token_amount"] = convert_to_decimal(tx_data.get('dest_token_amount', 0))

        if transaction:
            # 更新現有記錄
            await async_session.execute(
                update(Transaction)
                .where(Transaction.signature == signature)
                .values(
                    wallet_address=wallet_address,
                    wallet_balance=tx_data.get('wallet_balance', ''),
                    token_address=tx_data['token_address'],
                    token_icon=tx_data.get('token_icon', ''),
                    token_name=tx_data.get('token_name', ''),
                    price=tx_data['price'],
                    amount=tx_data['amount'],
                    marketcap=tx_data['marketcap'],
                    value=tx_data['value'],
                    holding_percentage=tx_data['holding_percentage'],
                    chain=tx_data.get('chain', 'Unknown'),
                    realized_profit=tx_data['realized_profit'],
                    realized_profit_percentage=tx_data['realized_profit_percentage'],
                    transaction_type=tx_data.get('transaction_type', 'unknown'),
                    transaction_time=tx_data['transaction_time'],
                    time=tx_data.get("time", datetime.utcnow()),
                    from_token_address=tx_data.get('from_token_address', ''),
                    from_token_symbol=tx_data.get('from_token_symbol', ''),
                    from_token_amount=tx_data['from_token_amount'],
                    dest_token_address=tx_data.get('dest_token_address', ''),
                    dest_token_symbol=tx_data.get('dest_token_symbol', ''),
                    dest_token_amount=tx_data['dest_token_amount']
                )
            )
        else:
            # 插入新交易
            new_transaction = Transaction(
                wallet_address=wallet_address,
                wallet_balance=tx_data.get('wallet_balance', ''),
                token_address=tx_data['token_address'],
                token_icon=tx_data.get('token_icon', ''),
                token_name=tx_data.get('token_name', ''),
                price=tx_data['price'],
                amount=tx_data['amount'],
                marketcap=tx_data['marketcap'],
                value=tx_data['value'],
                holding_percentage=tx_data['holding_percentage'],
                chain=tx_data.get('chain', 'Unknown'),
                realized_profit=tx_data['realized_profit'],
                realized_profit_percentage=tx_data['realized_profit_percentage'],
                transaction_type=tx_data.get('transaction_type', 'unknown'),
                transaction_time=tx_data['transaction_time'],
                time=tx_data.get("time", datetime.utcnow()),
                signature=signature,
                # 新增六個欄位
                from_token_address=tx_data.get('from_token_address', ''),
                from_token_symbol=tx_data.get('from_token_symbol', ''),
                from_token_amount=tx_data['from_token_amount'],
                dest_token_address=tx_data.get('dest_token_address', ''),
                dest_token_symbol=tx_data.get('dest_token_symbol', ''),
                dest_token_amount=tx_data['dest_token_amount']
            )
            async_session.add(new_transaction)
            
        await async_session.execute(
            update(WalletSummary)
            .where(WalletSummary.wallet_address == wallet_address)
            .values(
                last_transaction_time=tx_data['transaction_time']
            )
        )

        await async_session.commit()

    except IntegrityError as e:
        print(f"存储交易记录失败: {e}")
        await async_session.rollback()
    except Exception as e:
        print(f"未知错误: {e}")
        await async_session.rollback()

# async def save_holding(tx_data: dict, wallet_address: str, session: AsyncSession, chain):
#     """Save transaction record to the database"""
#     try:
#         schema = chain.lower()
#         Holding.with_schema(schema)

#         # 查询是否已存在相同 wallet_address 和 token_address 的记录
#         existing_holding = await session.execute(
#             select(Holding).filter(
#                 Holding.wallet_address == wallet_address,
#                 Holding.token_address == tx_data.get('token_address', '')
#             )
#         )
#         existing_holding = existing_holding.scalars().first()

#         holding_data = {
#             "wallet_address": wallet_address,
#             "token_address": tx_data.get('token_address', ''),
#             "token_icon": tx_data.get('token_icon', ''),
#             "token_name": tx_data.get('token_name', ''),
#             "chain": tx_data.get('chain', 'Unknown'),
#             "amount": tx_data.get('amount', 0),
#             "value": tx_data.get('value', 0),
#             "value_usdt": tx_data.get('value_usdt', 0),
#             "unrealized_profits": tx_data.get('unrealized_profit', 0),
#             "pnl": tx_data.get('pnl', 0),
#             "pnl_percentage": tx_data.get('pnl_percentage', 0),
#             "avg_price": tx_data.get('avg_price', 0),
#             "marketcap": tx_data.get('marketcap', 0),
#             "is_cleared": tx_data.get('sell_amount', 0) >= tx_data.get('buy_amount', 0),
#             "cumulative_cost": tx_data.get('cost', 0),
#             "cumulative_profit": tx_data.get('profit', 0),
#             "last_transaction_time": make_naive_time(tx_data.get('last_transaction_time', datetime.now())),
#             "time": make_naive_time(tx_data.get('time', datetime.now())),
#         }

#         if existing_holding:
#             for key, value in holding_data.items():
#                 setattr(existing_holding, key, value)
#             await session.commit()
#         else:
#             holding = Holding(**holding_data)
#             session.add(holding)
#             await session.commit()

#     except Exception as e:
#         await session.rollback()
#         print(f"Error while saving holding for wallet {wallet_address}, token {tx_data.get('token_name', '')}: {str(e)}")
#         await log_error(
#             session,
#             str(e),
#             "models",
#             "save_holding",
#             f"Failed to save holding for wallet {wallet_address}, token {tx_data.get('token_name', '')}"
#         )

async def save_holding(tx_data_list: list, wallet_address: str, session: AsyncSession, chain: str):
    """Save transaction record to the database, and delete tokens no longer held in bulk"""
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Holding.with_schema(schema)

        # 查询数据库中钱包的所有持仓
        existing_holdings = await session.execute(
            select(Holding).filter(Holding.wallet_address == wallet_address)
        )
        existing_holdings = existing_holdings.scalars().all()

        # 提取数据库中现有的 token_address 集合
        existing_token_addresses = {holding.token_address for holding in existing_holdings}

        # 提取 tx_data_list 中当前持有的 token_address 集合
        current_token_addresses = {token.get("token_address") for token in tx_data_list}

        # 计算需要删除的 tokens
        tokens_to_delete = existing_token_addresses - current_token_addresses

        # 删除不再持有的代币记录
        if tokens_to_delete:
            await session.execute(
                delete(Holding).filter(
                    Holding.wallet_address == wallet_address,
                    Holding.token_address.in_(tokens_to_delete)
                )
            )

        # 更新或新增持仓
        for token_data in tx_data_list:
            token_address = token_data.get("token_address")
            if not token_address:
                print(f"Invalid token data: {token_data}")
                continue

            existing_holding = next((h for h in existing_holdings if h.token_address == token_address), None)

            holding_data = {
                "wallet_address": wallet_address,
                "token_address": token_address,
                "token_icon": token_data.get('token_icon', ''),
                "token_name": token_data.get('token_name', ''),
                "chain": token_data.get('chain', 'Unknown'),
                "amount": token_data.get('amount', 0),
                "value": token_data.get('value', 0),
                "value_usdt": token_data.get('value_usdt', 0),
                "unrealized_profits": token_data.get('unrealized_profit', 0),
                "pnl": token_data.get('pnl', 0),
                "pnl_percentage": token_data.get('pnl_percentage', 0),
                "avg_price": convert_to_decimal(token_data.get('avg_price', 0)),
                "marketcap": token_data.get('marketcap', 0),
                "is_cleared": token_data.get('sell_amount', 0) >= token_data.get('buy_amount', 0),
                "cumulative_cost": token_data.get('cost', 0),
                "cumulative_profit": token_data.get('profit', 0),
                "last_transaction_time": make_naive_time(token_data.get('last_transaction_time', datetime.now())),
                "time": make_naive_time(token_data.get('time', datetime.now())),
            }

            if existing_holding:
                # 更新现有记录
                for key, value in holding_data.items():
                    setattr(existing_holding, key, value)
            else:
                # 新增记录
                holding = Holding(**holding_data)
                session.add(holding)

        # 提交数据库变更
        await session.commit()

    except Exception as e:
        # 错误处理
        await session.rollback()
        print(f"Error while saving holding for wallet {wallet_address}: {str(e)}")

async def save_holding2(tx_data_list: list, wallet_address: str, session: AsyncSession, chain: str):
    """Save transaction record to the database, and delete tokens no longer held in bulk"""
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Holding.with_schema(schema)
        # print(f"開始保存錢包 {wallet_address} 的持倉數據，共 {len(tx_data_list)} 筆")
        # 查询数据库中钱包的所有持仓
        existing_holdings = await session.execute(
            select(Holding).filter(Holding.wallet_address == wallet_address)
        )
        existing_holdings = existing_holdings.scalars().all()
        # print(f"現有持倉記錄數: {len(existing_holdings)}")

        # 提取数据库中现有的 token_address 集合
        existing_token_addresses = {holding.token_address for holding in existing_holdings}

        # 提取 tx_data_list 中当前持有的 token_address 集合
        current_token_addresses = {token.get("token_address") for token in tx_data_list}

        # 计算需要删除的 tokens
        tokens_to_delete = existing_token_addresses - current_token_addresses

        # 删除不再持有的代币记录
        if tokens_to_delete:
            # print(f"刪除不再持有的代幣: {tokens_to_delete}")
            await session.execute(
                delete(Holding).filter(
                    Holding.wallet_address == wallet_address,
                    Holding.token_address.in_(tokens_to_delete)
                )
            )
        updated_count = 0
        added_count = 0
        # 更新或新增持仓
        for token_data in tx_data_list:
            token_address = token_data.get("token_address")
            if not token_address:
                print(f"Invalid token data: {token_data}")
                continue

            existing_holding = next((h for h in existing_holdings if h.token_address == token_address), None)

            holding_data = {
                "wallet_address": wallet_address,
                "token_address": token_address,
                "token_icon": token_data.get('token_icon', ''),
                "token_name": token_data.get('token_name', ''),
                "chain": token_data.get('chain', 'Unknown'),
                "amount": token_data.get('amount', 0),
                "value": token_data.get('value', 0),
                "value_usdt": token_data.get('value_usdt', 0),
                "unrealized_profits": token_data.get('unrealized_profit', 0),
                "pnl": token_data.get('pnl', 0),
                "pnl_percentage": token_data.get('pnl_percentage', 0),
                "avg_price": convert_to_decimal(token_data.get('avg_price', 0)),
                "marketcap": token_data.get('marketcap', 0),
                "is_cleared": token_data.get('sell_amount', 0) >= token_data.get('buy_amount', 0),
                "cumulative_cost": token_data.get('cost', 0),
                "cumulative_profit": token_data.get('profit', 0),
                "last_transaction_time": make_naive_time(token_data.get('last_transaction_time', datetime.now())),
                "time": make_naive_time(token_data.get('time', datetime.now())),
            }

            if existing_holding:
                # 更新现有记录
                for key, value in holding_data.items():
                    setattr(existing_holding, key, value)
                updated_count += 1
            else:
                # 新增记录
                holding = Holding(**holding_data)
                session.add(holding)
                added_count += 1

        # 提交数据库变更
        await session.commit()
        # print(f"錢包 {wallet_address} 持倉保存完成：更新 {updated_count} 筆，新增 {added_count} 筆")
        return True

    except Exception as e:
        # 错误处理
        await session.rollback()
        print(f"保存持倉數據時出錯，錢包 {wallet_address}: {str(e)}")
        print(traceback.format_exc())  # 打印完整錯誤堆疊
        return False

async def get_wallet_token_holdings(wallet_address, session, chain):
    """
    獲取錢包中所有有持倉的代幣記錄
    
    Args:
        wallet_address (str): 錢包地址
        session (AsyncSession): 資料庫會話
        chain (str): 區塊鏈名稱
        
    Returns:
        list: TokenBuyData記錄列表
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        
        # 查詢此錢包所有代幣的TokenBuyData記錄
        token_buy_data_query = select(TokenBuyData).filter(
            TokenBuyData.wallet_address == wallet_address,
            TokenBuyData.total_amount > 0  # 只獲取仍有持倉的代幣
        )
        result = await session.execute(token_buy_data_query)
        token_buy_data_records = result.scalars().all()
        
        # print(f"錢包 {wallet_address} 找到 {len(token_buy_data_records)} 個有持倉的代幣")
        
        return token_buy_data_records
    
    except Exception as e:
        print(f"查詢錢包 {wallet_address} 的代幣持倉記錄時出錯: {e}")
        import traceback
        traceback.print_exc()
        return []

async def clear_all_holdings(wallet_address: str, session: AsyncSession, chain: str):
    """清除錢包的所有持倉記錄"""
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Holding.with_schema(schema)

        await session.execute(
            delete(Holding).filter(Holding.wallet_address == wallet_address)
        )
        await session.commit()
        # print(f"Cleared all holdings for wallet {wallet_address}.")
    except Exception as e:
        await session.rollback()
        print(f"Error while clearing holdings for wallet {wallet_address}: {e}")

async def save_wallet_buy_data(tx_data: dict, wallet_address: str, session: AsyncSession, chain):
    """
    優化後的保存或更新 TokenBuyData 表的持倉和歷史數據函數
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        
        # 使用單個查詢來獲取或創建記錄
        stmt = select(TokenBuyData).filter(
            TokenBuyData.wallet_address == wallet_address,
            TokenBuyData.token_address == tx_data.get('token_address', '')
        )
        
        # 使用 with_for_update 來避免並發問題
        result = await session.execute(stmt.with_for_update())
        existing_holding = result.scalars().first()

        # 準備所有數據
        current_time = get_utc8_time()  # 只調用一次時間函數
        
        # 當前持倉數據
        total_amount = tx_data.get('total_amount', 0.0)
        total_cost = tx_data.get('total_cost', 0.0)
        avg_buy_price = tx_data.get('avg_buy_price', 0.0)
        position_opened_at = tx_data.get('position_opened_at')
        
        # 歷史累計數據
        historical_total_buy_amount = tx_data.get('historical_total_buy_amount', 0.0) 
        historical_total_buy_cost = tx_data.get('historical_total_buy_cost', 0.0)
        historical_total_sell_amount = tx_data.get('historical_total_sell_amount', 0.0)
        historical_total_sell_value = tx_data.get('historical_total_sell_value', 0.0)
        historical_avg_buy_price = tx_data.get('historical_avg_buy_price', 0.0)
        historical_avg_sell_price = tx_data.get('historical_avg_sell_price', 0.0)
        last_active_position_closed_at = tx_data.get('last_active_position_closed_at')
        last_transaction_time = tx_data.get('last_transaction_time')
        realized_profit = tx_data.get('realized_profit')

        # 處理時間字段的數據類型轉換
        def convert_timestamp_field(value):
            """安全地轉換時間戳字段"""
            if value is None:
                return None
            elif isinstance(value, (int, float)):
                # 如果是時間戳，轉換為 datetime
                return datetime.fromtimestamp(value)
            elif isinstance(value, datetime):
                # 如果已經是 datetime，直接返回
                return value
            else:
                # 其他情況，嘗試轉換
                try:
                    return datetime.fromtimestamp(float(value))
                except (ValueError, TypeError):
                    return None

        # 轉換時間字段 - 但對於 position_opened_at 和 last_active_position_closed_at，我們需要特殊處理
        # 因為數據庫中這些字段可能是 bigint 類型
        if position_opened_at is not None:
            if isinstance(position_opened_at, (int, float)):
                # 如果是時間戳，保持為整數
                position_opened_at = int(position_opened_at)
            elif isinstance(position_opened_at, datetime):
                # 如果是 datetime，轉換為時間戳
                position_opened_at = int(position_opened_at.timestamp())
            else:
                try:
                    position_opened_at = int(float(position_opened_at))
                except (ValueError, TypeError):
                    position_opened_at = None
        else:
            position_opened_at = None

        if last_active_position_closed_at is not None:
            if isinstance(last_active_position_closed_at, (int, float)):
                # 如果是時間戳，保持為整數
                last_active_position_closed_at = int(last_active_position_closed_at)
            elif isinstance(last_active_position_closed_at, datetime):
                # 如果是 datetime，轉換為時間戳
                last_active_position_closed_at = int(last_active_position_closed_at.timestamp())
            else:
                try:
                    last_active_position_closed_at = int(float(last_active_position_closed_at))
                except (ValueError, TypeError):
                    last_active_position_closed_at = None
        else:
            last_active_position_closed_at = None

        if existing_holding:
            # 更新現有記錄
            update_values = {
                # 當前持倉數據
                'total_amount': total_amount,
                'total_cost': total_cost,
                'avg_buy_price': avg_buy_price,
                'updated_at': current_time
            }
            
            # 只有當提供了有效值時才更新開倉和關倉時間
            if position_opened_at is not None:
                update_values['position_opened_at'] = position_opened_at
            
            # 確保歷史數據不會被小於現有值的數據覆蓋
            if historical_total_buy_amount >= existing_holding.historical_total_buy_amount:
                update_values['historical_total_buy_amount'] = historical_total_buy_amount
                
            if historical_total_buy_cost >= existing_holding.historical_total_buy_cost:
                update_values['historical_total_buy_cost'] = historical_total_buy_cost
                
            if historical_total_sell_amount >= existing_holding.historical_total_sell_amount:
                update_values['historical_total_sell_amount'] = historical_total_sell_amount
                
            if historical_total_sell_value >= existing_holding.historical_total_sell_value:
                update_values['historical_total_sell_value'] = historical_total_sell_value
                
            # 更新歷史平均買入價格 - 只在有意義時更新
            if historical_avg_buy_price > 0:
                update_values['historical_avg_buy_price'] = historical_avg_buy_price
                
            if last_active_position_closed_at is not None:
                update_values['last_active_position_closed_at'] = last_active_position_closed_at

            if last_transaction_time is not None:
                update_values['last_transaction_time'] = last_transaction_time

            if realized_profit is not None:
                update_values['realized_profit'] = realized_profit
            
            await session.execute(
                update(TokenBuyData)
                .where(
                    TokenBuyData.wallet_address == wallet_address,
                    TokenBuyData.token_address == tx_data.get('token_address', '')
                )
                .values(**update_values)
            )
        else:
            # 創建新記錄
            new_holding = TokenBuyData(
                wallet_address=wallet_address,
                token_address=tx_data.get('token_address', ''),
                # 當前持倉數據
                total_amount=total_amount,
                total_cost=total_cost,
                avg_buy_price=avg_buy_price,
                position_opened_at=position_opened_at,
                # 歷史累計數據
                historical_total_buy_amount=historical_total_buy_amount,
                historical_total_buy_cost=historical_total_buy_cost,
                historical_total_sell_amount=historical_total_sell_amount,
                historical_total_sell_value=historical_total_sell_value,
                historical_avg_buy_price=historical_avg_buy_price,
                historical_avg_sell_price=historical_avg_sell_price,
                last_active_position_closed_at=last_active_position_closed_at,
                last_transaction_time=last_transaction_time,
                realized_profit=realized_profit,
                updated_at=current_time
            )
            session.add(new_holding)

        # 注意：事務提交由呼叫方處理
        # await session.commit()

    except Exception as e:
        await session.rollback()
        print(f"Error saving holding for wallet {wallet_address}, "
              f"token {tx_data.get('token_address', '')}: {str(e)}")
        raise  # 重新拋出異常以便上層處理

async def get_token_buy_data(wallet_address: str, token_address: str, session: AsyncSession, chain):
    """
    查询 TokenBuyData 数据表，获取指定钱包地址和代币地址的持仓数据。

    :param wallet_address: 钱包地址
    :param token_address: 代币地址
    :param session: 数据库异步会话
    :return: 包含 avg_buy_price 和 total_amount 的字典。如果没有找到记录，则返回 None。
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        # 查询是否存在指定钱包和代币的记录
        result = await session.execute(
            select(TokenBuyData).filter(
                TokenBuyData.wallet_address == wallet_address,
                TokenBuyData.token_address == token_address
            )
        )
        token_data = result.scalars().first()

        if token_data:
            # 返回所需的字段
            return {
                "token_address": token_data.token_address,
                "avg_buy_price": token_data.avg_buy_price,
                "total_amount": token_data.total_amount,
                "total_cost": token_data.total_cost
            }
        else:
            return {
                "token_address": token_address,
                "avg_buy_price": 0,
                "total_amount": 0,
                "total_cost": 0
            }

    except Exception as e:
        print(f"Error while querying TokenBuyData for wallet {wallet_address}, token {token_address}: {str(e)}")

async def reset_wallet_buy_data(wallet_address: str, session: AsyncSession, chain):
    """
    重置指定錢包的所有代幣購買數據，但保留歷史累計數據
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        TokenBuyData.with_schema(schema)
        
        # 獲取該錢包的所有代幣記錄
        stmt = select(TokenBuyData).filter(TokenBuyData.wallet_address == wallet_address)
        result = await session.execute(stmt)
        holdings = result.scalars().all()
        
        # 重置所有代幣的當前持倉數據，但保留歷史累計數據
        for holding in holdings:
            # 在重置前，將當前持倉數據轉移到歷史數據
            if hasattr(holding, 'historical_total_buy_amount') and holding.total_amount > 0:
                # 確保歷史數據欄位存在
                if not holding.historical_total_buy_amount:
                    holding.historical_total_buy_amount = 0
                if not holding.historical_total_buy_cost:
                    holding.historical_total_buy_cost = 0
                if not holding.historical_total_sell_amount:
                    holding.historical_total_sell_amount = 0
                if not holding.historical_total_sell_value:
                    holding.historical_total_sell_value = 0
                
                # 記錄關閉時間 - 修復數據類型問題
                if holding.total_amount > 0:
                    # 使用當前時間戳而不是 datetime 對象
                    current_timestamp = int(datetime.now().timestamp())
                    holding.last_active_position_closed_at = current_timestamp
            
            # 重置當前持倉數據
            holding.total_amount = 0
            holding.total_cost = 0
            holding.avg_buy_price = 0
            holding.position_opened_at = None
            holding.last_transaction_time = None
            holding.updated_at = get_utc8_time()
        
        await asyncio.shield(session.commit())
    except Exception as e:
        await session.rollback()
        print(f"重置錢包 {wallet_address} 的買入數據時發生錯誤: {str(e)}")
        traceback.print_exc()  # 列印完整的錯誤堆疊追蹤

async def log_error(session, error_message: str, module_name: str, function_name: str = None, additional_info: str = None):
    """記錄錯誤訊息到資料庫"""
    try:
        error_log = ErrorLog(
            error_message=error_message,
            module_name=module_name,
            function_name=function_name,
        )
        session.add(error_log)  # 使用異步添加操作
        await session.commit()  # 提交變更
    except Exception as e:
        try:
            await session.rollback()  # 在錯誤發生時進行回滾
        except Exception as rollback_error:
            print(f"回滾錯誤: {rollback_error}")
        print(f"無法記錄錯誤訊息: {e}")
    finally:
        try:
            await session.close()  # 確保 session 被正確關閉
        except Exception as close_error:
            print(f"關閉 session 時錯誤: {close_error}")

# -------------------------------------------------------------------API--------------------------------------------------------------------------------

async def query_all_wallets(sessions, chain_filter=None):
    """
    查詢所有鏈的錢包資訊，直接查詢 dex_query_v1.wallet 表，根據 chain 欄位區分。
    """
    try:
        # 只需用一個 session（任一鏈的 session_factory 都可，這裡取第一個）
        session_factory = next(iter(sessions.values()))
        async with session_factory() as session:
            schema = 'dex_query_v1'
            if chain_filter:
                query = f"""
                    SELECT * FROM {schema}.wallet
                    WHERE chain = :chain_filter
                    ORDER BY last_transaction_time DESC
                """
                result = await session.execute(text(query), {"chain_filter": chain_filter})
            else:
                query = f"""
                    SELECT * FROM {schema}.wallet
                    ORDER BY last_transaction_time DESC
                """
                result = await session.execute(text(query))
            rows = result.fetchall()
            columns = result.keys()

            wallet_dicts = []
            for row in rows:
                wallet_dict = dict(zip(columns, row))
                # 確保 token_list 是字符串格式
                if 'token_list' in wallet_dict and wallet_dict['token_list'] is not None:
                    if isinstance(wallet_dict['token_list'], str):
                        wallet_dict['token_list'] = wallet_dict['token_list'].split(', ')
                    else:
                        logger.warning(f"token_list 不是字符串格式，值: {wallet_dict['token_list']}")
                wallet_dicts.append(wallet_dict)
            # 按 last_transaction_time 排序
            wallet_dicts.sort(
                key=lambda x: x.get('last_transaction_time', 0) or 0,
                reverse=True
            )
            return wallet_dicts
    except Exception as e:
        logging.error(f"查詢錢包數據時發生錯誤: {str(e)}")
        raise

async def query_wallet_holdings(session_factory, wallet_address, chain):
    """
    查詢指定鏈和錢包地址的持倉數據
    :param session_factory: 對應鏈的 session factory
    :param wallet_address: 錢包地址
    :param chain: 區塊鏈名稱
    :return: 持倉數據列表
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Holding.with_schema(schema)
        async with session_factory() as session:
            query = (
                select(Holding)
                .where(Holding.wallet_address == wallet_address)
                .where(Holding.chain == chain)
            )
            result = await session.execute(query)
            holdings = result.scalars().all()
            return holdings
    except Exception as e:
        logging.error(f"查詢持倉數據失敗: {e}")
        raise Exception(f"查詢持倉數據失敗: {str(e)}")

# -------------------------------------------------------------------------------------------------------
async def get_wallet_transactions(
    session: AsyncSession,
    chain: str,
    wallet_address: str,
    token_address: str = None,
    transaction_type: str = None,
    fetch_all: bool = False,
    limit: int = 30
):
    """查詢單個錢包的交易記錄"""
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        
        # 讀取過濾的代幣地址列表
        filtered_token_addresses = []
        try:
            with open('filtered_tokens.json', 'r') as f:
                filtered_token_addresses = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"讀取過濾代幣列表失敗: {e}")
        
        # 準備查詢參數
        params = {}
        params["chain"] = chain
        params["wallet_address"] = wallet_address
        
        # 使用 UTC+8 時區設置時間範圍
        TZ_UTC8 = timezone(timedelta(hours=8))
        now = datetime.now(TZ_UTC8)
        now_timestamp = int(now.timestamp())
        one_hour_ago_timestamp = now_timestamp - 3600
        params["one_hour_ago"] = one_hour_ago_timestamp
        params["now_timestamp"] = now_timestamp
        
        # 設置時間限制
        if not fetch_all:
            time_threshold = int((now - timedelta(days=30)).timestamp())
            params["time_threshold"] = time_threshold
        
        # 構建基本查詢條件
        conditions = [
            "t.chain = :chain",
            "t.wallet_address = :wallet_address"
        ]
        
        # 過濾代幣條件
        if filtered_token_addresses:
            filtered_tokens_str = ','.join([f"'{addr}'" for addr in filtered_token_addresses])
            conditions.append(f"t.token_address NOT IN ({filtered_tokens_str})")
        
        # 代幣地址條件
        if token_address:
            conditions.append("t.token_address = :token_address")
            params["token_address"] = token_address
        
        # 交易類型條件
        if transaction_type:
            conditions.append("t.transaction_type = :transaction_type")
            params["transaction_type"] = transaction_type
        
        # 時間範圍條件
        if not fetch_all:
            conditions.append("t.transaction_time >= :time_threshold")
        
        # 組合條件
        where_clause = " AND ".join(conditions)
        
        # 構建高效的單一SQL查詢
        query = f"""
        WITH matched_transactions AS (
            SELECT t.*
            FROM {schema}.wallet_transaction t
            WHERE {where_clause}
            ORDER BY t.transaction_time DESC
            LIMIT :limit
        ),
        token_stats AS (
            SELECT 
                token_address,
                COUNT(DISTINCT wallet_address) as wallet_count,
                COUNT(CASE WHEN transaction_type = 'buy' THEN 1 END) as buy_count,
                COUNT(CASE WHEN transaction_type = 'sell' THEN 1 END) as sell_count
            FROM {schema}.wallet_transaction
            WHERE chain = :chain
              AND transaction_time >= :one_hour_ago
              AND transaction_time <= :now_timestamp
              AND token_address IN (SELECT token_address FROM matched_transactions)
            GROUP BY token_address
        )
        SELECT 
            t.*,
            COALESCE(s.wallet_count, 0) as wallet_count_last_hour,
            COALESCE(s.buy_count, 0) as buy_count_last_hour,
            COALESCE(s.sell_count, 0) as sell_count_last_hour,
            h.current_avg_buy_price,
            h.current_total_cost,
            h.historical_buy_cost,
            h.current_amount,
            h.historical_buy_amount,
            h.historical_sell_amount,
            h.historical_sell_value
        FROM 
            matched_transactions t
        LEFT JOIN 
            token_stats s ON t.token_address = s.token_address
        LEFT JOIN
            {schema}.wallet_token_state h ON t.wallet_address = h.wallet_address AND t.token_address = h.token_address
        ORDER BY 
            t.transaction_time DESC
        """
        
        # 添加限制條件
        params["limit"] = limit
        
        # 執行優化的單一SQL查詢
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(text(query), params)
        transactions = result.all()
        
        # 格式化結果 (與 get_transactions_by_params 格式相同)
        formatted_data = []
        for row in transactions:
            # 根據不同的鏈處理 token_address 格式
            token_address_value = row.token_address
            if chain.upper() != "SOLANA" and token_address_value:
                token_address_value = token_address_value.lower()
            
            # 處理持倉數據
            avg_buy_price = row.current_avg_buy_price or 0
                
            total_cost = row.current_total_cost
            if total_cost is None:
                total_cost = row.historical_total_buy_cost or 0
                
            total_amount = row.current_amount
            if total_amount is None:
                total_buy_amount = row.historicall_buy_amount or 0
                total_sell_amount = row.historical_sell_amount or 0
                total_amount = total_buy_amount - total_sell_amount
                # 防止持倉數量變為負值
                if total_amount < 0:
                    total_amount = 0
            
            # 計算 PNL
            total_sell_amount = row.historical_sell_amount or 0
            avg_sell_price = row.historical_sell_value / total_sell_amount if total_sell_amount > 0 else 0
            
            pnl = total_sell_amount * (avg_sell_price - avg_buy_price) if total_sell_amount > 0 and avg_buy_price > 0 else 0
            
            # 計算 PNL 百分比
            denominator = total_sell_amount * avg_buy_price
            pnl_percentage = (pnl / denominator * 100) if denominator > 0 else 0
            
            # 創建格式化的交易記錄
            tx_data = {
                "wallet_address": row.wallet_address,
                "wallet_balance": format_price(getattr(row, 'wallet_balance', 0)),
                "signature": row.signature,
                "token_address": token_address_value,
                "token_icon": row.token_icon,
                "token_name": row.token_name,
                "price": format_price(row.price),
                "amount": format_price(row.amount),
                "marketcap": format_price(row.marketcap),
                "value": format_price(row.value),
                "holding_percentage": format_price(row.holding_percentage),
                "chain": row.chain,
                "realized_profit": format_price(row.realized_profit),
                "realized_profit_percentage": format_price(row.realized_profit_percentage),
                "transaction_type": row.transaction_type,
                "wallet_count_last_hour": row.wallet_count_last_hour,
                "buy_count_last_hour": row.buy_count_last_hour,
                "sell_count_last_hour": row.sell_count_last_hour,
                "transaction_time": row.transaction_time,
                "time": row.time.isoformat() if hasattr(row, 'time') and row.time else None,
                "from_token_address": row.from_token_address,
                "from_token_symbol": row.from_token_symbol,
                "from_token_amount": format_price(getattr(row, 'from_token_amount', 0)),
                "dest_token_address": row.dest_token_address,
                "dest_token_symbol": row.dest_token_symbol,
                "dest_token_amount": format_price(getattr(row, 'dest_token_amount', 0)),
                
                # 持倉相關數據
                "avg_price": format_price(avg_buy_price),
                "total_cost": format_price(total_cost),
                "total_pnl": format_price(pnl),
                "total_pnl_percentage": format_price(pnl_percentage),
                "total_amount": format_price(total_amount)
            }
            formatted_data.append(tx_data)
            
        return formatted_data

    except Exception as e:
        logger.error(f"查詢錢包 {wallet_address} 交易出錯: {str(e)}", exc_info=True)
        # 返回空列表而不是引發錯誤，以便繼續處理其他錢包
        return []

async def get_transactions_by_params(
    session: AsyncSession,
    chain: str,
    wallet_addresses: List[str] = None,
    token_address: str = None,
    name: str = None,
    query_string: str = None,
    fetch_all: bool = False,
    transaction_type: str = None,
    min_value: float = None,
    limit: int = 30,
    smart_wallet_only: bool = False  # 新增參數，用於指示是否只查詢聰明錢包
):
    try:
        logger.info(f"查詢參數: chain={chain}, wallet_addresses={wallet_addresses}, token_address={token_address}, transaction_type={transaction_type}, fetch_all={fetch_all}, limit={limit}, smart_wallet_only={smart_wallet_only}")
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)
        TokenBuyData.with_schema(schema)

        # 讀取過濾的代幣地址列表
        filtered_token_addresses = []
        try:
            with open('filtered_tokens.json', 'r') as f:
                filtered_token_addresses = json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            logger.warning(f"讀取過濾代幣列表失敗: {e}")
        
        # 準備查詢參數
        params = {}
        params["chain"] = chain
        
        # 使用 UTC+8 時區設置時間範圍
        TZ_UTC8 = timezone(timedelta(hours=8))
        now = datetime.now(TZ_UTC8)
        now_timestamp = int(now.timestamp())
        one_hour_ago_timestamp = now_timestamp - 3600
        params["one_hour_ago"] = one_hour_ago_timestamp
        params["now_timestamp"] = now_timestamp
        
        # 設置非 fetch_all 情況下的時間限制
        if not fetch_all:
            time_threshold = int((now - timedelta(days=30)).timestamp())
            params["time_threshold"] = time_threshold
        
        # 如果沒有提供錢包地址，使用默認查詢
        if not wallet_addresses:
            conditions = ["t.chain = :chain"]
            
            # 過濾代幣條件
            if filtered_token_addresses:
                filtered_tokens_str = ','.join([f"'{addr}'" for addr in filtered_token_addresses])
                conditions.append(f"t.token_address NOT IN ({filtered_tokens_str})")
            
            # 只有在 smart_wallet_only 為 True 時，添加聰明錢包過濾
            if smart_wallet_only:
                conditions.append("t.wallet_address IN (SELECT wallet_address FROM smart_wallets)")
            
            # 其他條件
            if token_address:
                conditions.append("t.token_address = :token_address")
                params["token_address"] = token_address
            
            if name:
                conditions.append("t.token_name = :name")
                params["name"] = name
            
            if transaction_type:
                conditions.append("t.transaction_type = :transaction_type")
                params["transaction_type"] = transaction_type
            
            if min_value is not None:
                conditions.append("t.value >= :min_value")
                params["min_value"] = min_value
            
            # 時間範圍條件
            if not fetch_all:
                conditions.append("t.transaction_time >= :time_threshold")
            
            # 組合所有條件
            where_clause = " AND ".join(conditions)
            
            # 構建 WITH 子句 - 關鍵修正：必須始終有 WITH 關鍵字
            if smart_wallet_only:
                with_clause = f"""
                WITH smart_wallets AS (
                    SELECT wallet_address FROM {schema}.wallet WHERE is_smart_wallet = true
                ),
                """
            else:
                with_clause = "WITH "  # 始終需要 WITH 關鍵字
            
            # 構建高效的單一SQL查詢
            query = f"""
            {with_clause}matched_transactions AS (
                SELECT t.*
                FROM {schema}.wallet_transaction t
                WHERE {where_clause}
                ORDER BY t.transaction_time DESC
                LIMIT :limit
            ),
            token_stats AS (
                SELECT 
                    token_address,
                    COUNT(DISTINCT t.wallet_address) as wallet_count,
                    COUNT(CASE WHEN t.transaction_type = 'buy' THEN 1 END) as buy_count,
                    COUNT(CASE WHEN t.transaction_type = 'sell' THEN 1 END) as sell_count
                FROM {schema}.wallet_transaction t
                INNER JOIN {schema}.wallet w ON t.wallet_address = w.wallet_address AND w.is_smart_wallet = true
                WHERE t.chain = :chain
                AND t.transaction_time >= :one_hour_ago
                AND t.transaction_time <= :now_timestamp
                AND t.token_address IN (SELECT token_address FROM matched_transactions)
                GROUP BY t.token_address
            )
            SELECT 
                t.*,
                COALESCE(s.wallet_count, 0) as wallet_count_last_hour,
                COALESCE(s.buy_count, 0) as buy_count_last_hour,
                COALESCE(s.sell_count, 0) as sell_count_last_hour,
                h.current_avg_buy_price,
                h.current_total_cost,
                h.historical_buy_cost,
                h.current_amount,
                h.historical_buy_amount,
                h.historical_sell_amount,
                h.historical_sell_value
            FROM 
                matched_transactions t
            LEFT JOIN 
                token_stats s ON t.token_address = s.token_address
            LEFT JOIN
                {schema}.wallet_token_state h ON t.wallet_address = h.wallet_address AND t.token_address = h.token_address
            ORDER BY 
                t.transaction_time DESC
            """
            
            params["limit"] = limit
            
            # 執行查詢
            await session.execute(text("SET search_path TO dex_query_v1;"))
            result = await session.execute(text(query), params)
            transactions = result.all()
            
            # 格式化結果
            formatted_data = []
            for row in transactions:
                formatted_data.append(format_transaction_row(row, chain))
                
            return formatted_data
            
        else:
            # 為每個錢包單獨查詢，每個錢包最多返回limit條記錄
            all_transactions = []
            
            for wallet in wallet_addresses:
                # 構建單個錢包的查詢條件
                wallet_conditions = [
                    "t.chain = :chain",
                    "t.wallet_address = :wallet_address"
                ]
                
                wallet_params = {
                    "chain": chain,
                    "wallet_address": wallet,
                    "one_hour_ago": one_hour_ago_timestamp,
                    "now_timestamp": now_timestamp
                }
                
                if token_address:
                    wallet_conditions.append("t.token_address = :token_address")
                    wallet_params["token_address"] = token_address
                
                if name:
                    wallet_conditions.append("t.token_name = :name")
                    wallet_params["name"] = name
                
                if transaction_type:
                    wallet_conditions.append("t.transaction_type = :transaction_type")
                    wallet_params["transaction_type"] = transaction_type
                
                if min_value is not None:
                    wallet_conditions.append("t.value >= :min_value")
                    wallet_params["min_value"] = min_value
                
                if not fetch_all:
                    wallet_conditions.append("t.transaction_time >= :time_threshold")
                    wallet_params["time_threshold"] = time_threshold
                
                # 組合條件
                wallet_where_clause = " AND ".join(wallet_conditions)
                
                # 查詢單個錢包的交易ID
                wallet_query = f"""
                SELECT id
                FROM {schema}.wallet_transaction t
                WHERE {wallet_where_clause}
                ORDER BY t.transaction_time DESC
                LIMIT :limit
                """
                
                wallet_params["limit"] = limit
                
                # 執行查詢獲取ID
                wallet_result_ids = await session.execute(text(wallet_query), wallet_params)
                wallet_tx_ids = [row[0] for row in wallet_result_ids]
                
                if not wallet_tx_ids:
                    continue  # 如果這個錢包沒有交易，跳過
                
                # 使用ID查詢詳細交易信息
                wallet_detail_query = f"""
                WITH matched_transactions AS (
                    SELECT t.*
                    FROM {schema}.wallet_transaction t
                    WHERE t.id = ANY(:tx_ids)
                ),
                token_stats AS (
                    SELECT 
                        token_address,
                        COUNT(DISTINCT wallet_address) as wallet_count,
                        COUNT(CASE WHEN transaction_type = 'buy' THEN 1 END) as buy_count,
                        COUNT(CASE WHEN transaction_type = 'sell' THEN 1 END) as sell_count
                    FROM {schema}.wallet_transaction
                    WHERE chain = :chain
                      AND transaction_time >= :one_hour_ago
                      AND transaction_time <= :now_timestamp
                      AND token_address IN (SELECT token_address FROM matched_transactions)
                    GROUP BY token_address
                )
                SELECT 
                    t.*,
                    COALESCE(s.wallet_count, 0) as wallet_count_last_hour,
                    COALESCE(s.buy_count, 0) as buy_count_last_hour,
                    COALESCE(s.sell_count, 0) as sell_count_last_hour,
                    h.current_avg_buy_price,
                    h.current_total_cost,
                    h.historical_buy_cost,
                    h.current_amount,
                    h.historical_buy_amount,
                    h.historical_sell_amount,
                    h.historical_sell_value
                FROM 
                    matched_transactions t
                LEFT JOIN 
                    token_stats s ON t.token_address = s.token_address
                LEFT JOIN
                    {schema}.wallet_token_state h ON t.wallet_address = h.wallet_address AND t.token_address = h.token_address
                ORDER BY 
                    t.transaction_time DESC
                """
                
                wallet_params["tx_ids"] = wallet_tx_ids
                
                # 執行詳細查詢
                await session.execute(text("SET search_path TO dex_query_v1;"))
                wallet_detail_result = await session.execute(text(wallet_detail_query), wallet_params)
                wallet_transactions = wallet_detail_result.all()
                
                # 格式化該錢包的交易記錄並添加到總結果中
                for row in wallet_transactions:
                    all_transactions.append(format_transaction_row(row, chain))
                
                logger.info(f"錢包 {wallet} 獲取 {len(wallet_transactions)} 條交易記錄")
            
            logger.info(f"總共獲取 {len(all_transactions)} 條交易記錄，來自 {len(wallet_addresses)} 個錢包")
            return all_transactions

    except KeyError as ke:
        logger.error(f"查詢參數錯誤: {ke}", exc_info=True)
        raise RuntimeError(f"查詢參數錯誤: {str(ke)}")
    except Exception as e:
        logger.error(f"優化查詢函數出錯: {e}", exc_info=True)
        raise RuntimeError(f"查詢交易記錄時發生錯誤: {str(e)}")

# 輔助函數：格式化交易行數據
def format_transaction_row(row, chain):
    # 根據不同的鏈處理 token_address 格式
    token_address_value = row.token_address
    if chain.upper() != "SOLANA" and token_address_value:
        token_address_value = token_address_value.lower()
    
    # 處理持倉數據
    avg_buy_price = row.current_avg_buy_price or 0
        
    total_cost = row.current_total_cost
    if total_cost is None:
        total_cost = row.current_total_cost or 0
        
    total_amount = row.current_amount
    if total_amount is None:
        total_buy_amount = row.historical_buy_amount or 0
        total_sell_amount = row.historical_sell_amount or 0
        total_amount = total_buy_amount - total_sell_amount
    historical_sell_value = row.historical_sell_value
    
    # 計算 PNL
    total_sell_amount = row.historical_sell_amount or 0
    avg_sell_price = historical_sell_value / total_sell_amount if total_sell_amount > 0 else 0
    
    pnl = total_sell_amount * (avg_sell_price - avg_buy_price) if total_sell_amount > 0 and avg_buy_price > 0 else 0
    
    # 計算 PNL 百分比
    denominator = total_sell_amount * avg_buy_price
    pnl_percentage = (pnl / denominator * 100) if denominator > 0 else 0
    
    # 創建格式化的交易記錄
    return {
        "wallet_address": row.wallet_address,
        "wallet_balance": format_price(getattr(row, 'wallet_balance', 0)),
        "signature": row.signature,
        "token_address": token_address_value,
        "token_icon": row.token_icon,
        "token_name": row.token_name,
        "price": format_price(row.price),
        "amount": format_price(row.amount),
        "marketcap": format_price(row.marketcap),
        "value": format_price(row.value),
        "holding_percentage": format_price(row.holding_percentage),
        "chain": row.chain,
        "realized_profit": format_price(row.realized_profit),
        "realized_profit_percentage": format_price(row.realized_profit_percentage),
        "transaction_type": row.transaction_type,
        "wallet_count_last_hour": row.wallet_count_last_hour,
        "buy_count_last_hour": row.buy_count_last_hour,
        "sell_count_last_hour": row.sell_count_last_hour,
        "transaction_time": row.transaction_time,
        "time": row.time.isoformat() if hasattr(row, 'time') and row.time else None,
        "from_token_address": row.from_token_address,
        "from_token_symbol": row.from_token_symbol,
        "from_token_amount": row.from_token_amount,
        "dest_token_address": row.dest_token_address,
        "dest_token_symbol": row.dest_token_symbol,
        "dest_token_amount": row.dest_token_amount,
        
        # 持倉相關數據
        "avg_price": format_price(avg_buy_price),
        "total_cost": format_price(total_cost),
        "total_pnl": format_price(pnl),
        "total_pnl_percentage": format_price(pnl_percentage),
        "total_amount": format_price(total_amount)
    }

async def get_latest_transactions(session: AsyncSession, chain: str, limit: int = 30):
    """
    查詢最新的交易數據
    :param session: 資料庫會話
    :param chain: 區塊鏈名稱
    :param limit: 返回的交易數量限制
    :return: 最新交易的列表
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)
        query = (
            select(Transaction)
            .where(Transaction.chain == chain)
            .order_by(Transaction.transaction_time.desc())
            .limit(limit)
        )
        result = await session.execute(query)
        return result.scalars().all()
    except Exception as e:
        logging.error(f"查詢最新交易數據失敗: {e}")
        raise RuntimeError(f"查詢最新交易數據失敗: {str(e)}")
    
async def get_transactions_for_wallet(session: AsyncSession, chain: str, wallet_address: str, days: int = 30):
    """
    查詢指定 wallet_address 在過去指定天數內的交易記錄。
    :param session: 資料庫會話
    :param chain: 區塊鏈名稱
    :param wallet_address: 要查詢的錢包地址
    :param days: 查詢的天數範圍，預設為 90 天
    :return: 符合條件的交易列表，每條記錄以字典形式返回。
    """
    try:
        logger.info(f"[get_transactions_for_wallet] chain: {chain}, wallet_address: {wallet_address}, days: {days}")
        # 計算 30 天前的時間戳
        cutoff_time = int((datetime.utcnow() - timedelta(days=days)).timestamp())

        # 設置使用的 schema
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)

        # 構建查詢
        query = (
            select(Transaction)
            .where(
                Transaction.chain == chain,
                func.lower(Transaction.wallet_address) == wallet_address.lower(),
                Transaction.transaction_time >= cutoff_time
            )
            .order_by(Transaction.transaction_time.asc())  # 按照 transaction_time 從最舊到最新排序
        )

        # 執行查詢
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(query)
        transactions = result.scalars().all()

        # 將交易記錄轉換為字典列表
        return [
            {
                "id": tx.id,
                "wallet_address": tx.wallet_address,
                "wallet_balance": tx.wallet_balance,
                "token_address": tx.token_address,
                "token_icon": tx.token_icon,
                "token_name": tx.token_name,
                "price": tx.price,
                "amount": tx.amount,
                "marketcap": tx.marketcap,
                "value": tx.value,
                "holding_percentage": tx.holding_percentage,
                "chain": tx.chain,
                "realized_profit": tx.realized_profit,
                "realized_profit_percentage": tx.realized_profit_percentage,
                "transaction_type": tx.transaction_type,
                "transaction_time": tx.transaction_time,
                "time": tx.time,
                "signature": tx.signature
            }
            for tx in transactions
        ]
    except Exception as e:
        logging.error(f"查詢錢包 {wallet_address} 的交易記錄失敗: {e}")
        raise RuntimeError(f"查詢錢包 {wallet_address} 的交易記錄失敗: {str(e)}")

async def enrich_transactions(session, transactions, chain, now_timestamp, one_hour_ago_timestamp):
    """
    按照每個 token_address 統計查詢當下時間往前推一小時內的所有交易情況
    """
    enriched_transactions = []
    
    for transaction in transactions:
        # 針對當前交易的 token_address 進行統計
        wallet_count_query = (
            select(func.count(func.distinct(Transaction.wallet_address)))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
        )
        wallet_count_result = await session.execute(wallet_count_query)
        wallet_count_last_hour = wallet_count_result.scalar() or 0  # 如果沒有數據返回0

        buy_count_query = (
            select(func.count(Transaction.id))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
            .where(Transaction.transaction_type == "buy")
        )
        buy_count_result = await session.execute(buy_count_query)
        buy_count_last_hour = buy_count_result.scalar() or 0  # 如果沒有數據返回0

        sell_count_query = (
            select(func.count(Transaction.id))
            .where(Transaction.chain == chain)
            .where(Transaction.token_address == transaction.token_address)
            .where(Transaction.transaction_time >= one_hour_ago_timestamp)
            .where(Transaction.transaction_time <= now_timestamp)
            .where(Transaction.transaction_type == "sell")
        )
        sell_count_result = await session.execute(sell_count_query)
        sell_count_last_hour = sell_count_result.scalar() or 0  # 如果沒有數據返回0

        enriched_transactions.append({
            "transaction": transaction,
            "wallet_count_last_hour": wallet_count_last_hour,
            "buy_count_last_hour": buy_count_last_hour,
            "sell_count_last_hour": sell_count_last_hour
        })

    return enriched_transactions

async def get_token_trend_data(session, token_addresses, chain, time_range):
    """
    查詢資料庫中的代幣趨勢數據，並分別返回每個代幣的數據
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)
        all_trends = []

        # 計算時間範圍，根據傳入的時間參數生成 datetime 對象
        # 轉換為 UTC+8 時間
        TZ_UTC8 = timezone(timedelta(hours=8))
        time_threshold = datetime.now(TZ_UTC8) - timedelta(minutes=time_range)
        time_threshold_timestamp = int(time_threshold.timestamp())
        now_timestamp = int(datetime.now(TZ_UTC8).timestamp())

        # 1. 查詢所有相關交易數據，加入 time_range 及限制最多返回 30 筆資料
        transactions_query = select(Transaction).where(
            Transaction.token_address.in_(token_addresses),
            Transaction.chain == chain,
            Transaction.transaction_time >= time_threshold_timestamp,  # 时间 >= 起始时间戳
            Transaction.transaction_time <= now_timestamp,  # 时间 <= 当前时间戳
            Transaction.wallet_address.in_(
                select(WalletSummary.wallet_address).where(
                    WalletSummary.is_smart_wallet == True,
                    WalletSummary.chain == chain
                )
            )
        ).order_by(Transaction.transaction_time.desc()).limit(30)

        transactions_result = await session.execute(transactions_query)
        transactions = transactions_result.scalars().all()

        # 2. 查詢所有相關 wallet 資料
        wallet_addresses = list(set([tx.wallet_address for tx in transactions]))
        wallet_query = select(WalletSummary.wallet_address, WalletSummary.asset_multiple, WalletSummary.wallet_type).where(
            WalletSummary.wallet_address.in_(wallet_addresses)
        )
        wallet_data_result = await session.execute(wallet_query)
        wallet_data = {entry[0]: {"asset_multiple": entry[1], "wallet_type": entry[2]} for entry in wallet_data_result.fetchall()}

        # 3. 按照 token_address 分組交易數據
        grouped_transactions = {}
        for tx in transactions:
            if tx.token_address not in grouped_transactions:
                grouped_transactions[tx.token_address] = {'buy': [], 'sell': []}
            if tx.transaction_type == 'buy':
                grouped_transactions[tx.token_address]['buy'].append(tx)
            else:
                grouped_transactions[tx.token_address]['sell'].append(tx)

        # 4. 遍歷每個 token_address，計算並組織資料
        for token_address, tx_data in grouped_transactions.items():
            buy_transactions = tx_data['buy']
            sell_transactions = tx_data['sell']
            is_pump = token_address.endswith("pump")

            # 計算買賣地址數量
            buy_addr_set = set(tx.wallet_address for tx in buy_transactions)  # 使用 set 來去重
            sell_addr_set = set(tx.wallet_address for tx in sell_transactions)  # 使用 set 來去重

            # 合併買賣地址集合，保證同一個地址只會計算一次
            total_addr_set = buy_addr_set.union(sell_addr_set)  # 計算總共的唯一地址數量

            buy_addr_amount = len(buy_addr_set)
            sell_addr_amount = len(sell_addr_set)
            total_addr_amount = len(total_addr_set)  # 計算唯一地址數量

            token_name = buy_transactions[0].token_name if buy_transactions else (sell_transactions[0].token_name if sell_transactions else "")

            # 統計買入交易數據
            buy_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_buy_vol": tx.amount,
                    "wallet_buy_marketcap": tx.marketcap,
                    "wallet_buy_usd": tx.value,
                    "wallet_buy_holding": tx.holding_percentage,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in buy_transactions
            ]

            # 統計賣出交易數據
            sell_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_sell_vol": tx.amount,
                    "wallet_sell_marketcap": tx.marketcap,
                    "wallet_sell_usd": tx.value,
                    "wallet_sell_pnl": tx.realized_profit or 0,
                    "wallet_sell_pnl_percentage": tx.realized_profit_percentage or 0,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in sell_transactions
            ]

            # 將結果加入到總結果列表
            all_trends.append({
                "buy_addrAmount": buy_addr_amount,
                "sell_addrAmount": sell_addr_amount,
                "total_addr_amount": total_addr_amount,
                "token_name": token_name,
                "token_address": token_address,
                "chain": chain,
                "is_pump": is_pump,
                "time": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                "buy": buy_data,
                "sell": sell_data,
            })

        # 返回所有代幣的趨勢數據
        return all_trends

    except Exception as e:
        print(f"查詢代幣趨勢數據失敗，原因 {e}")
        # await log_error(
        #     session,
        #     str(e),
        #     "models",
        #     "get_token_trend_data",
        #     f"查詢代幣趨勢數據失敗，原因 {e}"
        # )
        return None

async def get_token_trend_data_allchain(session, token_addresses, chain, time_range):
    """
    查詢資料庫中的代幣趨勢數據，並分別返回每個代幣的數據
    """
    try:
        # schema = chain.lower()
        schema = 'dex_query_v1'
        Transaction.with_schema(schema)
        WalletSummary.with_schema(schema)
        all_trends = []

        # 計算時間範圍，根據傳入的時間參數生成 datetime 對象
        time_threshold = datetime.now(timezone.utc) - timedelta(minutes=time_range)
        time_threshold_timestamp = int(time_threshold.timestamp())
        now_timestamp = int(datetime.now(timezone.utc).timestamp())

        # 1. 查詢所有相關交易數據，加入 time_range 及限制最多返回 30 筆資料
        transactions_query = select(Transaction).where(
            Transaction.token_address.in_(token_addresses),
            Transaction.chain == chain,
            Transaction.transaction_time >= time_threshold_timestamp,
            Transaction.transaction_time <= now_timestamp,
            Transaction.wallet_address.in_(
                select(WalletSummary.wallet_address).where(WalletSummary.is_smart_wallet == True)
            )
        ).order_by(Transaction.transaction_time.desc()).limit(30)

        transactions_result = await session.execute(transactions_query)
        transactions = transactions_result.scalars().all()

        # 如果該chain沒有找到任何交易，直接返回空列表
        if not transactions:
            return []

        # 2. 查詢所有相關 wallet 資料
        wallet_addresses = list(set([tx.wallet_address for tx in transactions]))
        wallet_query = select(WalletSummary.wallet_address, WalletSummary.asset_multiple, WalletSummary.wallet_type).where(
            WalletSummary.wallet_address.in_(wallet_addresses)
        )
        wallet_data_result = await session.execute(wallet_query)
        wallet_data = {entry[0]: {"asset_multiple": entry[1], "wallet_type": entry[2]} for entry in wallet_data_result.fetchall()}

        # 3. 按照 token_address 分組交易數據
        grouped_transactions = {}
        for tx in transactions:
            if tx.token_address not in grouped_transactions:
                grouped_transactions[tx.token_address] = {'buy': [], 'sell': []}
            if tx.transaction_type == 'buy':
                grouped_transactions[tx.token_address]['buy'].append(tx)
            else:
                grouped_transactions[tx.token_address]['sell'].append(tx)

        # 4. 遍歷每個 token_address，計算並組織資料
        for token_address, tx_data in grouped_transactions.items():
            buy_transactions = tx_data['buy']
            sell_transactions = tx_data['sell']
            is_pump = token_address.endswith("pump")

            buy_addr_amount = len(set(tx.wallet_address for tx in buy_transactions))
            sell_addr_amount = len(set(tx.wallet_address for tx in sell_transactions))
            total_addr_amount = buy_addr_amount + sell_addr_amount

            token_name = buy_transactions[0].token_name if buy_transactions else (sell_transactions[0].token_name if sell_transactions else "")

            buy_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_buy_vol": tx.amount,
                    "wallet_buy_marketcap": tx.marketcap,
                    "wallet_buy_usd": tx.value,
                    "wallet_buy_holding": tx.holding_percentage,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in buy_transactions
            ]

            sell_data = [
                {
                    "wallet_address": tx.wallet_address,
                    "wallet_sell_vol": tx.amount,
                    "wallet_sell_marketcap": tx.marketcap,
                    "wallet_sell_usd": tx.value,
                    "wallet_sell_pnl": tx.realized_profit or 0,
                    "wallet_asset_multiple": wallet_data.get(tx.wallet_address, {}).get("asset_multiple", None),
                    "wallet_type": wallet_data.get(tx.wallet_address, {}).get("wallet_type", None),
                    "transaction_time": tx.transaction_time,
                }
                for tx in sell_transactions
            ]

            all_trends.append({
                "buy_addrAmount": buy_addr_amount,
                "sell_addrAmount": sell_addr_amount,
                "total_addr_amount": total_addr_amount,
                "token_name": token_name,
                "token_address": token_address,
                "chain": chain,
                "is_pump": is_pump,
                "time": datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d %H:%M:%S"),
                "buy": buy_data,
                "sell": sell_data,
            })

        return all_trends

    except Exception as e:
        print(f"查詢代幣趨勢數據失敗，原因 {e}")
        # await log_error(
        #     session,
        #     str(e),
        #     "models",
        #     "get_token_trend_data",
        #     f"查詢代幣趨勢數據失敗，原因 {e}"
        # )
        return None

async def migrate_token_buy_data(conn, schema_name):
    """
    資料表結構升級後的資料遷移
    
    Args:
        conn: 資料庫連接
        schema_name: Schema 名稱
    """
    try:
        print(f"正在遷移 {schema_name}.wallet_buy_data 表數據...")
        
        # 檢查必要欄位是否已存在
        required_columns = ['historical_total_buy_amount', 'historical_avg_buy_price', 'historical_avg_sell_price']
        columns_query = f"""
        SELECT column_name 
        FROM information_schema.columns 
        WHERE table_schema = '{schema_name}' 
        AND table_name = 'wallet_buy_data' 
        AND column_name IN ('{"', '".join(required_columns)}')
        """
        result = await conn.execute(text(columns_query))
        existing_columns = [row[0] for row in result]
        
        # 檢查必要欄位是否缺失
        missing_columns = [col for col in required_columns if col not in existing_columns]
        
        # 如果有缺失欄位，添加這些欄位
        if missing_columns:
            column_definitions = {
                'historical_total_buy_amount': 'NUMERIC DEFAULT 0',
                'historical_total_buy_cost': 'NUMERIC DEFAULT 0',
                'historical_total_sell_amount': 'NUMERIC DEFAULT 0',
                'historical_total_sell_value': 'NUMERIC DEFAULT 0',
                'historical_avg_buy_price': 'NUMERIC DEFAULT 0',
                'historical_avg_sell_price': 'NUMERIC DEFAULT 0'
            }
            
            for col in missing_columns:
                if col in column_definitions:
                    alter_query = f"""
                    ALTER TABLE {schema_name}.wallet_buy_data 
                    ADD COLUMN IF NOT EXISTS {col} {column_definitions[col]}
                    """
                    await conn.execute(text(alter_query))
                    print(f"已添加欄位 {col} 到 {schema_name}.wallet_buy_data")
        
        # 遷移基本數據
        migration_query = f"""
        UPDATE {schema_name}.wallet_buy_data
        SET 
            historical_total_buy_amount = CASE WHEN total_amount > 0 THEN total_amount ELSE historical_total_buy_amount END,
            historical_total_buy_cost = CASE WHEN total_cost > 0 THEN total_cost ELSE historical_total_buy_cost END,
            historical_total_sell_amount = COALESCE(historical_total_sell_amount, 0),
            historical_total_sell_value = COALESCE(historical_total_sell_value, 0),
            position_opened_at = COALESCE(position_opened_at, updated_at)
        WHERE 
            historical_total_buy_amount IS NULL OR historical_total_buy_amount = 0
        """
        await conn.execute(text(migration_query))
        
        # 更新歷史平均買入價格
        avg_buy_query = f"""
        UPDATE {schema_name}.wallet_buy_data
        SET historical_avg_buy_price = 
            CASE 
                WHEN historical_total_buy_amount > 0 THEN historical_total_buy_cost / historical_total_buy_amount
                WHEN total_amount > 0 THEN total_cost / total_amount
                ELSE COALESCE(avg_buy_price, 0)
            END
        WHERE historical_avg_buy_price IS NULL OR historical_avg_buy_price = 0
        """
        await conn.execute(text(avg_buy_query))
        print(f"已更新 {schema_name}.wallet_buy_data 的 historical_avg_buy_price 欄位")
        
        # 更新歷史平均賣出價格
        avg_sell_query = f"""
        UPDATE {schema_name}.wallet_buy_data
        SET historical_avg_sell_price = 
            CASE 
                WHEN historical_total_sell_amount > 0 THEN historical_total_sell_value / historical_total_sell_amount
                ELSE 0
            END
        WHERE historical_avg_sell_price IS NULL OR historical_avg_sell_price = 0
        """
        await conn.execute(text(avg_sell_query))
        print(f"已更新 {schema_name}.wallet_buy_data 的 historical_avg_sell_price 欄位")
        
        print(f"{schema_name}.wallet_buy_data 表數據遷移完成")
        
    except Exception as e:
        logging.error(f"{schema_name} 數據遷移失敗: {e}")
        raise  # 重新抛出异常，便于上层捕获

async def alter_token_buy_data_tables():
    """手動更新 wallet_buy_data 表結構"""
    for chain, engine in engines.items():
        schema_name = chain.lower()
        async with engine.begin() as conn:
            print(f"正在更新 {schema_name}.wallet_buy_data 表結構...")
            
            # 添加新欄位
            alter_queries = [
                # 先檢查 position_opened_at 的類型，如果是 bigint 則修改為 timestamp
                f"ALTER TABLE {schema_name}.wallet_buy_data ALTER COLUMN position_opened_at TYPE timestamp USING CASE WHEN position_opened_at IS NULL THEN NULL ELSE to_timestamp(position_opened_at) END",
                f"ALTER TABLE {schema_name}.wallet_buy_data ADD COLUMN IF NOT EXISTS historical_total_buy_amount float NOT NULL DEFAULT 0.0",
                f"ALTER TABLE {schema_name}.wallet_buy_data ADD COLUMN IF NOT EXISTS historical_total_buy_cost float NOT NULL DEFAULT 0.0",
                f"ALTER TABLE {schema_name}.wallet_buy_data ADD COLUMN IF NOT EXISTS historical_total_sell_amount float NOT NULL DEFAULT 0.0",
                f"ALTER TABLE {schema_name}.wallet_buy_data ADD COLUMN IF NOT EXISTS historical_total_sell_value float NOT NULL DEFAULT 0.0",
                f"ALTER TABLE {schema_name}.wallet_buy_data ADD COLUMN IF NOT EXISTS last_active_position_closed_at timestamp"
            ]
            
            for query in alter_queries:
                try:
                    await conn.execute(text(query))
                except Exception as e:
                    print(f"執行查詢時出錯: {query}")
                    print(f"錯誤信息: {e}")
                    # 如果是欄位已存在或類型已正確，則跳過
                    if "already exists" in str(e) or "does not exist" in str(e):
                        print(f"跳過此查詢: {query}")
                        continue
                    else:
                        raise
                
            print(f"{schema_name}.wallet_buy_data 表結構更新完成")

# 主程式
if __name__ == '__main__':
    app = Flask(__name__)
    app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv('DATABASE_URI_SWAP')
    app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

    # 初始化資料庫
    asyncio.run(initialize_database())