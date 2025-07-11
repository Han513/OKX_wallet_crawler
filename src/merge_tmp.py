import asyncio
from models import sessions
from sqlalchemy import text
from datetime import datetime, timezone, timedelta

def now_utc8():
    """獲取UTC+8當前時間"""
    tz_utc8 = timezone(timedelta(hours=8))
    return datetime.now(tz_utc8)

# 各表主鍵與更新欄位
TABLES = [
    {
        "table": "wallet_holding",
        "conflict_cols": ["wallet_address", "token_address", "chain"],
        "update_cols": [
            'token_icon', 'token_name', 'chain_id', 'amount', 'value', 'value_usdt', 'unrealized_profits', 'pnl', 'pnl_percentage', 'avg_price', 'marketcap', 'is_cleared', 'cumulative_cost', 'cumulative_profit', 'last_transaction_time', 'time'
        ]
    },
    {
        "table": "wallet",
        "conflict_cols": ["wallet_address"],
        "update_cols": [
            'balance', 'balance_usd', 'chain', 'chain_id', 'tag', 'twitter_name', 'twitter_username', 'is_smart_wallet', 'wallet_type', 'asset_multiple', 'token_list', 'avg_cost_30d', 'avg_cost_7d', 'avg_cost_1d', 'total_transaction_num_30d', 'total_transaction_num_7d', 'total_transaction_num_1d', 'buy_num_30d', 'buy_num_7d', 'buy_num_1d', 'sell_num_30d', 'sell_num_7d', 'sell_num_1d', 'win_rate_30d', 'win_rate_7d', 'win_rate_1d', 'pnl_30d', 'pnl_7d', 'pnl_1d', 'pnl_percentage_30d', 'pnl_percentage_7d', 'pnl_percentage_1d', 'pnl_pic_30d', 'pnl_pic_7d', 'pnl_pic_1d', 'unrealized_profit_30d', 'unrealized_profit_7d', 'unrealized_profit_1d', 'total_cost_30d', 'total_cost_7d', 'total_cost_1d', 'avg_realized_profit_30d', 'avg_realized_profit_7d', 'avg_realized_profit_1d', 'distribution_gt500_30d', 'distribution_200to500_30d', 'distribution_0to200_30d', 'distribution_0to50_30d', 'distribution_lt50_30d', 'distribution_gt500_percentage_30d', 'distribution_200to500_percentage_30d', 'distribution_0to200_percentage_30d', 'distribution_0to50_percentage_30d', 'distribution_lt50_percentage_30d', 'distribution_gt500_7d', 'distribution_200to500_7d', 'distribution_0to200_7d', 'distribution_0to50_7d', 'distribution_lt50_7d', 'distribution_gt500_percentage_7d', 'distribution_200to500_percentage_7d', 'distribution_0to200_percentage_7d', 'distribution_0to50_percentage_7d', 'distribution_lt50_percentage_7d', 'update_time', 'last_transaction_time', 'is_active'
        ]
    },
    {
        "table": "wallet_transaction",
        "conflict_cols": ["signature", "wallet_address", "token_address", "transaction_time"],
        "update_cols": [
            'wallet_address', 'wallet_balance', 'token_address', 'token_icon', 'token_name', 'price', 'amount', 'marketcap', 'value', 'holding_percentage', 'chain', 'chain_id', 'realized_profit', 'realized_profit_percentage', 'transaction_type', 'transaction_time', 'time', 'from_token_address', 'from_token_symbol', 'from_token_amount', 'dest_token_address', 'dest_token_symbol', 'dest_token_amount'
        ]
    },
    {
        "table": "wallet_buy_data",
        "conflict_cols": ["wallet_address", "token_address"],
        "update_cols": [
            'total_amount', 'total_cost', 'avg_buy_price', 'position_opened_at', 'chain', 'chain_id', 'historical_total_buy_amount', 'historical_total_buy_cost', 'historical_total_sell_amount', 'historical_total_sell_value', 'historical_avg_buy_price', 'historical_avg_sell_price', 'last_active_position_closed_at', 'last_transaction_time', 'date', 'realized_profit', 'updated_at'
        ]
    },
    {
        "table": "wallet_token_state",
        "conflict_cols": ["wallet_address", "token_address"],
        "update_cols": [
            'chain', 'chain_id', 'current_amount', 'current_total_cost', 'current_avg_buy_price', 'position_opened_at', 'historical_buy_amount', 'historical_buy_cost', 'historical_sell_amount', 'historical_sell_value', 'historical_buy_count', 'historical_sell_count', 'last_transaction_time', 'historical_realized_pnl', 'updated_at'
        ]
    }
]

BATCH_SIZE = 10000

async def merge_table_in_batches(session, table, conflict_cols, update_cols, batch_size=BATCH_SIZE):
    tmp_table = f"dex_query_v1.{table}_tmp"
    main_table = f"dex_query_v1.{table}"

    # 查主表欄位
    col_sql = text(f"""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema = 'dex_query_v1' AND table_name = '{table}'
        ORDER BY ordinal_position
    """)
    result = await session.execute(col_sql)
    columns = [row[0] for row in result.fetchall()]
    col_str = ', '.join(columns)

    count_sql = text(f"SELECT COUNT(*) FROM {tmp_table};")
    result = await session.execute(count_sql)
    total = result.scalar()
    if total == 0:
        print(f"[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] {table}: 無需合併，tmp表為空")
        return
    print(f"[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] {table}: 準備合併 {total} 筆資料，分批進行")
    for offset in range(0, total, batch_size):
        sql = text(f'''
        INSERT INTO {main_table} ({col_str})
        SELECT {col_str} FROM {tmp_table} OFFSET {offset} LIMIT {batch_size}
        ON CONFLICT ({', '.join(conflict_cols)})
        DO UPDATE SET {', '.join([f"{col}=EXCLUDED.{col}" for col in update_cols])};
        ''')
        await session.execute(sql)
        await session.commit()
        print(f"[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] {table}: 已合併 {min(offset+batch_size, total)}/{total}")

async def merge_chain(chain_name):
    print(f"\n[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] ==== 開始合併 {chain_name} ====")
    session_factory = sessions[chain_name]
    async with session_factory() as session:
        for t in TABLES:
            await merge_table_in_batches(session, t["table"], t["conflict_cols"], t["update_cols"])
    print(f"[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] ==== {chain_name} 合併完成 ====")

async def merge_all_chains():
    """合併所有鏈的tmp表到主表（因為都寫到同一張表，所以只需要合併一次）"""
    print(f"\n[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] ==== 開始合併所有tmp表到主表 ====")
    
    # 使用 SOLANA 的 session factory 即可（因為表結構相同）
    session_factory = sessions["SOLANA"]
    async with session_factory() as session:
        for t in TABLES:
            await merge_table_in_batches(session, t["table"], t["conflict_cols"], t["update_cols"])
    
    print(f"[{now_utc8().strftime('%Y-%m-%d %H:%M:%S')}] ==== 所有tmp表合併完成 ====")

async def main():
    await merge_all_chains()

if __name__ == "__main__":
    asyncio.run(main())