from sqlalchemy import text, inspect, Integer, BIGINT, Float, String, Boolean, DateTime, Text
import models

TMP_TABLES = [
    ("wallet_holding", "wallet_holding_tmp", "Holding"),
    ("wallet", "wallet_tmp", "WalletSummary"),
    ("wallet_transaction", "wallet_transaction_tmp", "Transaction"),
    ("wallet_buy_data", "wallet_buy_data_tmp", "TokenBuyData"),
    ("wallet_token_state", "wallet_token_state_tmp", "TokenState"),
]

CREATE_TMP_SQL = """
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'dex_query_v1' AND table_name = '{tmp}'
    ) THEN
        EXECUTE 'CREATE TABLE dex_query_v1.{tmp} (LIKE dex_query_v1.{main} INCLUDING ALL)';
    END IF;
END$$;
"""

async def ensure_tmp_table_columns(session, model):
    """
    檢查並自動補齊臨時表缺少的欄位
    """
    table_name = model.__tablename__
    
    # 正確處理 __table_args__，它可能是元組或字典
    schema = 'dex_query_v1'  # 預設值
    if hasattr(model, '__table_args__') and model.__table_args__:
        if isinstance(model.__table_args__, dict):
            schema = model.__table_args__.get('schema', 'dex_query_v1')
        elif isinstance(model.__table_args__, tuple):
            # 如果是元組，通常第一個元素是字典
            if len(model.__table_args__) > 0 and isinstance(model.__table_args__[0], dict):
                schema = model.__table_args__[0].get('schema', 'dex_query_v1')
    
    # 簡化處理：直接從 information_schema 查詢欄位
    try:
        # 查詢現有欄位
        columns_sql = text("""
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = :schema AND table_name = :table_name
        """)
        result = await session.execute(columns_sql, {"schema": schema, "table_name": table_name})
        columns_in_db = [row[0] for row in result.fetchall()]
        
        # 取得模型定義的欄位
        columns_in_model = [col.name for col in model.__table__.columns]
        missing = set(columns_in_model) - set(columns_in_db)
        
        for col in model.__table__.columns:
            if col.name in missing:
                # 只處理常見型別
                col_type = None
                if isinstance(col.type, (Integer, BIGINT)):
                    col_type = 'bigint' if isinstance(col.type, BIGINT) else 'integer'
                elif isinstance(col.type, Float):
                    col_type = 'double precision'
                elif isinstance(col.type, String):
                    col_type = f'character varying({col.type.length or 255})'
                elif isinstance(col.type, Boolean):
                    col_type = 'boolean'
                elif isinstance(col.type, DateTime):
                    col_type = 'timestamp'
                elif isinstance(col.type, Text):
                    col_type = 'text'
                else:
                    print(f"[ensure_tmp_table_columns] 跳過不支援型別: {col.name} {col.type}")
                    continue
                
                alter_sql = f'ALTER TABLE {schema}.{table_name} ADD COLUMN IF NOT EXISTS {col.name} {col_type};'
                await session.execute(text(alter_sql))
                print(f"[ensure_tmp_table_columns] 自動補齊欄位: {col.name} ({col_type}) 到 {table_name}")
        
        await session.commit()
        
    except Exception as e:
        print(f"[ensure_tmp_table_columns] 處理 {schema}.{table_name} 欄位失敗: {e}")
        await session.rollback()

async def ensure_tmp_tables(session):
    for main, tmp, model_name in TMP_TABLES:
        sql = CREATE_TMP_SQL.format(main=main, tmp=tmp)
        await session.execute(text(sql))
        # 自動補欄位
        model = getattr(models, model_name, None)
        if model is not None:
            await ensure_tmp_table_columns(session, model)
        else:
            print(f"[ensure_tmp_tables] 找不到 model: {model_name}，略過欄位檢查")
    await session.commit()

async def merge_tmp_to_main(session, table, conflict_cols, update_cols):
    # 根據主鍵 upsert
    set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in update_cols])
    sql = f'''
    INSERT INTO dex_query_v1.{table} (SELECT * FROM dex_query_v1.{table}_tmp)
    ON CONFLICT ({', '.join(conflict_cols)})
    DO UPDATE SET {set_clause};
    '''
    await session.execute(text(sql))
    await session.commit()

async def clear_tmp_table(session, table):
    sql = f"TRUNCATE TABLE dex_query_v1.{table}_tmp;"
    await session.execute(text(sql))
    await session.commit() 