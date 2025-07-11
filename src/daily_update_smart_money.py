import logging
import traceback
import asyncio
from asyncio import create_task
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.executors.asyncio import AsyncIOExecutor
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import text, select, or_, and_
from models import *
from WalletAnalysis import *
from smart_wallet_filter import filter_smart_wallets
from WalletHolding import calculate_remaining_tokens
import os
import aiohttp
from decimal import Decimal
from dotenv import load_dotenv
from utils_tmp_table import ensure_tmp_tables
load_dotenv()

# 配置日志
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

WALLET_SYNC_API_ENDPOINT = os.getenv("WALLET_SYNC_API_ENDPOINT", "http://172.16.60.141:30008/internal/sync_kol_wallets")

def schedule_daily_updates():
    """
    Schedule the Solana smart money data update and database cleanup to run daily.
    Also starts a continuous task to check for missed transactions and clean inactive wallets.
    """
    scheduler = AsyncIOScheduler()
    scheduler.add_executor(AsyncIOExecutor())

    # Main daily update at midnight
    scheduler.add_job(
        func=update_solana_smart_money_data,
        trigger="cron",
        hour=0,
        minute=0,
        id="daily_solana_update"
    )

    # Clean inactive wallets at 1:00 AM
    scheduler.add_job(
        func=clean_all_inactive_wallets_and_related_data,
        trigger="cron",
        hour=0,
        minute=0,
        id="clean_inactive_wallets"
    )

    scheduler.start()
    logging.info("排程每日任務：智能錢包數據更新在每天 00:00 進行，不活躍錢包清理在每天 01:00 進行。")

    # 立即運行數據更新一次
    asyncio.create_task(update_solana_smart_money_data())

    # 啟動持續檢查未監聽交易的任務（不通過排程器）
    # asyncio.create_task(check_and_update_missed_transactions())

    asyncio.create_task(clean_all_inactive_wallets_and_related_data())

    logging.info("已啟動持續檢查未監聽交易的任務。")

async def clean_inactive_smart_wallets(session: AsyncSession):
    """
    清理 solana.wallet 表中符合以下條件的記錄：
    1. last_transaction_time 超過 30 天
    2. is_active = false
    3. is_smart_wallet = true
    4. total_transaction_num_30d = 0
    """
    try:
        logging.info("開始清理不活躍的智能錢包...")

        # 構建查詢語句
        query = text("""
            DELETE FROM dex_query_v1.wallet
            WHERE last_transaction_time < extract(epoch from now() - interval '30 days')
            AND is_active = false
            AND is_smart_wallet = true
            AND (total_transaction_num_30d = 0 OR total_transaction_num_30d > 3000)
            AND tag != 'kol'
        """)

        # 執行刪除操作
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(query)
        await session.commit()

        # 嘗試獲取受影響的行數 (不是所有資料庫驅動都支援)
        try:
            rows_deleted = result.rowcount
            logging.info(f"成功清理了 {rows_deleted} 個不活躍的智能錢包。")
        except:
            logging.info("成功清理了不活躍的智能錢包。")

    except Exception as e:
        logging.error(f"清理不活躍智能錢包時出錯: {e}")
        logging.error(traceback.format_exc())

        # 出錯時回滾事務
        try:
            await session.rollback()
        except:
            pass

async def clean_inactive_smart_wallets_holdings(session: AsyncSession):
    """
    清理多條區塊鏈（如 Solana、Ethereum 等）中已刪除錢包的持倉記錄
    """
    try:
        query = text(f"""
            DELETE FROM dex_query_v1.wallet_holding
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
            AND chain = 'SOLANA'
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(query)
        await session.commit()

        # 嘗試獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logging.info(f"成功清理了 {rows_deleted} 條已刪除錢包的持倉記錄")
        except:
            logging.info(f"成功清理了已刪除錢包的持倉記錄")

    except Exception as e:
        logging.error(f"清理已刪除錢包的持倉記錄時出錯: {e}")
        logging.error(traceback.format_exc())

async def clean_wallet_transaction_for_deleted_wallets(session: AsyncSession):
    """
    清理多條區塊鏈中已刪除錢包的交易記錄
    """
    try:
        query = text(f"""
            DELETE FROM dex_query_v1.wallet_transaction
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
            AND chain = 'SOLANA'
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(query)
        await session.commit()

        # 嘗試獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logging.info(f"成功清理了 {rows_deleted} 條已刪除錢包的交易記錄")
        except:
            logging.info(f"成功清理了已刪除錢包的交易記錄")

    except Exception as e:
        logging.error(f"清理已刪除錢包的交易紀錄時出錯: {e}")
        logging.error(traceback.format_exc())

async def clean_wallet_buy_data_for_deleted_wallets(session: AsyncSession):
    """
    清理多條區塊鏈中已刪除錢包的代幣購買數據
    """
    try:
        query = text(f"""
            DELETE FROM dex_query_v1.wallet_buy_data
            WHERE wallet_address NOT IN (
                SELECT wallet_address FROM dex_query_v1.wallet
            )
            AND chain = 'SOLANA'
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        result = await session.execute(query)
        await session.commit()

        # 嘗試獲取受影響的行數
        try:
            rows_deleted = result.rowcount
            logging.info(f"成功清理了 {rows_deleted} 條已刪除錢包的代幣購買數據")
        except:
            logging.info(f"成功清理了已刪除錢包的代幣購買數據")

    except Exception as e:
        logging.error(f"清理已刪除錢包的代幣購買數據時出錯: {e}")
        logging.error(traceback.format_exc())

async def clean_all_inactive_wallets_and_related_data():
    """
    執行所有清理任務：清理不活躍的智能錢包及其相關數據
    """
    try:
        logging.info("開始執行定期清理不活躍錢包及相關數據...")

        chain = "SOLANA"
        session_factory = sessions.get(chain.upper())

        if not session_factory:
            logging.error("Solana 鏈的 session factory 未配置。")
            return

        async with session_factory() as session:
            # 1. 首先清理符合條件的錢包
            await clean_inactive_smart_wallets(session)

            # 2. 清理已刪除錢包的相關數據
            await clean_inactive_smart_wallets_holdings(session)
            await clean_wallet_transaction_for_deleted_wallets(session)
            await clean_wallet_buy_data_for_deleted_wallets(session)

        logging.info("不活躍錢包及相關數據的清理已完成。")

    except Exception as e:
        logging.error(f"執行定期清理時發生錯誤: {e}")
        logging.error(traceback.format_exc())

async def process_wallet_with_new_session(session_factory, wallet_address, chain):
    """
    为每个钱包创建独立的 AsyncSession 并处理数据
    """
    try:
        # logging.info(f"[START] 準備分析錢包: {wallet_address} ({chain})")
        async with session_factory() as session:
            try:
                await update_smart_money_data(session, wallet_address, chain)
                # 查詢最新的 WalletSummary 並回傳
                wallet_info = await session.execute(
                    select(WalletSummary).where(WalletSummary.wallet_address == wallet_address)
                )
                wallet_obj = wallet_info.scalar_one_or_none()
                # logging.info(f"[SUCCESS] 完成分析錢包: {wallet_address} ({chain})")
                return wallet_obj
            except Exception as e:
                logging.error(f"[ERROR] update_smart_money_data 發生異常: {wallet_address} ({chain}) - {e}")
                logging.error(traceback.format_exc())
                return None
    except Exception as e:
        logging.error(f"[FATAL] process_wallet_with_new_session 外層異常: {wallet_address} ({chain}) - {e}")
        logging.error(traceback.format_exc())
        return None

def wallet_to_api_dict(wallet) -> dict:
    """將錢包數據轉換為 API 格式"""
    try:
        # 檢查是否有任何交易記錄
        has_transactions = (
            (getattr(wallet, 'total_transaction_num_30d', 0) or 0) > 0 or
            (getattr(wallet, 'total_transaction_num_7d', 0) or 0) > 0 or
            (getattr(wallet, 'total_transaction_num_1d', 0) or 0) > 0
        )
        if not has_transactions:
            logging.info(f"錢包 {wallet.wallet_address} 沒有交易記錄，跳過 API 推送")
            return None
        # logging.info(f"開始轉換錢包數據: {wallet.wallet_address}")
        # 創建新的字典，首先添加必填項
        api_data = {
            "address": wallet.wallet_address,
            "chain": wallet.chain.upper() if wallet.chain else "BSC",
            "last_transaction_time": wallet.last_transaction_time,
            "isActive": wallet.is_active if wallet.is_active is not None else True,
            "walletType": wallet.wallet_type if wallet.wallet_type is not None else 0
        }
        fields_mapping = {
            "balance": "balance",
            "balance_usd": "balanceUsd",
            "tag": "tag",
            "twitter_name": "twitterName",
            "twitter_username": "twitterUsername",
            "is_active": True,
            "is_smart_wallet": True,
            "asset_multiple": "assetMultiple",
            "token_list": "tokenList",
            "avg_cost_30d": "avgCost30d",
            "avg_cost_7d": "avgCost7d",
            "avg_cost_1d": "avgCost1d",
            "total_transaction_num_30d": "totalTransactionNum30d",
            "total_transaction_num_7d": "totalTransactionNum7d",
            "total_transaction_num_1d": "totalTransactionNum1d",
            "buy_num_30d": "buyNum30d",
            "buy_num_7d": "buyNum7d",
            "buy_num_1d": "buyNum1d",
            "sell_num_30d": "sellNum30d",
            "sell_num_7d": "sellNum7d",
            "sell_num_1d": "sellNum1d",
            "win_rate_30d": "winRate30d",
            "win_rate_7d": "winRate7d",
            "win_rate_1d": "winRate1d",
            "pnl_30d": "pnl30d",
            "pnl_7d": "pnl7d",
            "pnl_1d": "pnl1d",
            "pnl_percentage_30d": "pnlPercentage30d",
            "pnl_percentage_7d": "pnlPercentage7d",
            "pnl_percentage_1d": "pnlPercentage1d",
            "pnl_pic_30d": "pnlPic30d",
            "pnl_pic_7d": "pnlPic7d",
            "pnl_pic_1d": "pnlPic1d",
            "unrealized_profit_30d": "unrealizedProfit30d",
            "unrealized_profit_7d": "unrealizedProfit7d",
            "unrealized_profit_1d": "unrealizedProfit1d",
            "total_cost_30d": "totalCost30d",
            "total_cost_7d": "totalCost7d",
            "total_cost_1d": "totalCost1d",
            "avg_realized_profit_30d": "avgRealizedProfit30d",
            "avg_realized_profit_7d": "avgRealizedProfit7d",
            "avg_realized_profit_1d": "avgRealizedProfit1d",
            # distribution 30d
            "distribution_gt500_30d": "distribution_gt500_30d",
            "distribution_200to500_30d": "distribution_200to500_30d",
            "distribution_0to200_30d": "distribution_0to200_30d",
            "distribution_0to50_30d": "distribution_0to50_30d",
            "distribution_lt50_30d": "distribution_lt50_30d",
            "distribution_gt500_percentage_30d": "distribution_gt500_percentage_30d",
            "distribution_200to500_percentage_30d": "distribution_200to500_percentage_30d",
            "distribution_0to200_percentage_30d": "distribution_0to200_percentage_30d",
            "distribution_0to50_percentage_30d": "distribution_0to50_percentage_30d",
            "distribution_lt50_percentage_30d": "distribution_lt50_percentage_30d",
            # distribution 7d
            "distribution_gt500_7d": "distribution_gt500_7d",
            "distribution_200to500_7d": "distribution_200to500_7d",
            "distribution_0to200_7d": "distribution_0to200_7d",
            "distribution_0to50_7d": "distribution_0to50_7d",
            "distribution_lt50_7d": "distribution_lt50_7d",
            "distribution_gt500_percentage_7d": "distribution_gt500_percentage_7d",
            "distribution_200to500_percentage_7d": "distribution_200to500_percentage_7d",
            "distribution_0to200_percentage_7d": "distribution_0to200_percentage_7d",
            "distribution_0to50_percentage_7d": "distribution_0to50_percentage_7d",
            "distribution_lt50_percentage_7d": "distribution_lt50_percentage_7d",
        }
        decimal_fields = {
            "balance", "balance_usd", "asset_multiple",
            "avg_cost_30d", "avg_cost_7d", "avg_cost_1d",
            "total_transaction_num_30d", "total_transaction_num_7d", "total_transaction_num_1d",
            "buy_num_30d", "buy_num_7d", "buy_num_1d",
            "sell_num_30d", "sell_num_7d", "sell_num_1d",
            "win_rate_30d", "win_rate_7d", "win_rate_1d",
            "pnl_30d", "pnl_7d", "pnl_1d",
            "pnl_percentage_30d", "pnl_percentage_7d", "pnl_percentage_1d",
            "unrealized_profit_30d", "unrealized_profit_7d", "unrealized_profit_1d",
            "total_cost_30d", "total_cost_7d", "total_cost_1d",
            "avg_realized_profit_30d", "avg_realized_profit_7d", "avg_realized_profit_1d",
            # distribution 欄位
            "distribution_gt500_30d", "distribution_200to500_30d", "distribution_0to200_30d", "distribution_0to50_30d", "distribution_lt50_30d",
            "distribution_gt500_percentage_30d", "distribution_200to500_percentage_30d", "distribution_0to200_percentage_30d", "distribution_0to50_percentage_30d", "distribution_lt50_percentage_30d",
            "distribution_gt500_7d", "distribution_200to500_7d", "distribution_0to200_7d", "distribution_0to50_7d", "distribution_lt50_7d",
            "distribution_gt500_percentage_7d", "distribution_200to500_percentage_7d", "distribution_0to200_percentage_7d", "distribution_0to50_percentage_7d", "distribution_lt50_percentage_7d"
        }
        for db_field, api_field in fields_mapping.items():
            if hasattr(wallet, db_field):
                value = getattr(wallet, db_field)
                if value is not None:
                    if isinstance(value, Decimal):
                        value = float(value)
                    if db_field in decimal_fields and isinstance(value, (int, float)):
                        value = round(float(value), 10)
                    elif isinstance(value, (int, float)) and abs(value) > 1e8:
                        continue
                    api_data[api_field] = value
        # logging.info(f"成功轉換錢包數據: {wallet.wallet_address}")
        return api_data
    except Exception as e:
        logging.error(f"轉換錢包數據時發生錯誤: {str(e)}")
        logging.error(f"錢包地址: {getattr(wallet, 'wallet_address', 'unknown')}")
        logging.error(traceback.format_exc())
        return None

async def push_wallets_to_api(api_data_list: list) -> bool:
    """批量推送錢包數據到 API"""
    try:
        if not api_data_list:
            logging.warning("API 數據為空，跳過推送")
            return False
        logging.info(f"準備批量推送 {len(api_data_list)} 個錢包數據到 API: {[d.get('address') for d in api_data_list]}")
        async with aiohttp.ClientSession() as session:
            async with session.post(WALLET_SYNC_API_ENDPOINT, json=api_data_list) as resp:
                text = await resp.text()
                if resp.status == 200:
                    logging.info(f"成功批量推送 {len(api_data_list)} 個錢包數據到 API")
                    return True
                else:
                    logging.error(f"API 批量推送失敗: {resp.status}, {text}")
                    logging.error(f"失敗推送的錢包: {[d.get('address') for d in api_data_list]}")
                    return False
    except Exception as e:
        logging.error(f"批量推送到 API 時發生錯誤: {str(e)}")
        logging.error(f"推送內容: {[d.get('address') for d in api_data_list]}")
        return False

async def update_solana_smart_money_data():
    """
    每日更新 Solana 链的活跃钱包数据
    """
    try:
        logging.info("Starting daily update for Solana smart money data...")

        chain = "SOLANA"
        session_factory = sessions.get(chain.upper())
        if not session_factory:
            logging.error("Session factory for Solana chain is not configured.")
            return

        async with session_factory() as session:
            await session.execute(text("SET search_path TO dex_query_v1;"))
            wallets_query = select(WalletSummary).where(
                and_(
                    WalletSummary.chain == 'SOLANA',
                    WalletSummary.is_smart_wallet == True
                )
            )
            result = await session.execute(wallets_query)
            active_wallets = [row.wallet_address for row in result.scalars()]

            if not active_wallets:
                logging.info("No active wallets found for update.")
                return

            batch_size = 5
            for i in range(0, len(active_wallets), batch_size):
                batch_wallets = active_wallets[i:i + batch_size]
                logging.info(f"[BATCH] 開始處理第 {i//batch_size + 1} 批，錢包數: {len(batch_wallets)}")
                tasks = []
                for wallet in batch_wallets:
                    # logging.info(f"[BATCH] 準備分析錢包: {wallet}")
                    task = process_wallet_with_new_session(session_factory, wallet, chain)
                    tasks.append(task)
                results = await asyncio.gather(*tasks)
                # 收集所有成功的 WalletSummary 物件
                updated_wallet_objs = [r for r in results if r is not None]
                # 轉換為 API 格式
                api_data_list = []
                for wallet_obj in updated_wallet_objs:
                    api_dict = wallet_to_api_dict(wallet_obj)
                    if api_dict:
                        api_data_list.append(api_dict)
                # 批量推送
                if api_data_list:
                    await push_wallets_to_api(api_data_list)
                logging.info(f"[BATCH] 完成第 {i//batch_size + 1} 批錢包分析")
            logging.info("Completed daily update for Solana smart money data")
    except Exception as e:
        logging.error(f"Error in update_solana_smart_money_data: {str(e)}")
        traceback.print_exc()

async def clean_old_transactions(session: AsyncSession):
    """
    清理多條區塊鏈（SOLANA、ETH、BASE、TRON）的交易資料。
    """
    chains = ['SOLANA', 'ETH', 'BASE', 'TRON']  # 你想要清理的區塊鏈列表

    try:

        # 清理 30 天前的交易
        query = text(f"""
            DELETE FROM dex_query_v1.wallet_transaction
            WHERE transaction_time < extract(epoch from now() - interval '30 days')
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        await session.execute(query)
        await session.commit()

    except Exception as e:
        logging.error(f"Error cleaning old transactions for multiple chains: {e}")
        logging.error(traceback.format_exc())

async def delete_invalid_smart_wallets(session: AsyncSession):
    """
    刪除 solana.wallet 表中 pnl_percentage_7d > 1000000 或 pnl_percentage_30d > 1000000
    且 is_smart_wallet = True 的資料
    """
    try:
        logging.info("Deleting invalid smart wallets from solana.wallet...")
        query = text("""
            DELETE FROM dex_query_v1.wallet
            WHERE is_smart_wallet = TRUE
              AND (pnl_percentage_7d > 1000000 OR pnl_percentage_30d > 1000000)
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        await session.execute(query)
        await session.commit()
        logging.info("Invalid smart wallet deletion completed.")
    except Exception as e:
        logging.error(f"Error deleting invalid smart wallets: {e}")
        logging.error(traceback.format_exc())

async def clean_invalid_marketcap(session: AsyncSession):
    """
    更新 solana.wallet_transaction 表中 marketcap 大於 100,000,000,000 的交易
    將其 marketcap 除以 10^6
    """
    try:
        logging.info("Updating transactions with marketcap > 100,000,000,000 in solana.wallet_transaction...")

        # SQL 更新語句
        query = text("""
            UPDATE dex_query_v1.wallet_transaction
            SET marketcap = marketcap / (10 ^ 6)
            WHERE marketcap > 100000000000
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        await session.execute(query)
        await session.commit()

        logging.info("Invalid marketcap transactions update completed.")
    except Exception as e:
        logging.error(f"Error updating invalid marketcap transactions: {e}")
        logging.error(traceback.format_exc())

async def fix_holding_percentage(session: AsyncSession):
    """
    將 solana.wallet_transaction 表中 holding_percentage > 100 的記錄修正為 100
    """
    try:
        logging.info("Fixing transactions with holding_percentage > 100 in solana.wallet_transaction...")
        query = text("""
            UPDATE dex_query_v1.wallet_transaction
            SET holding_percentage = 100
            WHERE holding_percentage > 100
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        await session.execute(query)
        await session.commit()
        logging.info("Holding percentage fix completed.")
    except Exception as e:
        logging.error(f"Error fixing holding percentage: {e}")
        logging.error(traceback.format_exc())

async def fix_realized_profit_percentage(session: AsyncSession):
    """
    將 solana.wallet_transaction 表中 realized_profit_percentage < -100 的記錄修正為 -100
    """
    try:
        logging.info("Fixing transactions with realized_profit_percentage < -100 in solana.wallet_transaction...")
        query = text("""
            UPDATE dex_query_v1.wallet_transaction
            SET realized_profit_percentage = -100
            WHERE realized_profit_percentage < -100
        """)
        await session.execute(text("SET search_path TO dex_query_v1;"))
        await session.execute(query)
        await session.commit()
        logging.info("Realized profit percentage fix completed.")
    except Exception as e:
        logging.error(f"Error fixing realized profit percentage: {e}")
        logging.error(traceback.format_exc())

async def main():
    """
    主入口，啟動事件循環並調用定時任務。
    """
    # 啟動時檢查/建立臨時表
    chain = "SOLANA"
    session_factory = sessions.get(chain.upper())
    if session_factory:
        async with session_factory() as session:
            await ensure_tmp_tables(session)
    schedule_daily_updates()  # 啟動定時任務
    logging.info("Scheduler started. Running the event loop...")
    while True:
        await asyncio.sleep(3600)  # 避免主協程結束，讓事件循環保持運行

if __name__ == "__main__":
    try:
        asyncio.run(main())  # 使用 asyncio.run 啟動事件循環
    except (KeyboardInterrupt, SystemExit):
        logging.info("Shutting down scheduler...")