import base58
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from models import save_wallet_buy_data, clear_all_holdings, get_token_buy_data, save_holding2
from token_info import TokenUtils
from decimal import Decimal, getcontext
from loguru_logger import *
from solders.pubkey import Pubkey
from models import get_wallet_token_holdings

token_supply_cache = {}
getcontext().prec = 28 

USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
WSOL_MINT = "So11111111111111111111111111111111111111112"

def make_naive_time(dt):
    """将带时区的时间转换为无时区时间"""
    if isinstance(dt, datetime) and dt.tzinfo is not None:
        return dt.replace(tzinfo=None)
    return dt

async def get_token_supply(client, mint: str) -> float:
    if mint in token_supply_cache:
        return token_supply_cache[mint]

    try:
        supply_data = await client.get_token_supply(Pubkey(base58.b58decode(mint)))
        supply = int(supply_data.value.amount) / (10 ** supply_data.value.decimals)
        
        # 檢查supply是否小於或等於0，如果是則替換為1000000000
        if supply <= 0:
            supply = 1000000000
            
        token_supply_cache[mint] = supply
        return supply
    except Exception:
        return 1000000000

@async_log_execution_time
async def calculate_remaining_tokens(transactions, wallet_address, session, chain, client):
    try:
        # 初始化字典來存儲每個代幣的交易數據
        token_summary = defaultdict(lambda: {
            'buy_amount': Decimal('0'),
            'sell_amount': Decimal('0'),
            'cost': Decimal('0'),
            'profit': Decimal('0'),
            'marketcap': Decimal('0'),
            'buy_count': 0,
            'last_transaction_time': 0
        })

        # print(f"處理錢包 {wallet_address} 的持倉，交易數量: {len(transactions[wallet_address])}")

        # 遍歷所有交易（格式為 {token_mint: {數據}, 'timestamp': 時間}）
        for tx in transactions[wallet_address]:
            tx_timestamp = tx.get("timestamp", 0)
            token_mint = tx.get("token_mint")
            
            if not token_mint or token_mint.lower() in [USDC_MINT.lower(), USDT_MINT.lower(), WSOL_MINT.lower()]:
                continue
            sol_price_usd = Decimal(str(tx.get('sol_price_usd', 0)))
            # 根據交易類型更新對應的數據
            if tx.get("transaction_type") == "BUY" or tx.get("transaction_type") == "buy":
                buy_amount = Decimal(str(tx.get('token_amount', 0)))
                sol_amount = Decimal(str(tx.get('sol_amount', 0)))
                
                token_summary[token_mint]['buy_amount'] += buy_amount
                token_summary[token_mint]['buy_count'] += 1
                
                # 追蹤成本 (使用 SOL 金額作為成本)
                token_summary[token_mint]['cost'] += sol_amount * sol_price_usd
            elif tx.get("transaction_type") == "SELL" or tx.get("transaction_type") == "sell":
                sell_amount = Decimal(str(tx.get('token_amount', 0)))
                sol_amount = Decimal(str(tx.get('sol_amount', 0)))
                
                token_summary[token_mint]['sell_amount'] += sell_amount
                
                # 追蹤利潤 (使用 SOL 金額作為利潤)
                token_summary[token_mint]['profit'] += sol_amount * sol_price_usd
            
            # 更新最後交易時間
            token_summary[token_mint]['last_transaction_time'] = max(
                token_summary[token_mint]['last_transaction_time'], 
                tx_timestamp
            )
        # 打印每個代幣的買入/賣出數量，用於調試
        # for token_mint, data in token_summary.items():
        #     buy_amount = data['buy_amount']
        #     sell_amount = data['sell_amount']
        #     remaining = buy_amount - sell_amount
        #     print(f"代幣 {token_mint}: 買入={float(buy_amount)}, 賣出={float(sell_amount)}, 剩餘={float(remaining)}")
        
        # 篩選出仍然持有代幣的交易（放寬條件）
        remaining_tokens = {
            token_address: data for token_address, data in token_summary.items()
            if (data['buy_amount'] - data['sell_amount']) > Decimal('0.0000001')  # 使用更小的閾值
        }
        
        # print(f"找到 {len(remaining_tokens)} 個有持倉的代幣")
        
        # 如果沒有任何持倉，刪除所有持倉記錄
        if not remaining_tokens:
            # print(f"錢包 {wallet_address} 沒有剩餘持倉，清理所有持倉記錄")
            await clear_all_holdings(wallet_address, session, chain)
            return  # 直接返回，避免繼續執行

        tx_data_list = []
        # 保存持倉到資料庫
        for token_address, token_data in remaining_tokens.items():
            token_info = TokenUtils.get_token_info(token_address)
            
            supply = Decimal(str(await get_token_supply(client, token_address) or 1000000000))
            
            # 確保 token_buy_data 中的值也是 Decimal
            # token_buy_data = await get_token_buy_data(wallet_address, token_address, session, chain)
            # avg_buy_price = Decimal(str(token_buy_data.get("avg_buy_price", 0)))

            # 計算買入均價 (使用 Decimal)
            if token_data['buy_amount'] > 0:
                buy_price = token_data['cost'] / token_data['buy_amount']
            else:
                buy_price = Decimal('0')  # 沒有買入量時，買入均價為 0
            formatted_buy_price = buy_price.quantize(Decimal('0.0000000000'))
            marketcap = formatted_buy_price * supply

            # 獲取代幣即時價格 (確保都是 Decimal)
            url = token_info.get('url', {})
            symbol = token_info.get('symbol', "")
            token_price = Decimal(str(token_info.get('priceNative', 0)))
            token_price_USDT = Decimal(str(token_info.get('priceUsd', 0)))

            # 計算當前持倉量 (使用 Decimal)
            current_amount = token_data['buy_amount'] - token_data['sell_amount']
            
            # 防止持倉數量變為負值
            if current_amount < 0:
                current_amount = Decimal('0')

            # 若 current_amount < 0.001，則當作清倉
            if current_amount < Decimal('0.001'):
                continue  # 跳過該代幣，不計入持倉

            # 計算持倉價值 (使用 Decimal)
            value = current_amount * token_price_USDT
            unrealized_profit = (token_price_USDT - buy_price) * current_amount

            # 計算總 PnL (使用 Decimal)
            total_pnl = (value - token_data['cost']) + token_data['profit']
            if token_data['cost'] > 0:
                pnl_percentage = max((token_data['profit'] - token_data['cost']) / token_data['cost'] * Decimal("100"), -100)
            else:
                pnl_percentage = Decimal("0")

            # 計算 PnL 百分比 (使用 Decimal)
            if buy_price > 0:
                unrealized_pnl_percentage = max(((token_price_USDT - buy_price) / buy_price) * Decimal("100"), -100)
            else:
                unrealized_pnl_percentage = Decimal("0")

            remaining_cost = buy_price * current_amount

            # 準備寫入資料庫的數據 (轉換為 float 時使用 str 格式化)
            tx_data = {
                "token_address": token_address,
                "token_icon": url,
                "token_name": symbol,
                "chain": "SOLANA",
                "buy_amount": float("{:.10f}".format(token_data['buy_amount'])),
                "sell_amount": float("{:.10f}".format(token_data['sell_amount'])),
                "amount": float("{:.10f}".format(current_amount)),
                "value": float("{:.10f}".format(value)),
                "value_usdt": float("{:.10f}".format(current_amount * token_price_USDT)),
                "cost": float("{:.10f}".format(token_data['cost'])),
                "profit": float("{:.10f}".format(token_data['profit'])),
                "pnl": float("{:.10f}".format(total_pnl)),
                "unrealized_profit": float("{:.10f}".format(unrealized_profit)),
                "unrealized_pnl_percentage": float("{:.10f}".format(unrealized_pnl_percentage)),
                "pnl_percentage": float("{:.10f}".format(pnl_percentage)),
                "avg_price": float("{:.10f}".format(formatted_buy_price)),
                "marketcap": float("{:.10f}".format(marketcap)),
                "last_transaction_time": make_naive_time(token_data['last_transaction_time']),
                "time": make_naive_time(datetime.now()),
            }
            tx_data_list.append(tx_data)
        # for token_address, data in token_summary.items():
        #     buy_amount = data['buy_amount']
        #     sell_amount = data['sell_amount']
        #     remaining = buy_amount - sell_amount
        #     print(f"代幣 {token_address}: 買入={buy_amount}, 賣出={sell_amount}, 剩餘={remaining}")
        #     if remaining > Decimal('0.000001'):
        #         print(f"保留持倉: {token_address}, 剩餘數量: {remaining}")
        # 調用 save_holding 函數寫入到資料庫
        await save_holding2(tx_data_list, wallet_address, session, chain)
        # print(f"成功為錢包 {wallet_address} 保存了 {len(tx_data_list)} 筆持倉記錄")

    except Exception as e:
        print(f"Error while processing and saving holdings: {e}")
        import traceback
        traceback.print_exc()

async def calculate_remaining_tokens_optimized(wallet_address, session, chain, client):
    """
    優化版本的calculate_remaining_tokens函數 - 直接從TokenBuyData獲取數據
    """
    try:
        # 獲取此錢包所有有持倉的代幣記錄
        token_buy_data_records = await get_wallet_token_holdings(wallet_address, session, chain)
        
        # 如果沒有任何持倉，清理持倉記錄
        if not token_buy_data_records:
            # print(f"錢包 {wallet_address} 沒有剩餘持倉，清理所有持倉記錄")
            await clear_all_holdings(wallet_address, session, chain)
            return
        
        tx_data_list = []
        
        # 為每個有持倉的代幣準備holding記錄
        for token_data in token_buy_data_records:
            token_address = token_data.token_address
            
            # 從TokenUtils獲取代幣信息
            token_info = TokenUtils.get_token_info(token_address)
            
            # 獲取代幣供應量
            supply = Decimal(str(await get_token_supply(client, token_address) or 1000000000))
            
            # 從token_data獲取買入均價
            buy_price = Decimal(str(token_data.avg_buy_price))
            formatted_buy_price = buy_price.quantize(Decimal('0.0000000000'))
            marketcap = formatted_buy_price * supply
            
            # 獲取代幣即時價格
            url = token_info.get('url', {})
            symbol = token_info.get('symbol', "")
            token_price = Decimal(str(token_info.get('priceNative', 0)))
            token_price_USDT = Decimal(str(token_info.get('priceUsd', 0)))
            
            # 當前持倉量
            current_amount = Decimal(str(token_data.total_amount))
            
            # 防止持倉數量變為負值
            if current_amount < 0:
                current_amount = Decimal('0')
            
            # 若 current_amount < 0.001，則當作清倉
            if current_amount < Decimal('0.001'):
                continue
            
            # 計算持倉價值
            value = current_amount * token_price_USDT
            cost = Decimal(str(token_data.total_cost))
            
            # 計算未實現損益
            unrealized_profit = (token_price_USDT - buy_price) * current_amount
            
            # 計算歷史已實現損益
            historical_sell_value = Decimal(str(token_data.historical_total_sell_value))
            historical_buy_cost = Decimal(str(token_data.historical_total_buy_cost))
            historical_sell_amount = Decimal(str(token_data.historical_total_sell_amount))
            historical_buy_amount = Decimal(str(token_data.historical_total_buy_amount))
            
            # 計算已實現損益 (假設按比例分配成本)
            if historical_buy_amount > 0:
                realized_profit = Decimal(str(token_data.realized_profit))
            else:
                realized_profit = Decimal('0')
            
            # 計算總PnL
            total_pnl = unrealized_profit + realized_profit
            
            # 計算PnL百分比
            if cost > 0:
                pnl_percentage = max((realized_profit - cost) / cost * Decimal("100"), -100)
            else:
                pnl_percentage = Decimal("0")
            
            # 計算未實現PnL百分比
            if buy_price > 0:
                unrealized_pnl_percentage = max(((token_price_USDT - buy_price) / buy_price) * Decimal("100"), -100)
            else:
                unrealized_pnl_percentage = Decimal("0")
            
            position_opened_at = token_data.position_opened_at
            # 處理 position_opened_at，確保它是正確的時間戳格式
            if position_opened_at is None:
                timestamp_in_seconds = 0
            elif isinstance(position_opened_at, (int, float)):
                # 如果已經是時間戳，直接使用
                timestamp_in_seconds = int(position_opened_at)
            elif hasattr(position_opened_at, 'timestamp'):
                # 如果是 datetime 對象，轉換為時間戳
                timestamp_in_seconds = int(position_opened_at.timestamp())
            else:
                # 其他情況，設為 0
                timestamp_in_seconds = 0

            # 準備holding數據
            tx_data = {
                "token_address": token_address,
                "token_icon": url,
                "token_name": symbol,
                "chain": "SOLANA",
                "buy_amount": float("{:.10f}".format(historical_buy_amount)),
                "sell_amount": float("{:.10f}".format(historical_sell_amount)),
                "amount": float("{:.10f}".format(current_amount)),
                "value": float("{:.10f}".format(value)),
                "value_usdt": float("{:.10f}".format(value)),
                "cost": float("{:.10f}".format(historical_buy_cost)),
                "profit": float("{:.10f}".format(realized_profit)),
                "pnl": float("{:.10f}".format(total_pnl)),
                "unrealized_profit": float("{:.10f}".format(unrealized_profit)),
                "unrealized_pnl_percentage": float("{:.10f}".format(unrealized_pnl_percentage)),
                "pnl_percentage": float("{:.10f}".format(pnl_percentage)),
                "avg_price": float("{:.10f}".format(formatted_buy_price)),
                "marketcap": float("{:.10f}".format(marketcap)),
                "last_transaction_time": timestamp_in_seconds,
                "time": make_naive_time(datetime.now()),
            }
            
            tx_data_list.append(tx_data)
        
        # 保存holding記錄
        await save_holding2(tx_data_list, wallet_address, session, chain)
        # print(f"成功為錢包 {wallet_address} 保存了 {len(tx_data_list)} 筆持倉記錄")
        
    except Exception as e:
        print(f"Error while processing and saving holdings: {e}")
        import traceback
        traceback.print_exc()

async def calculate_token_statistics(transactions, wallet_address, session, chain, client):
    """
    計算錢包中所有代幣的交易統計資料，包括歷史買賣數據和當前持倉，並寫入TokenBuyData表
    """
    try:
        # 初始化字典來存儲每個代幣的交易數據
        token_summary = defaultdict(lambda: {
            # 當前持倉數據
            'total_amount': Decimal('0'),
            'total_cost': Decimal('0'),
            'buys': [],  # 用於FIFO計算的買入批次
            'position_opened_at': None,
            'realized_profit': Decimal('0'),
            
            # 歷史累計數據
            'historical_total_buy_amount': Decimal('0'),
            'historical_total_buy_cost': Decimal('0'),
            'historical_total_sell_amount': Decimal('0'),
            'historical_total_sell_value': Decimal('0'),
            'last_active_position_closed_at': None,
            
            # 其他數據
            'last_transaction_time': 0,
            'has_transactions': False  # 新增標記，表示是否有交易記錄
        })

        # print(f"處理錢包 {wallet_address} 的交易數據，交易數量: {len(transactions[wallet_address])}")

        # 按照時間戳排序交易，確保FIFO計算正確
        all_txs = sorted(transactions[wallet_address], key=lambda tx: tx.get('timestamp', 0))

        # 遍歷所有交易
        for tx in all_txs:
            tx_timestamp = tx.get("timestamp", 0)
            token_mint = tx.get("token_mint")
            realized_profit = Decimal(str(tx.get('realized_profit', 0)))
            
            # 跳過穩定幣、SOL等代幣
            if not token_mint or token_mint.lower() in [USDC_MINT.lower(), USDT_MINT.lower(), WSOL_MINT.lower()]:
                continue
                
            tx_type = tx.get("transaction_type", "").upper()
            token_amount = Decimal(str(tx.get('token_amount', 0)))
            tx_value = Decimal(str(tx.get('value', 0)))  # 交易價值
            
            # 更新該代幣的最後交易時間
            token_summary[token_mint]['last_transaction_time'] = max(
                token_summary[token_mint]['last_transaction_time'], 
                tx_timestamp
            )
            
            # 處理買入交易
            if tx_type == "BUY" or tx_type == "BUILD":
                # 記錄買入批次(用於FIFO計算)
                token_summary[token_mint]['buys'].append({
                    'amount': token_amount,
                    'cost': tx_value,
                    'timestamp': tx_timestamp
                })
                
                # 更新當前持倉數據
                token_summary[token_mint]['total_amount'] += token_amount
                token_summary[token_mint]['total_cost'] += tx_value
                
                # 更新首次買入時間（如果尚未設置且當前持倉大於0）
                if token_summary[token_mint]['position_opened_at'] is None and token_summary[token_mint]['total_amount'] > 0:
                    token_summary[token_mint]['position_opened_at'] = datetime.fromtimestamp(tx_timestamp)
                
                # 更新歷史累計買入數據
                token_summary[token_mint]['historical_total_buy_amount'] += token_amount
                token_summary[token_mint]['historical_total_buy_cost'] += tx_value
                token_summary[token_mint]['has_transactions'] = True
                
            # 處理賣出交易
            elif tx_type == "SELL" or tx_type == "CLEAN":
                # 更新歷史累計賣出數據
                token_summary[token_mint]['historical_total_sell_amount'] += token_amount
                token_summary[token_mint]['historical_total_sell_value'] += tx_value
                token_summary[token_mint]['realized_profit'] += realized_profit
                
                # 使用FIFO方法計算持倉變化
                remaining_to_sell = token_amount
                
                # 從最早的買入批次開始賣出
                while remaining_to_sell > 0 and token_summary[token_mint]['buys']:
                    oldest_buy = token_summary[token_mint]['buys'][0]
                    
                    if oldest_buy['amount'] <= remaining_to_sell:
                        # 整個批次都被賣出
                        remaining_to_sell -= oldest_buy['amount']
                        token_summary[token_mint]['buys'].pop(0)
                    else:
                        # 只賣出部分批次
                        sell_ratio = remaining_to_sell / oldest_buy['amount']
                        partial_cost = oldest_buy['cost'] * sell_ratio
                        oldest_buy['cost'] -= partial_cost
                        oldest_buy['amount'] -= remaining_to_sell
                        remaining_to_sell = Decimal('0')
                
                # 更新當前持倉量和成本
                token_summary[token_mint]['total_amount'] = max(Decimal('0'), token_summary[token_mint]['total_amount'] - token_amount)
                token_summary[token_mint]['total_cost'] = sum(buy['cost'] for buy in token_summary[token_mint]['buys'])
                
                # 如果完全賣出，更新相關時間並重置
                if token_summary[token_mint]['total_amount'] <= Decimal('0.000001'):
                    token_summary[token_mint]['total_amount'] = Decimal('0')
                    token_summary[token_mint]['total_cost'] = Decimal('0')
                    token_summary[token_mint]['last_active_position_closed_at'] = datetime.fromtimestamp(tx_timestamp)
                    
                    # 只有在完全清倉後才重置開倉時間
                    if len(token_summary[token_mint]['buys']) == 0:
                        token_summary[token_mint]['position_opened_at'] = None
                token_summary[token_mint]['has_transactions'] = True

        # 向資料庫寫入每個代幣的統計數據（無論是否有持倉）
        for token_address, stats in token_summary.items():
            if not stats['has_transactions'] and stats['total_amount'] <= Decimal('0.000001'):
                continue
            # 計算平均買入價格
            historical_avg_buy_price = Decimal('0')
            if stats['historical_total_buy_amount'] > 0:
                historical_avg_buy_price = stats['historical_total_buy_cost'] / stats['historical_total_buy_amount']
            
            # 計算平均賣出價格
            historical_avg_sell_price = Decimal('0')
            if stats['historical_total_sell_amount'] > 0:
                historical_avg_sell_price = stats['historical_total_sell_value'] / stats['historical_total_sell_amount']
            
            # 計算當前持倉的平均買入價格
            avg_buy_price = Decimal('0')
            if stats['total_amount'] > 0:
                avg_buy_price = stats['total_cost'] / stats['total_amount']
            
            # 組裝資料庫所需格式
            token_data = {
                'token_address': token_address,
                
                # 當前持倉數據
                'total_amount': float(stats['total_amount']),
                'total_cost': float(stats['total_cost']),
                'avg_buy_price': float(avg_buy_price),
                'position_opened_at': stats['position_opened_at'],
                'realized_profit': stats['realized_profit'],
                
                # 歷史累計數據
                'historical_total_buy_amount': float(stats['historical_total_buy_amount']),
                'historical_total_buy_cost': float(stats['historical_total_buy_cost']),
                'historical_total_sell_amount': float(stats['historical_total_sell_amount']),
                'historical_total_sell_value': float(stats['historical_total_sell_value']),
                'historical_avg_buy_price': float(historical_avg_buy_price),
                'historical_avg_sell_price': float(historical_avg_sell_price),
                'last_active_position_closed_at': stats['last_active_position_closed_at'],
                'last_transaction_time': stats['last_transaction_time']
            }

            try:
                await save_wallet_buy_data(token_data, wallet_address, session, chain)
            except Exception as e:
                print(f"保存錢包 {wallet_address} 代幣 {token_address} 統計數據時出錯: {e}")
                import traceback
                traceback.print_exc()

        # print(f"成功為錢包 {wallet_address} 保存了 {len(token_summary)} 筆代幣交易統計數據")
        
        # 呼叫原有的 calculate_remaining_tokens 函數處理剩餘代幣的顯示邏輯
        await calculate_remaining_tokens_optimized(wallet_address, session, chain, client)

    except Exception as e:
        print(f"處理錢包 {wallet_address} 交易統計數據時出錯: {e}")
        import traceback
        traceback.print_exc()