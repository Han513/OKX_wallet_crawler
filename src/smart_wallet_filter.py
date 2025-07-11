import asyncio
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from models import *
from loguru_logger import *
from token_info import TokenUtils

async def calculate_distribution(aggregated_tokens, days):
    """
    计算7天或30天内的收益分布
    """
    distribution = {
        'distribution_gt500': 0,
        'distribution_200to500': 0,
        'distribution_0to200': 0,
        'distribution_0to50': 0,
        'distribution_lt50': 0
    }

    # 计算分布
    for stats in aggregated_tokens.values():
        if stats['cost'] > 0:  # 防止除零错误
            pnl_percentage = ((stats['profit'] - stats['cost']) / stats['cost']) * 100
            if pnl_percentage > 500:
                distribution['distribution_gt500'] += 1
            elif 200 <= pnl_percentage <= 500:
                distribution['distribution_200to500'] += 1
            elif 0 <= pnl_percentage < 200:
                distribution['distribution_0to200'] += 1
            elif -50 <= pnl_percentage < 0:
                distribution['distribution_0to50'] += 1
            elif pnl_percentage < -50:
                distribution['distribution_lt50'] += 1

    # 计算分布百分比
    total_distribution = sum(distribution.values())    
    distribution_percentage = {
        'distribution_gt500_percentage': round((distribution['distribution_gt500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_200to500_percentage': round((distribution['distribution_200to500'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to200_percentage': round((distribution['distribution_0to200'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_0to50_percentage': round((distribution['distribution_0to50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
        'distribution_lt50_percentage': round((distribution['distribution_lt50'] / total_distribution) * 100, 2) if total_distribution > 0 else 0,
    }

    return distribution, distribution_percentage

async def calculate_statistics(transactions, days, sol_usdt_price):
    """
    计算统计数据，包括总买卖次数、总成本、平均成本、PNL、每日PNL图等。
    """
    # 定义稳定币地址
    STABLECOINS = [
        "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB",  # USDT
        "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"   # USDC
    ]

    # now = datetime.now(timezone(timedelta(hours=8))).replace(hour=0, minute=0, second=0, microsecond=0)
    # start_of_range = now - timedelta(days=days)
    
    # # 转换为 Unix 时间戳
    # start_timestamp = int(start_of_range.timestamp())

    now = datetime.now(timezone(timedelta(hours=8)))
    end_of_yesterday = now.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    start_of_range = end_of_yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    start_timestamp = int(start_of_range.timestamp())

    # 过滤交易
    filtered_transactions = [
        tx for tx in transactions 
        if tx.get('timestamp', 0) >= start_timestamp and tx.get('timestamp', 0) <= int(now.timestamp())
    ]
    filtered_transactions.sort(key=lambda tx: tx.get('timestamp', 0))

    aggregated_tokens = defaultdict(lambda: {
        'buy_amount': 0,
        'sell_amount': 0,
        'cost': 0,
        'profit': 0,
        'remaining_amount': 0,
        'unrealized_value': 0
    })

    total_buy = total_sell = total_cost = total_profit = total_unrealized_profit = 0
    daily_pnl = {}
    profitable_tokens = total_tokens = 0

    for tx in filtered_transactions:
        for token, stats in tx.items():
            if token in ["timestamp", "signature"]:
                continue

            # 跳过稳定币交易
            if token in STABLECOINS:
                continue
            
            total_buy += 1 if stats['buy_amount'] > 0 else 0
            total_sell += 1 if stats['sell_amount'] > 0 else 0

            aggregated_tokens[token]['buy_amount'] += stats['buy_amount']
            aggregated_tokens[token]['sell_amount'] += stats['sell_amount']
            aggregated_tokens[token]['cost'] += stats['cost']
            aggregated_tokens[token]['profit'] += stats['profit']

            date_str = time.strftime("%Y-%m-%d", time.gmtime(tx['timestamp']))
            daily_pnl[date_str] = daily_pnl.get(date_str, 0) + (stats['profit'] - stats['cost'])

    for token, stats in aggregated_tokens.items():
        remaining_amount = stats['buy_amount'] - stats['sell_amount']
        stats['remaining_amount'] = remaining_amount

        if remaining_amount > 0:
            token_info = TokenUtils.get_token_info(token)
            current_price = token_info.get("priceUsd", 0)

            # 计算买入均价
            if stats['buy_amount'] > 0:
                buy_price = stats['cost'] / stats['buy_amount']
            else:
                buy_price = 0

            # 计算未实现利润
            if current_price == 0:
                stats['unrealized_value'] = 0
                total_unrealized_profit += stats['unrealized_value']
            else:
                # Convert all numeric values to Decimal to ensure consistent types
                remaining_amount_decimal = Decimal(str(remaining_amount))
                current_price_decimal = Decimal(str(current_price))
                buy_price_decimal = Decimal(str(buy_price))
                
                # Perform the calculation with consistent Decimal types
                stats['unrealized_value'] = float(remaining_amount_decimal * (current_price_decimal - buy_price_decimal))
                total_unrealized_profit += stats['unrealized_value']
        else:
            stats['unrealized_value'] = 0

    total_cost = sum(stats['cost'] for stats in aggregated_tokens.values())
    total_profit = sum(stats['profit'] for stats in aggregated_tokens.values())

    # 计算总 PNL
    pnl = total_profit - total_cost

    profitable_tokens = sum(1 for stats in aggregated_tokens.values() if stats['profit'] > stats['cost'])
    total_tokens = len(aggregated_tokens)

    # 修改后的逻辑，排除当天的数据，使用整点时间作为结束
    daily_pnl_chart = [
        f"{daily_pnl.get((now - timedelta(days=i+1)).strftime('%Y-%m-%d'), 0):.2f}"
        for i in range(days)
    ]

    average_cost = total_cost / total_buy if total_buy > 0 else 0
    pnl_percentage = max((pnl / total_cost) * 100, -100) if total_cost > 0 else 0
    win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0

    realized_profits = [stats['profit'] - stats['cost'] for stats in aggregated_tokens.values() if stats['profit'] > stats['cost']]
    avg_realized_profit = sum(realized_profits) / len(realized_profits) if realized_profits else 0

    # 调用计算收益分布
    distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)

    # 计算资产杠杆
    asset_multiple = float(pnl_percentage / 100) if total_cost > 0 else 0.0

    return {
        "asset_multiple": round(asset_multiple, 2),
        "total_buy": total_buy,
        "total_sell": total_sell,
        "total_transaction_num": total_buy + total_sell,
        "total_cost": total_cost,
        "average_cost": average_cost,
        "total_profit": total_profit,
        "pnl": pnl,
        "pnl_percentage": round(pnl_percentage, 2),
        "win_rate": round(win_rate, 2),
        "avg_realized_profit": round(avg_realized_profit, 2),
        "daily_pnl_chart": ",".join(daily_pnl_chart),
        "total_unrealized_profit": total_unrealized_profit,
        **distribution,
        **distribution_percentage
    }

# async def calculate_statistics2(transactions, days, sol_usdt_price):
#     """
#     計算統計數據，包括總買賣次數、總成本、平均成本、PNL、每日PNL圖等。
#     """
#     # 獲取當前時間並調整時間範圍
#     now = datetime.now(timezone(timedelta(hours=8)))
#     end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
#     start_of_range = end_of_day - timedelta(days=days)
    
#     start_timestamp = int(start_of_range.timestamp())

#     # 過濾並排序交易
#     filtered_transactions = [tx for tx in transactions if tx.get('timestamp', 0) >= start_timestamp]
#     filtered_transactions.sort(key=lambda tx: tx.get('timestamp', 0))
    
#     # 追蹤每個代幣的數據
#     token_stats = {}
    
#     # 處理每筆交易
#     for tx in filtered_transactions:
#         token_mint = tx.get('token_mint')
#         tx_type = tx.get('transaction_type', '').lower()
#         token_amount = tx.get('token_amount', 0)
        
#         # 初始化代幣統計
#         if token_mint not in token_stats:
#             token_stats[token_mint] = {
#                 'total_buy_amount': 0,
#                 'total_buy_cost_usd': 0,
#                 'total_sell_amount': 0,
#                 'total_sell_value_usd': 0,
#                 'current_holdings': 0,
#                 'current_cost_basis': 0,
#                 'buy_lots': [],  # 使用FIFO方法
#                 'realized_pnl_usd': 0,
#                 'buy_count': 0,
#                 'sell_count': 0,
#                 'avg_buy_price': 0
#             }
        
#         # 計算交易的USD價值
#         tx_value_usd = 0
#         if tx.get('sol_amount', 0) > 0:
#             tx_value_usd = tx['sol_amount'] * sol_usdt_price
#         elif tx.get('usdc_amount', 0) > 0:
#             tx_value_usd = tx['usdc_amount']
#         else:
#             continue
            
#         stats = token_stats[token_mint]
        
#         if tx_type == 'buy':
#             # 記錄買入批次(用於FIFO計算)
#             stats['buy_lots'].append({
#                 'amount': token_amount,
#                 'cost': tx_value_usd,
#                 'price': tx_value_usd / token_amount if token_amount > 0 else 0
#             })
            
#             # 更新統計數據
#             stats['buy_count'] += 1
#             stats['total_buy_amount'] += token_amount
#             stats['total_buy_cost_usd'] += tx_value_usd
#             stats['current_holdings'] += token_amount
#             stats['current_cost_basis'] += tx_value_usd
            
#             # 更新平均買入價格
#             stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings'] if stats['current_holdings'] > 0 else 0
            
#         elif tx_type == 'sell':
#             # 確保有足夠的買入記錄
#             sell_amount = min(token_amount, stats['current_holdings'])
            
#             if sell_amount > 0 and stats['buy_lots']:
#                 # 使用FIFO計算已實現利潤
#                 remaining_to_sell = sell_amount
#                 cost_basis = 0
                
#                 # 計算賣出的成本基礎
#                 sell_lots = []
#                 temp_buy_lots = stats['buy_lots'].copy()
                
#                 while remaining_to_sell > 0 and temp_buy_lots:
#                     oldest_lot = temp_buy_lots[0]
                    
#                     if oldest_lot['amount'] <= remaining_to_sell:
#                         # 整個批次都被賣出
#                         cost_basis += oldest_lot['cost']
#                         remaining_to_sell -= oldest_lot['amount']
#                         sell_lots.append(oldest_lot)
#                         temp_buy_lots.pop(0)
#                     else:
#                         # 部分批次被賣出
#                         sell_ratio = remaining_to_sell / oldest_lot['amount']
#                         partial_cost = oldest_lot['cost'] * sell_ratio
#                         cost_basis += partial_cost
                        
#                         # 創建一個新的部分批次用於賣出
#                         sell_lots.append({
#                             'amount': remaining_to_sell,
#                             'cost': partial_cost,
#                             'price': oldest_lot['price']
#                         })
                        
#                         # 更新原批次
#                         oldest_lot['amount'] -= remaining_to_sell
#                         oldest_lot['cost'] -= partial_cost
#                         remaining_to_sell = 0
                
#                 # 計算賣出部分在USD的價值
#                 sell_price_per_token = tx_value_usd / token_amount if token_amount > 0 else 0
#                 sell_value_usd = sell_amount * sell_price_per_token
                
#                 # 計算已實現PNL
#                 realized_pnl = sell_value_usd - cost_basis
#                 stats['realized_pnl_usd'] += realized_pnl
                
#                 # 更新統計數據
#                 stats['sell_count'] += 1
#                 stats['total_sell_amount'] += sell_amount
#                 stats['total_sell_value_usd'] += sell_value_usd
#                 stats['current_holdings'] -= sell_amount
                
#                 # 實際更新買入批次
#                 stats['buy_lots'] = temp_buy_lots
                
#                 # 重新計算當前成本基礎
#                 stats['current_cost_basis'] = sum(lot['cost'] for lot in stats['buy_lots'])
                
#                 # 如果完全清倉，重置平均買入價格
#                 if stats['current_holdings'] <= 0:
#                     stats['current_cost_basis'] = 0
#                     stats['avg_buy_price'] = 0
#                 else:
#                     # 更新平均買入價格
#                     stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings']
#             else:
#                 # 外部轉入或無買入記錄的賣出，只記錄統計不計入PNL
#                 stats['sell_count'] += 1
#                 stats['total_sell_amount'] += token_amount
#                 stats['total_sell_value_usd'] += tx_value_usd
    
#     # 計算總統計數據
#     total_buy = sum(stats['buy_count'] for stats in token_stats.values())
#     total_sell = sum(stats['sell_count'] for stats in token_stats.values())
#     total_cost = sum(stats['total_buy_cost_usd'] for stats in token_stats.values())
#     total_sell_value = sum(stats['total_sell_value_usd'] for stats in token_stats.values())
#     total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in token_stats.values())
    
#     # 計算勝率：只考慮有買入記錄的代幣
#     tokens_with_buys = [token for token, stats in token_stats.items() if stats['total_buy_amount'] > 0]
#     profitable_tokens = sum(1 for token in tokens_with_buys if token_stats[token]['realized_pnl_usd'] > 0)
#     total_tokens = len(tokens_with_buys)
    
#     # 計算總PNL
#     pnl = total_realized_pnl
    
#     # 計算PNL百分比：已實現PNL / 總賣出價值
#     pnl_percentage = (pnl / total_sell_value) * 100 if total_sell_value > 0 else 0
#     # 確保不低於-100%
#     pnl_percentage = max(pnl_percentage, -100)
    
#     # 勝率：盈利代幣數量/總代幣數量
#     win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0
    
#     # 其他統計指標
#     average_cost = total_cost / total_buy if total_buy > 0 else 0
#     asset_multiple = pnl_percentage / 100
    
#     # 計算每日PNL數據
#     daily_pnl = {}
#     for tx in filtered_transactions:
#         if tx.get('transaction_type', '').lower() == 'sell':
#             date_str = time.strftime("%Y-%m-%d", time.localtime(tx.get('timestamp', 0)))
#             token_mint = tx.get('token_mint')
            
#             if token_mint in token_stats and token_stats[token_mint]['total_buy_amount'] > 0:
#                 # 只考慮有買入記錄的賣出
#                 tx_value_usd = 0
#                 if tx.get('sol_amount', 0) > 0:
#                     tx_value_usd = tx['sol_amount'] * sol_usdt_price
#                 elif tx.get('usdc_amount', 0) > 0:
#                     tx_value_usd = tx['usdc_amount']
#                 else:
#                     continue
                
#                 # 為簡化，這裡使用當日的總賣出值減去對應比例的買入成本
#                 token_amount = tx.get('token_amount', 0)
#                 avg_buy_price = token_stats[token_mint]['avg_buy_price']
#                 sell_price = tx_value_usd / token_amount if token_amount > 0 else 0
                
#                 # 只計算不超過持有量的部分
#                 sell_amount = min(token_amount, token_stats[token_mint]['current_holdings'] + token_amount)
#                 this_pnl = (sell_price - avg_buy_price) * sell_amount

#                 if token_mint == "G5e2XonmccmdKc98g3eNQe5oBYGw9m8xdMUvVtcZpump":
#                     print(date_str)
#                     print(avg_buy_price)
#                     print(sell_price)
#                     print(this_pnl)
                
#                 # 累加到當日PNL
#                 daily_pnl[date_str] = daily_pnl.get(date_str, 0) + this_pnl

#     aggregated_tokens = {}
#     for token_mint, stats in token_stats.items():
#         if stats['total_buy_amount'] > 0:  # 只考慮有買入記錄的代幣
#             aggregated_tokens[token_mint] = {
#                 'profit': stats['total_sell_value_usd'],
#                 'cost': stats['total_buy_cost_usd']
#             }

#     distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)
    
#     return {
#         "asset_multiple": round(asset_multiple, 2),
#         "total_buy": total_buy,
#         "total_sell": total_sell,
#         "total_transaction_num": total_buy + total_sell,
#         "total_cost": total_cost,
#         "average_cost": average_cost,
#         "total_profit": total_sell_value,  # 總賣出價值
#         "pnl": pnl,  # 已實現PNL
#         "pnl_percentage": round(pnl_percentage, 2),
#         "win_rate": round(win_rate, 2),
#         "avg_realized_profit": round(pnl / total_tokens if total_tokens > 0 else 0, 2),
#         "daily_pnl_chart": ",".join(daily_pnl_chart),
#         **distribution,
#         **distribution_percentage
#     }

# async def calculate_statistics2(transactions, days, sol_usdt_price):
#     """
#     計算統計數據，包括總買賣次數、總成本、平均成本、PNL、每日PNL圖等。
#     改進版本：先處理全部交易記錄以獲取每個代幣的平均買入價格，再根據時間範圍過濾
#     """
#     # 獲取當前時間並調整時間範圍
#     now = datetime.now(timezone(timedelta(hours=8)))
#     end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
#     start_of_range = end_of_day - timedelta(days=days)
    
#     start_timestamp = int(start_of_range.timestamp())

#     # 先對所有交易進行排序（按時間順序）
#     all_transactions = sorted(transactions, key=lambda tx: tx.get('timestamp', 0))
    
#     # 追蹤每個代幣的數據（基於全部交易記錄）
#     all_token_stats = {}
    
#     # 第一步：處理全部交易記錄，計算每個代幣的買入平均價格
#     for tx in all_transactions:
#         token_mint = tx.get('token_mint')
#         tx_type = tx.get('transaction_type', '').lower()
#         token_amount = tx.get('token_amount', 0)
        
#         # 初始化代幣統計
#         if token_mint not in all_token_stats:
#             all_token_stats[token_mint] = {
#                 'total_buy_amount': 0,
#                 'total_buy_cost_usd': 0,
#                 'total_sell_amount': 0,
#                 'total_sell_value_usd': 0,
#                 'current_holdings': 0,
#                 'current_cost_basis': 0,
#                 'buy_lots': [],  # 使用FIFO方法
#                 'realized_pnl_usd': 0,
#                 'buy_count': 0,
#                 'sell_count': 0,
#                 'avg_buy_price': 0
#             }
        
#         # 計算交易的USD價值
#         tx_value_usd = 0
#         if tx.get('sol_amount', 0) > 0:
#             tx_value_usd = tx['sol_amount'] * sol_usdt_price
#         elif tx.get('usdc_amount', 0) > 0:
#             tx_value_usd = tx['usdc_amount']
#         else:
#             continue
            
#         stats = all_token_stats[token_mint]
        
#         if tx_type == 'buy':
#             # 記錄買入批次(用於FIFO計算)
#             stats['buy_lots'].append({
#                 'amount': token_amount,
#                 'cost': tx_value_usd,
#                 'price': tx_value_usd / token_amount if token_amount > 0 else 0,
#                 'timestamp': tx.get('timestamp', 0)
#             })
            
#             # 更新統計數據
#             stats['total_buy_amount'] += token_amount
#             stats['total_buy_cost_usd'] += tx_value_usd
#             stats['current_holdings'] += token_amount
#             stats['current_cost_basis'] += tx_value_usd
            
#             # 更新平均買入價格
#             stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings'] if stats['current_holdings'] > 0 else 0
            
#         elif tx_type == 'sell':
#             # 確保有足夠的買入記錄
#             sell_amount = min(token_amount, stats['current_holdings'])
            
#             if sell_amount > 0 and stats['buy_lots']:
#                 # 使用FIFO計算已實現利潤
#                 remaining_to_sell = sell_amount
#                 cost_basis = 0
                
#                 # 計算賣出的成本基礎
#                 sell_lots = []
#                 temp_buy_lots = stats['buy_lots'].copy()
                
#                 while remaining_to_sell > 0 and temp_buy_lots:
#                     oldest_lot = temp_buy_lots[0]
                    
#                     if oldest_lot['amount'] <= remaining_to_sell:
#                         # 整個批次都被賣出
#                         cost_basis += oldest_lot['cost']
#                         remaining_to_sell -= oldest_lot['amount']
#                         sell_lots.append(oldest_lot)
#                         temp_buy_lots.pop(0)
#                     else:
#                         # 部分批次被賣出
#                         sell_ratio = remaining_to_sell / oldest_lot['amount']
#                         partial_cost = oldest_lot['cost'] * sell_ratio
#                         cost_basis += partial_cost
                        
#                         # 創建一個新的部分批次用於賣出
#                         sell_lots.append({
#                             'amount': remaining_to_sell,
#                             'cost': partial_cost,
#                             'price': oldest_lot['price'],
#                             'timestamp': oldest_lot['timestamp']
#                         })
                        
#                         # 更新原批次
#                         oldest_lot['amount'] -= remaining_to_sell
#                         oldest_lot['cost'] -= partial_cost
#                         remaining_to_sell = 0
                
#                 # 實際更新買入批次
#                 stats['buy_lots'] = temp_buy_lots
                
#                 # 重新計算當前成本基礎
#                 stats['current_cost_basis'] = sum(lot['cost'] for lot in stats['buy_lots'])
                
#                 # 如果完全清倉，重置平均買入價格
#                 if stats['current_holdings'] <= 0:
#                     stats['current_cost_basis'] = 0
#                     stats['avg_buy_price'] = 0
#                 else:
#                     # 更新平均買入價格
#                     stats['avg_buy_price'] = stats['current_cost_basis'] / stats['current_holdings']
                
#                 stats['current_holdings'] -= sell_amount
#             else:
#                 # 外部轉入或無買入記錄的賣出，只記錄統計不計入PNL
#                 pass
    
#     # 第二步：過濾並分析指定時間範圍內的交易
#     # 應該過濾all_transactions而不是transactions
#     filtered_transactions = [tx for tx in all_transactions if tx.get('timestamp', 0) >= start_timestamp]
#     filtered_transactions.sort(key=lambda tx: tx.get('timestamp', 0))

#     # 追蹤每個代幣在指定時間範圍內的數據
#     token_stats = {}
    
#     # 處理每筆過濾後的交易
#     for tx in filtered_transactions:
#         token_mint = tx.get('token_mint')
#         tx_type = tx.get('transaction_type', '').lower()
#         token_amount = tx.get('token_amount', 0)
        
#         # 初始化代幣統計
#         if token_mint not in token_stats:
#             token_stats[token_mint] = {
#                 'total_buy_amount': 0,
#                 'total_buy_cost_usd': 0,
#                 'total_sell_amount': 0,
#                 'total_sell_value_usd': 0,
#                 'realized_pnl_usd': 0,
#                 'buy_count': 0,
#                 'sell_count': 0
#             }
        
#         # 計算交易的USD價值
#         tx_value_usd = 0
#         if tx.get('sol_amount', 0) > 0:
#             tx_value_usd = tx['sol_amount'] * sol_usdt_price
#         elif tx.get('usdc_amount', 0) > 0:
#             tx_value_usd = tx['usdc_amount']
#         else:
#             continue
            
#         stats = token_stats[token_mint]
        
#         if tx_type == 'buy':
#             # 更新統計數據
#             stats['buy_count'] += 1
#             stats['total_buy_amount'] += token_amount
#             stats['total_buy_cost_usd'] += tx_value_usd
            
#         elif tx_type == 'sell':
#             # 獲取從全部交易計算出的平均買入價格
#             avg_buy_price = 0
#             # 找到賣出時間點之前的平均買入價格
#             all_buys_before_sell = [
#                 lot for lot in all_token_stats.get(token_mint, {}).get('buy_lots', [])
#                 if lot['timestamp'] < tx.get('timestamp', 0)
#             ]
            
#             total_cost = sum(lot['cost'] for lot in all_buys_before_sell)
#             total_amount = sum(lot['amount'] for lot in all_buys_before_sell)
            
#             if total_amount > 0:
#                 avg_buy_price = total_cost / total_amount
            
#             # 計算本次賣出的PNL
#             sell_price = tx_value_usd / token_amount if token_amount > 0 else 0
#             realized_pnl = (sell_price - avg_buy_price) * token_amount
#             print(realized_pnl)
            
#             # 只有在有買入記錄的情況下才計算PNL
#             # if avg_buy_price > 0:
#             stats['realized_pnl_usd'] += realized_pnl
            
#             # 更新統計數據
#             stats['sell_count'] += 1
#             stats['total_sell_amount'] += token_amount
#             stats['total_sell_value_usd'] += tx_value_usd
    
#     # 計算總統計數據
#     total_buy = len([tx for tx in filtered_transactions if tx.get('transaction_type', '').lower() == 'buy'])
#     total_sell = len([tx for tx in filtered_transactions if tx.get('transaction_type', '').lower() == 'sell'])
#     total_cost = sum(stats['total_buy_cost_usd'] for stats in token_stats.values())
#     total_sell_value = sum(stats['total_sell_value_usd'] for stats in token_stats.values())
#     total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in token_stats.values())
#     print(total_realized_pnl)
    
#     # 計算勝率：只考慮有買入記錄的代幣
#     tokens_with_buys = [
#         token for token, stats in token_stats.items() 
#         if stats['total_buy_amount'] > 0 or all_token_stats.get(token, {}).get('total_buy_amount', 0) > 0
#     ]
#     profitable_tokens = sum(
#         1 for token in tokens_with_buys 
#         if token_stats[token]['realized_pnl_usd'] > 0
#     )
#     total_tokens = len(tokens_with_buys)
    
#     # 計算總PNL
#     pnl = total_realized_pnl
    
#     # 計算PNL百分比：已實現PNL / 總賣出價值
#     pnl_percentage = (pnl / total_sell_value) * 100 if total_sell_value > 0 else 0
#     # 確保不低於-100%
#     pnl_percentage = max(pnl_percentage, -100)
    
#     # 勝率：盈利代幣數量/總代幣數量
#     win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0
    
#     # 其他統計指標
#     average_cost = total_cost / total_buy if total_buy > 0 else 0
#     asset_multiple = pnl_percentage / 100
    
#     # 計算每日PNL數據
#     daily_pnl = {}
#     for tx in filtered_transactions:
#         if tx.get('transaction_type', '').lower() in ['sell', 'clean']:
#             date_str = time.strftime("%Y-%m-%d", time.localtime(tx.get('timestamp', 0)))
#             realized_profit = tx.get('fifo_realized_profit', 0)
#             daily_pnl[date_str] = daily_pnl.get(date_str, 0) + realized_profit

#     # 生成每日PNL圖表數據 - 只顯示PNL值，省略日期，並確保所有日期都填充
#     # 創建日期範圍（從結束日期往前推days天）
#     date_range = []
#     current_date = end_of_day
#     for i in range(days):
#         date_str = current_date.strftime("%Y-%m-%d")
#         date_range.append(date_str)
#         current_date = current_date - timedelta(days=1)
    
#     # 日期範圍保持從最近到最早（倒序）
#     # date_range.reverse()  # 移除這行，保持倒序排列
    
#     # 填充每一天的PNL數據，如果沒有則為0
#     filled_daily_pnl = []
#     for date_str in date_range:
#         if date_str in daily_pnl:
#             filled_daily_pnl.append(round(daily_pnl[date_str], 2))
#         else:
#             filled_daily_pnl.append(0)
    
#     # 根據指定的天數範圍生成PNL圖表數據
#     if days == 1:
#         # 1天的數據只保留最新一天的PNL
#         if filled_daily_pnl:
#             daily_pnl_chart = [f"{filled_daily_pnl[0]}"]
#         else:
#             daily_pnl_chart = ["0"]
#     else:
#         # 7天或30天的數據，顯示所有天的PNL值
#         daily_pnl_chart = [f"{pnl}" for pnl in filled_daily_pnl]

#     # 準備代幣分佈數據
#     aggregated_tokens = {}
#     for token_mint, stats in token_stats.items():
#         if stats['total_buy_amount'] > 0 or all_token_stats.get(token_mint, {}).get('total_buy_amount', 0) > 0:
#             aggregated_tokens[token_mint] = {
#                 'profit': stats['total_sell_value_usd'],
#                 'cost': stats['total_buy_cost_usd']
#             }

#     distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)
    
#     return {
#         "asset_multiple": round(asset_multiple, 2),
#         "total_buy": total_buy,
#         "total_sell": total_sell,
#         "total_transaction_num": total_buy + total_sell,
#         "total_cost": total_cost,
#         "average_cost": average_cost,
#         "total_profit": total_sell_value,
#         "pnl": pnl,
#         "pnl_percentage": round(pnl_percentage, 2),
#         "win_rate": round(win_rate, 2),
#         "avg_realized_profit": round(pnl / total_tokens if total_tokens > 0 else 0, 2),
#         "daily_pnl_chart": ",".join(daily_pnl_chart),
#         "pnl_pic": ",".join(daily_pnl_chart),
#         **distribution,
#         **distribution_percentage
#     }

async def calculate_statistics2(transactions, days, sol_usdt_price):
    """
    計算統計數據，包括總買賣次數、總成本、平均成本、PNL、每日PNL圖等。
    改進版本：使用與save_transactions_to_db相同的FIFO方法計算利潤
    """
    # 獲取當前時間並調整時間範圍
    now = datetime.now(timezone(timedelta(hours=8)))
    end_of_day = now.replace(hour=0, minute=0, second=0, microsecond=0)
    start_of_range = end_of_day - timedelta(days=days)
    
    start_timestamp = int(start_of_range.timestamp())

    # 先對所有交易進行排序（按時間順序）
    all_transactions = sorted(transactions, key=lambda tx: tx.get('timestamp', 0))
    
    # 追蹤每個代幣的數據
    token_stats = {}
    current_holdings = {}
    
    # 處理全部交易記錄，使用FIFO方法計算PNL
    for tx in all_transactions:
        token_mint = tx.get('token_mint')
        tx_type = tx.get('transaction_type', '').lower()
        token_amount = tx.get('token_amount', 0)
        
        # 初始化代幣統計和持倉記錄
        if token_mint not in token_stats:
            token_stats[token_mint] = {
                'total_buy_amount': 0,
                'total_buy_cost_usd': 0,
                'total_sell_amount': 0,
                'total_sell_value_usd': 0,
                'realized_pnl_usd': 0,
                'buy_count': 0,
                'sell_count': 0
            }
        
        if token_mint not in current_holdings:
            current_holdings[token_mint] = {
                'total_amount': 0,
                'total_cost': 0,
                'buys': [],
                'avg_buy_price': 0
            }
        
        # 計算交易的USD價值
        tx_value_usd = 0
        if tx.get('sol_amount', 0) > 0:
            tx_value_usd = tx['sol_amount'] * sol_usdt_price
        elif tx.get('usdc_amount', 0) > 0:
            tx_value_usd = tx['usdc_amount']
        elif tx.get('value', 0) > 0:  # 如果交易中直接有value字段，優先使用
            tx_value_usd = tx['value']
        else:
            continue
            
        stats = token_stats[token_mint]
        holding = current_holdings[token_mint]
        
        # 只處理時間範圍內的交易統計
        is_in_timerange = tx.get('timestamp', 0) >= start_timestamp
        
        if tx_type in ['buy', 'build']:
            # 記錄買入批次(用於FIFO計算)
            holding['buys'].append({
                'amount': token_amount,
                'cost': tx_value_usd,
                'price': tx_value_usd / token_amount if token_amount > 0 else 0
            })
            
            # 更新持倉數據
            holding['total_amount'] += token_amount
            holding['total_cost'] += tx_value_usd
            
            # 更新平均買入價格
            if holding['total_amount'] > 0:
                holding['avg_buy_price'] = holding['total_cost'] / holding['total_amount']
            
            # 更新時間範圍內的統計數據
            if is_in_timerange:
                stats['buy_count'] += 1
                stats['total_buy_amount'] += token_amount
                stats['total_buy_cost_usd'] += tx_value_usd
            
        elif tx_type in ['sell', 'clean']:
            # 計算已實現利潤（使用FIFO方法）
            realized_profit = 0
            cost_basis = 0
            remaining_to_sell = token_amount
            
            if holding['buys']:
                # 創建臨時買入批次列表進行計算，避免影響原始數據
                temp_buys = [dict(buy) for buy in holding['buys']]
                
                # 使用FIFO方法計算賣出成本和利潤
                while remaining_to_sell > 0 and temp_buys:
                    oldest_buy = temp_buys[0]
                    
                    if oldest_buy['amount'] <= remaining_to_sell:
                        # 整個批次都被賣出
                        cost_basis += oldest_buy['cost']
                        remaining_to_sell -= oldest_buy['amount']
                        temp_buys.pop(0)
                    else:
                        # 只賣出部分批次
                        sell_ratio = remaining_to_sell / oldest_buy['amount']
                        partial_cost = oldest_buy['cost'] * sell_ratio
                        cost_basis += partial_cost
                        oldest_buy['amount'] -= remaining_to_sell
                        oldest_buy['cost'] *= (1 - sell_ratio)
                        remaining_to_sell = 0
                
                # 計算已實現利潤
                realized_profit = tx_value_usd - cost_basis
                tx['fifo_realized_profit'] = realized_profit
                
                # 更新實際的持倉數據
                holding['buys'] = temp_buys
                holding['total_amount'] = max(0, holding['total_amount'] - token_amount)
                
                # 如果完全賣出，重置成本
                if holding['total_amount'] <= 0:
                    holding['total_cost'] = 0
                    holding['avg_buy_price'] = 0
                else:
                    # 重新計算總成本和平均價格
                    holding['total_cost'] = sum(buy['cost'] for buy in holding['buys'])
                    holding['avg_buy_price'] = holding['total_cost'] / holding['total_amount'] if holding['total_amount'] > 0 else 0
            else:
                # 如果沒有買入記錄（可能是空投），全部視為利潤
                realized_profit = tx_value_usd
            
            # 更新時間範圍內的統計數據
            if is_in_timerange:
                stats['sell_count'] += 1
                stats['total_sell_amount'] += token_amount
                stats['total_sell_value_usd'] += tx_value_usd
                stats['realized_pnl_usd'] += realized_profit
    
    # 只考慮時間範圍內的交易來計算統計數據
    filtered_stats = {token: stats for token, stats in token_stats.items() 
                     if stats['buy_count'] > 0 or stats['sell_count'] > 0}
    
    # 計算總統計數據
    total_buy = sum(stats['buy_count'] for stats in filtered_stats.values())
    total_sell = sum(stats['sell_count'] for stats in filtered_stats.values())
    total_cost = sum(stats['total_buy_cost_usd'] for stats in filtered_stats.values())
    total_sell_value = sum(stats['total_sell_value_usd'] for stats in filtered_stats.values())
    total_realized_pnl = sum(stats['realized_pnl_usd'] for stats in filtered_stats.values())
    
    # 計算勝率：只考慮有買入記錄的代幣
    tokens_with_buys = [token for token, stats in filtered_stats.items() if stats['total_buy_amount'] > 0]
    profitable_tokens = sum(1 for token in tokens_with_buys if filtered_stats[token]['realized_pnl_usd'] > 0)
    total_tokens = len(tokens_with_buys)
    
    # 計算總PNL
    pnl = total_realized_pnl
    
    # 計算PNL百分比：已實現PNL / 總賣出價值
    pnl_percentage = (pnl / total_cost) * 100 if total_cost > 0 else 0
    # 確保不低於-100%
    pnl_percentage = max(pnl_percentage, -100)
    
    # 勝率：盈利代幣數量/總代幣數量
    win_rate = (profitable_tokens / total_tokens) * 100 if total_tokens > 0 else 0
    
    # 其他統計指標
    average_cost = total_cost / total_buy if total_buy > 0 else 0
    asset_multiple = pnl_percentage / 100
    
    # 計算每日PNL數據（基於原始交易記錄的realized_profit字段，或使用我們計算的值）
    daily_pnl = {}
    filtered_transactions = [tx for tx in all_transactions if tx.get('timestamp', 0) >= start_timestamp]
    
    for tx in filtered_transactions:
        if tx.get('transaction_type', '').lower() in ['sell', 'clean']:
            date_str = time.strftime("%Y-%m-%d", time.localtime(tx.get('timestamp', 0)))
            realized_profit = tx.get('fifo_realized_profit', 0)
            daily_pnl[date_str] = daily_pnl.get(date_str, 0) + realized_profit

    # 生成每日PNL圖表數據
    date_range = []
    current_date = end_of_day
    for i in range(days):
        date_str = current_date.strftime("%Y-%m-%d")
        date_range.append(date_str)
        current_date = current_date - timedelta(days=1)
    
    # 填充每一天的PNL數據，如果沒有則為0
    filled_daily_pnl = []
    for date_str in date_range:
        if date_str in daily_pnl:
            filled_daily_pnl.append(round(daily_pnl[date_str], 2))
        else:
            filled_daily_pnl.append(0)
    
    # 根據指定的天數範圍生成PNL圖表數據
    if days == 1:
        daily_pnl_chart = [f"{filled_daily_pnl[0]}"] if filled_daily_pnl else ["0"]
    else:
        daily_pnl_chart = [f"{pnl}" for pnl in filled_daily_pnl]

    # 準備代幣分佈數據（假設有calculate_distribution函數）
    aggregated_tokens = {}
    for token_mint, stats in filtered_stats.items():
        if stats['total_buy_amount'] > 0:
            aggregated_tokens[token_mint] = {
                'profit': stats['total_sell_value_usd'],
                'cost': stats['total_buy_cost_usd']
            }

    # 假設calculate_distribution函數存在
    distribution, distribution_percentage = await calculate_distribution(aggregated_tokens, days)
    
    return {
        "asset_multiple": round(asset_multiple, 2),
        "total_buy": total_buy,
        "total_sell": total_sell,
        "total_transaction_num": total_buy + total_sell,
        "total_cost": total_cost,
        "average_cost": average_cost,
        "total_profit": total_sell_value,
        "pnl": pnl,
        "pnl_percentage": round(pnl_percentage, 2),
        "win_rate": round(win_rate, 2),
        "avg_realized_profit": round(pnl / total_tokens if total_tokens > 0 else 0, 2),
        "daily_pnl_chart": ",".join(daily_pnl_chart),
        "pnl_pic": ",".join(daily_pnl_chart),
        **distribution,
        **distribution_percentage
    }

# @async_log_execution_time
# async def filter_smart_wallets(wallet_transactions, sol_usdt_price, session, client, chain, wallet_type):
#     """
#     根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
#     """
#     # smart_wallets = []
#     print(wallet_transactions)
#     for wallet_address, transactions in wallet_transactions.items():
#         token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
#         token_last_trade_time = {}
#         for transaction in transactions:
#             for token_mint, data in transaction.items():
#                 if not isinstance(data, dict):
#                     continue
#                 token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
#                 token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
#                 token_summary[token_mint]['cost'] += data.get('cost', 0)
#                 token_summary[token_mint]['profit'] += data.get('profit', 0)
#                 token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))
#         sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
#         recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  # 获取最近的三个代币

#         token_list=[]
#         for token in recent_tokens:
#             token_info = TokenUtils.get_token_info(token)
            
#             # Check if token_info is a dictionary and contains the 'symbol' field
#             if isinstance(token_info, dict):
#                 symbol = token_info.get('symbol', {})
#                 if isinstance(symbol, str) and symbol:  # If symbol is valid
#                     token_list.append(symbol)

#         # Join token symbols into a comma-separated string
#         token_list_str = ','.join(filter(None, token_list))

#         # 计算 1 日、7 日、30 日统计数据
#         stats_1d = await calculate_statistics(transactions, 1, sol_usdt_price)
#         stats_7d = await calculate_statistics(transactions, 7, sol_usdt_price)
#         stats_30d = await calculate_statistics(transactions, 30, sol_usdt_price)

#         sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
#         wallet_data = {
#             "wallet_address": wallet_address,
#             "balance": round(sol_balance_data["balance"]["float"], 3),
#             "balance_usd": round(sol_balance_data["balance_usd"], 2),
#             "chain": "SOLANA",
#             "tag": "",
#             "is_smart_wallet": True,
#             "wallet_type": wallet_type,
#             "asset_multiple": float(stats_30d["asset_multiple"]),
#             "token_list": str(token_list_str),
#             "stats_1d": stats_1d,
#             "stats_7d": stats_7d,
#             "stats_30d": stats_30d,
#             "token_summary": token_summary,
#             "last_transaction_time": max(tx["timestamp"] for tx in transactions),
#         }

#         if (
#             stats_7d.get("total_cost", 0) > 0 and
#             stats_30d.get("pnl", 0) > 0 and
#             stats_30d.get("win_rate", 0) > 30 and
#             float(stats_30d.get("asset_multiple", 0)) > 0.3 and
#             stats_30d.get("total_transaction_num", 0) < 2000
#         ):
#             await write_wallet_data_to_db(session, wallet_data, chain)  # 写入数据库
#             return True  # 返回 True 表示满足条件
#         else:
#             return False

@async_log_execution_time
async def filter_smart_wallets(wallet_transactions, sol_usdt_price, session, client, chain, wallet_type):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = {}
        token_last_trade_time = {}

        for transaction in transactions:
            
            token_mint = transaction.get("token_mint")
            if not token_mint:
                continue  # 如果没有 token_mint，跳过

            # 存储交易数据到 token_summary
            if token_mint not in token_summary:
                token_summary[token_mint] = []
            
            token_summary[token_mint].append(transaction)

            # 存储最新交易时间
            token_last_trade_time[token_mint] = transaction.get('timestamp', 0)

        # 按时间排序，获取最近的 3 个代币
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  

        # 修正: recent_tokens 已經是代幣地址的列表，直接用逗號連接即可
        token_list = ','.join(recent_tokens) if recent_tokens else None

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics2(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics2(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics2(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_usd": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": True,
            "wallet_type": wallet_type,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": token_list,
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(data.get('timestamp', 0) for data in transactions),
        }
        # await write_wallet_data_to_db(session, wallet_data, chain)
        if (
            stats_7d.get("total_cost", 0) > 0 and
            stats_30d.get("pnl", 0) > 0 and
            stats_30d.get("win_rate", 0) > 40 and
            stats_30d.get("win_rate", 0) != 100 and
            float(stats_30d.get("asset_multiple", 0)) > 0.3 and
            stats_30d.get("total_transaction_num", 0) < 2000 and
            sol_balance_data["balance_usd"] > 0
        ):
            await write_wallet_data_to_db(session, wallet_data, chain)
            return True
        else:
            return False

@async_log_execution_time
async def filter_smart_wallets2(wallet_transactions, sol_usdt_price, session, client, chain, wallet_type):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = {}
        token_last_trade_time = {}

        for transaction in transactions:
            
            token_mint = transaction.get("token_mint")
            if not token_mint:
                continue  # 如果没有 token_mint，跳过

            # 存储交易数据到 token_summary
            if token_mint not in token_summary:
                token_summary[token_mint] = []
            
            token_summary[token_mint].append(transaction)

            # 存储最新交易时间
            token_last_trade_time[token_mint] = transaction.get('timestamp', 0)

        # 按时间排序，获取最近的 3 个代币
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  

        # 修正: recent_tokens 已經是代幣地址的列表，直接用逗號連接即可
        token_list = ','.join(recent_tokens) if recent_tokens else None

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics2(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics2(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics2(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_usd": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": True,
            "wallet_type": wallet_type,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": token_list,
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(data.get('timestamp', 0) for data in transactions),
        }
        await write_wallet_data_to_db(session, wallet_data, chain)

@async_log_execution_time
async def update_smart_wallets_filter(wallet_transactions, sol_usdt_price, session, client, chain):
    """根據交易記錄計算盈亏、胜率，並篩選聰明錢包"""
    for wallet_address, transactions in wallet_transactions.items():
        if not transactions:
            # print(f"錢包 {wallet_address} 沒有交易記錄，跳過處理")
            return False
            
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        
        for transaction in transactions:
            tx_timestamp = transaction.get("timestamp", 0)  # 獲取交易時間戳
            
            for token_mint, data in transaction.items():
                if token_mint == "timestamp" or not isinstance(data, dict):
                    continue
                    
                # 更新代幣的交易數據
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                
                # 更新代幣的最後交易時間
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), tx_timestamp)

        # 按交易時間排序，獲取最近交易的3個代幣
        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  
                
        # 修正: recent_tokens 已經是代幣地址的列表，直接用逗號連接即可
        token_list = ','.join(recent_tokens) if recent_tokens else None
        
        # 計算 1 日、7 日、30 日統計數據
        stats_1d = await calculate_statistics2(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics2(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics2(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        # 安全處理最後交易時間
        last_transaction_time = 0
        if transactions:
            last_transaction_time = max(tx.get("timestamp", 0) for tx in transactions)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_usd": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": True,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": token_list,  # 即使有些代幣沒有symbol或icon也包含在內
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": last_transaction_time,
        }

        # await write_wallet_data_to_db(session, wallet_data, chain)
        current_timestamp = int(time.time())
        thirty_days_ago = current_timestamp - (30 * 24 * 60 * 60)
        if (
            stats_30d.get("pnl", 0) > 0 and
            stats_30d.get("win_rate", 0) > 30 and
            stats_30d.get("win_rate", 0) != 100 and
            stats_30d.get("total_transaction_num", 0) < 2000 and
            last_transaction_time >= thirty_days_ago
        ):            
            return True  # 返回 True 表示满足条件
        else:
            return False

@async_log_execution_time
async def filter_smart_wallets_true(
        wallet_transactions, 
        sol_usdt_price, 
        session, 
        client, 
        chain, 
        is_smart_wallet=None,  # 可选参数
        wallet_type=None       # 可选参数
    ):
    """
    根据交易记录计算盈亏、胜率，并筛选聪明钱包，包含 1 日、7 日、30 日统计。
    """
    # smart_wallets = []
    for wallet_address, transactions in wallet_transactions.items():
        token_summary = defaultdict(lambda: {'buy_amount': 0, 'sell_amount': 0, 'cost': 0, 'profit': 0, 'marketcap': 0})
        token_last_trade_time = {}
        for transaction in transactions:
            for token_mint, data in transaction.items():
                if not isinstance(data, dict):
                    continue
                token_summary[token_mint]['buy_amount'] += data.get('buy_amount', 0)
                token_summary[token_mint]['sell_amount'] += data.get('sell_amount', 0)
                token_summary[token_mint]['cost'] += data.get('cost', 0)
                token_summary[token_mint]['profit'] += data.get('profit', 0)
                token_last_trade_time[token_mint] = max(token_last_trade_time.get(token_mint, 0), data.get('timestamp', 0))

        sorted_tokens_by_time = sorted(token_last_trade_time.items(), key=lambda x: x[1], reverse=True)
        recent_tokens = [token for token, _ in sorted_tokens_by_time[:3]]  
                
        # 修正: recent_tokens 已經是代幣地址的列表，直接用逗號連接即可
        token_list = ','.join(recent_tokens) if recent_tokens else None

        # 计算 1 日、7 日、30 日统计数据
        stats_1d = await calculate_statistics2(transactions, 1, sol_usdt_price)
        stats_7d = await calculate_statistics2(transactions, 7, sol_usdt_price)
        stats_30d = await calculate_statistics2(transactions, 30, sol_usdt_price)

        sol_balance_data = await TokenUtils.get_usd_balance(client, wallet_address)
        
        wallet_data = {
            "wallet_address": wallet_address,
            "balance": round(sol_balance_data["balance"]["float"], 3),
            "balance_usd": round(sol_balance_data["balance_usd"], 2),
            "chain": "SOLANA",
            "tag": "",
            "is_smart_wallet": is_smart_wallet,
            "wallet_type": wallet_type,
            "asset_multiple": float(stats_30d["asset_multiple"]),
            "token_list": token_list,
            "stats_1d": stats_1d,
            "stats_7d": stats_7d,
            "stats_30d": stats_30d,
            "token_summary": token_summary,
            "last_transaction_time": max(tx["timestamp"] for tx in transactions),
        }

        await write_wallet_data_to_db(session, wallet_data, chain)
        return True