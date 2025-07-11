import os
import base58
import requests
from solana.rpc.async_api import AsyncClient
from solders.pubkey import Pubkey
from solana.rpc.types import TokenAccountOpts

Helius_API = os.getenv("Helius_API")

class TokenUtils:

    # @staticmethod
    # def get_token_info(token_mint_address: str) -> dict:
    #     """
    #     獲取代幣的一般信息，結合DexScreener和Jupiter API的數據。
    #     當DexScreener缺少關鍵數據時，使用Jupiter API補充。
    #     特定幣種（如USDT和USDC）直接返回靜態數據，避免查詢。
    #     """
    #     # 針對特定幣種直接返回靜態數據
    #     stablecoins = {
    #         "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
    #             "symbol": "USDC",
    #             "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png",
    #             "priceUsd": 1
    #         },
    #         "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
    #             "symbol": "USDT",
    #             "url": "https://dd.dexscreener.com/ds-data/tokens/solana/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB.png?key=413023",
    #             "priceUsd": 1
    #         }
    #     }

    #     # 檢查是否為USDT或USDC
    #     if token_mint_address in stablecoins:
    #         return stablecoins[token_mint_address]
        
    #     dex_data = {}
    #     # 1. 首先嘗試使用DexScreener API
    #     try:
    #         url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
    #         response = requests.get(url)
            
    #         if response.status_code == 200:
    #             data = response.json()
                
    #             if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
    #                 first_pair = data['pairs'][0]  # 取第一個交易對
                    
    #                 dex_data = {
    #                     "symbol": first_pair.get('baseToken', {}).get('symbol', None),
    #                     "url": first_pair.get('info', {}).get('imageUrl', None),
    #                     "marketcap": first_pair.get('marketCap', 0),
    #                     "priceNative": float(first_pair.get('priceNative', 0)),
    #                     "priceUsd": float(first_pair.get('priceUsd', 0)),
    #                     "volume": first_pair.get('volume', 0),
    #                     "liquidity": first_pair.get('liquidity', 0),
    #                     "dex": first_pair.get('dexId', 0),
    #                     "social": first_pair.get('info', {}).get('socials', None),
    #                 }
    #     except Exception as e:
    #         print(f"DexScreener API error: {e}")
        
    #     # 檢查DexScreener是否返回了完整數據
    #     missing_key_data = not dex_data or dex_data.get("url") is None
        
    #     # 2. 如果DexScreener API失敗或關鍵數據缺失，嘗試使用Jupiter API
    #     if missing_key_data:
    #         try:
    #             url = f"https://api.jup.ag/tokens/v1/token/{token_mint_address}"
    #             headers = {'Accept': 'application/json'}
    #             response = requests.get(url, headers=headers)
                
    #             if response.status_code == 200:
    #                 token_data = response.json()
    #                 # 如果DexScreener完全失敗，使用Jupiter的全部數據
    #                 if not dex_data:
    #                     return {
    #                         "symbol": token_data.get("symbol", None),
    #                         "url": token_data.get("icon", None),  # 使用logo作為URL
    #                         "marketcap": 0,   # Jupiter API沒有提供市值
    #                         "priceNative": 0, # Jupiter API沒有提供原生價格
    #                         "priceUsd": 0,    # Jupiter API沒有提供USD價格
    #                         "volume": 0,      # Jupiter API沒有提供交易量
    #                         "liquidity": 0,   # Jupiter API沒有提供流動性
    #                         "name": token_data.get("name", None),  # 額外信息
    #                         "decimals": token_data.get("decimals", 0),  # 額外信息
    #                         "source": "jupiter"  # 標記數據來源
    #                     }
    #                 # 如果DexScreener只缺少部分數據，補充這些數據
    #                 else:
    #                     # 檢查並補充缺失的數據
    #                     if dex_data.get("symbol") is None:
    #                         dex_data["symbol"] = token_data.get("symbol", None)
    #                     if dex_data.get("url") == "no url":
    #                         dex_data["url"] = token_data.get("logoURI", "no url")
    #                     # 添加Jupiter API獨有的有用數據
    #                     dex_data["name"] = token_data.get("name", None)
    #                     dex_data["icon"] = token_data.get("logoURI", None)
    #                     dex_data["decimals"] = token_data.get("decimals", 0)
    #                     dex_data["source"] = "dexscreener+jupiter"  # 標記為組合數據源
    #                     return dex_data
    #         except Exception as e:
    #             print(f"Jupiter API error: {e}")
        
    #     # 如果有DexScreener數據，返回它
    #     if dex_data:
    #         return dex_data
            
    #     # 如果兩個API都失敗，返回默認值
    #     return {"priceUsd": 0}

    @staticmethod
    def get_token_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，優先使用Helius API，如果失敗則結合DexScreener和Jupiter API的數據。
        當DexScreener缺少關鍵數據時，使用Jupiter API補充。
        特定幣種（如USDT和USDC）直接返回靜態數據，避免查詢。
        
        Parameters:
        token_mint_address (str): 代幣的Mint地址
        helius_api_key (str): Helius API的密鑰
        
        Returns:
        dict: 包含代幣信息的字典
        """
        # 針對特定幣種直接返回靜態數據
        stablecoins = {
            "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v": {
                "symbol": "USDC",
                "url": "https://raw.githubusercontent.com/solana-labs/token-list/main/assets/mainnet/EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v/logo.png",
                "priceUsd": 1
            },
            "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB": {
                "symbol": "USDT",
                "url": "https://dd.dexscreener.com/ds-data/tokens/solana/Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB.png?key=413023",
                "priceUsd": 1
            }
        }

        # 檢查是否為USDT或USDC
        if token_mint_address in stablecoins:
            return stablecoins[token_mint_address]
        
        # 1. 首先嘗試使用Helius API
        # try:
        #     helius_url = f"https://mainnet.helius-rpc.com/?api-key={Helius_API}"
        #     payload = {
        #         "jsonrpc": "2.0",
        #         "id": "helius_request",
        #         "method": "getAsset",
        #         "params": {"id": token_mint_address}
        #     }
        #     headers = {"Content-Type": "application/json"}
            
        #     response = requests.post(helius_url, headers=headers, json=payload)
            
        #     if response.status_code == 200:
        #         data = response.json()
        #         if "result" in data and data["result"]:
        #             result = data["result"]
        #             token_info = result.get("token_info", {})
        #             content = result.get("content", {})
                    
        #             # 從Helius提取所需信息
        #             symbol = token_info.get("symbol") or content.get("metadata", {}).get("symbol")
                    
        #             # 獲取圖像URL
        #             image_url = None
        #             if content.get("links", {}).get("image"):
        #                 image_url = content["links"]["image"]
        #             elif content.get("files") and len(content["files"]) > 0:
        #                 for file in content["files"]:
        #                     if file.get("mime", "").startswith("image/"):
        #                         image_url = file.get("cdn_uri") or file.get("uri")
        #                         break
                    
        #             # 獲取價格信息
        #             price_usd = 0
        #             if token_info.get("price_info"):
        #                 price_usd = token_info["price_info"].get("price_per_token", 0)
                    
        #             return {
        #                 "symbol": symbol,
        #                 "url": image_url,
        #                 "priceUsd": price_usd,
        #                 "decimals": token_info.get("decimals", 0),
        #                 "supply": token_info.get("supply", 1000000000) / (10 ** token_info.get("decimals", 6)),
        #                 "name": content.get("metadata", {}).get("name"),
        #                 "description": content.get("metadata", {}).get("description"),
        #                 "source": "helius"
        #             }
        # except Exception as e:
        #     print(f"Helius API error: {e}")
        
        # 如果Helius API失敗，繼續使用其他API
        dex_data = {}
        # 2. 嘗試使用DexScreener API
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    first_pair = data['pairs'][0]  # 取第一個交易對
                    
                    dex_data = {
                        "symbol": first_pair.get('baseToken', {}).get('symbol', None),
                        "url": first_pair.get('info', {}).get('imageUrl', None),
                        "marketcap": first_pair.get('marketCap', 0),
                        "priceNative": float(first_pair.get('priceNative', 0)),
                        "priceUsd": float(first_pair.get('priceUsd', 0)),
                        "volume": first_pair.get('volume', 0),
                        "liquidity": first_pair.get('liquidity', 0),
                        "dex": first_pair.get('dexId', 0),
                        "social": first_pair.get('info', {}).get('socials', None),
                        "source": "dexscreener"
                    }
        except Exception as e:
            print(f"DexScreener API error: {e}")
        
        # 檢查DexScreener是否返回了完整數據
        missing_key_data = not dex_data or dex_data.get("url") is None
        
        # 3. 如果DexScreener API失敗或關鍵數據缺失，嘗試使用Jupiter API
        if missing_key_data:
            try:
                url = f"https://api.jup.ag/tokens/v1/token/{token_mint_address}"
                headers = {'Accept': 'application/json'}
                response = requests.get(url, headers=headers)
                
                if response.status_code == 200:
                    token_data = response.json()
                    # 如果DexScreener完全失敗，使用Jupiter的全部數據
                    if not dex_data:
                        return {
                            "symbol": token_data.get("symbol", None),
                            "url": token_data.get("icon", None),  # 使用logo作為URL
                            "marketcap": 0,   # Jupiter API沒有提供市值
                            "priceNative": 0, # Jupiter API沒有提供原生價格
                            "priceUsd": 0,    # Jupiter API沒有提供USD價格
                            "volume": 0,      # Jupiter API沒有提供交易量
                            "liquidity": 0,   # Jupiter API沒有提供流動性
                            "name": token_data.get("name", None),  # 額外信息
                            "decimals": token_data.get("decimals", 0),  # 額外信息
                            "source": "jupiter"  # 標記數據來源
                        }
                    # 如果DexScreener只缺少部分數據，補充這些數據
                    else:
                        # 檢查並補充缺失的數據
                        if dex_data.get("symbol") is None:
                            dex_data["symbol"] = token_data.get("symbol", None)
                        if dex_data.get("url") is None or dex_data.get("url") == "no url":
                            dex_data["url"] = token_data.get("logoURI", "no url")
                        # 添加Jupiter API獨有的有用數據
                        dex_data["name"] = token_data.get("name", None)
                        dex_data["icon"] = token_data.get("logoURI", None)
                        dex_data["decimals"] = token_data.get("decimals", 0)
                        dex_data["source"] = "dexscreener+jupiter"  # 標記為組合數據源
                        return dex_data
            except Exception as e:
                print(f"Jupiter API error: {e}")
        
        # 如果有DexScreener數據，返回它
        if dex_data:
            return dex_data
            
        # 如果所有API都失敗，返回默認值
        return {"priceUsd": 0, "source": "default"}
    
    @staticmethod
    def get_sol_info(token_mint_address: str) -> dict:
        """
        獲取代幣的一般信息，返回包括價格的數據。
        """
        try:
            url = f"https://api.dexscreener.com/latest/dex/tokens/{token_mint_address}"
            response = requests.get(url)
            
            if response.status_code == 200:
                data = response.json()
                if 'pairs' in data and isinstance(data['pairs'], list) and len(data['pairs']) > 0:
                    return {
                        "symbol": data['pairs'][0].get('baseToken', {}).get('symbol', None),
                        "url": data['pairs'][0].get('url', "no url"),
                        "marketcap": data['pairs'][0].get('marketCap', 0),
                        "priceNative": float(data['pairs'][0].get('priceNative', 0)),
                        "priceUsd": float(data['pairs'][0].get('priceUsd', 0)),
                        "volume": data['pairs'][0].get('volume', 0),
                        "liquidity": data['pairs'][0].get('liquidity', 0)
                    }
            else:
                data = response.json()
        except Exception as e:
            return {"priceUsd": 125}
        return {"priceUsd": 125}

    @staticmethod
    async def get_token_balance(client: AsyncClient, token_account: str) -> dict:
        """
        獲取 SPL 代幣餘額。
        :param client: AsyncClient Solana 客戶端
        :param token_account: 代幣賬戶地址
        :return: 包含餘額數據的字典
        """
        try:
            token_pubkey = Pubkey(base58.b58decode(token_account))
            balance_response = await client.get_token_account_balance(token_pubkey)
            balance = {
                'decimals': balance_response.value.decimals,
                'balance': {
                    'int': int(balance_response.value.amount),
                    'float': float(balance_response.value.ui_amount)
                }
            }
            return balance
        except Exception as e:
            print(f"獲取 SPL 代幣餘額時出錯: {e}")
            return {"decimals": 0, "balance": {"int": 0, "float": 0.0}}

    @staticmethod
    async def get_sol_balance(client: AsyncClient, wallet_address: str) -> dict:
        """
        獲取 SOL 餘額。
        :param client: AsyncClient Solana 客戶端
        :param wallet_address: 錢包地址
        :return: 包含 SOL 餘額的字典
        """
        try:
            pubkey = Pubkey(base58.b58decode(wallet_address))
            balance_response = await client.get_balance(pubkey=pubkey)
            balance = {
                'decimals': 9,
                'balance': {
                    'int': balance_response.value,
                    'float': float(balance_response.value / 10**9)
                }
            }
            return balance
        except Exception as e:
            return {"decimals": 9, "balance": {"int": 0, "float": 0.0}}
        
    @staticmethod
    async def get_token_accounts_balance(client: AsyncClient, wallet_address: str, token_mint: str) -> dict:
        """
        獲取指定代幣的所有賬戶餘額總和。
        :param client: AsyncClient Solana 客戶端
        :param wallet_address: 錢包地址
        :param token_mint: 代幣合約地址
        :return: 包含代幣餘額的字典
        """
        try:
            # 正確使用 Pubkey 對象
            owner_pubkey = Pubkey.from_string(wallet_address)
            mint_pubkey = Pubkey.from_string(token_mint)
            
            # 使用正確的過濾器格式 - 必須使用 TokenAccountOpts
            opts = TokenAccountOpts(mint=mint_pubkey)
            
            # 獲取所有與此代幣相關的賬戶
            token_accounts = await client.get_token_accounts_by_owner(
                owner_pubkey,
                opts
            )
            
            if not token_accounts.value:
                return {
                    "decimals": 6,  # USDC/USDT通常為6位小數
                    "balance": {"int": 0, "float": 0.0}
                }
            
            # 假設 USDC 和 USDT 都是 6 位小數（這是標準）
            decimals = 6
            if token_mint == "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v":  # USDC
                decimals = 6
            elif token_mint == "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB":  # USDT
                decimals = 6
            else:
                # 嘗試獲取精度，但可能會失敗
                try:
                    token_info = await client.get_token_supply(mint_pubkey)
                    decimals = token_info.value.decimals
                except Exception as e:
                    print(f"無法獲取代幣精度，使用預設值6: {str(e)}")
            
            # 計算總餘額
            total_balance = 0
            for account in token_accounts.value:
                try:
                    # 直接使用 get_token_account_balance 獲取餘額
                    account_pubkey = account.pubkey
                    account_info = await client.get_token_account_balance(account_pubkey)
                    if account_info and hasattr(account_info, 'value'):
                        total_balance += int(account_info.value.amount)
                except Exception as e:
                    print(f"獲取代幣賬戶餘額時出錯: {str(e)}")
                    continue
            
            float_balance = total_balance / (10 ** decimals)
            
            return {
                "decimals": decimals,
                "balance": {
                    "int": total_balance,
                    "float": float_balance
                }
            }
        except Exception as e:
            print(f"獲取代幣餘額時出錯: {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                "decimals": 6,  # 默認為穩定幣的6位小數
                "balance": {"int": 0, "float": 0.0}
            }

    @staticmethod
    async def get_usd_balance(client: AsyncClient, wallet_address: str) -> dict:
        """
        獲取錢包的 SOL、USDC、USDT 餘額以及對應的 USD 總價值。
        """
        try:
            # 定義穩定幣合約地址
            USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
            USDT_MINT = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"
            
            # 查詢 SOL 餘額
            # print(f"查詢 SOL 餘額...")
            sol_balance = await TokenUtils.get_sol_balance(client, wallet_address)
            sol_float = sol_balance["balance"]["float"] if sol_balance else 0.0
            
            # 獲取 SOL 價格
            sol_price = TokenUtils.get_sol_info("So11111111111111111111111111111111111111112").get("priceUsd", 0)
            
            # 查詢 USDC 餘額
            # print(f"查詢 USDC 餘額...")
            usdc_balance = await TokenUtils.get_token_accounts_balance(client, wallet_address, USDC_MINT)
            usdc_float = usdc_balance["balance"]["float"] if usdc_balance else 0.0
            
            # 查詢 USDT 餘額
            # print(f"查詢 USDT 餘額...")
            usdt_balance = await TokenUtils.get_token_accounts_balance(client, wallet_address, USDT_MINT)
            usdt_float = usdt_balance["balance"]["float"] if usdt_balance else 0.0
            
            # 計算總美元價值
            sol_usd = sol_float * sol_price
            total_usd = sol_usd + usdc_float + usdt_float
            
            # 輸出結果
            # print("\n錢包餘額查詢結果:")
            # print(f"SOL: {sol_float} (${sol_usd:.2f} USD)")
            # print(f"USDC: {usdc_float} (${usdc_float:.2f} USD)")
            # print(f"USDT: {usdt_float} (${usdt_float:.2f} USD)")
            # print(f"總美元價值: ${total_usd:.2f} USD")
            
            return {
                "sol": {
                    "balance": sol_float,
                    "usd_value": sol_usd
                },
                "usdc": {
                    "balance": usdc_float,
                    "usd_value": usdc_float  # USDC價值就是它的數量
                },
                "usdt": {
                    "balance": usdt_float,
                    "usd_value": usdt_float  # USDT價值就是它的數量
                },
                "total_usd": total_usd,
                "decimals": 9,
                "balance": {"int": sol_balance["balance"]["int"], "float": sol_float},
                "balance_usd": total_usd  # 保持與原始格式兼容
            }
        except Exception as e:
            print(f"獲取 USD 餘額時出錯: {e}")
            import traceback
            traceback.print_exc()
            return {
                "sol": {"balance": 0.0, "usd_value": 0.0},
                "usdc": {"balance": 0.0, "usd_value": 0.0},
                "usdt": {"balance": 0.0, "usd_value": 0.0},
                "total_usd": 0.0,
                "decimals": 9,
                "balance": {"int": 0, "float": 0.0},
                "balance_usd": 0.0
            }
