import os
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

# 数据库配置s
DATABASE_URI_SWAP_SOL = os.getenv('DATABASE_URI_SWAP_SOL')
DATABASE_URI_SWAP_ETH = os.getenv('DATABASE_URI_SWAP_ETH')
DATABASE_URI_SWAP_BASE = os.getenv('DATABASE_URI_SWAP_BASE')
DATABASE_URI_SWAP_BSC = os.getenv('DATABASE_URI_SWAP_BSC')
DATABASE_URI_SWAP_TRON = os.getenv('DATABASE_URI_SWAP_TRON')
DATABASE_URI_Ian = os.getenv('DATABASE_URI_Ian')

RPC_URL = os.getenv('RPC_URL', "https://solana-mainnet.rpc.url")
RPC_URL_backup = os.getenv('RPC_URL_backup', "https://solana-mainnet.rpc.url")
BSC_RPC_URL = os.getenv('BSC_RPC_URL', "")

# HELIUS_API_KEY = "16e9dd4d-4cf7-4c69-8c2d-fafa13b03423"
HELIUS_API_KEY = os.getenv('Helius_API', "16e9dd4d-4cf7-4c69-8c2d-fafa13b03423")