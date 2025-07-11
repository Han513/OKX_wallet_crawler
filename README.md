# OKX Wallet Crawler

OKX Web3 聰明錢包爬蟲系統，自動爬取並分析 OKX 平台的聰明錢包數據，支援 SOLANA 和 BSC 鏈。

## 功能特色

- 🚀 **多鏈支援**：同時支援 SOLANA 和 BSC 鏈的聰明錢包分析
- 📊 **智能分析**：自動分析錢包交易記錄、持倉狀況、盈虧統計
- 🔄 **自動合併**：分析完成後自動將臨時表數據合併到主表
- 📈 **實時監控**：提供詳細的進度顯示和時間戳記錄
- 🛡️ **穩定運行**：支援 supervisor 守護進程，確保程序穩定運行

## 系統要求

- Python 3.9+
- PostgreSQL 12+
- Redis (可選，用於快取)
- 網路連接（用於爬取 OKX API）

## 安裝步驟

### 1. 克隆專案

```bash
git clone <repository-url>
cd OKX_wallet_crawler
```

### 2. 自動安裝（推薦）

```bash
# 給予安裝腳本執行權限
chmod +x install.sh

# 運行自動安裝腳本
./install.sh
```

### 3. 手動安裝（可選）

如果自動安裝失敗，可以手動執行以下步驟：

#### 3.1 創建虛擬環境

```bash
python3 -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate  # Windows
```

#### 3.2 安裝依賴

```bash
pip install -r requirements.txt
```

#### 3.3 環境配置

```bash
# 複製環境變數範例文件
cp .env.example .env

# 編輯環境變數文件
nano .env
```

### 5. 配置環境變數

編輯 `.env` 文件，填入以下必要配置：

```env
# 資料庫連接
DATABASE_URI_SWAP_SOL=postgresql+asyncpg://username:password@localhost:5432/database_name
DATABASE_URI_SWAP_ETH=postgresql+asyncpg://username:password@localhost:5432/database_name
DATABASE_URI_SWAP_BASE=postgresql+asyncpg://username:password@localhost:5432/database_name
DATABASE_URI_SWAP_BSC=postgresql+asyncpg://username:password@localhost:5432/database_name
DATABASE_URI_SWAP_TRON=postgresql+asyncpg://username:password@localhost:5432/database_name
DATABASE_URI_Ian=postgresql+asyncpg://username:password@localhost:5432/database_name

# RPC 節點
RPC_URL=https://solana-mainnet.rpc.url
RPC_URL_backup=https://solana-mainnet.rpc.url
BSC_RPC_URL=https://bsc-dataseed.binance.org/

# API 金鑰
Helius_API=your_helius_api_key

# 功能開關
USE_TMP_TABLE=true
```

## 啟動方式

### 添加run/OKX_crawler.conf到supervisor

```bash
supervisorctl update
supervisorctl reread
```

## 程序流程

1. **初始化**：創建臨時表（tmp tables）
2. **爬取 BSC 鏈**：獲取 BSC 聰明錢包數據並分析
3. **爬取 SOLANA 鏈**：獲取 SOLANA 聰明錢包數據並分析
4. **數據合併**：將臨時表數據合併到主表
5. **完成**：輸出完成訊息

## 日誌文件

程序運行日誌位於：
- 日誌：`logs/okx_wallet_crawler.log`

## 資料庫表結構

### 主要表
- `wallet`：錢包基本信息
- `wallet_transaction`：交易記錄
- `wallet_holding`：持倉信息
- `wallet_buy_data`：買入數據
- `wallet_token_state`：代幣狀態

### 臨時表
- `wallet_tmp`：錢包臨時表
- `wallet_transaction_tmp`：交易臨時表
- `wallet_holding_tmp`：持倉臨時表
- `wallet_buy_data_tmp`：買入數據臨時表
- `wallet_token_state_tmp`：代幣狀態臨時表

## 故障排除

### 常見問題

1. **資料庫連接失敗**
   - 檢查 `.env` 中的資料庫連接字串
   - 確認資料庫服務正在運行
   - 檢查防火牆設置

2. **RPC 節點連接失敗**
   - 檢查網路連接
   - 更換 RPC 節點
   - 檢查 API 金鑰是否有效

3. **權限問題**
   - 確保程序有讀寫日誌目錄的權限
   - 檢查資料庫用戶權限

### 日誌查看

```bash
# 查看主日誌
tail -f logs/okx_wallet_crawler.log

```

## 開發說明

### 項目結構

```
OKX_wallet_crawler/
├── src/                    # 源代碼目錄
│   ├── okx_crawler.py     # 主程序
│   ├── merge_tmp.py       # 數據合併工具
│   ├── models.py          # 數據模型
│   ├── config.py          # 配置文件
│   └── ...
├── run/                   # 運行配置
│   └── OKX_crawler.conf   # Supervisor 配置
├── logs/                  # 日誌目錄
├── venv/                  # 虛擬環境
├── requirements.txt       # Python 依賴
├── .env.example          # 環境變數範例
├── install.sh           # 自動安裝腳本
├── setup_supervisor.sh  # Supervisor 設置腳本
├── start_crawler.sh      # 啟動腳本
└── README.md             # 說明文檔
```

### 添加新功能

1. 在 `src/` 目錄下創建新的 Python 模組
2. 在 `models.py` 中定義數據模型
3. 更新 `requirements.txt`（如果需要新依賴）
4. 更新文檔