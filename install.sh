#!/bin/bash

# OKX Wallet Crawler 安裝腳本

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日誌函數
log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $1"
}

# 檢查系統要求
check_system_requirements() {
    log_info "檢查系統要求..."
    
    # 檢查 Python 版本
    if command -v python3 &> /dev/null; then
        PYTHON_VERSION=$(python3 --version 2>&1 | awk '{print $2}')
        log_info "Python 版本: $PYTHON_VERSION"
        
        # 檢查版本是否 >= 3.9
        if python3 -c "import sys; exit(0 if sys.version_info >= (3, 9) else 1)" 2>/dev/null; then
            log_info "Python 版本符合要求 (>= 3.9)"
        else
            log_error "Python 版本過低，需要 3.9 或更高版本"
            exit 1
        fi
    else
        log_error "未找到 Python3，請先安裝 Python 3.9+"
        exit 1
    fi
    
    # 檢查 pip
    if command -v pip3 &> /dev/null; then
        log_info "pip3 已安裝"
    else
        log_error "未找到 pip3，請先安裝 pip"
        exit 1
    fi
    
    # 檢查 git
    if command -v git &> /dev/null; then
        log_info "git 已安裝"
    else
        log_warn "未找到 git，建議安裝 git 以便版本控制"
    fi
    
    log_info "系統要求檢查完成"
}

# 創建虛擬環境
create_virtual_environment() {
    log_info "創建虛擬環境..."
    
    if [ -d "$PROJECT_DIR/venv" ]; then
        log_warn "虛擬環境已存在，是否重新創建？(y/N)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            log_info "刪除現有虛擬環境..."
            rm -rf "$PROJECT_DIR/venv"
        else
            log_info "使用現有虛擬環境"
            return 0
        fi
    fi
    
    log_info "創建新的虛擬環境..."
    python3 -m venv "$PROJECT_DIR/venv"
    
    if [ $? -eq 0 ]; then
        log_info "虛擬環境創建成功"
    else
        log_error "虛擬環境創建失敗"
        exit 1
    fi
}

# 安裝依賴
install_dependencies() {
    log_info "安裝 Python 依賴..."
    
    # 激活虛擬環境
    source "$PROJECT_DIR/venv/bin/activate"
    
    # 升級 pip
    log_info "升級 pip..."
    pip install --upgrade pip
    
    # 安裝依賴
    if [ -f "$PROJECT_DIR/requirements.txt" ]; then
        log_info "安裝 requirements.txt 中的依賴..."
        pip install -r "$PROJECT_DIR/requirements.txt"
        
        if [ $? -eq 0 ]; then
            log_info "依賴安裝成功"
        else
            log_error "依賴安裝失敗"
            exit 1
        fi
    else
        log_error "requirements.txt 文件不存在"
        exit 1
    fi
}

# 創建必要目錄
create_directories() {
    log_info "創建必要目錄..."
    
    # 創建日誌目錄
    mkdir -p "$PROJECT_DIR/logs"
    mkdir -p "$PROJECT_DIR/src/logs"
    
    # 創建運行配置目錄
    mkdir -p "$PROJECT_DIR/run"
    
    log_info "目錄創建完成"
}

# 設置環境變數
setup_environment() {
    log_info "設置環境變數..."
    
    if [ -f "$PROJECT_DIR/.env" ]; then
        log_warn ".env 文件已存在，是否覆蓋？(y/N)"
        read -r response
        if [[ "$response" =~ ^[Yy]$ ]]; then
            log_info "覆蓋現有 .env 文件"
        else
            log_info "保留現有 .env 文件"
            return 0
        fi
    fi
    
    if [ -f "$PROJECT_DIR/.env.example" ]; then
        cp "$PROJECT_DIR/.env.example" "$PROJECT_DIR/.env"
        log_info "已複製 .env.example 到 .env"
        log_warn "請編輯 .env 文件並填入實際的配置信息"
    else
        log_error ".env.example 文件不存在"
        exit 1
    fi
}

# 設置腳本權限
setup_permissions() {
    log_info "設置腳本權限..."
    
    # 設置啟動腳本權限
    if [ -f "$PROJECT_DIR/start_crawler.sh" ]; then
        chmod +x "$PROJECT_DIR/start_crawler.sh"
        log_info "啟動腳本權限設置完成"
    fi
    
    # 設置安裝腳本權限
    chmod +x "$PROJECT_DIR/install.sh"
    log_info "安裝腳本權限設置完成"
}

# 驗證安裝
verify_installation() {
    log_info "驗證安裝..."
    
    # 檢查虛擬環境
    if [ ! -d "$PROJECT_DIR/venv" ]; then
        log_error "虛擬環境不存在"
        return 1
    fi
    
    # 檢查主程序
    if [ ! -f "$PROJECT_DIR/src/okx_crawler.py" ]; then
        log_error "主程序文件不存在"
        return 1
    fi
    
    # 檢查啟動腳本
    if [ ! -f "$PROJECT_DIR/start_crawler.sh" ]; then
        log_error "啟動腳本不存在"
        return 1
    fi
    
    # 測試 Python 導入
    source "$PROJECT_DIR/venv/bin/activate"
    if python3 -c "import aiohttp, asyncio, sqlalchemy" 2>/dev/null; then
        log_info "Python 依賴導入測試成功"
    else
        log_error "Python 依賴導入測試失敗"
        return 1
    fi
    
    log_info "安裝驗證完成"
    return 0
}

# 顯示安裝完成信息
show_completion_info() {
    echo ""
    echo "=========================================="
    echo "🎉 OKX Wallet Crawler 安裝完成！"
    echo "=========================================="
    echo ""
    echo "下一步操作："
    echo "1. 編輯環境變數文件："
    echo "   nano .env"
    echo ""
    echo "2. 啟動程序："
    echo "   ./start_crawler.sh start"
    echo ""
    echo "3. 查看狀態："
    echo "   ./start_crawler.sh status"
    echo ""
    echo "4. 查看日誌："
    echo "   ./start_crawler.sh logs"
    echo ""
    echo "5. 停止程序："
    echo "   ./start_crawler.sh stop"
    echo ""
    echo "更多信息請查看 README.md"
    echo "=========================================="
}

# 主安裝流程
main() {
    echo "開始安裝 OKX Wallet Crawler..."
    echo ""
    
    # 檢查系統要求
    check_system_requirements
    
    # 創建虛擬環境
    create_virtual_environment
    
    # 安裝依賴
    install_dependencies
    
    # 創建必要目錄
    create_directories
    
    # 設置環境變數
    setup_environment
    
    # 設置腳本權限
    setup_permissions
    
    # 驗證安裝
    if verify_installation; then
        show_completion_info
    else
        log_error "安裝驗證失敗，請檢查錯誤信息"
        exit 1
    fi
}

# 執行主流程
main "$@" 