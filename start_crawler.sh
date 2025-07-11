#!/bin/bash

# OKX Wallet Crawler 啟動腳本
# 支援 start, stop, restart, status 功能

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$SCRIPT_DIR"
SRC_DIR="$PROJECT_DIR/src"
LOG_DIR="$PROJECT_DIR/logs"
VENV_DIR="$PROJECT_DIR/venv"
PYTHON_PATH="$VENV_DIR/bin/python"
MAIN_SCRIPT="$SRC_DIR/okx_crawler.py"
PID_FILE="$PROJECT_DIR/crawler.pid"
LOG_FILE="$LOG_DIR/okx_wallet_crawler.log"

# 顏色定義
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 日誌函數
log_info() {
    echo -e "${GREEN}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_debug() {
    echo -e "${BLUE}[DEBUG]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# 檢查環境
check_environment() {
    log_info "檢查運行環境..."
    
    # 檢查虛擬環境
    if [ ! -f "$PYTHON_PATH" ]; then
        log_error "虛擬環境不存在，請先創建虛擬環境"
        log_info "執行: python3 -m venv venv"
        exit 1
    fi
    
    # 檢查主程序
    if [ ! -f "$MAIN_SCRIPT" ]; then
        log_error "主程序文件不存在: $MAIN_SCRIPT"
        exit 1
    fi
    
    # 檢查日誌目錄
    if [ ! -d "$LOG_DIR" ]; then
        log_info "創建日誌目錄: $LOG_DIR"
        mkdir -p "$LOG_DIR"
    fi
    
    # 檢查 .env 文件
    if [ ! -f "$PROJECT_DIR/.env" ]; then
        log_warn ".env 文件不存在，請先配置環境變數"
        log_info "執行: cp .env.example .env && nano .env"
    fi
    
    log_info "環境檢查完成"
}

# 獲取進程ID
get_pid() {
    if [ -f "$PID_FILE" ]; then
        cat "$PID_FILE"
    else
        echo ""
    fi
}

# 檢查進程是否運行
is_running() {
    local pid=$(get_pid)
    if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
        return 0
    else
        return 1
    fi
}

# 啟動程序
start() {
    log_info "啟動 OKX Wallet Crawler..."
    
    check_environment
    
    if is_running; then
        log_warn "程序已在運行中 (PID: $(get_pid))"
        return 1
    fi
    
    # 切換到源代碼目錄
    cd "$SRC_DIR"
    
    # 啟動程序
    nohup "$PYTHON_PATH" "$MAIN_SCRIPT" > "$LOG_FILE" 2>&1 &
    local pid=$!
    
    # 保存PID
    echo "$pid" > "$PID_FILE"
    
    # 等待一下檢查是否啟動成功
    sleep 2
    if kill -0 "$pid" 2>/dev/null; then
        log_info "程序啟動成功 (PID: $pid)"
        log_info "日誌文件: $LOG_FILE"
        return 0
    else
        log_error "程序啟動失敗"
        rm -f "$PID_FILE"
        return 1
    fi
}

# 停止程序
stop() {
    log_info "停止 OKX Wallet Crawler..."
    
    local pid=$(get_pid)
    if [ -z "$pid" ]; then
        log_warn "程序未運行"
        return 0
    fi
    
    if kill -0 "$pid" 2>/dev/null; then
        log_info "發送停止信號到進程 $pid..."
        kill "$pid"
        
        # 等待進程結束
        local count=0
        while kill -0 "$pid" 2>/dev/null && [ $count -lt 30 ]; do
            sleep 1
            count=$((count + 1))
        done
        
        if kill -0 "$pid" 2>/dev/null; then
            log_warn "程序未在30秒內停止，強制終止..."
            kill -9 "$pid"
        fi
        
        log_info "程序已停止"
    else
        log_warn "進程 $pid 不存在"
    fi
    
    rm -f "$PID_FILE"
}

# 重啟程序
restart() {
    log_info "重啟 OKX Wallet Crawler..."
    stop
    sleep 2
    start
}

# 查看狀態
status() {
    local pid=$(get_pid)
    
    echo "=== OKX Wallet Crawler 狀態 ==="
    echo "項目目錄: $PROJECT_DIR"
    echo "虛擬環境: $VENV_DIR"
    echo "主程序: $MAIN_SCRIPT"
    echo "日誌文件: $LOG_FILE"
    echo "PID文件: $PID_FILE"
    
    if [ -n "$pid" ]; then
        if kill -0 "$pid" 2>/dev/null; then
            echo -e "運行狀態: ${GREEN}運行中${NC} (PID: $pid)"
            
            # 顯示進程信息
            echo "進程信息:"
            ps -p "$pid" -o pid,ppid,cmd,etime,pcpu,pmem --no-headers 2>/dev/null || echo "無法獲取進程信息"
            
            # 顯示最近日誌
            if [ -f "$LOG_FILE" ]; then
                echo ""
                echo "最近日誌 (最後10行):"
                tail -n 10 "$LOG_FILE"
            fi
        else
            echo -e "運行狀態: ${RED}已停止${NC} (PID文件存在但進程不存在)"
            rm -f "$PID_FILE"
        fi
    else
        echo -e "運行狀態: ${YELLOW}未運行${NC}"
    fi
    
    echo "================================"
}

# 查看日誌
logs() {
    if [ -f "$LOG_FILE" ]; then
        tail -f "$LOG_FILE"
    else
        log_error "日誌文件不存在: $LOG_FILE"
    fi
}

# 清理
clean() {
    log_info "清理臨時文件..."
    rm -f "$PID_FILE"
    log_info "清理完成"
}

# 幫助信息
usage() {
    echo "OKX Wallet Crawler 管理腳本"
    echo ""
    echo "用法: $0 {start|stop|restart|status|logs|clean|help}"
    echo ""
    echo "命令:"
    echo "  start    啟動程序"
    echo "  stop     停止程序"
    echo "  restart  重啟程序"
    echo "  status   查看程序狀態"
    echo "  logs     查看實時日誌"
    echo "  clean    清理臨時文件"
    echo "  help     顯示此幫助信息"
    echo ""
    echo "示例:"
    echo "  $0 start    # 啟動程序"
    echo "  $0 status   # 查看狀態"
    echo "  $0 logs     # 查看日誌"
}

# 主邏輯
case "$1" in
    start)
        start
        ;;
    stop)
        stop
        ;;
    restart)
        restart
        ;;
    status)
        status
        ;;
    logs)
        logs
        ;;
    clean)
        clean
        ;;
    help|--help|-h)
        usage
        ;;
    *)
        echo "未知命令: $1"
        echo "使用 '$0 help' 查看幫助信息"
        exit 1
        ;;
esac

exit $? 