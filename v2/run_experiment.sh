#!/bin/bash
# =============================================================================
# run_experiment.sh - 在本地 WSL 执行的主控脚本
# 功能：上传脚本 → 协调主备节点实验 → 收集结果 → 生成报告 → 恢复环境
#
# 用法:
#   ./run_experiment.sh [PROTOCOL] [--services SVC1,SVC2]
#
#   PROTOCOL:
#     A            只运行 Protocol A 实验（默认）
#     C            只运行 Protocol C 实验
#     both         依次运行 A 和 C 并生成对比报告
#
#   --services:
#     mysql        只测试 MySQL
#     nsq          只测试 NSQ
#     mysql,nsq    同时测试 MySQL 和 NSQ（默认，等同于 all）
#     all          同上
#
# 示例:
#   ./run_experiment.sh C                         # Protocol C，测全部服务
#   ./run_experiment.sh A --services mysql         # Protocol A，只测 MySQL
#   ./run_experiment.sh both --services nsq        # A+C 对比，只测 NSQ
#   ./run_experiment.sh C --services mysql,nsq     # Protocol C，测全部
# =============================================================================

set -euo pipefail

# =============================================================================
# 配置
# =============================================================================

PRIMARY_IP="192.168.171.130"
SECONDARY_IP="192.168.171.131"
SSH_USER="root"
SSH_PASS="123456"
SSH_OPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 \
          -o ServerAliveInterval=15 -o ServerAliveCountMax=3"

DRBD_RES="r0"
DRBD_PORT=7789

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PRIMARY_SCRIPT="${SCRIPT_DIR}/primary_node.sh"
SECONDARY_SCRIPT="${SCRIPT_DIR}/secondary_node.sh"
RESULTS_DIR="${SCRIPT_DIR}/experiment_results_$(date '+%Y%m%d_%H%M%S')"

PROTOCOL="A"
SERVICES="all"

# 解析参数
_args=("$@")
_i=0
while [ $_i -lt ${#_args[@]} ]; do
    _arg="${_args[$_i]}"
    case "$_arg" in
        A|C|both)
            PROTOCOL="$_arg"
            ;;
        --services)
            _i=$((_i + 1))
            SERVICES="${_args[$_i]:-all}"
            ;;
        --services=*)
            SERVICES="${_arg#--services=}"
            ;;
        -h|--help)
            sed -n '/#/p' "${BASH_SOURCE[0]}" | head -20
            exit 0
            ;;
        *)
            echo "未知参数: $_arg" >&2
            exit 1
            ;;
    esac
    _i=$((_i + 1))
done

# 规范化 SERVICES：all / mysql,nsq 均转为 "mysql nsq"
case "$SERVICES" in
    all|"mysql,nsq"|"nsq,mysql") SERVICES="mysql nsq" ;;
    mysql)   SERVICES="mysql" ;;
    nsq)     SERVICES="nsq" ;;
    *)
        echo "无效 --services 值: ${SERVICES}（可用: mysql / nsq / mysql,nsq / all）" >&2
        exit 1
        ;;
esac

# 构建传给节点脚本的 SERVICES 参数（空格替换为逗号方便 shell 传参）
SERVICES_ARG=$(echo "$SERVICES" | tr ' ' ',')

# 颜色
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; MAGENTA='\033[0;35m'; NC='\033[0m'; BOLD='\033[1m'

log()     { echo -e "${CYAN}[$(date '+%H:%M:%S')]${NC} $*"; }
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
err()     { echo -e "${RED}[ERR ]${NC} $*"; }
section() {
    echo ""
    echo -e "${BOLD}${MAGENTA}▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓${NC}"
    echo -e "${BOLD}${MAGENTA}  ✦  $*${NC}"
    echo -e "${BOLD}${MAGENTA}▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓${NC}"
    echo ""
}

# =============================================================================
# 依赖检查
# =============================================================================

check_deps() {
    local missing=()
    for cmd in ssh scp sshpass; do
        command -v "$cmd" &>/dev/null || missing+=("$cmd")
    done

    if [ ${#missing[@]} -gt 0 ]; then
        err "缺少依赖工具: ${missing[*]}"
        info "安装命令: sudo apt-get install -y openssh-client sshpass"
        exit 1
    fi
    ok "依赖检查通过"
}

ok() { echo -e "${GREEN}[✓]${NC} $*"; }

# SSH/SCP 封装（自动处理密码）
ssh_cmd() {
    local host="$1"; shift
    sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "${SSH_USER}@${host}" "$@"
}

scp_upload() {
    local src="$1"
    local host="$2"
    local dst="$3"
    sshpass -p "${SSH_PASS}" scp ${SSH_OPTS} "$src" "${SSH_USER}@${host}:${dst}"
}

scp_download() {
    local host="$1"
    local src="$2"
    local dst="$3"
    sshpass -p "${SSH_PASS}" scp ${SSH_OPTS} "${SSH_USER}@${host}:${src}" "$dst"
}

# SSH 执行并实时输出（带节点标签）
ssh_stream() {
    local host="$1"
    local label="$2"
    shift 2
    local color
    [ "$host" = "$PRIMARY_IP" ] && color="${GREEN}" || color="${YELLOW}"

    local exit_file
    exit_file=$(mktemp)
    echo "0" > "$exit_file"   # 默认值，防止读到空

    # 使用 coproc 或 fd 重定向方式保证退出码在管道结束前落盘
    set +o pipefail
    {
        sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} "${SSH_USER}@${host}" "$@"
        echo $? > "$exit_file"
    } 2>&1 | while IFS= read -r line; do
        echo -e "${color}[${label}]${NC} ${line}"
    done
    # 等待子 shell 彻底结束后再读退出码
    wait 2>/dev/null || true
    set -o pipefail

    local rc
    rc=$(cat "$exit_file" 2>/dev/null || echo 0)
    rm -f "$exit_file"
    return "$rc"
}

# =============================================================================
# 环境检查
# =============================================================================

preflight_check() {
    section "预检：连通性与环境验证"

    info "检查本地依赖..."
    check_deps

    info "检查 primary (${PRIMARY_IP}) 连通性..."
    if ! sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} \
                 "${SSH_USER}@${PRIMARY_IP}" "echo ok" &>/dev/null; then
        err "无法连接 primary 节点 ${PRIMARY_IP}"
        exit 1
    fi
    ok "Primary 节点连通"

    info "检查 secondary (${SECONDARY_IP}) 连通性..."
    if ! sshpass -p "${SSH_PASS}" ssh ${SSH_OPTS} \
                 "${SSH_USER}@${SECONDARY_IP}" "echo ok" &>/dev/null; then
        err "无法连接 secondary 节点 ${SECONDARY_IP}"
        exit 1
    fi
    ok "Secondary 节点连通"

    info "检查 Primary DRBD 角色..."
    DRBD_ROLE=$(ssh_cmd "${PRIMARY_IP}" \
                "drbdadm status r0 2>/dev/null" \
                2>/dev/null | grep -E "role:" | head -3 || true)
    info "Primary DRBD 状态: ${DRBD_ROLE:-（未获取到，继续）}"

    info "检查 Primary MySQL..."
    MYSQL_STATUS=$(ssh_cmd "${PRIMARY_IP}" \
                   "systemctl is-active mysqld 2>/dev/null || echo inactive" \
                   2>/dev/null || echo "unknown")
    info "Primary MySQL: ${MYSQL_STATUS}"

    info "检查 Primary NSQ..."
    NSQ_STATUS=$(ssh_cmd "${PRIMARY_IP}" \
                 "systemctl is-active nsqd 2>/dev/null || echo inactive" \
                 2>/dev/null || echo "unknown")
    info "Primary NSQ: ${NSQ_STATUS}"

    info "验证脚本文件存在..."
    [ -f "$PRIMARY_SCRIPT" ]   || { err "缺少 primary_node.sh"; exit 1; }
    [ -f "$SECONDARY_SCRIPT" ] || { err "缺少 secondary_node.sh"; exit 1; }
    ok "脚本文件就绪"
}

# =============================================================================
# 上传脚本
# =============================================================================

upload_scripts() {
    section "上传实验脚本到虚拟机"

    info "上传 primary_node.sh → ${PRIMARY_IP}:/tmp/..."
    scp_upload "$PRIMARY_SCRIPT"   "$PRIMARY_IP"   "/tmp/primary_node.sh"
    ssh_cmd "$PRIMARY_IP" "chmod +x /tmp/primary_node.sh"
    ok "primary_node.sh 上传完成"

    info "上传 secondary_node.sh → ${SECONDARY_IP}:/tmp/..."
    scp_upload "$SECONDARY_SCRIPT" "$SECONDARY_IP" "/tmp/secondary_node.sh"
    ssh_cmd "$SECONDARY_IP" "chmod +x /tmp/secondary_node.sh"
    ok "secondary_node.sh 上传完成"
}

# =============================================================================
# 运行单次实验（单个 Protocol）
# =============================================================================

run_single_experiment() {
    local proto="$1"
    local result_subdir="${RESULTS_DIR}/protocol_${proto}"
    mkdir -p "$result_subdir"

    section "开始实验 - DRBD Protocol ${proto}"
    log "结果目录: ${result_subdir}"

    # ── 阶段1：运行主节点脚本 ──────────────────────────────────────────────

    section "[Protocol ${proto}] 主节点实验阶段"
    info "在 Primary (${PRIMARY_IP}) 上执行 primary_node.sh ${proto}..."
    info "实时输出如下（标记 [PRIMARY]）："
    echo ""

    # 执行主节点脚本，实时流式输出
    PRIMARY_EXIT=0
    ssh_stream "$PRIMARY_IP" "PRIMARY" \
        "bash /tmp/primary_node.sh ${proto} ${SERVICES_ARG}" || PRIMARY_EXIT=$?

    echo ""

    # 判断主节点是否成功完成（检查 EXPERIMENT_DONE 标记或结果包）
    PRIMARY_SUCCESS=0
    if [ $PRIMARY_EXIT -eq 0 ]; then
        PRIMARY_SUCCESS=1
        ok "主节点实验阶段完成（退出码 0）"
    elif ssh_cmd "$PRIMARY_IP" "[ -f /tmp/primary_results.tar.gz ]" 2>/dev/null; then
        PRIMARY_SUCCESS=1
        ok "主节点实验阶段完成（结果包存在）"
    else
        err "主节点脚本异常退出（退出码 ${PRIMARY_EXIT}）"
        echo ""
        warn "── 主节点错误详情 ──────────────────────────────────────"
        ssh_cmd "$PRIMARY_IP" \
            "cat /tmp/primary_error.txt 2>/dev/null || \
             { echo '（未捕获到具体错误，以下为日志末尾）'; \
               tail -10 /tmp/primary_experiment.log 2>/dev/null; }" \
            2>/dev/null | while IFS= read -r line; do
                echo -e "  ${line}"
            done
        echo ""
        warn "主节点执行失败，跳过备节点接管，直接进入恢复阶段"
    fi

    # 下载主节点结果（无论成功失败都尝试）
    if [ $PRIMARY_SUCCESS -eq 1 ]; then
        info "下载主节点结果..."
        scp_download "$PRIMARY_IP" \
                     "/tmp/primary_results.tar.gz" \
                     "${result_subdir}/primary_results.tar.gz" 2>/dev/null && \
            tar -xzf "${result_subdir}/primary_results.tar.gz" \
                -C "${result_subdir}/" 2>/dev/null && \
            ok "主节点结果已下载" || \
            warn "主节点结果下载失败，继续..."
    fi

    # 主节点失败则跳过备节点，直接恢复
    if [ $PRIMARY_SUCCESS -eq 0 ]; then
        warn "跳过备节点接管阶段"
        # 直接跳到恢复，用 return 1 通知调用方标记本次实验为失败
        _do_restore "$proto"
        return 1
    fi

    # ── 等待主节点完全隔离，并确认 DRBD 已降级 ──────────────────────────

    log "等待主节点故障状态稳定（3秒）..."
    sleep 3

    # 强制确认主节点已降为 Secondary，防止双 Primary 脑裂
    info "验证主节点 DRBD 已降级为 Secondary..."
    _primary_drbd_role=$(ssh_cmd "$PRIMARY_IP" \
        "drbdadm status ${DRBD_RES} 2>/dev/null | grep 'role:' | head -1 | grep -o 'role:[A-Za-z]*' | cut -d: -f2" \
        2>/dev/null || echo "Unknown")

    if [ "$_primary_drbd_role" = "Primary" ]; then
        warn "主节点仍为 Primary（可能降级未完成），强制执行降级..."
        ssh_cmd "$PRIMARY_IP" \
            "drbdadm secondary ${DRBD_RES} 2>/dev/null || true" 2>/dev/null || true
        sleep 2
        _primary_drbd_role=$(ssh_cmd "$PRIMARY_IP" \
            "drbdadm status ${DRBD_RES} 2>/dev/null | grep 'role:' | head -1 | grep -o 'role:[A-Za-z]*' | cut -d: -f2" \
            2>/dev/null || echo "Unknown")
    fi

    if [ "$_primary_drbd_role" = "Secondary" ] || [ "$_primary_drbd_role" = "Unknown" ]; then
        ok "主节点 DRBD 角色确认: ${_primary_drbd_role}，可安全启动备节点接管"
    else
        warn "主节点 DRBD 角色为 ${_primary_drbd_role}，继续执行但存在脑裂风险"
    fi

    # ── 阶段2：运行备节点接管脚本 ─────────────────────────────────────────

    section "[Protocol ${proto}] 备节点接管阶段"
    info "在 Secondary (${SECONDARY_IP}) 上执行 secondary_node.sh ${proto}..."
    info "实时输出如下（标记 [SECONDARY]）："
    echo ""

    SECONDARY_EXIT=0
    ssh_stream "$SECONDARY_IP" "SECONDARY" \
        "bash /tmp/secondary_node.sh ${proto} ${SERVICES_ARG}" || SECONDARY_EXIT=$?

    echo ""
    if [ $SECONDARY_EXIT -eq 0 ]; then
        ok "备节点接管阶段完成"
    elif ssh_cmd "$SECONDARY_IP" "[ -f /tmp/secondary_results.tar.gz ]" 2>/dev/null; then
        ok "备节点接管阶段完成（结果包存在）"
    else
        err "备节点脚本异常退出（退出码 ${SECONDARY_EXIT}）"
        echo ""
        warn "── 备节点错误详情 ──────────────────────────────────────"
        ssh_cmd "$SECONDARY_IP" \
            "cat /tmp/secondary_error.txt 2>/dev/null || \
             { echo '（未捕获到具体错误，以下为日志末尾）'; \
               tail -10 /tmp/secondary_experiment.log 2>/dev/null; }" \
            2>/dev/null | while IFS= read -r line; do
                echo -e "  ${line}"
            done
        echo ""
    fi

    # 下载备节点结果
    info "下载备节点结果..."
    scp_download "$SECONDARY_IP" \
                 "/tmp/secondary_results.tar.gz" \
                 "${result_subdir}/secondary_results.tar.gz" 2>/dev/null && \
        tar -xzf "${result_subdir}/secondary_results.tar.gz" \
            -C "${result_subdir}/" 2>/dev/null && \
        ok "备节点结果已下载" || \
        warn "备节点结果下载失败"

    # ── 阶段3：恢复环境 ───────────────────────────────────────────────────

    _do_restore "$proto"

    # ── 生成单次实验报告 ───────────────────────────────────────────────────

    set +e
    generate_single_report "$proto" "$result_subdir"
    set -e
}

# =============================================================================
# 环境恢复函数（独立抽出，主节点失败时也能直接调用）
# =============================================================================

_do_restore() {
    local proto="$1"

    section "[Protocol ${proto}] 恢复主节点环境"
    info "恢复 Primary 节点 iptables 和 DRBD..."

    ssh_stream "$PRIMARY_IP" "RESTORE" bash << 'RESTORE_EOF'
echo "[restore] 清理 iptables DRBD 规则..."
iptables -D INPUT  -p tcp --sport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp --dport 7789 -j DROP 2>/dev/null || true
iptables -D INPUT  -p tcp --dport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp --sport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp -j NFQUEUE --queue-num 0 2>/dev/null || true

# 清理多余的 DROP 规则（可能有多条）
for i in $(seq 1 5); do
    iptables -D OUTPUT -p tcp --dport 7789 -j DROP 2>/dev/null || break
done
for i in $(seq 1 5); do
    iptables -D INPUT -p tcp --sport 7789 -j DROP 2>/dev/null || break
done

echo "[restore] 清理拦截器进程..."
kill $(cat /tmp/interceptor.pid 2>/dev/null) 2>/dev/null || true
pkill -f drbd_interceptor.py 2>/dev/null || true

echo "[restore] 重启 DRBD 连接..."
drbdadm connect r0 2>/dev/null || true
sleep 3

echo "[restore] 当前 DRBD 状态："
drbdadm status r0 2>/dev/null
RESTORE_EOF

    ok "主节点 iptables 已清理，DRBD 连接已恢复"

    # ── 等待 DRBD 重新同步 ─────────────────────────────────────────────────

    info "等待 DRBD 重新同步（最多 60 秒）..."
    for i in $(seq 1 30); do
        STATUS=$(ssh_cmd "$PRIMARY_IP" \
                 "drbdadm status r0 2>/dev/null | grep 'peer-disk:' | head -1" \
                 2>/dev/null || echo "")
        if echo "$STATUS" | grep -q "peer-disk:UpToDate" 2>/dev/null; then
            if echo "$STATUS" | grep -q "peer-disk:UpToDate"; then
                ok "DRBD 重新同步完成: ${STATUS}"
                break
            fi
        fi
        [ $((i % 5)) -eq 0 ] && info "DRBD 同步中... ${STATUS:-connecting} (${i}/30)"
        sleep 2
    done

    # 备节点降级并重建
    info "备节点降级为 Secondary 并重建 DRBD 连接..."
    ssh_stream "$SECONDARY_IP" "RESTORE" bash << 'RESTORE2_EOF'
# 确保服务停止
systemctl stop mysqld 2>/dev/null || true
systemctl stop nsqd   2>/dev/null || true
sleep 1

# 卸载所有 drbd1 挂载点（/proc/mounts 枚举，防止 EEXIST 残留）
grep -w '/dev/drbd1' /proc/mounts 2>/dev/null | awk '{print $2}' | while read -r _mnt; do
    echo "[restore] umount ${_mnt}..."
    umount "${_mnt}" 2>/dev/null || umount -l "${_mnt}" 2>/dev/null || true
done
umount /database 2>/dev/null || umount -l /database 2>/dev/null || true

# 降级为 secondary
drbdadm secondary r0 2>/dev/null || true
sleep 2

# 重新连接
drbdadm connect r0 2>/dev/null || true
sleep 3

echo "备节点 DRBD 状态:"
drbdadm status r0 2>/dev/null
RESTORE2_EOF

    # 主节点重新提升并重建 Primary 环境
    info "主节点重新提升 Primary 并恢复服务..."
    ssh_stream "$PRIMARY_IP" "RESTORE" bash << 'RESTORE3_EOF'
# 等待与备节点重新连接
sleep 3

# 如果当前不是 Primary，尝试提升
drbdadm primary r0 2>/dev/null || true
sleep 2

# 重新挂载
if ! mountpoint -q /database 2>/dev/null; then
    mount /dev/drbd1 /database 2>/dev/null || true
fi

# 重启服务
systemctl start mysqld 2>/dev/null && echo "[restore] MySQL 已重启" || \
    echo "[restore] MySQL 重启失败"
systemctl start nsqd 2>/dev/null   && echo "[restore] NSQ 已重启"   || \
    echo "[restore] NSQ 重启失败"

echo "[restore] 最终 DRBD 状态:"
drbdadm status r0 2>/dev/null

echo "[restore] MySQL 状态: $(systemctl is-active mysqld 2>/dev/null)"
echo "[restore] NSQ 状态:   $(systemctl is-active nsqd 2>/dev/null)"
RESTORE3_EOF

    ok "Protocol ${proto} 环境恢复完成"
}

# =============================================================================
# 生成单次实验报告
# =============================================================================

generate_single_report() {
    local proto="$1"
    local result_dir="$2"
    local report_file="${result_dir}/experiment_report_protocol_${proto}.txt"

    # ── 读取各结果文件 ────────────────────────────────────────────────────
    MYSQL_PRIMARY_FILE=$(find "${result_dir}" -name "mysql_primary_after_commit.txt" 2>/dev/null | head -1)
    MYSQL_SECONDARY_FILE=$(find "${result_dir}" -name "mysql_secondary_data.txt" 2>/dev/null | head -1)
    MYSQL_START_FILE=$(find "${result_dir}" -name "mysql_start_result.txt" 2>/dev/null | head -1)
    NSQ_START_FILE=$(find "${result_dir}" -name "nsq_start_result.txt" 2>/dev/null | head -1)
    NSQ_META_PRIMARY_FILE=$(find "${result_dir}" -name "nsq_meta_after_primary.txt" 2>/dev/null | head -1)
    NSQ_META_SECONDARY_FILE=$(find "${result_dir}" -name "nsq_meta_secondary.txt" 2>/dev/null | head -1)
    CRASH_LOG_FILE=$(find "${result_dir}" -name "mysql_crash_recovery.txt" 2>/dev/null | head -1)
    INTERCEPTOR_FILE=$(find "${result_dir}" -name "interceptor.log" 2>/dev/null | head -1)

    # ── 计算关键指标 ──────────────────────────────────────────────────────

    # MySQL 行数（文件不存在时安全降级为 0）
    PRIMARY_ROWS=0
    [ -n "$MYSQL_PRIMARY_FILE" ] && [ -f "$MYSQL_PRIMARY_FILE" ] && \
        PRIMARY_ROWS=$(wc -l < "$MYSQL_PRIMARY_FILE" 2>/dev/null | tr -d ' ') || true
    [ -z "$PRIMARY_ROWS" ] && PRIMARY_ROWS=0

    SECONDARY_ROWS=0
    [ -n "$MYSQL_SECONDARY_FILE" ] && [ -f "$MYSQL_SECONDARY_FILE" ] && \
        SECONDARY_ROWS=$(wc -l < "$MYSQL_SECONDARY_FILE" 2>/dev/null | tr -d ' ') || true
    [ -z "$SECONDARY_ROWS" ] && SECONDARY_ROWS=0

    # MySQL 启动状态
    MYSQL_START_STATUS="UNKNOWN"
    [ -n "$MYSQL_START_FILE" ] && [ -f "$MYSQL_START_FILE" ] && \
        MYSQL_START_STATUS=$(cat "$MYSQL_START_FILE" 2>/dev/null) || true
    if echo "$MYSQL_START_STATUS" | grep -q "SUCCESS"; then
        MYSQL_START_VERDICT="✓ 启动成功"
    elif echo "$MYSQL_START_STATUS" | grep -q "FAILED"; then
        MYSQL_START_VERDICT="✗ 启动失败"
    else
        MYSQL_START_VERDICT="? 未知（备节点脚本未完成）"
    fi

    # MySQL 数据一致性
    if [ "$MYSQL_START_STATUS" = "MYSQL_START_SUCCESS" ] && \
       [ "$PRIMARY_ROWS" -gt 0 ] && [ "$SECONDARY_ROWS" -eq "$PRIMARY_ROWS" ]; then
        MYSQL_DATA_VERDICT="✓ 数据一致（主 ${PRIMARY_ROWS} 行 = 备 ${SECONDARY_ROWS} 行）"
    elif [ "$MYSQL_START_STATUS" = "MYSQL_START_SUCCESS" ] && \
         [ "$SECONDARY_ROWS" -lt "$PRIMARY_ROWS" ]; then
        MYSQL_DATA_VERDICT="✗ 数据丢失（主 ${PRIMARY_ROWS} 行，备 ${SECONDARY_ROWS} 行）"
    elif [ "$MYSQL_START_STATUS" = "MYSQL_START_FAILED" ]; then
        MYSQL_DATA_VERDICT="✗ 无法验证（MySQL 未能启动）"
    else
        MYSQL_DATA_VERDICT="? 未知（备节点脚本未完成）"
    fi

    # Crash Recovery
    if [ -n "$CRASH_LOG_FILE" ] && [ -s "$CRASH_LOG_FILE" ]; then
        CRASH_LINES=$(wc -l < "$CRASH_LOG_FILE" | tr -d ' ')
        HAS_ROLLBACK=$(grep -ci "rollback\|rolled back" "$CRASH_LOG_FILE" 2>/dev/null || echo 0)
        if [ "$HAS_ROLLBACK" -gt 0 ]; then
            CRASH_VERDICT="⚠ 检测到回滚（${HAS_ROLLBACK} 条相关日志）"
        else
            CRASH_VERDICT="✓ 无回滚（${CRASH_LINES} 条恢复日志）"
        fi
    else
        CRASH_VERDICT="? 未获取到（MySQL 未启动或日志路径未找到）"
    fi

    # NSQ 启动状态
    NSQ_START_STATUS="UNKNOWN"
    [ -n "$NSQ_START_FILE" ] && [ -f "$NSQ_START_FILE" ] && \
        NSQ_START_STATUS=$(cat "$NSQ_START_FILE" 2>/dev/null) || true
    if echo "$NSQ_START_STATUS" | grep -q "SUCCESS"; then
        NSQ_START_VERDICT="✓ 启动成功"
    elif echo "$NSQ_START_STATUS" | grep -q "FAILED"; then
        NSQ_START_VERDICT="✗ 启动失败"
    else
        NSQ_START_VERDICT="? 未知（备节点脚本未完成）"
    fi

    # NSQ meta.dat 一致性
    PRI_WRITE_POS="?"
    SEC_WRITE_POS="?"
    [ -n "$NSQ_META_PRIMARY_FILE" ] && [ -f "$NSQ_META_PRIMARY_FILE" ] && \
        PRI_WRITE_POS=$(grep "writePos" "$NSQ_META_PRIMARY_FILE" 2>/dev/null | awk '{print $NF}' | head -1) || true
    [ -z "$PRI_WRITE_POS" ] && PRI_WRITE_POS="?"
    [ -n "$NSQ_META_SECONDARY_FILE" ] && [ -f "$NSQ_META_SECONDARY_FILE" ] && \
        SEC_WRITE_POS=$(grep "writePos" "$NSQ_META_SECONDARY_FILE" 2>/dev/null | awk '{print $NF}' | head -1) || true
    [ -z "$SEC_WRITE_POS" ] && SEC_WRITE_POS="?"
    if [ "$PRI_WRITE_POS" = "?" ] || [ "$SEC_WRITE_POS" = "?" ]; then
        NSQ_META_VERDICT="? 未获取到 meta.dat 数据"
    elif [ "$PRI_WRITE_POS" = "$SEC_WRITE_POS" ]; then
        NSQ_META_VERDICT="✓ meta.dat 一致（writePos=${PRI_WRITE_POS}）"
    else
        NSQ_META_VERDICT="✗ meta.dat 不一致（主=${PRI_WRITE_POS} 备=${SEC_WRITE_POS}）"
    fi

    # IO 拦截统计（匹配新日志格式）
    if [ -n "$INTERCEPTOR_FILE" ] && [ -s "$INTERCEPTOR_FILE" ]; then
        PASS_CNT=$(grep -c "PASS data_frame" "$INTERCEPTOR_FILE" 2>/dev/null || echo 0)
        DROP_CNT=$(grep -c "DROP data_frame" "$INTERCEPTOR_FILE" 2>/dev/null || echo 0)
        if grep -q "nfqueue" "$INTERCEPTOR_FILE" 2>/dev/null; then
            INTERCEPT_MODE="nfqueue+协议解析"
        elif grep -q "connbytes" "$INTERCEPTOR_FILE" 2>/dev/null; then
            INTERCEPT_MODE="connbytes 降级"
        else
            INTERCEPT_MODE="未知"
        fi
        IO_VERDICT="通过 ${PASS_CNT} 个数据帧，丢弃 ${DROP_CNT} 个（${INTERCEPT_MODE}）"
    else
        IO_VERDICT="（日志未找到）"
    fi

    # 理论预期
    if [ "$proto" = "A" ]; then
        THEORY_MYSQL="✗ 数据丢失（Protocol A 异步，未同步事务被回滚）"
        THEORY_NSQ="✗ 消息丢失（meta/data 可能不一致）"
    else
        THEORY_MYSQL="✓ 数据完整（Protocol C 同步，所有 ACK 数据均已落盘）"
        THEORY_NSQ="✓ 消息完整（每条 IO 均已同步确认）"
    fi

    {
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║       DRBD 部分IO同步实验报告 - Protocol ${proto}                        ║"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    printf "║  实验时间: %-59s ║\n" "$(date '+%Y-%m-%d %H:%M:%S')"
    printf "║  主节点:   %-59s ║\n" "${PRIMARY_IP}"
    printf "║  备节点:   %-59s ║\n" "${SECONDARY_IP}"
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  ★  实验结论一览（备节点接管后）"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    printf "  %-20s  %-30s  %s\n" "验证项" "实测结果" "理论预期"
    printf "  %-20s  %-30s  %s\n" "--------------------" "------------------------------" "--------------------"
    printf "  %-20s  %-30s  %s\n" "MySQL 启动"      "$MYSQL_START_VERDICT"  ""
    printf "  %-20s  %-30s  %s\n" "MySQL 数据一致性" "$MYSQL_DATA_VERDICT"  "$THEORY_MYSQL"
    printf "  %-20s  %-30s  %s\n" "InnoDB CrashRecov" "$CRASH_VERDICT"      ""
    printf "  %-20s  %-30s  %s\n" "NSQ 启动"        "$NSQ_START_VERDICT"   ""
    printf "  %-20s  %-30s  %s\n" "NSQ 数据一致性"  "$NSQ_META_VERDICT"    "$THEORY_NSQ"
    printf "  %-20s  %-30s\n"     "IO 拦截"          "$IO_VERDICT"
    echo ""

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  1. MySQL 数据对比"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  [主节点 - 事务提交后（共 ${PRIMARY_ROWS} 行）]"
    if [ -n "$MYSQL_PRIMARY_FILE" ] && [ -s "$MYSQL_PRIMARY_FILE" ]; then
        cat "$MYSQL_PRIMARY_FILE" | sed 's/^/    /'
    else
        echo "    (数据未找到)"
    fi
    echo ""
    echo "  [备节点 - 接管启动后（共 ${SECONDARY_ROWS} 行）]"
    if [ -n "$MYSQL_SECONDARY_FILE" ] && [ -s "$MYSQL_SECONDARY_FILE" ]; then
        cat "$MYSQL_SECONDARY_FILE" | sed 's/^/    /'
    else
        echo "    (数据未找到或 MySQL 未能启动)"
    fi
    echo ""
    echo "  [InnoDB Crash Recovery 日志]"
    if [ -n "$CRASH_LOG_FILE" ] && [ -s "$CRASH_LOG_FILE" ]; then
        cat "$CRASH_LOG_FILE" | head -20 | sed 's/^/    /'
    else
        echo "    (未找到)"
    fi
    echo ""

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  2. NSQ 数据对比"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  [主节点 meta.dat（NSQ 写入后）writePos=${PRI_WRITE_POS}]"
    if [ -n "$NSQ_META_PRIMARY_FILE" ] && [ -s "$NSQ_META_PRIMARY_FILE" ]; then
        cat "$NSQ_META_PRIMARY_FILE" | sed 's/^/    /'
    else
        echo "    (未找到 - NSQ 可能未写入或未找到 meta.dat)"
    fi
    echo ""
    echo "  [备节点 meta.dat（接管后）writePos=${SEC_WRITE_POS}]"
    if [ -n "$NSQ_META_SECONDARY_FILE" ] && [ -s "$NSQ_META_SECONDARY_FILE" ]; then
        cat "$NSQ_META_SECONDARY_FILE" | sed 's/^/    /'
    else
        echo "    (未找到)"
    fi
    echo ""

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  3. IO 拦截详情"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [ -n "$INTERCEPTOR_FILE" ] && [ -s "$INTERCEPTOR_FILE" ]; then
        cat "$INTERCEPTOR_FILE" | sed 's/^/  /'
    else
        echo "  (拦截器日志未找到)"
    fi
    echo ""

    } | tee "$report_file"

    # ── 终端直打醒目汇总（不写文件，直接输出到屏幕）────────────────────
    echo ""
    echo -e "${BOLD}${CYAN}▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓${NC}"
    echo -e "${BOLD}${CYAN}  ✦  实验结论 - Protocol ${proto}${NC}"
    echo -e "${BOLD}${CYAN}▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓▓${NC}"
    echo ""
    echo -e "  ${BOLD}问题一：切换到备节点后所有服务能否正常启动？${NC}"
    echo    "  ──────────────────────────────────────────"

    if echo "$MYSQL_START_VERDICT" | grep -q "✓"; then
        echo -e "    MySQL  ${GREEN}${MYSQL_START_VERDICT}${NC}"
    else
        echo -e "    MySQL  ${RED}${MYSQL_START_VERDICT}${NC}"
    fi
    if echo "$NSQ_START_VERDICT" | grep -q "✓"; then
        echo -e "    NSQ    ${GREEN}${NSQ_START_VERDICT}${NC}"
    else
        echo -e "    NSQ    ${RED}${NSQ_START_VERDICT}${NC}"
    fi

    echo ""
    echo -e "  ${BOLD}问题二：服务启动后数据是否和主节点一致？${NC}"
    echo    "  ──────────────────────────────────────────"
    if echo "$MYSQL_DATA_VERDICT" | grep -q "✓"; then
        echo -e "    MySQL  ${GREEN}${MYSQL_DATA_VERDICT}${NC}"
    else
        echo -e "    MySQL  ${RED}${MYSQL_DATA_VERDICT}${NC}"
    fi
    if echo "$NSQ_META_VERDICT" | grep -q "✓"; then
        echo -e "    NSQ    ${GREEN}${NSQ_META_VERDICT}${NC}"
    else
        echo -e "    NSQ    ${RED}${NSQ_META_VERDICT}${NC}"
    fi

    echo ""
    if [ -n "$MYSQL_PRIMARY_FILE" ] && [ -s "$MYSQL_PRIMARY_FILE" ]; then
        echo -e "  ${BOLD}主节点写入数据（共 ${PRIMARY_ROWS} 行）:${NC}"
        cat "$MYSQL_PRIMARY_FILE" | sed 's/^/    /'
        echo ""
    fi
    if [ -n "$MYSQL_SECONDARY_FILE" ] && [ -s "$MYSQL_SECONDARY_FILE" ]; then
        echo -e "  ${BOLD}备节点恢复数据（共 ${SECONDARY_ROWS} 行）:${NC}"
        cat "$MYSQL_SECONDARY_FILE" | sed 's/^/    /'
    else
        echo -e "  ${BOLD}备节点恢复数据:${NC} ${RED}（无数据）${NC}"
    fi
    echo ""
    echo -e "  详细报告: ${report_file}"
    echo ""
    ok "实验报告已生成: ${report_file}"
}

# =============================================================================
# 生成对比报告（仅 both 模式）
# =============================================================================

generate_comparison_report() {
    local report_file="${RESULTS_DIR}/comparison_report_A_vs_C.txt"

    {
        echo "╔══════════════════════════════════════════════════════════════════════╗"
        echo "║             DRBD Protocol A vs C 对比实验总报告                     ║"
        echo "╠══════════════════════════════════════════════════════════════════════╣"
        printf "║  时间: %-65s ║\n" "$(date '+%Y-%m-%d %H:%M:%S')"
        echo "╚══════════════════════════════════════════════════════════════════════╝"
        echo ""

        for proto in A C; do
            local rdir="${RESULTS_DIR}/protocol_${proto}"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
            echo "  Protocol ${proto} 摘要"
            echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

            # MySQL
            M_START=$(find "$rdir" -name "mysql_start_result.txt" \
                          2>/dev/null | head -1 | xargs cat 2>/dev/null || echo "未知")
            M_FILE=$(find "$rdir" -name "mysql_secondary_data.txt" 2>/dev/null | head -1)
            M_ROWS=0
            [ -n "$M_FILE" ] && [ -f "$M_FILE" ] && M_ROWS=$(wc -l < "$M_FILE" 2>/dev/null || echo 0)
            echo "  MySQL 启动: ${M_START}"
            echo "  MySQL 备节点行数: ${M_ROWS}"

            # NSQ
            N_START=$(find "$rdir" -name "nsq_start_result.txt" \
                          2>/dev/null | head -1 | xargs cat 2>/dev/null || echo "未知")
            echo "  NSQ 启动: ${N_START}"

            C_FILE=$(find "$rdir" -name "nsq_consumed.txt" 2>/dev/null | head -1)
            CONSUMED=0
            [ -n "$C_FILE" ] && [ -f "$C_FILE" ] && CONSUMED=$(wc -c < "$C_FILE" 2>/dev/null || echo 0)
            if [ "${CONSUMED}" -gt 0 ] 2>/dev/null; then
                echo "  NSQ 消息可消费: ✓"
            else
                echo "  NSQ 消息可消费: ✗ (消息丢失)"
            fi
            echo ""
        done

        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "  核心结论"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        echo "  ┌──────────────────┬──────────────────┬──────────────────┐"
        echo "  │ 验证项           │ Protocol A       │ Protocol C       │"
        echo "  ├──────────────────┼──────────────────┼──────────────────┤"
        echo "  │ MySQL 能否启动   │ 通常能（依赖     │ 能（IO完整）     │"
        echo "  │                  │ crash recovery） │                  │"
        echo "  ├──────────────────┼──────────────────┼──────────────────┤"
        echo "  │ MySQL 数据一致性 │ 已提交事务可能   │ 已确认写入完整   │"
        echo "  │                  │ 在备节点丢失     │ 一致             │"
        echo "  ├──────────────────┼──────────────────┼──────────────────┤"
        echo "  │ NSQ 能否启动     │ 能（但meta/data  │ 能               │"
        echo "  │                  │ 可能不一致）     │                  │"
        echo "  ├──────────────────┼──────────────────┼──────────────────┤"
        echo "  │ NSQ 消息一致性   │ 消息静默丢失     │ 完整一致         │"
        echo "  │                  │ 或读取越界崩溃   │                  │"
        echo "  └──────────────────┴──────────────────┴──────────────────┘"

    } | tee "$report_file"

    ok "对比报告: ${report_file}"
}

# =============================================================================
# 主入口
# =============================================================================

main() {
    mkdir -p "$RESULTS_DIR"

    echo ""
    echo -e "${BOLD}${CYAN}"
    echo "  ╔═══════════════════════════════════════════════════════╗"
    echo "  ║         DRBD 部分IO同步场景验证实验                   ║"
    echo "  ║         主控脚本 (WSL 本地执行)                       ║"
    echo "  ╚═══════════════════════════════════════════════════════╝"
    echo -e "${NC}"
    echo "  Primary  : ${PRIMARY_IP}"
    echo "  Secondary: ${SECONDARY_IP}"
    echo "  Protocol : ${PROTOCOL}"
    echo "  服务范围 : ${SERVICES}"
    echo "  结果目录 : ${RESULTS_DIR}"
    echo ""

    # 预检
    preflight_check

    # 上传脚本
    upload_scripts

    if [ "${PROTOCOL}" = "both" ]; then
        # 依次运行 A 和 C
        info "将依次运行 Protocol A 和 Protocol C 实验..."
        echo ""

        run_single_experiment "A"
        log "Protocol A 实验完成，等待 10 秒后运行 Protocol C..."
        sleep 10

        run_single_experiment "C"

        set +e
        generate_comparison_report
        set -e

        section "全部实验完成！"
        info "结果目录: ${RESULTS_DIR}"
        echo ""
        echo "生成的报告文件:"
        find "${RESULTS_DIR}" -name "*.txt" | sort | \
            while read -r f; do echo "  $f"; done

    else
        # 单次实验
        run_single_experiment "${PROTOCOL}"

        section "实验完成！"
        info "结果目录: ${RESULTS_DIR}"
        echo ""
        echo "生成的报告文件:"
        find "${RESULTS_DIR}" -name "*.txt" 2>/dev/null | sort | \
            while read -r f; do echo "  $f"; done
    fi

    echo ""
    ok "所有环境已恢复，实验结束。"
}

# 捕获中断，确保清理
cleanup_on_exit() {
    warn "实验被中断，尝试恢复环境..."
    ssh_cmd "$PRIMARY_IP" bash << 'EOF' 2>/dev/null || true
iptables -D INPUT  -p tcp --sport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp --dport 7789 -j DROP 2>/dev/null || true
iptables -D INPUT  -p tcp --dport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp --sport 7789 -j DROP 2>/dev/null || true
iptables -D OUTPUT -p tcp -j NFQUEUE --queue-num 0 2>/dev/null || true
pkill -f drbd_interceptor.py 2>/dev/null || true
drbdadm connect r0 2>/dev/null || true
mountpoint -q /database || mount /dev/drbd1 /database 2>/dev/null || true
systemctl start mysqld nsqd 2>/dev/null || true
EOF
    warn "清理完成（部分操作可能需要手动验证）"
}

trap cleanup_on_exit INT TERM

main "$@"
