#!/bin/bash
# =============================================================================
# secondary_node.sh - 在备节点 (192.168.171.131) 上执行
# 功能：接管 DRBD → 挂载文件系统 → 分析数据 → 启动服务 → 验证一致性 → 恢复环境
# =============================================================================
set -eu
# set -euo pipefail

# ── ERR trap：捕获触发 set -e 的具体命令、行号、退出码 ──────────────────
SECONDARY_ERROR_FILE="/tmp/secondary_error.txt"
: > "$SECONDARY_ERROR_FILE"

_err_trap() {
    local exit_code=$?
    local line_no=${BASH_LINENO[0]}
    local cmd="${BASH_COMMAND}"
    {
        echo "═══ 脚本异常退出 ═══"
        echo "退出码  : ${exit_code}"
        echo "行号    : ${line_no}"
        echo "失败命令: ${cmd}"
        echo ""
        echo "── 调用栈 ──"
        local i=0
        while caller $i; do
            i=$((i+1))
        done 2>/dev/null || true
        echo ""
        echo "── 当前段落上下文（前后5行）──"
        sed -n "$((line_no > 5 ? line_no - 5 : 1)),$((line_no + 5))p" \
            "$0" 2>/dev/null | cat -n || true
    } | tee -a "$SECONDARY_ERROR_FILE" >&2
}
trap '_err_trap' ERR

MYSQL_PASS='br7^L9x@NT}!GyAM43>LuYpKN]Jzjo6^'
MYSQL_SOCK="/opt/mysql/bin/mysql.sock"
NSQ_HTTP="http://127.0.0.1:4151"
NSQ_DATA_DIR_DEFAULT="/database/nsq/data/nsqd"
TOPIC="drbd_test_topic"
LOG="/tmp/secondary_experiment.log"
RESULT_DIR="/tmp/drbd_results_secondary"
PRIMARY_IP="192.168.171.130"
DRBD_PORT=7789
DRBD_RES="r0"
DRBD_DEV="/dev/drbd1"
MOUNT_POINT="/database"

# MySQL 服务名已确认为 mysqld，直接使用
MYSQL_SERVICE="mysqld"
MYSQL_BIN="/opt/mysql/bin/mysqld"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S.%3N')]${NC} $*" | tee -a "$LOG"; }
info() { echo -e "${GREEN}[INFO]${NC} $*" | tee -a "$LOG"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$LOG"; }
err()  { echo -e "${RED}[ERR ]${NC} $*" | tee -a "$LOG"; }
ok()   { echo -e "${GREEN}[✓]${NC} $*" | tee -a "$LOG"; }
fail() { echo -e "${RED}[✗]${NC} $*" | tee -a "$LOG"; }
section() {
    echo "" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}  $*${NC}" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}" | tee -a "$LOG"
}

PROTOCOL="${1:-A}"
# 第二个参数：要验证的服务，逗号分隔，默认 all
_svc_arg="${2:-all}"
case "$_svc_arg" in
    all|"mysql,nsq"|"nsq,mysql") TEST_MYSQL=1; TEST_NSQ=1 ;;
    mysql)  TEST_MYSQL=1; TEST_NSQ=0 ;;
    nsq)    TEST_MYSQL=0; TEST_NSQ=1 ;;
    *)      TEST_MYSQL=1; TEST_NSQ=1 ;;
esac

# =============================================================================
# 工具函数
# =============================================================================

mysql_cmd() {
    # 自动探测 MySQL socket 路径
    local sock="${MYSQL_SOCK}"
    if [ ! -S "$sock" ]; then
        for candidate in /opt/mysql/bin/mysql.sock \
                         /tmp/mysql.sock \
                         /var/lib/mysql/mysql.sock \
                         /run/mysqld/mysqld.sock \
                         /var/run/mysqld/mysqld.sock; do
            [ -S "$candidate" ] && sock="$candidate" && break
        done
    fi
    mysql -uroot -p"${MYSQL_PASS}" --socket="${sock}" \
          --connect-timeout=10 -N -s "$@"
}

check_drbd_status() {
    drbdadm status r0 2>/dev/null || echo "DRBD status unavailable"
}

# =============================================================================
# 主流程
# =============================================================================

mkdir -p "$RESULT_DIR"
: > "$LOG"

section "备节点接管实验开始 (Protocol ${PROTOCOL})"

log "备节点 IP: 192.168.171.131"
log "主节点 IP: ${PRIMARY_IP}"
_svc_display=""
[ "$TEST_MYSQL" -eq 1 ] && _svc_display="${_svc_display}mysql " || true
[ "$TEST_NSQ"   -eq 1 ] && _svc_display="${_svc_display}nsq"    || true
log "验证服务 : ${_svc_display:-（未指定）}"

# =============================================================================
# 步骤1：DRBD 状态检查
# =============================================================================

section "步骤1：DRBD 状态检查"

info "当前 DRBD 状态（接管前）："
DRBD_BEFORE=$(check_drbd_status)
echo "$DRBD_BEFORE" | tee "${RESULT_DIR}/drbd_before_takeover.txt" | tee -a "$LOG"

# 判断当前角色（drbdadm status r0 输出格式：r0 role:Secondary）
if echo "$DRBD_BEFORE" | grep -q "role:Primary"; then
    warn "当前节点已经是 Primary，可能主节点未完全隔离"
fi

# =============================================================================
# 步骤2：强制提升为 Primary
# =============================================================================

section "步骤2：强制提升 DRBD 为 Primary"

# DRBD9 不允许双 Primary，必须先让主节点降级
# 此时主节点的 MySQL/NSQ 已停止（由 primary_node.sh 的故障模拟完成）
# 但 DRBD 进程本身可能仍持有 Primary 角色，需要 SSH 通知其降级
info "通知主节点 DRBD 降级为 Secondary..."
ssh -o StrictHostKeyChecking=no -o ConnectTimeout=5 \
    root@${PRIMARY_IP} \
    "drbdadm secondary ${DRBD_RES} 2>/dev/null; \
     echo 'primary demoted'" 2>/dev/null | tee -a "$LOG" || {
    warn "SSH 通知主节点降级失败（可能已断网，属正常实验场景）"
    warn "将尝试通过 disconnect 方式强制接管..."
    # 断开与主节点的连接，使本节点进入 StandAlone 状态后再提升
    drbdadm disconnect ${DRBD_RES} 2>/dev/null || true
    sleep 2
}

sleep 2

info "尝试提升本节点为 Primary..."
if drbdadm primary ${DRBD_RES} 2>/dev/null; then
    ok "正常提升成功"
else
    warn "正常提升失败，尝试强制提升（drbdsetup primary --force）..."
    # DRBD9 的强制提升命令
    if drbdsetup primary ${DRBD_RES} --force 2>&1 | tee -a "$LOG"; then
        ok "强制提升成功"
    else
        warn "在线强制提升失败，尝试断开连接后提升..."
        drbdadm disconnect ${DRBD_RES} 2>/dev/null || true
        sleep 3
        # StandAlone 模式下提升
        drbdadm primary ${DRBD_RES} 2>&1 | tee -a "$LOG" || {
            err "所有提升方式均失败，检查 DRBD 配置中的 allow-two-primaries"
            err "可在 /etc/drbd.d/global_common.conf 的 net {} 块中添加:"
            err "  allow-two-primaries no;"
            err "并确认 primary_node.sh 已成功执行 drbdadm secondary"
            check_drbd_status | tee -a "$LOG"
            exit 1
        }
    fi
fi

sleep 2
info "提升后 DRBD 状态："
DRBD_AFTER=$(check_drbd_status)
echo "$DRBD_AFTER" | tee "${RESULT_DIR}/drbd_after_takeover.txt" | tee -a "$LOG"

if ! echo "$DRBD_AFTER" | grep -q "role:Primary"; then
    err "DRBD 提升似乎未成功，但继续尝试..."
fi

# =============================================================================
# 步骤3：挂载文件系统
# =============================================================================

section "步骤3：挂载 DRBD 文件系统"

info "DRBD 设备: ${DRBD_DEV}"

# 检测文件系统类型
FS_TYPE=$(blkid -o value -s TYPE "${DRBD_DEV}" 2>/dev/null || echo "ext4")
info "文件系统类型: ${FS_TYPE}"

mkdir -p "${MOUNT_POINT}"

# ── 彻底清理挂载状态 ────────────────────────────────────────────────────
# 问题场景：
#   a) /database 已被挂载（上次实验未清理）
#   b) drbd1 设备被挂载在其他路径
#   c) /proc/mounts 记录了挂载但 mountpoint -q 未检测到（EEXIST）
# 解决：先检查 /proc/mounts 里所有 drbd1 的挂载，全部 umount
info "清理 ${DRBD_DEV} 残留挂载..."
(grep -w "${DRBD_DEV}" /proc/mounts || true) | awk '{print $2}' | while read -r _mnt; do
    info "发现残留挂载: ${_mnt}，执行 umount..."
    umount "${_mnt}" 2>/dev/null || umount -l "${_mnt}" 2>/dev/null || true
done
# 再检查挂载点本身
if mountpoint -q "${MOUNT_POINT}" 2>/dev/null; then
    info "${MOUNT_POINT} 仍被挂载，执行 umount..."
    umount "${MOUNT_POINT}" 2>/dev/null || umount -l "${MOUNT_POINT}" 2>/dev/null || true
fi
sleep 1
# ────────────────────────────────────────────────────────────────────────

if ! mount "${DRBD_DEV}" "${MOUNT_POINT}" 2>/dev/null; then
    warn "挂载失败，尝试 fsck 修复..."
    fsck -y "${DRBD_DEV}" 2>&1 | tee -a "$LOG" || true
    if ! mount "${DRBD_DEV}" "${MOUNT_POINT}" 2>&1 | tee -a "$LOG"; then
        err "文件系统挂载失败！"
        info "── 内核日志 ──"
        dmesg | tail -20 | tee -a "$LOG"
        info "── /proc/mounts 中 drbd 相关 ──"
        grep drbd /proc/mounts 2>/dev/null | tee -a "$LOG" || echo "(无)"
        exit 1
    fi
fi

ok "文件系统挂载成功: ${DRBD_DEV} → ${MOUNT_POINT}"
df -h "${MOUNT_POINT}" | tee -a "$LOG"

info "DRBD 挂载点目录内容："
ls -la "${MOUNT_POINT}/" | tee -a "$LOG"

# =============================================================================
# 步骤4：MySQL 文件完整性检查（启动前）
# =============================================================================

section "步骤4：MySQL 文件完整性检查"

if [ "$TEST_MYSQL" -eq 0 ]; then
    info "（跳过 MySQL，--services 未包含 mysql）"
    MYSQL_DATADIR="/database/mysql/data"
    STARTED=0
    echo "MYSQL_SKIPPED" > "${RESULT_DIR}/mysql_start_result.txt"
else

# 使用已知的 MySQL datadir 路径
MYSQL_DATADIR="/database/mysql/data"
if [ ! -d "$MYSQL_DATADIR" ]; then
    MYSQL_DATADIR=$(grep -r "^datadir" /etc/my.cnf /etc/my.cnf.d/ /etc/mysql/ \
                        2>/dev/null | head -1 | cut -d= -f2 | tr -d ' ')
    MYSQL_DATADIR="${MYSQL_DATADIR:-/var/lib/mysql}"
fi
info "MySQL datadir: ${MYSQL_DATADIR}"

info "--- InnoDB 核心文件 ---"
ls -la "${MYSQL_DATADIR}"/*.ibd 2>/dev/null | head -5 | tee -a "$LOG" || true
ls -la "${MYSQL_DATADIR}"/ibdata* 2>/dev/null | tee -a "$LOG" || true

info "--- Redo Log 文件 ---"
ls -la "${MYSQL_DATADIR}"/ib_logfile* 2>/dev/null | tee -a "$LOG" || \
ls -la "${MYSQL_DATADIR}"/'#innodb_redo'/ 2>/dev/null | tee -a "$LOG" || \
    warn "未找到 redo log 文件"

info "--- Binlog 文件 ---"
ls -la "${MYSQL_DATADIR}"/binlog* 2>/dev/null | tee -a "$LOG" || \
ls -la "${MYSQL_DATADIR}"/mysql-bin* 2>/dev/null | tee -a "$LOG" || \
    warn "未找到 binlog 文件"

info "--- 测试表 ibd 文件 ---"
ls -la "${MYSQL_DATADIR}"/drbd_test/ 2>/dev/null | tee -a "$LOG" || \
    warn "drbd_test 数据库目录不存在"

ZERO_FILES=$(find "${MYSQL_DATADIR}" -name "*.ibd" -size 0 2>/dev/null | wc -l)
if [ "$ZERO_FILES" -gt 0 ]; then
    warn "发现 ${ZERO_FILES} 个大小为 0 的 .ibd 文件（可能损坏）："
    find "${MYSQL_DATADIR}" -name "*.ibd" -size 0 | tee -a "$LOG"
else
    ok "未发现大小为 0 的 .ibd 文件"
fi

find "${MYSQL_DATADIR}" \( -name "*.ibd" -o -name "ib_logfile*" \
     -o -name "ibdata*" \) 2>/dev/null | \
     xargs -I{} stat {} 2>/dev/null \
     > "${RESULT_DIR}/mysql_files_before_start.txt" || true

fi # TEST_MYSQL

# =============================================================================
# 步骤5：启动 MySQL 并分析 Crash Recovery
# =============================================================================

section "步骤5：启动 MySQL（观察 Crash Recovery）"

if [ "$TEST_MYSQL" -eq 0 ]; then
    info "（跳过 MySQL 启动，--services 未包含 mysql）"
    STARTED=0
else

# 确保 MySQL 停止
if [ -n "$MYSQL_SERVICE" ]; then
    systemctl stop "$MYSQL_SERVICE" 2>/dev/null || true
else
    pkill -f "$MYSQL_BIN" 2>/dev/null || true
fi
sleep 2

# 找 MySQL 错误日志路径（所有命令加 || true 防止 set -e）
MYSQL_ERR_LOG_FULL=""
# 先在已挂载的目录里找
_found=$(find /database/mysql /opt/mysql -name "*.err" 2>/dev/null | head -1 || true)
[ -z "$_found" ] && _found=$(find /database/mysql /opt/mysql -name "error.log" 2>/dev/null | head -1 || true)
[ -n "$_found" ] && MYSQL_ERR_LOG_FULL="$_found"
# 再找系统路径
if [ -z "$MYSQL_ERR_LOG_FULL" ]; then
    for candidate in /var/log/mysqld.log /var/log/mysql/error.log /var/log/mysql/mysqld.err; do
        [ -f "$candidate" ] && MYSQL_ERR_LOG_FULL="$candidate" && break
    done
fi
# 从 my.cnf 读取
if [ -z "$MYSQL_ERR_LOG_FULL" ] && [ -n "$MYSQL_SERVICE" ]; then
    MYSQL_ERR_LOG_FULL=$(systemctl cat "$MYSQL_SERVICE" 2>/dev/null \
        | grep -o 'log.error=[^ ]*' | cut -d= -f2 | head -1 || true)
fi
MYSQL_ERR_LOG_FULL="${MYSQL_ERR_LOG_FULL:-/var/log/mysqld.log}"
info "MySQL 错误日志路径: ${MYSQL_ERR_LOG_FULL}"
info "MySQL 服务名: ${MYSQL_SERVICE:-（无 systemd unit，将直接启动二进制）}"

ERR_LOG_LINES_BEFORE=0
[ -f "$MYSQL_ERR_LOG_FULL" ] && \
    ERR_LOG_LINES_BEFORE=$(wc -l < "$MYSQL_ERR_LOG_FULL" 2>/dev/null || echo 0)

MYSQL_STARTUP_LOG="/tmp/mysql_startup_secondary.log"
: > "${MYSQL_STARTUP_LOG}"
STARTED=0

info "启动 MySQL..."
if [ -n "$MYSQL_SERVICE" ]; then
    # 有 systemd unit：同步启动，捕获详细错误
    if systemctl start "$MYSQL_SERVICE" 2>"${MYSQL_STARTUP_LOG}"; then
        STARTED=1
        ok "MySQL 通过 systemctl 启动成功"
    else
        RC=$?
        fail "systemctl start ${MYSQL_SERVICE} 失败（退出码 ${RC}）"

        # 打印 systemctl 的错误输出
        if [ -s "${MYSQL_STARTUP_LOG}" ]; then
            info "systemctl stderr:"
            cat "${MYSQL_STARTUP_LOG}" | tee -a "$LOG"
        fi

        # 单独运行 ExecStartPre 脚本，捕获它的具体报错
        info "── 运行 ExecStartPre 诊断（/opt/mysql/bin/mysql-systemd-start pre）──"
        /opt/mysql/bin/mysql-systemd-start pre 2>&1 | tee -a "$LOG" || true

        # journalctl 最近日志
        info "── journalctl 最近日志 ──"
        journalctl -u "$MYSQL_SERVICE" --since "2 minutes ago" --no-pager \
                   2>/dev/null | tail -40 | tee -a "$LOG" || true
    fi
fi

# systemd 启动失败或无 unit 时，尝试直接调用 mysqld 二进制
if [ $STARTED -eq 0 ]; then
    warn "尝试直接启动 mysqld 二进制: ${MYSQL_BIN}"
    MYSQL_DEFAULTS=""
    for cnf in /etc/my.cnf /etc/mysql/my.cnf /opt/mysql/etc/my.cnf; do
        [ -f "$cnf" ] && MYSQL_DEFAULTS="--defaults-file=${cnf}" && break
    done
    "$MYSQL_BIN" $MYSQL_DEFAULTS \
        --user=mysql \
        --datadir="${MYSQL_DATADIR}" \
        --socket="${MYSQL_SOCK}" \
        --daemonize \
        >> "${MYSQL_STARTUP_LOG}" 2>&1 || true
    sleep 5
fi

# 等待 socket 出现（最多 60 秒）
info "等待 MySQL socket 就绪..."
for i in $(seq 1 60); do
    if [ -S "${MYSQL_SOCK}" ]; then
        STARTED=1
        ok "MySQL 启动成功（socket 已出现，等待约 ${i} 秒）"
        break
    fi
    sleep 1
    [ $((i % 10)) -eq 0 ] && info "等待中... ${i}/60s，当前日志末尾:"
    [ $((i % 10)) -eq 0 ] && tail -3 "${MYSQL_ERR_LOG_FULL}" 2>/dev/null | tee -a "$LOG" || true
done

# 输出启动期间的新增日志
info "--- MySQL 启动期间新增日志 ---"
if [ -f "$MYSQL_ERR_LOG_FULL" ]; then
    tail -n +"$((ERR_LOG_LINES_BEFORE + 1))" "$MYSQL_ERR_LOG_FULL" \
         2>/dev/null | tee "${MYSQL_STARTUP_LOG}" | tee -a "$LOG" || true
fi

if [ $STARTED -eq 0 ]; then
    fail "MySQL 启动失败，以下是完整日志末尾 50 行："
    tail -50 "${MYSQL_ERR_LOG_FULL}" 2>/dev/null | tee -a "$LOG" || true
    echo "MYSQL_START_FAILED" > "${RESULT_DIR}/mysql_start_result.txt"
else
    echo "MYSQL_START_SUCCESS" > "${RESULT_DIR}/mysql_start_result.txt"
fi

# 提取 Crash Recovery 关键日志
section "MySQL Crash Recovery 分析"

info "--- InnoDB Crash Recovery 日志 ---"
grep -iE "crash recovery|starting crash|applying batch|redo|rollback|\
rolled back|log sequence|corruption|innodb: \[error\]|starting up" \
    "${MYSQL_ERR_LOG_FULL}" 2>/dev/null | tail -30 \
    | tee "${RESULT_DIR}/mysql_crash_recovery.txt" | tee -a "$LOG" || true

if grep -qi "corruption\|corrupt\|failed" \
        "${RESULT_DIR}/mysql_crash_recovery.txt" 2>/dev/null; then
    warn "⚠  发现潜在损坏标志！"
else
    info "未检测到明显损坏标志"
fi || true

# =============================================================================
# 步骤6：MySQL 数据一致性验证
# =============================================================================

section "步骤6：MySQL 数据一致性验证"

if [ $STARTED -eq 1 ]; then
    info "查询备节点恢复后的数据..."
    mysql_cmd drbd_test -e \
        "SELECT id, val, ts FROM iops_trace ORDER BY id;" \
        > "${RESULT_DIR}/mysql_secondary_data.txt" 2>/dev/null

    info "备节点恢复后的数据："
    AFTER_ROW_COUNT=$(wc -l < "${RESULT_DIR}/mysql_secondary_data.txt" 2>/dev/null || echo "0")
    info "主节点提交后 iops_trace 表行数: ${AFTER_ROW_COUNT}"
    # cat "${RESULT_DIR}/mysql_secondary_data.txt" | tee -a "$LOG"

    # 行数对比
    PRIMARY_ROWS=$(wc -l < /tmp/primary_mysql_rows.txt 2>/dev/null || echo "?")
    SECONDARY_ROWS=$(wc -l < "${RESULT_DIR}/mysql_secondary_data.txt")
    info "行数统计 - 备节点: ${SECONDARY_ROWS} 行"

    # InnoDB 事务状态
    info "--- InnoDB 事务状态 ---"
    mysql_cmd -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null | \
        grep -A 10 "TRANSACTIONS" | head -15 | tee -a "$LOG" || true

    # Binlog 状态
    info "--- Binlog 状态 ---"
    mysql_cmd -e "SHOW MASTER STATUS\G" 2>/dev/null | \
        tee "${RESULT_DIR}/mysql_binlog_secondary.txt" | tee -a "$LOG" || true

    # LSN 信息
    info "--- InnoDB LSN ---"
    mysql_cmd -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null | \
        grep "Log sequence number" | tee -a "$LOG" || true

else
    fail "MySQL 未能启动，跳过数据验证"
    echo "无法获取数据（MySQL 启动失败）" \
         > "${RESULT_DIR}/mysql_secondary_data.txt"
fi

fi # TEST_MYSQL (步骤5/6)

# =============================================================================
# 步骤7：NSQ 文件完整性检查（启动前）
# =============================================================================

section "步骤7：NSQ diskqueue 文件完整性检查"

if [ "$TEST_NSQ" -eq 0 ]; then
    info "（跳过 NSQ，--services 未包含 nsq）"
    NSQ_DATA_DIR="${NSQ_DATA_DIR_DEFAULT}"
    NSQ_STARTED=0
    echo "NSQ_SKIPPED" > "${RESULT_DIR}/nsq_start_result.txt"
else

# 直接使用已知的 NSQ 数据目录路径
NSQ_DATA_DIR="${NSQ_DATA_DIR_DEFAULT}"
if [ ! -d "$NSQ_DATA_DIR" ]; then
    NSQ_DATA_DIR="${MOUNT_POINT}/nsq/data/nsqd"
fi
if [ ! -d "$NSQ_DATA_DIR" ]; then
    NSQ_DATA_DIR="${MOUNT_POINT}/nsq"
fi
info "NSQ 数据目录: ${NSQ_DATA_DIR}"

info "--- diskqueue 文件列表 ---"
find "${NSQ_DATA_DIR}" -name "*.dat" 2>/dev/null | \
    while read -r f; do
        echo "  $f  $(stat -c'size=%s bytes mtime=%y' "$f" 2>/dev/null)"
    done | tee -a "$LOG"

info "--- meta.dat 内容解析（备节点，接管后）---"
python3 << PYEOF | tee "${RESULT_DIR}/nsq_meta_secondary.txt" | tee -a "$LOG"
import glob, os

files = glob.glob('${NSQ_DATA_DIR}/**/*.meta.dat', recursive=True)
if not files:
    print("  未找到 meta.dat 文件（可能 IO 未同步到备节点）")
else:
    for f in files:
        print(f"\n  文件: {f}")
        print(f"  文件大小: {os.path.getsize(f)} bytes")
        try:
            with open(f, 'r') as fp:
                lines = [line.strip() for line in fp.readlines()]
            if len(lines) >= 3:
                count = lines[0]
                read_pos, write_pos = map(int, lines[1].split(','))
                next_read_file, next_write_file = map(int, lines[2].split(','))
                if write_pos == read_pos and count == 0:
                    print(f"    ★ 队列为空（meta 未更新 或 消息从未同步）")
                elif write_pos > read_pos:
                    print(f"    ★ 队列有数据（writePos > readPos）")
            else:
                print(f"    文件行数不足，内容: {lines}")
        except Exception as e:
            print(f"    读取失败: {e}")
PYEOF

info "--- .dat 数据文件内容（备节点，16进制）---"
find "${NSQ_DATA_DIR}" -name "*.dat" ! -name "*.meta.dat" 2>/dev/null | \
    while read -r f; do
        echo "=== $f ($(stat -c%s "$f") bytes) ==="
        (trap '' PIPE; hexdump -C "$f" 2>/dev/null | head -30) || true
    done | tee "${RESULT_DIR}/nsq_dat_secondary.txt" | tee -a "$LOG"

# 对比 .dat 文件大小（检查数据是否写入但 meta 未更新）
info "--- .dat vs meta.dat 一致性检查 ---"
python3 << PYEOF | tee -a "$LOG"
import struct, glob, os

data_files = glob.glob('${NSQ_DATA_DIR}/**/*.dat', recursive=True)
data_files = [f for f in data_files if '.meta.' not in f]
meta_files = glob.glob('${NSQ_DATA_DIR}/**/*.meta.dat', recursive=True)

for dat in data_files:
    dat_size = os.path.getsize(dat)
    # 找对应的 meta
    base = dat.replace('.dat', '')
    meta = base + '.meta.dat'
    if not os.path.exists(meta):
        # 尝试同目录下的 meta
        meta_candidates = [m for m in meta_files
                           if os.path.dirname(m) == os.path.dirname(dat)]
        meta = meta_candidates[0] if meta_candidates else None

    print(f"\n  .dat  文件: {dat}")
    print(f"  .dat  大小: {dat_size} bytes")

    if meta and os.path.exists(meta):
        data = open(meta, 'rb').read()
        with open(meta, 'r') as fp:
            lines = [line.strip() for line in fp.readlines()]
        if len(lines) >= 3:
            version = lines[0]
            read_pos, write_pos = map(int, lines[1].split(','))
            next_read_file, next_write_file = map(int, lines[2].split(','))
            print(f"  meta writePos: {write_pos} bytes")
            if dat_size > write_pos:
                diff = dat_size - write_pos
                print(f"  ⚠  .dat 比 meta.writePos 大 {diff} bytes")
                print(f"     → .dat 有数据但 meta 未记录（IO 部分同步的典型表现）")
            elif dat_size == write_pos:
                print(f"  ✓  .dat 大小与 meta.writePos 一致")
            else:
                print(f"  ⚠  .dat 比 meta.writePos 小（meta 超前，可能导致读取错误）")
    else:
        print(f"  ！未找到对应 meta.dat 文件")
PYEOF

# =============================================================================
# 步骤8：启动 NSQ 并验证
# =============================================================================

section "步骤8：启动 NSQ 并验证消息"

systemctl stop nsqd 2>/dev/null || true
sleep 1

NSQ_STARTUP_LOG="/tmp/nsq_startup_secondary.log"
: > "${NSQ_STARTUP_LOG}"

info "启动 NSQd..."
systemctl start nsqd > "${NSQ_STARTUP_LOG}" 2>&1 &
sleep 4

NSQ_STARTED=0
if kill -0 $(pidof nsqd 2>/dev/null | awk '{print $1}') 2>/dev/null; then
    NSQ_STARTED=1
    ok "NSQ 启动成功"
else
    # 检查 journalctl
    journalctl -u nsqd --since "1 minute ago" \
               >> "${NSQ_STARTUP_LOG}" 2>/dev/null || true
fi

if [ $NSQ_STARTED -eq 0 ]; then
    fail "NSQ 启动失败！"
    cat "${NSQ_STARTUP_LOG}" | tail -30 | tee -a "$LOG"
    echo "NSQ_START_FAILED" > "${RESULT_DIR}/nsq_start_result.txt"
else
    echo "NSQ_START_SUCCESS" > "${RESULT_DIR}/nsq_start_result.txt"

    sleep 2

    info "备节点 NSQ 队列状态："
    curl -s "${NSQ_HTTP}/stats?format=json" 2>/dev/null | \
    python3 -c "
import json, sys
try:
    d = json.load(sys.stdin)
    topics = d.get('topics', [])
    if not topics:
        print('  队列为空（没有 topic）')
    for t in topics:
        print(f\"  topic={t['topic_name']}\")
        print(f\"    depth         = {t['depth']}\")
        print(f\"    message_count = {t['message_count']}\")
        print(f\"    backend_depth = {t.get('backend_depth', 'N/A')}\")
except Exception as e:
    print(f'  解析失败: {e}')
" | tee "${RESULT_DIR}/nsq_stats_secondary.txt" | tee -a "$LOG"

    # 尝试消费消息（验证是否可消费）
    info "尝试消费消息（超时 5 秒）..."
    NSQ_TAIL_BIN=$(which nsq_tail 2>/dev/null || \
                   find /usr/local/bin /usr/bin -name "nsq_tail" 2>/dev/null | head -1)

    if [ -n "$NSQ_TAIL_BIN" ]; then
        timeout 5 "$NSQ_TAIL_BIN" \
                --topic="${TOPIC}" \
                --nsqd-tcp-address="127.0.0.1:4150" \
                -n 1 2>/dev/null \
                > "${RESULT_DIR}/nsq_consumed.txt" || true

        if [ -s "${RESULT_DIR}/nsq_consumed.txt" ]; then
            ok "消息可消费: $(cat "${RESULT_DIR}/nsq_consumed.txt")"
        else
            fail "无法消费消息（队列为空 或 消息已丢失）"
        fi
    else
        warn "nsq_tail 未找到，跳过消费验证"
        DEPTH=$(curl -s "${NSQ_HTTP}/stats?format=json" 2>/dev/null | \
                python3 -c "
import json,sys
d=json.load(sys.stdin)
total=sum(t.get('depth',0)+t.get('backend_depth',0)
          for t in d.get('topics',[]))
print(total)" 2>/dev/null || echo "0")
        info "队列总深度: ${DEPTH}"
        if [ "$DEPTH" -gt 0 ] 2>/dev/null; then
            ok "队列有消息（depth=${DEPTH}）"
        else
            fail "队列为空（消息可能已丢失）"
        fi
    fi
fi

fi # TEST_NSQ (步骤7/8)

# =============================================================================
# 步骤9：汇总分析报告
# =============================================================================

section "步骤9：最终数据一致性汇总报告"

# 读取各项结果
MYSQL_RESULT=$(cat "${RESULT_DIR}/mysql_start_result.txt" 2>/dev/null || echo "UNKNOWN")
NSQ_RESULT=$(cat "${RESULT_DIR}/nsq_start_result.txt" 2>/dev/null || echo "UNKNOWN")
SEC_ROWS=$(wc -l < "${RESULT_DIR}/mysql_secondary_data.txt" 2>/dev/null | tr -d ' ' || echo "0")
[ -z "$SEC_ROWS" ] && SEC_ROWS=0
NSQ_DEPTH=$(cat "${RESULT_DIR}/nsq_stats_secondary.txt" 2>/dev/null | \
    grep -o 'depth *= *[0-9]*' | grep -o '[0-9]*' | head -1 || echo "?")

# ── 服务启动结论（区分跳过/成功/失败）────────────────────────────────
if [ "$TEST_MYSQL" -eq 0 ]; then
    SVC_MYSQL="─ 未测试（--services 未包含 mysql）"
    DATA_MYSQL="─ 未测试"
elif [ "$MYSQL_RESULT" = "MYSQL_START_SUCCESS" ]; then
    SVC_MYSQL="✓ 正常启动"
    if [ "$SEC_ROWS" -gt 0 ]; then
        DATA_MYSQL="✓ 有数据（${SEC_ROWS} 行）"
    else
        DATA_MYSQL="✗ 数据为空（事务已回滚）"
    fi
else
    SVC_MYSQL="✗ 启动失败（查看上方 MySQL 日志）"
    DATA_MYSQL="? 无法验证（MySQL 未启动）"
fi

if [ "$TEST_NSQ" -eq 0 ]; then
    SVC_NSQ="─ 未测试（--services 未包含 nsq）"
    DATA_NSQ="─ 未测试"
elif [ "$NSQ_RESULT" = "NSQ_START_SUCCESS" ]; then
    SVC_NSQ="✓ 正常启动"
    if [ -s "${RESULT_DIR}/nsq_consumed.txt" ]; then
        DATA_NSQ="✓ 消息可消费"
    elif [ "${NSQ_DEPTH:-0}" -gt 0 ] 2>/dev/null; then
        DATA_NSQ="✓ 队列有消息（depth=${NSQ_DEPTH}）"
    else
        DATA_NSQ="✗ 队列为空（消息丢失 或 meta/data 不一致）"
    fi
else
    SVC_NSQ="✗ 启动失败"
    DATA_NSQ="? 无法验证（NSQ 未启动）"
fi

{
    echo ""
    echo "╔══════════════════════════════════════════════════════════════════════╗"
    echo "║          备节点接管验证报告  Protocol ${PROTOCOL}                        ║"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    printf "║  时间: %-64s ║\n" "$(date '+%Y-%m-%d %H:%M:%S')"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    echo "║                                                                      ║"
    echo "║  问题一：切换到备节点后所有服务能否正常启动？                        ║"
    echo "║  ─────────────────────────────────────────────────────────────────   ║"
    printf "║    MySQL : %-59s ║\n" "$SVC_MYSQL"
    printf "║    NSQ   : %-59s ║\n" "$SVC_NSQ"
    echo "║                                                                      ║"
    echo "║  问题二：服务启动后数据是否和主节点一致？                            ║"
    echo "║  ─────────────────────────────────────────────────────────────────   ║"
    printf "║    MySQL : %-59s ║\n" "$DATA_MYSQL"
    printf "║    NSQ   : %-59s ║\n" "$DATA_NSQ"
    echo "║                                                                      ║"
    echo "╠══════════════════════════════════════════════════════════════════════╣"
    ROW_COUNT=$(wc -l < "${RESULT_DIR}/mysql_secondary_data.txt" 2>/dev/null || echo "0")
    echo "║  主节点写入后的数据行数: ${ROW_COUNT} "
    echo "╚══════════════════════════════════════════════════════════════════════╝"
    echo ""

    # 详细支撑数据
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  补充：InnoDB Crash Recovery 日志"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    if [ -s "${RESULT_DIR}/mysql_crash_recovery.txt" ]; then
        cat "${RESULT_DIR}/mysql_crash_recovery.txt" | head -20 | sed 's/^/  /'
    else
        echo "  （未找到 Crash Recovery 日志）"
    fi
    echo ""

    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  补充：NSQ meta.dat 对比"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "  [备节点 meta.dat]"
    cat "${RESULT_DIR}/nsq_meta_secondary.txt" 2>/dev/null | sed 's/^/    /' || echo "  （未找到）"
    echo ""

} | tee "${RESULT_DIR}/final_report.txt" | tee -a "$LOG"

# =============================================================================
# 步骤10：恢复环境
# =============================================================================

section "步骤10：恢复环境"

info "停止备节点服务..."
[ "$TEST_MYSQL" -eq 1 ] && {
    systemctl stop "${MYSQL_SERVICE:-mysqld}" 2>/dev/null || \
        pkill -f "$MYSQL_BIN" 2>/dev/null || true
}
[ "$TEST_NSQ" -eq 1 ] && systemctl stop nsqd 2>/dev/null || true
sleep 1

info "卸载 DRBD 文件系统..."
umount "${MOUNT_POINT}" 2>/dev/null || umount -l "${MOUNT_POINT}" 2>/dev/null || true

info "将 DRBD 降级回 Secondary..."
drbdadm secondary ${DRBD_RES} 2>/dev/null || true

info "备节点 DRBD 最终状态："
check_drbd_status | tee -a "$LOG"

# =============================================================================
# 打包结果
# =============================================================================

tar -czf /tmp/secondary_results.tar.gz \
    -C /tmp drbd_results_secondary/ \
    mysql_startup_secondary.log \
    nsq_startup_secondary.log \
    2>/dev/null || true

info "✓ 备节点结果已打包到 /tmp/secondary_results.tar.gz"

echo ""
echo "SECONDARY_DONE"
