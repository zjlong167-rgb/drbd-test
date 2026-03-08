#!/bin/bash
# =============================================================================
# primary_node.sh - 在主节点 (192.168.171.130) 上执行
# 功能：记录初始状态 → 设置IO拦截 → 执行SQL/NSQ写入 → 模拟故障
# =============================================================================

set -euo pipefail

# ── ERR trap：捕获触发 set -e 的具体命令、行号、退出码 ──────────────────
PRIMARY_ERROR_FILE="/tmp/primary_error.txt"
: > "$PRIMARY_ERROR_FILE"

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
    } | tee -a "$PRIMARY_ERROR_FILE" >&2
}
trap '_err_trap' ERR

MYSQL_PASS='br7^L9x@NT}!GyAM43>LuYpKN]Jzjo6^'
MYSQL_SOCK="/opt/mysql/bin/mysql.sock"
NSQ_HTTP="http://127.0.0.1:4151"
NSQ_DATA_DIR="/database/nsq"
TOPIC="drbd_test_topic"
LOG="/tmp/primary_experiment.log"
RESULT_DIR="/tmp/drbd_results"
SECONDARY_IP="192.168.171.131"
DRBD_PORT=7789
DRBD_RES="r0"
DRBD_DEV="/dev/drbd1"
MOUNT_POINT="/database"
DRBD_CONF="/etc/drbd.d/r0.res"
GLOBAL_CONF="/etc/drbd.d/global_common.conf"

# 颜色输出
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'
CYAN='\033[0;36m'; NC='\033[0m'; BOLD='\033[1m'

log()  { echo -e "${CYAN}[$(date '+%H:%M:%S.%3N')]${NC} $*" | tee -a "$LOG"; }
info() { echo -e "${GREEN}[INFO]${NC} $*" | tee -a "$LOG"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*" | tee -a "$LOG"; }
err()  { echo -e "${RED}[ERR ]${NC} $*" | tee -a "$LOG"; }
section() {
    echo "" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}  $*${NC}" | tee -a "$LOG"
    echo -e "${BOLD}${CYAN}══════════════════════════════════════════${NC}" | tee -a "$LOG"
}

PROTOCOL="${1:-A}"   # 第一个参数：A 或 C

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

get_drbd_bytes_sent() {
    # 通过 drbdsetup 获取已发送字节数
    local bytes
    bytes=$(drbdsetup status ${DRBD_RES} --statistics 2>/dev/null | \
            grep -o 'send:[0-9]*' | cut -d: -f2 | head -1)
    echo "${bytes:-0}"
}

wait_drbd_synced() {
    local max_wait=30
    local i=0
    while [ $i -lt $max_wait ]; do
        local status
        status=$(check_drbd_status)
        if echo "$status" | grep -q "UpToDate/UpToDate\|Connected"; then
            return 0
        fi
        sleep 1
        ((i++))
    done
    warn "DRBD 未能在 ${max_wait}s 内完全同步"
    return 1
}

# =============================================================================
# 主流程
# =============================================================================

mkdir -p "$RESULT_DIR"
: > "$LOG"

section "阶段0：环境初始化与预检"

log "Protocol 模式: ${BOLD}${PROTOCOL}${NC}"
log "主节点 IP: 192.168.171.130"
log "备节点 IP: ${SECONDARY_IP}"

# 检查 DRBD
info "检查 DRBD 状态..."
DRBD_STATUS=$(check_drbd_status)
echo "$DRBD_STATUS" | tee -a "$LOG"

if ! echo "$DRBD_STATUS" | grep -q "role:Primary"; then
    err "当前节点不是 DRBD Primary，请检查配置！"
    exit 1
fi
info "✓ 确认为 Primary 节点"

# 切换 Protocol
info "切换 DRBD Protocol 为 ${PROTOCOL}..."

# 优先在 r0.res 资源配置里设置，若无则在 global_common.conf
if grep -q "protocol" "${DRBD_CONF}" 2>/dev/null; then
    sed -i "s/protocol [ABC]/protocol ${PROTOCOL}/" "${DRBD_CONF}"
elif grep -q "protocol" "${GLOBAL_CONF}" 2>/dev/null; then
    sed -i "s/protocol [ABC]/protocol ${PROTOCOL}/" "${GLOBAL_CONF}"
else
    # 在 global_common.conf 的 common { net { } } 中追加
    sed -i "/common {/a\\    net { protocol ${PROTOCOL}; }" "${GLOBAL_CONF}"
fi

# 断线重连以应用新 protocol（不影响数据）
drbdadm disconnect ${DRBD_RES} 2>/dev/null || true
sleep 2
drbdadm connect ${DRBD_RES} 2>/dev/null || true
sleep 3

info "当前 DRBD 协议配置："
grep -r "protocol" /etc/drbd.d/ | tee -a "$LOG"

# =============================================================================
# 准备 MySQL 测试库表
# =============================================================================

section "阶段1：MySQL 环境准备"

info "检查 MySQL 服务..."
if ! systemctl is-active mysqld --quiet; then
    info "启动 MySQL..."
    systemctl start mysqld
    sleep 3
fi

info "创建测试库表..."
mysql_cmd -e "CREATE DATABASE IF NOT EXISTS drbd_test;" 2>/dev/null || true
mysql_cmd drbd_test -e "DROP TABLE IF EXISTS iops_trace;" 2>/dev/null || true
mysql_cmd drbd_test -e "
CREATE TABLE iops_trace (
    id   BIGINT AUTO_INCREMENT PRIMARY KEY,
    val  VARCHAR(512) NOT NULL,
    ts   TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
) ENGINE=InnoDB;" 2>/dev/null || {
    err "建表失败，检查 MySQL 连接..."
    # 输出 MySQL 错误详情帮助诊断
    mysql_cmd -e "SELECT 1;" 2>&1 | tee -a "$LOG" || true
    exit 1
}
info "✓ 测试表 drbd_test.iops_trace 已就绪"

# 记录 MySQL 初始状态
info "记录 MySQL 初始状态..."
mysql_cmd drbd_test -e \
    "SELECT COUNT(*) AS row_count, IFNULL(MAX(id),0) AS max_id FROM iops_trace;" \
    > "${RESULT_DIR}/mysql_baseline.txt" 2>/dev/null
cat "${RESULT_DIR}/mysql_baseline.txt" | tee -a "$LOG"

# =============================================================================
# 准备 NSQ
# =============================================================================

section "阶段2：NSQ 环境准备"

# 直接使用已知路径，不依赖 systemctl cat 提取
NSQ_DATA_DIR="/database/nsq/data/nsqd"
info "NSQ 数据目录: ${NSQ_DATA_DIR}"
mkdir -p "${NSQ_DATA_DIR}" 2>/dev/null || true

if ! systemctl is-active nsqd --quiet 2>/dev/null; then
    info "启动 NSQd..."
    systemctl start nsqd 2>/dev/null || true
    sleep 2
fi

# 确认运行状态
NSQ_RUNNING="inactive"
if systemctl is-active nsqd --quiet 2>/dev/null; then
    NSQ_RUNNING="active"
else
    # pgrep 未找到时返回1，用 true 避免触发 set -e
    if pgrep -x nsqd > /dev/null 2>&1 || true; then
        pgrep -x nsqd > /dev/null 2>&1 && NSQ_RUNNING="active(direct)" || true
    fi
fi
info "NSQd 状态: ${NSQ_RUNNING}"

# 创建 Topic（发送一条消息确保 topic 存在）
curl -s -d "init_msg" "${NSQ_HTTP}/pub?topic=${TOPIC}" > /dev/null 2>&1 || true
sleep 1
curl -s "${NSQ_HTTP}/stats?format=json" \
     > "${RESULT_DIR}/nsq_baseline.json" 2>/dev/null || true

info "NSQ 初始队列状态："
python3 -c "
import json, sys
try:
    with open('${RESULT_DIR}/nsq_baseline.json') as f:
        raw = f.read().strip()
    if not raw:
        print('  (stats 返回空，NSQ 刚启动或无数据)')
        sys.exit(0)
    d = json.loads(raw)
    topics = d.get('topics', [])
    if not topics:
        print('  (暂无 topic)')
    for t in topics:
        if '${TOPIC}' in t['topic_name']:
            print(f\"  topic={t['topic_name']} depth={t['depth']} messages={t['message_count']}\")
except Exception as e:
    print(f'  解析失败: {e}')
" 2>/dev/null | tee -a "$LOG" || true

# 记录 NSQ diskqueue 初始文件状态
find "${NSQ_DATA_DIR}" -name "*.dat" 2>/dev/null \
     | while read -r f; do stat "$f" 2>/dev/null; done \
     > "${RESULT_DIR}/nsq_files_baseline.txt" || true

# =============================================================================
# 设置 IO 拦截器（nfqueue / iptables 方案）
# =============================================================================

section "阶段3：安装并启动 IO 拦截器"

# 安装依赖
info "检查 python3-netfilterqueue..."
python3 -c "import netfilterqueue" 2>/dev/null || {
    warn "安装 python3-netfilterqueue..."
    pip3 install --quiet netfilterqueue scapy 2>/dev/null || \
    apt-get install -y -q python3-netfilterqueue 2>/dev/null || {
        warn "nfqueue 安装失败，降级使用 connbytes 方案"
        USE_CONNBYTES=1
    }
}
USE_CONNBYTES="${USE_CONNBYTES:-0}"

# 写出 nfqueue 拦截器脚本
cat > /tmp/drbd_interceptor.py << 'PYEOF'
#!/usr/bin/env python3
"""
DRBD IO 帧级拦截器
用法: python3 drbd_interceptor.py <max_frames> <ready_file>
  max_frames : 允许通过的 DRBD 数据帧数量
  ready_file : 就绪信号文件路径
"""
import sys, os, signal, struct

sys.modules['scapy.layers.dcerpc'] = None
sys.modules['scapy.layers.kerberos'] = None
sys.modules['scapy.layers.smbclient'] = None

MAX_FRAMES   = int(sys.argv[1]) if len(sys.argv) > 1 else 2
READY_FILE   = sys.argv[2]       if len(sys.argv) > 2 else "/tmp/interceptor_ready"
DRBD_PORT    = 7789
MIN_PAYLOAD  = 32   # 小于此值视为纯 ACK/控制包

frame_count  = 0
drop_count   = 0
drop_mode    = False

def cleanup(sig=None, frame=None):
    import subprocess
    subprocess.run(['iptables', '-D', 'OUTPUT', '-p', 'tcp',
                    '--dport', str(DRBD_PORT), '-j', 'NFQUEUE',
                    '--queue-num', '0'], capture_output=True)
    print(f"[interceptor] 清理完成 passed={frame_count} dropped={drop_count}",
          flush=True)
    sys.exit(0)

signal.signal(signal.SIGTERM, cleanup)
signal.signal(signal.SIGINT,  cleanup)

try:
    from netfilterqueue import NetfilterQueue
    from scapy.all import IP, TCP

    def process(pkt):
        global frame_count, drop_count, drop_mode
        payload = pkt.get_payload()
        scapy_pkt = IP(payload)

        if drop_mode:
            drop_count += 1
            print(f"[interceptor] DROP frame={frame_count} "
                  f"drop={drop_count} size={len(payload)}", flush=True)
            pkt.drop()
            return

        if scapy_pkt.haslayer(TCP):
            tcp_payload = len(scapy_pkt[TCP].payload)
            if tcp_payload >= MIN_PAYLOAD:
                frame_count += 1
                print(f"[interceptor] PASS frame={frame_count} "
                      f"tcp_data={tcp_payload}B", flush=True)
                if frame_count >= MAX_FRAMES:
                    print(f"[interceptor] *** 已通过 {MAX_FRAMES} 帧，"
                          f"切换 DROP 模式 ***", flush=True)
                    drop_mode = True

        pkt.accept()

    import subprocess
    subprocess.run(['iptables', '-I', 'OUTPUT', '-p', 'tcp',
                    '--dport', str(DRBD_PORT), '-j', 'NFQUEUE',
                    '--queue-num', '0'], check=False)

    # 写入就绪信号
    open(READY_FILE, 'w').write('ready')
    print(f"[interceptor] 启动成功，最多允许 {MAX_FRAMES} 个数据帧通过",
          flush=True)

    nfq = NetfilterQueue()
    nfq.bind(0, process)
    nfq.run()

except ImportError:
    # 降级：使用 connbytes iptables 规则
    import subprocess, sys
    ALLOW_BYTES = MAX_FRAMES * 600  # 粗略估算每帧约 600 字节
    result = subprocess.run([
        'iptables', '-A', 'OUTPUT', '-p', 'tcp',
        '-d', '192.168.171.131', '--dport', str(DRBD_PORT),
        '-m', 'connbytes',
        '--connbytes', f'{ALLOW_BYTES}:',
        '--connbytes-dir', 'original',
        '--connbytes-mode', 'bytes',
        '-j', 'DROP'
    ], capture_output=True, text=True)
    if result.returncode != 0:
        print(f"[interceptor] connbytes 规则添加失败（可能缺少 xt_connbytes 模块）: {result.stderr.strip()}",
              flush=True)
        print(f"[interceptor] 降级为纯 iptables DROP 方式", flush=True)
        # 最终降级：直接 DROP 所有 DRBD OUTPUT（无字节计数）
        subprocess.run([
            'iptables', '-A', 'OUTPUT', '-p', 'tcp',
            '-d', '192.168.171.131', '--dport', str(DRBD_PORT),
            '-j', 'DROP'
        ], capture_output=True)
    else:
        print(f"[interceptor] connbytes 模式启动，允许前 {ALLOW_BYTES} 字节", flush=True)
    open(READY_FILE, 'w').write('ready')
    # 保持进程存活
    import time
    while True:
        time.sleep(1)
PYEOF

READY_FILE="/tmp/interceptor_ready"
INTERCEPTOR_LOG="/tmp/interceptor.log"
rm -f "$READY_FILE"

info "启动 IO 拦截器（允许 20 个 DRBD 数据帧通过后切断）..."
python3 /tmp/drbd_interceptor.py 20 "$READY_FILE" \
        > "$INTERCEPTOR_LOG" 2>&1 &
INTERCEPT_PID=$!
echo "$INTERCEPT_PID" > /tmp/interceptor.pid

# 等待拦截器就绪（最多 10 秒）
for i in $(seq 1 20); do
    [ -f "$READY_FILE" ] && break
    sleep 0.5
done

if [ ! -f "$READY_FILE" ]; then
    warn "拦截器启动可能失败，继续实验（将使用 iptables 硬切断方式）"
    kill "$INTERCEPT_PID" 2>/dev/null || true
    # fallback: 直接在 IO 执行中途用 iptables DROP
    FALLBACK_MODE=1
else
    info "✓ 拦截器已就绪 (PID=${INTERCEPT_PID})"
    FALLBACK_MODE=0
fi

# =============================================================================
# ─── MySQL 实验 ───────────────────────────────────────────────────────────────
# =============================================================================

section "阶段4：MySQL 写入实验（Protocol ${PROTOCOL}）"

# 启动 DRBD 流量抓包
tcpdump -i any -w "${RESULT_DIR}/drbd_traffic.pcap" \
        "tcp port ${DRBD_PORT}" \
        2>/dev/null &
TCPDUMP_PID=$!

# 记录执行前 LSN
BEFORE_LSN=$(mysql_cmd -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null \
             | grep "Log sequence number" | awk '{print $NF}' | head -1 || true)
info "事务前 InnoDB LSN: ${BEFORE_LSN:-unavailable}"

# fallback 模式：在事务执行过程中用 iptables 切断
if [ "${FALLBACK_MODE}" = "1" ]; then
    info "[Fallback] 将在事务 SQL 执行过程中切断 DRBD 连接"
    (sleep 0.3
     iptables -I OUTPUT -p tcp -d "${SECONDARY_IP}" \
              --dport "${DRBD_PORT}" -j DROP 2>/dev/null || true
     echo "$(date '+%H:%M:%S.%3N') [fallback] DRBD OUTPUT 已切断" \
          >> "$INTERCEPTOR_LOG"
    ) &
fi

log "【MySQL 事务开始】$(date '+%H:%M:%S.%6N')"

# 执行测试事务（多行INSERT + UPDATE，产生多个 IO：undo+redo+binlog+数据页）
mysql_cmd drbd_test -e "
START TRANSACTION;
INSERT INTO iops_trace (val) VALUES
    ('ROW_A_drbd_partial_io_test_data_payload_aaaaaaaaaaaaaaaaaaaaaaaaaaaa'),
    ('ROW_B_drbd_partial_io_test_data_payload_bbbbbbbbbbbbbbbbbbbbbbbbbbbb'),
    ('ROW_C_drbd_partial_io_test_data_payload_cccccccccccccccccccccccccccc');
UPDATE iops_trace SET val = CONCAT(val, '_UPDATED') WHERE id <= 3;
COMMIT;" 2>/dev/null || warn "事务执行失败或部分完成（IO拦截导致，属预期现象）"

MYSQL_EXIT=$?
log "【MySQL 事务结束】$(date '+%H:%M:%S.%6N') exit=${MYSQL_EXIT}"

# 记录主节点提交后的数据（事务已在主节点提交）
mysql_cmd drbd_test -e \
    "SELECT id, val, ts FROM iops_trace ORDER BY id;" \
    > "${RESULT_DIR}/mysql_primary_after_commit.txt" 2>/dev/null || true

AFTER_LSN=$(mysql_cmd -e "SHOW ENGINE INNODB STATUS\G" 2>/dev/null \
            | grep "Log sequence number" | awk '{print $NF}' | head -1 || true)
info "事务后 InnoDB LSN: ${AFTER_LSN:-unavailable}"

# 记录 binlog 位置
mysql_cmd -e "SHOW MASTER STATUS\G" \
    > "${RESULT_DIR}/mysql_binlog_pos.txt" 2>/dev/null || true

info "主节点提交后数据："
cat "${RESULT_DIR}/mysql_primary_after_commit.txt" | tee -a "$LOG"

# =============================================================================
# ─── NSQ 实验 ─────────────────────────────────────────────────────────────────
# =============================================================================

section "阶段5：NSQ 写入实验（Protocol ${PROTOCOL}）"

# 记录 NSQ meta.dat 写入前状态
info "写入前 NSQ diskqueue 文件状态："
find "${NSQ_DATA_DIR}" -name "*.dat" 2>/dev/null | while read -r f; do
    echo "  $f  size=$(stat -c%s "$f" 2>/dev/null) bytes"
done | tee -a "$LOG"

# 解析并记录 meta.dat（写入前）
python3 << PYEOF | tee "${RESULT_DIR}/nsq_meta_before.txt" | tee -a "$LOG"
import struct, glob
files = glob.glob('${NSQ_DATA_DIR}/**/*.meta.dat', recursive=True)
if not files:
    print("  未找到 meta.dat 文件")
for f in files:
    print(f"\n  文件: {f}")
    try:
        data = open(f, 'rb').read()
        if len(data) >= 24:
            r, w, d = struct.unpack('>qqq', data[:24])
            print(f"    readPos  = {r}")
            print(f"    writePos = {w}")
            print(f"    depth    = {d}")
        else:
            print(f"    文件大小 {len(data)} bytes, 内容: {data.hex()}")
    except Exception as e:
        print(f"    读取失败: {e}")
PYEOF

NSQ_MSG="DRBD_TEST_MSG_$(date +%s%N)_partial_io_experiment_payload"
log "发送 NSQ 消息: ${NSQ_MSG}"

# 先发送消息（DRBD 连接存在时发送，让 nsqd 正常写入并触发 DRBD 同步）
# Protocol C 的实验意义：消息写入后 DRBD 同步过程被中断，观察备节点是否有完整数据
log "【NSQ 消息发送开始】$(date '+%H:%M:%S.%6N')"
NSQ_RESULT=$(curl -s --max-time 10 -w "\n%{http_code}" \
                  -d "${NSQ_MSG}" \
                  "${NSQ_HTTP}/pub?topic=${TOPIC}" 2>/dev/null || echo -e "\n000")
NSQ_HTTP_CODE=$(echo "$NSQ_RESULT" | tail -1)
log "【NSQ 消息发送结束】$(date '+%H:%M:%S.%6N') HTTP=${NSQ_HTTP_CODE}"

if [ "$NSQ_HTTP_CODE" != "200" ]; then
    warn "NSQ 发送返回 HTTP=${NSQ_HTTP_CODE}，nsqd 可能未就绪或写入失败"
fi

# 等待 nsqd 完成 fsync（mem-queue-size=0 时每条消息都会同步落盘）
sleep 2

# 发送完成后再切断 DRBD，制造「消息已写入主节点但同步状态未知」的场景
info "切断 DRBD 连接（制造部分同步场景）..."
drbdadm disconnect ${DRBD_RES} 2>/dev/null || true
sleep 1

# 记录写入后 meta.dat 状态
python3 << PYEOF | tee "${RESULT_DIR}/nsq_meta_after_primary.txt" | tee -a "$LOG"
import struct, glob
files = glob.glob('${NSQ_DATA_DIR}/**/*.meta.dat', recursive=True)
if not files:
    print("  未找到 meta.dat 文件（主节点）")
for f in files:
    print(f"\n  文件: {f}")
    try:
        data = open(f, 'rb').read()
        if len(data) >= 24:
            r, w, d = struct.unpack('>qqq', data[:24])
            print(f"    readPos  = {r}")
            print(f"    writePos = {w}")
            print(f"    depth    = {d}")
            print(f"    未读消息字节数 = {w - r}")
        else:
            print(f"    文件大小 {len(data)} bytes, 内容: {data.hex()}")
    except Exception as e:
        print(f"    读取失败: {e}")
PYEOF

# 记录 .dat 数据文件内容（16进制，用于对比）
find "${NSQ_DATA_DIR}" -name "*.dat" ! -name "*.meta.dat" 2>/dev/null | \
    while read -r f; do
        hexdump -C "$f" 2>/dev/null
    done > "${RESULT_DIR}/nsq_dat_primary.txt"

# 记录 NSQ 队列统计
curl -s "${NSQ_HTTP}/stats?format=json" \
     > "${RESULT_DIR}/nsq_stats_primary.json" 2>/dev/null || true

# =============================================================================
# 模拟主节点故障
# =============================================================================

section "阶段6：模拟主节点故障"

kill "$TCPDUMP_PID" 2>/dev/null || true
sleep 1

# 统计已同步帧数（tr 去掉换行防止输出截断）
PASSED=$(grep -c "PASS frame" "$INTERCEPTOR_LOG" 2>/dev/null || echo "0")
DROPPED=$(grep -c "DROP frame" "$INTERCEPTOR_LOG" 2>/dev/null || echo "0")
PASSED=$(echo "$PASSED" | tr -d '[:space:]')
DROPPED=$(echo "$DROPPED" | tr -d '[:space:]')
info "拦截统计：已通过 ${PASSED} 个 DRBD 帧，已丢弃 ${DROPPED} 个帧"
cat "$INTERCEPTOR_LOG" 2>/dev/null | tee -a "$LOG" || true

# 分析抓包文件（统计 DRBD 数据帧数量）
if [ -f "${RESULT_DIR}/drbd_traffic.pcap" ]; then
    DRBD_FRAMES=$(tcpdump -r "${RESULT_DIR}/drbd_traffic.pcap" -n \
                          "tcp and len > 40" 2>/dev/null | wc -l || echo "0")
    info "PCAP 中 DRBD 数据帧总数: ${DRBD_FRAMES}"
fi

info "停止 MySQL 和 NSQ 服务（模拟主节点应用层故障）..."
systemctl stop mysqld 2>/dev/null || true
systemctl stop nsqd   2>/dev/null || true

info "主节点 DRBD 降级为 Secondary（为备节点接管让路）..."
drbdadm secondary ${DRBD_RES} 2>/dev/null || true
sleep 1

info "完全隔离 DRBD 网络（模拟主节点断电/断网）..."
iptables -I INPUT  -p tcp --sport "${DRBD_PORT}" -j DROP 2>/dev/null || true
iptables -I OUTPUT -p tcp --dport "${DRBD_PORT}" -j DROP 2>/dev/null || true
iptables -I INPUT  -p tcp --dport "${DRBD_PORT}" -j DROP 2>/dev/null || true
iptables -I OUTPUT -p tcp --sport "${DRBD_PORT}" -j DROP 2>/dev/null || true

# 停止拦截器进程
kill "$INTERCEPT_PID" 2>/dev/null || true
iptables -D OUTPUT -p tcp --dport "${DRBD_PORT}" -j NFQUEUE \
         --queue-num 0 2>/dev/null || true

# 保存最终 DRBD 状态
check_drbd_status > "${RESULT_DIR}/drbd_after_fault.txt" 2>&1 || true
info "主节点故障模拟完成"

# =============================================================================
# 打包结果
# =============================================================================

section "阶段7：打包主节点结果"

# 汇总所有关键信息到单一结果文件
{
    echo "===== DRBD 主节点实验结果 ====="
    echo "Protocol: ${PROTOCOL}"
    echo "时间: $(date)"
    echo ""

    echo "--- MySQL 事务前后数据对比 ---"
    echo "[事务前]"
    cat "${RESULT_DIR}/mysql_baseline.txt" 2>/dev/null
    echo "[事务后（主节点已提交）]"
    cat "${RESULT_DIR}/mysql_primary_after_commit.txt" 2>/dev/null
    echo "[LSN 变化] before=${BEFORE_LSN} after=${AFTER_LSN}"
    echo "[Binlog 位置]"
    cat "${RESULT_DIR}/mysql_binlog_pos.txt" 2>/dev/null

    echo ""
    echo "--- NSQ meta.dat 写入前后对比 ---"
    echo "[写入前]"
    cat "${RESULT_DIR}/nsq_meta_before.txt" 2>/dev/null
    echo "[写入后（主节点）]"
    cat "${RESULT_DIR}/nsq_meta_after_primary.txt" 2>/dev/null

    echo ""
    echo "--- DRBD IO 拦截统计 ---"
    cat "$INTERCEPTOR_LOG" 2>/dev/null

    echo ""
    echo "--- 故障后 DRBD 状态 ---"
    cat "${RESULT_DIR}/drbd_after_fault.txt" 2>/dev/null

} > "${RESULT_DIR}/primary_summary.txt"

tar -czf /tmp/primary_results.tar.gz -C /tmp drbd_results/ 2>/dev/null
info "✓ 结果已打包到 /tmp/primary_results.tar.gz"

echo ""
info "主节点实验阶段完成，等待备节点接管..."
echo "EXPERIMENT_DONE"
