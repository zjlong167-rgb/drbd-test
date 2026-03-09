#!/bin/bash

# DRBD 主备切换脚本（适用于 WSL）
# 节点 IP 和凭据
NODE1="192.168.171.130"
NODE2="192.168.171.131"
PASSWORD="123456"
RESOURCE="r0"
MOUNT_POINT="/database"

SSH_OPTS="-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null"

# 函数：在指定节点执行命令
run_on_node() {
    local node=$1
    shift
    sshpass -p "$PASSWORD" ssh "$SSH_OPTS" root@"$node" "$*"
}

# 函数：检查节点是否可达
check_node() {
    if ! ping -c1 -W2 "$1" &>/dev/null; then
        echo "❌ 节点 $1 不可达！"
        exit 1
    fi
}

# 检查依赖
if ! command -v sshpass &> /dev/null; then
    echo "❌ 请先安装 sshpass: sudo apt install sshpass"
    exit 1
fi

# 检查节点连通性
echo "🔍 检查节点连通性..."
check_node "$NODE1"
check_node "$NODE2"

# 获取当前 Primary 节点
echo "📡 查询当前 DRBD Primary 节点..."
PRIMARY=""
if run_on_node "$NODE1" "drbdadm role $RESOURCE" 2>/dev/null | grep -q "Primary"; then
    PRIMARY="$NODE1"
elif run_on_node "$NODE2" "drbdadm role $RESOURCE" 2>/dev/null | grep -q "Primary"; then
    PRIMARY="$NODE2"
else
    echo "⚠️  警告：未检测到 Primary 节点！"
    read -p "请选择当前 Primary 节点 (1: $NODE1, 2: $NODE2): " choice
    case $choice in
        1) PRIMARY="$NODE1" ;;
        2) PRIMARY="$NODE2" ;;
        *) echo "❌ 无效选择"; exit 1 ;;
    esac
fi

echo "✅ 当前 Primary: $PRIMARY"

# 选择目标 Primary
if [[ "$PRIMARY" == "$NODE1" ]]; then
    TARGET="$NODE2"
    echo "👉 将切换 Primary 到 $NODE2"
else
    TARGET="$NODE1"
    echo "👉 将切换 Primary 到 $NODE1"
fi

read -p "确认切换？(y/N): " confirm
[[ "${confirm,,}" != "y" ]] && echo "🛑 操作取消" && exit 0

# === 开始切换流程 ===

# 1. 在当前 Primary 上卸载 /database
echo "📤 卸载当前 Primary ($PRIMARY) 上的 $MOUNT_POINT ..."
run_on_node "$PRIMARY" "systemctl stop mysqld redis nsqd 2>/dev/null || true"
run_on_node "$PRIMARY" "umount $MOUNT_POINT 2>/dev/null || true"

# 2. 将当前 Primary 降级为 Secondary
echo "🔄 将 $PRIMARY 降级为 Secondary ..."
run_on_node "$PRIMARY" "drbdadm secondary $RESOURCE"

# 3. 将目标节点升级为 Primary
echo "🚀 将 $TARGET 升级为 Primary ..."
run_on_node "$TARGET" "drbdadm primary $RESOURCE"

# 4. 在新 Primary 上挂载 /database
echo "📥 在 $TARGET 上挂载 $MOUNT_POINT ..."
run_on_node "$TARGET" "mount /dev/drbd1 $MOUNT_POINT || mount /dev/drbd/by-res/$RESOURCE/1 $MOUNT_POINT"

# 5. 验证状态
echo "✅ 切换完成！当前状态："
echo "----------------------------------------"
run_on_node "$NODE1" "echo -n '$NODE1: '; drbdadm role $RESOURCE"
run_on_node "$NODE2" "echo -n '$NODE2: '; drbdadm role $RESOURCE"
echo "----------------------------------------"
run_on_node "$TARGET" "df -h $MOUNT_POINT"

echo "💡 提示：请确保应用服务在新 Primary 上启动。"