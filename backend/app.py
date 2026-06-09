import os
import socket
from flask import Flask, jsonify
import redis

app = Flask(__name__)

# 从环境变量获取 Redis 连接配置
REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
REDIS_PORT = int(os.environ.get('REDIS_PORT', 6379))
REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)

# 初始化 Redis 客户端
# decode_responses=True 自动将 Redis 返回的 bytes 转换为 string
try:
    r = redis.Redis(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PASSWORD,
        socket_connect_timeout=3,
        decode_responses=True
    )
except Exception as e:
    print(f"Error initializing Redis client: {e}")
    r = None

@app.route('/api/ping', methods=['GET'])
def ping():
    """验收要求必须返回 {"status": "ok"} 的 API"""
    return jsonify({"status": "ok"})

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """获取系统状态，包含 Pod 主机名和 Redis 通信状态"""
    pod_name = socket.gethostname()
    redis_connected = False
    visits = 0
    error_msg = None

    if r:
        try:
            # 测试 Redis 连接
            redis_connected = r.ping()
            # 获取访问计数
            visits = r.get('visits')
            if visits is None:
                visits = 0
            else:
                visits = int(visits)
        except Exception as e:
            error_msg = str(e)
    else:
        error_msg = "Redis client not initialized"

    return jsonify({
        "pod_name": pod_name,
        "redis_connected": redis_connected,
        "visits": visits,
        "error": error_msg
    })

@app.route('/api/increment', methods=['POST'])
def increment_counter():
    """点击交互：递增 Redis 中的计数"""
    pod_name = socket.gethostname()
    if not r:
        return jsonify({"success": False, "error": "Redis not initialized"}), 500
    
    try:
        new_val = r.incr('visits')
        return jsonify({
            "success": True,
            "visits": new_val,
            "pod_name": pod_name
        })
    except Exception as e:
        return jsonify({"success": False, "error": str(e)}), 500

if __name__ == '__main__':
    # 监听 0.0.0.0，使容器外可访问
    app.run(host='0.0.0.0', port=5000)
