import concurrent.futures
import requests
import time

# 后端服务的负载均衡公网 IP
url = "http://113.46.198.59/api/ping"

def send_request(i):
    try:
        resp = requests.get(url, timeout=2)
        if i % 1000 == 0:
            print(f"已发送 {i} 次请求，当前状态码: {resp.status_code}")
    except Exception as e:
        pass

def main():
    print("开始压力测试，持续向后端服务生成 CPU 负载...")
    print(f"目标 URL: {url}")
    print("使用 60 个线程并发发送 30,000 次请求...")
    
    start_time = time.time()
    
    # 使用 60 个并发线程发送 30000 次请求（约持续 1.5 - 2 分钟）
    with concurrent.futures.ThreadPoolExecutor(max_workers=60) as executor:
        executor.map(send_request, range(30000))
        
    duration = time.time() - start_time
    print(f"压力测试完成！总耗时: {duration:.2f} 秒")

if __name__ == '__main__':
    main()
