import asyncio
import json
import time
import websockets

from openjiuwen_runtime.management.session.k8s_service_handler import K8sServiceHandler
#from .service_handler import K8sServiceHandler
from openjiuwen_runtime.foundation.log import get_logger

logger = get_logger(__name__)


async def test(pod_ip, port):
    # --- 网络配置 ---
    uri = f"ws://{pod_ip}:{port}"

    # --- 构造业务负载 (1:1 还原成功格式) ---
    current_ts = time.time()
    # 模拟生成 session_id，确保不为 None 以避免 startswith 报错
    session_id = f"sess_{int(current_ts)}_test"

    test_payload = {
        "request_id": f"req_{int(current_ts)}",
        "channel_id": "web",
        "session_id": session_id,
        "req_method": "chat.send",
        "params": {
            "session_id": session_id,
            "content": "1+1等于几",
            "mode": "agent",
            "query": "1+1等于几,直接告诉我答案，回答字数10个字内"
        },
        "is_stream": True,
        "timestamp": current_ts,
        "metadata": {
            "query": {},
            "method": "chat.send"
        },
        "service_id": "groupIDuserIDbotID",
        "agent_id": "abc"
    }

    logger.info(f"[*] 正在建立 WebSocket 连接: {uri}")

    try:
        async with websockets.connect(
                uri,
                open_timeout=120,  # 极大延长连接超时
                ping_interval=20,  # 增加心跳，防止被网关断开
                ping_timeout=20
        ) as websocket:
            logger.info("[+] 连接成功！")

            # 发送 JSON 数据
            logger.info("[*] 正在发送业务指令...")
            await websocket.send(json.dumps(test_payload))

            logger.info("[*] 等待响应流 (Ctrl+C 停止)...")
            overall_timeout = 120  # 整体超时兜底，防止服务端一直不发终止信号
            try:
                while True:
                    try:
                        message = await asyncio.wait_for(
                            websocket.recv(), timeout=overall_timeout
                        )
                    except asyncio.TimeoutError:
                        logger.info(f"\n[!] {overall_timeout}s 未收到消息，主动结束")
                        break

                    try:
                        resp_data = json.loads(message)
                        logger.info(f"\n>>> 收到消息: {message}")
                    except json.JSONDecodeError:
                        logger.info(f"\n>>> 收到非 JSON 消息: {message}")
                        continue

                    # 根据业务协议判断流是否结束
                    if resp_data.get("is_final") and resp_data.get("response_kind") == "e2a.complete":
                        logger.info("[+] 收到终止信号 (is_final & e2a.complete)，主动结束")
                        break
                    if resp_data.get("status") in ("failed", "cancelled"):
                        logger.info(f"[!] 服务端返回 status={resp_data.get('status')}，主动结束")
                        break
            finally:
                logger.info("[*] 消息收完了")
                await websocket.close()

    except Exception as e:
        logger.info(f"\n[!] 运行出错: {e}")


class MyHandler(K8sServiceHandler):
    async def handle_message(self, msg):
        return None


async def main():
    handler = MyHandler(
        image="swr.cn-north-4.xxxxxxx",
        env_vars={
            "MODEL_PROVIDER": "OpenAI",
            "MODEL_NAME": "Qwen/Qwen3-32B",
            "API_BASE": "https://api.siliconflow.cn/v1",
            "API_KEY": "xxxxxx",
        },
    )
    info = await handler.deploy()
    logger.info("namespace: ", info.namespace)          # -> 10.244.1.145
    logger.info("pod_name: ", info.pod_name)
    logger.info("port: ", info.port)
    logger.info("pod_ip: ", info.pod_ip)
    logger.info("host_ip: ", info.host_ip)
    logger.info("node_name: ", info.node_name)

    await asyncio.sleep(5)
    logger.info("开始发消息: ")

    await test(info.pod_ip, info.port)
    await asyncio.sleep(5)

    pod_name = await handler.delete()
    logger.info("delete: ", pod_name)                # -> jiuwenclaw-xxxxxxxxxx-xxxxx

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\n[*] 测试已由用户手动停止。")
