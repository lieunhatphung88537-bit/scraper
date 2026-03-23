import multiprocessing
import subprocess
import sys
import time


def run_script(script_name):
    """
    这是一个通用的运行函数，负责启动具体的爬虫脚本
    """
    print(f"🚀 [系统消息] 正在启动模块: {script_name}...")
    try:
        # 使用 subprocess 开启新进程运行 python 脚本
        # 这样能保证各脚本的环境变量和运行逻辑完全独立
        subprocess.run(["python3", script_name], check=True)
    except Exception as e:
        print(f"❌ [系统报错] 模块 {script_name} 意外停止: {e}")


if __name__ == "__main__":
    # 三个核心脚本的名字，确保名字跟你文件夹里的一模一样
    scripts = [
        "typhoon_allDOCKER.py",
        "noaa_combined_scraper.py",
        "baowen_combined_scraper.py"
    ]

    print("=" * 30)
    print("🌊 全球气象数据采集系统 - 总控制台")
    print("=" * 30)

    # 1. 询问用户模式
    print("请选择启动模式:")
    print("1. [ALL] 并行启动所有模块 (生产环境推荐)")
    print("2. [ONLY] 仅启动台风模块")
    print("3. [ONLY] 仅启动 NOAA 模块")
    print("4. [ONLY] 仅启动气象报文模块")

    choice = input("\n请输入数字 (默认1): ") or "1"

    processes = []

    # 2. 根据选择分配任务
    if choice == "1":
        tasks = scripts
    elif choice == "2":
        tasks = [scripts[0]]
    elif choice == "3":
        tasks = [scripts[1]]
    elif choice == "4":
        tasks = [scripts[2]]
    else:
        print("⚠️ 输入有误，系统将默认启动所有模块...")
        tasks = scripts

    # 3. 核心逻辑：多进程并行启动
    for script in tasks:
        # 为每个脚本开辟一个独立的进程
        p = multiprocessing.Process(target=run_script, args=(script,))
        p.start()
        processes.append(p)
        time.sleep(2)  # 稍微错开启动时间，防止瞬间撑爆内存

    print(f"\n✅ 成功启动 {len(processes)} 个任务。按 Ctrl+C 尝试停止所有任务。")

    # 保持主进程运行
    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\n🛑 [系统消息] 收到退出指令，正在关闭所有采集任务...")
        for p in processes:
            p.terminate()
        print("👋 任务已全部安全停止。")