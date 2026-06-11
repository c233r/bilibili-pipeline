"""
Pipeline主入口文件
整合下载线程池和分析线程池，实现完整的流水线功能
"""
import os
import sys
import time
import json
from typing import Optional

# 添加爬虫模块路径
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'bilibili爬虫', 'src'))

from model.bilibili_api import BilibiliAPI
from .task_queue import TaskQueue
from .downloader_pool import DownloadPool
from .analyzer_pool import AnalyzerPool


class VideoPipeline:
    """视频下载与分析流水线"""
    
    def __init__(self, num_download_workers: int = 2, num_analyze_workers: int = 3):
        """
        初始化流水线
        
        Args:
            num_download_workers: 下载线程数量
            num_analyze_workers: 分析线程数量
        """
        # 创建共享任务队列
        self.task_queue = TaskQueue()
        
        # 创建下载线程池
        self.download_pool = DownloadPool(
            num_workers=num_download_workers,
            task_queue=self.task_queue
        )
        
        # 创建分析线程池
        self.analyzer_pool = AnalyzerPool(
            num_workers=num_analyze_workers,
            task_queue=self.task_queue
        )
        
        # 初始化B站API
        self.api = BilibiliAPI()
        
        # 配置文件路径
        self.config_file = os.path.join(
            os.path.dirname(__file__), 
            '..', 
            'bilibili爬虫', 
            'config.json'
        )
        
        # 加载配置
        self._load_config()
        
        # 统计信息
        self.start_time: Optional[float] = None
        self.end_time: Optional[float] = None
    
    def _load_config(self):
        """加载配置文件"""
        try:
            if os.path.exists(self.config_file):
                with open(self.config_file, 'r', encoding='utf-8') as f:
                    config = json.load(f)
                    
                    # 设置下载路径
                    download_path = config.get('download_path', '')
                    if download_path:
                        self.download_pool.set_download_path(download_path)
                    
                    # 设置Cookie路径
                    cookies_path = config.get('cookies_path', '')
                    if cookies_path:
                        self.download_pool.set_cookies_path(cookies_path)
                    
                    # 设置是否使用yt-dlp
                    use_ytdlp = config.get('use_ytdlp', True)
                    self.download_pool.set_use_ytdlp(use_ytdlp)
                    
        except Exception as e:
            print(f"[流水线] 加载配置文件失败: {e}")
    
    def set_filter_prompt(self, prompt: str, use_refined: bool = False):
        """
        设置筛选提示词
        
        Args:
            prompt: 筛选提示词
            use_refined: 是否使用精细化分析
        """
        self.analyzer_pool.set_filter_prompt(prompt, use_refined)
    
    def add_download_tasks(self, videos: list[dict]) -> int:
        """
        批量添加下载任务
        
        Args:
            videos: 视频列表，每个元素包含bvid和title
        
        Returns:
            成功添加的任务数量
        """
        added_count = 0
        for video in videos:
            bvid = video.get('bvid', '')
            title = video.get('title', '未知')
            
            if bvid:
                success = self.download_pool.add_task(bvid, title)
                if success:
                    added_count += 1
        
        print(f"[流水线] 已添加 {added_count} 个下载任务")
        return added_count
    
    def search_and_add_videos(self, keyword: str, max_count: int = 0) -> int:
        """
        搜索视频并添加到下载队列
        
        Args:
            keyword: 搜索关键词
            max_count: 最大下载数量（0表示全部）
        
        Returns:
            添加的任务数量
        """
        print(f"[流水线] 搜索关键词: {keyword}")
        
        page = 1
        page_size = 20
        added_count = 0
        total_added = 0
        
        while True:
            print(f"[流水线] 获取第 {page} 页...")
            
            try:
                items, total = self.api.search_videos(keyword, page=page, page_size=page_size)
            except Exception as e:
                print(f"[流水线] 获取第 {page} 页失败: {str(e)}")
                page += 1
                continue
            
            if not items:
                print(f"[流水线] 已获取全部内容")
                break
            
            # 计算当前页需要添加的数量
            if max_count > 0:
                remaining = max_count - total_added
                if remaining <= 0:
                    break
                to_add = min(remaining, len(items))
            else:
                to_add = len(items)
            
            # 添加当前页的项目
            for i in range(to_add):
                item = items[i]
                bvid = item.get('bvid', '')
                title = item.get('title', '未知')
                
                if bvid:
                    success = self.download_pool.add_task(bvid, title)
                    if success:
                        added_count += 1
            
            total_added += added_count
            print(f"[流水线] 第 {page} 页已添加 {added_count} 个任务，累计 {total_added} 个")
            
            # 检查是否达到最大数量
            if max_count > 0 and total_added >= max_count:
                print(f"[流水线] 已达到设定的下载数量 ({max_count} 个)")
                break
            
            # 翻页
            page += 1
        
        return total_added
    
    def search_up_and_add_videos(self, up_name: str, max_count: int = 0) -> int:
        """
        搜索UP主并添加其视频到下载队列
        
        Args:
            up_name: UP主名称
            max_count: 最大下载数量（0表示全部）
        
        Returns:
            添加的任务数量
        """
        print(f"[流水线] 搜索UP主: {up_name}")
        
        # 搜索UP主
        users = self.api.search_users(up_name, max_results=10)
        
        if not users:
            print(f"[流水线] 未找到相关UP主")
            return 0
        
        print(f"[流水线] 找到 {len(users)} 个UP主，选择第一个: {users[0]['name']}")
        
        selected_user = users[0]
        mid = selected_user['mid']
        
        print(f"[流水线] 获取UP主视频: {selected_user['name']} (UID: {mid})")
        
        page = 1
        page_size = 20
        added_count = 0
        total_added = 0
        
        while True:
            print(f"[流水线] 获取第 {page} 页...")
            
            try:
                items, total = self.api.get_user_videos(mid=mid, page=page, page_size=page_size)
            except Exception as e:
                print(f"[流水线] 获取第 {page} 页失败: {str(e)}")
                page += 1
                continue
            
            if not items:
                print(f"[流水线] 已获取全部内容")
                break
            
            # 计算当前页需要添加的数量
            if max_count > 0:
                remaining = max_count - total_added
                if remaining <= 0:
                    break
                to_add = min(remaining, len(items))
            else:
                to_add = len(items)
            
            # 添加当前页的项目
            for i in range(to_add):
                item = items[i]
                bvid = item.get('bvid', '')
                title = item.get('title', '未知')
                
                if bvid:
                    success = self.download_pool.add_task(bvid, title)
                    if success:
                        added_count += 1
            
            total_added += added_count
            print(f"[流水线] 第 {page} 页已添加 {added_count} 个任务，累计 {total_added} 个")
            
            # 检查是否达到最大数量
            if max_count > 0 and total_added >= max_count:
                print(f"[流水线] 已达到设定的下载数量 ({max_count} 个)")
                break
            
            # 翻页
            page += 1
        
        return total_added
    
    def start(self):
        """启动流水线"""
        print(f"[流水线] 启动流水线...")
        print(f"[流水线] 下载线程数: {self.download_pool.num_workers}")
        print(f"[流水线] 分析线程数: {self.analyzer_pool.num_workers}")
        
        # 启动下载线程池
        self.download_pool.start()
        
        # 启动分析线程池
        self.analyzer_pool.start()
        
        # 记录开始时间
        self.start_time = time.time()
        
        print(f"[流水线] 流水线已启动")
    
    def stop(self):
        """停止流水线"""
        print(f"[流水线] 停止流水线...")
        
        # 停止下载线程池
        self.download_pool.stop()
        
        # 停止分析线程池
        self.analyzer_pool.stop()
        
        # 记录结束时间
        self.end_time = time.time()
        
        print(f"[流水线] 流水线已停止")
    
    def wait_until_complete(self, check_interval: float = 2.0):
        """
        等待直到所有任务完成
        
        Args:
            check_interval: 检查间隔（秒）
        """
        print(f"[流水线] 等待所有任务完成...")
        
        while True:
            # 检查队列是否为空
            if self.task_queue.is_empty():
                print(f"[流水线] 所有任务已完成")
                break
            
            # 打印统计信息
            self.print_statistics()
            
            # 等待
            time.sleep(check_interval)
        
        # 记录结束时间
        if self.start_time and not self.end_time:
            self.end_time = time.time()
    
    def print_statistics(self):
        """打印流水线统计信息"""
        print(f"\n{'='*60}")
        print(f"[流水线统计]")
        print(f"{'='*60}")
        
        # 打印任务队列统计
        self.task_queue.print_statistics()
        
        # 打印下载线程池统计
        self.download_pool.print_statistics()
        
        # 打印分析线程池统计
        self.analyzer_pool.print_statistics()
        
        # 打印运行时间
        if self.start_time:
            current_time = self.end_time or time.time()
            elapsed_time = current_time - self.start_time
            print(f"\n[运行时间] {elapsed_time:.1f} 秒")
        
        print(f"{'='*60}\n")
    
    def get_final_report(self) -> dict:
        """
        获取最终报告
        
        Returns:
            报告字典
        """
        # 确保所有任务完成
        self.wait_until_complete()
        
        # 获取统计信息
        queue_stats = self.task_queue.get_statistics()
        download_stats = self.download_pool.get_statistics()
        analyze_stats = self.analyzer_pool.get_statistics()
        
        # 计算运行时间
        elapsed_time = 0
        if self.start_time and self.end_time:
            elapsed_time = self.end_time - self.start_time
        
        # 生成报告
        report = {
            'elapsed_time': elapsed_time,
            'total_tasks': queue_stats['total_tasks'],
            'completed_tasks': queue_stats['completed_count'],
            'failed_tasks': queue_stats['failed_count'],
            'success_rate': queue_stats['success_rate'],
            'downloaded_count': download_stats['total_downloaded'],
            'analyzed_count': analyze_stats['total_analyzed'],
            'satisfied_count': analyze_stats['total_satisfied'],
            'satisfaction_rate': analyze_stats['satisfaction_rate'],
            'download_workers': download_stats['num_workers'],
            'analyze_workers': analyze_stats['num_workers']
        }
        
        return report
    
    def __enter__(self):
        """上下文管理器入口"""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """上下文管理器出口"""
        self.stop()


def main():
    """主函数 - 演示流水线使用"""
    print("="*60)
    print("      B站视频下载与分析流水线")
    print("="*60)
    
    # 创建流水线实例
    with VideoPipeline(num_download_workers=2, num_analyze_workers=3) as pipeline:
        
        # 设置筛选提示词（可选）
        # pipeline.set_filter_prompt("视频内容是否包含技术讲解", use_refined=False)
        
        # 方式1: 搜索视频并添加到队列
        # pipeline.search_and_add_videos("Python教程", max_count=5)
        
        # 方式2: 搜索UP主并添加其视频到队列
        # pipeline.search_up_and_add_videos("某UP主名称", max_count=10)
        
        # 方式3: 手动添加视频任务
        videos = [
            {'bvid': 'BV1xx411c7mD', 'title': '示例视频1'},
            {'bvid': 'BV1yy411c7mE', 'title': '示例视频2'},
            # 添加更多视频...
        ]
        pipeline.add_download_tasks(videos)
        
        # 等待所有任务完成
        pipeline.wait_until_complete()
        
        # 打印最终统计信息
        pipeline.print_statistics()
        
        # 获取最终报告
        report = pipeline.get_final_report()
        print(f"\n[最终报告]")
        print(f"  总任务数: {report['total_tasks']}")
        print(f"  完成任务: {report['completed_tasks']}")
        print(f"  失败任务: {report['failed_tasks']}")
        print(f"  成功率: {report['success_rate']:.1f}%")
        print(f"  运行时间: {report['elapsed_time']:.1f} 秒")


if __name__ == '__main__':
    main()