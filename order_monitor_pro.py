#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
工单监控脚本 - 智能增强版
作者：小可爱 🎀
版本：4.0 Pro (智能通知 + 统计报表 + 告警升级)

功能：
- ✅ 增量通知：只通知新出现的失败工单，避免重复
- ✅ 统计报表：支持日报/周报汇总
- ✅ 告警升级：连续失败自动升级通知级别
- ✅ 状态持久化：记录历史状态，重启不丢失
"""

import requests
import json
import os
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from pathlib import Path

# ============ 配置区域 ============
BASE_URL = "https://cep.chanjet.com.cn"
ORDER_LIST_URL = f"{BASE_URL}/new-prod/order/V1/simple_order_list/"
API_TOKEN = os.getenv("ORDER_API_TOKEN", "bk-acb93eae-0112-4cff-af24-a3b476355839")
FEISHU_WEBHOOK = os.getenv("FEISHU_WEBHOOK", "")

# 状态文件路径
STATE_FILE = Path(__file__).parent / "monitor_state.json"
REPORT_DIR = Path(__file__).parent / "reports"

# 告警配置
ALERT_CONFIG = {
    "normal_threshold": 5,      # 正常阈值：失败工单 < 5
    "warning_threshold": 10,    # 警告阈值：失败工单 5-10
    "critical_threshold": 20,   # 严重阈值：失败工单 > 10
    "consecutive_count": 3,     # 连续多少次触发升级告警
}

# 报表配置
REPORT_CONFIG = {
    "daily_report_time": "09:00",    # 日报发送时间
    "weekly_report_day": "Monday",   # 周报发送星期
    "weekly_report_time": "10:00",   # 周报发送时间
}

# ============ 状态管理类 ============
class MonitorState:
    def __init__(self, state_file: Path):
        self.state_file = state_file
        self.state = self._load_state()
    
    def _load_state(self) -> Dict:
        """加载状态文件"""
        if self.state_file.exists():
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except:
                pass
        
        return {
            "notified_order_ids": [],      # 已通知的工单ID列表
            "last_check_time": None,       # 上次检查时间
            "consecutive_failures": 0,     # 连续失败检查次数
            "last_alert_level": "normal",  # 上次告警级别
            "daily_stats": {},             # 每日统计
            "weekly_stats": {},            # 每周统计
            "last_daily_report": None,     # 上次日报时间
            "last_weekly_report": None,    # 上次周报时间
        }
    
    def save(self):
        """保存状态文件"""
        self.state_file.parent.mkdir(parents=True, exist_ok=True)
        with open(self.state_file, 'w', encoding='utf-8') as f:
            json.dump(self.state, f, ensure_ascii=False, indent=2)
    
    def get_notified_ids(self) -> Set[str]:
        """获取已通知的工单ID集合"""
        return set(self.state.get("notified_order_ids", []))
    
    def add_notified_ids(self, order_ids: List[str]):
        """添加已通知的工单ID（保留最近1000条）"""
        current = self.state.get("notified_order_ids", [])
        current.extend(order_ids)
        self.state["notified_order_ids"] = current[-1000:]
        self.save()
    
    def get_consecutive_failures(self) -> int:
        """获取连续失败检查次数"""
        return self.state.get("consecutive_failures", 0)
    
    def increment_consecutive_failures(self):
        """增加连续失败计数"""
        self.state["consecutive_failures"] = self.state.get("consecutive_failures", 0) + 1
        self.save()
    
    def reset_consecutive_failures(self):
        """重置连续失败计数"""
        self.state["consecutive_failures"] = 0
        self.save()
    
    def get_last_alert_level(self) -> str:
        """获取上次告警级别"""
        return self.state.get("last_alert_level", "normal")
    
    def set_last_alert_level(self, level: str):
        """设置上次告警级别"""
        self.state["last_alert_level"] = level
        self.save()
    
    def should_send_daily_report(self) -> bool:
        """判断是否应该发送日报"""
        last = self.state.get("last_daily_report")
        if not last:
            return True
        
        try:
            last_date = datetime.strptime(last, "%Y-%m-%d").date()
            return datetime.now().date() > last_date
        except:
            return True
    
    def mark_daily_report_sent(self):
        """标记日报已发送"""
        self.state["last_daily_report"] = datetime.now().strftime("%Y-%m-%d")
        self.save()
    
    def should_send_weekly_report(self) -> bool:
        """判断是否应该发送周报"""
        last = self.state.get("last_weekly_report")
        if not last:
            return True
        
        try:
            last_date = datetime.strptime(last, "%Y-%m-%d").date()
            # 检查是否是周一且距离上次周报超过6天
            today = datetime.now().date()
            is_monday = today.weekday() == 0
            days_since_last = (today - last_date).days
            return is_monday and days_since_last >= 6
        except:
            return True
    
    def mark_weekly_report_sent(self):
        """标记周报已发送"""
        self.state["last_weekly_report"] = datetime.now().strftime("%Y-%m-%d")
        self.save()
    
    def record_order_stat(self, order: Dict):
        """记录工单统计"""
        today = datetime.now().strftime("%Y-%m-%d")
        week = datetime.now().strftime("%Y-W%W")
        
        if today not in self.state["daily_stats"]:
            self.state["daily_stats"][today] = {"count": 0, "types": {}}
        
        self.state["daily_stats"][today]["count"] += 1
        
        order_type = order.get("order_type", {}).get("name", "未知")
        types = self.state["daily_stats"][today]["types"]
        types[order_type] = types.get(order_type, 0) + 1
        
        # 保留最近30天统计
        cutoff = (datetime.now() - timedelta(days=30)).strftime("%Y-%m-%d")
        self.state["daily_stats"] = {
            k: v for k, v in self.state["daily_stats"].items() 
            if k >= cutoff
        }
        
        self.save()


# ============ 飞书通知类 ============
class FeishuNotifier:
    def __init__(self, webhook: str):
        self.webhook = webhook
    
    def send_text(self, content: str):
        """发送纯文本消息"""
        if not self.webhook:
            return False
        
        try:
            data = {
                "msg_type": "text",
                "content": {"text": content}
            }
            response = requests.post(self.webhook, json=data, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def send_post(self, title: str, content: List[List[Dict]]):
        """发送富文本卡片消息"""
        if not self.webhook:
            return False
        
        try:
            data = {
                "msg_type": "post",
                "content": {
                    "post": {
                        "zh_cn": {"title": title, "content": content}
                    }
                }
            }
            response = requests.post(self.webhook, json=data, timeout=10)
            return response.status_code == 200
        except:
            return False
    
    def send_urgent(self, content: str):
        """发送紧急通知（@所有人）"""
        if not self.webhook:
            return False
        
        try:
            # 飞书紧急通知格式
            data = {
                "msg_type": "text",
                "content": {
                    "text": f"🚨【紧急告警】\n\n{content}\n\n@所有人"
                }
            }
            response = requests.post(self.webhook, json=data, timeout=10)
            return response.status_code == 200
        except:
            return False


# ============ 工单监控类 ============
class OrderMonitorPro:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
            "Accept": "application/json",
            "authorization": API_TOKEN
        })
        self.feishu = FeishuNotifier(FEISHU_WEBHOOK) if FEISHU_WEBHOOK else None
        self.state = MonitorState(STATE_FILE)
    
    def get_failed_orders(self, hours: int = 1) -> List[Dict]:
        """获取最近N小时内的失败工单"""
        try:
            params = {
                "current": 1,
                "pageSize": 100,
                "order_status_memo": "API执行失败",
                "table_type": "all_table",
                "pageNumber": 1
            }
            
            response = self.session.get(ORDER_LIST_URL, params=params, timeout=30)
            
            if response.status_code != 200:
                return []
            
            result = response.json()
            orders = result.get("data", {}).get("list", []) or \
                     result.get("list", []) or \
                     result.get("records", []) or []
            
            return orders
            
        except Exception as e:
            print(f"❌ 获取工单异常：{e}")
            return []
    
    def get_new_orders(self, orders: List[Dict]) -> List[Dict]:
        """获取新增的工单（未通知过的）"""
        notified_ids = self.state.get_notified_ids()
        new_orders = []
        
        for order in orders:
            order_id = str(order.get("id", ""))
            if order_id and order_id not in notified_ids:
                new_orders.append(order)
        
        return new_orders
    
    def calculate_alert_level(self, order_count: int) -> str:
        """计算告警级别"""
        if order_count >= ALERT_CONFIG["critical_threshold"]:
            return "critical"
        elif order_count >= ALERT_CONFIG["warning_threshold"]:
            return "warning"
        else:
            return "normal"
    
    def _format_time(self, time_str: str) -> str:
        """格式化时间"""
        if not time_str:
            return "N/A"
        
        try:
            if "+" in time_str:
                time_str = time_str.split("+")[0]
            elif time_str.endswith("Z"):
                time_str = time_str[:-1]
            if "." in time_str:
                time_str = time_str.split(".")[0]
            dt = datetime.strptime(time_str, "%Y-%m-%dT%H:%M:%S")
            return dt.strftime("%Y-%m-%d %H:%M")
        except:
            return time_str
    
    def send_incremental_notification(self, new_orders: List[Dict], total_count: int):
        """发送增量通知（只通知新工单）"""
        if not self.feishu or not new_orders:
            return
        
        # 计算告警级别
        alert_level = self.calculate_alert_level(total_count)
        last_level = self.state.get_last_alert_level()
        consecutive = self.state.get_consecutive_failures()
        
        # 判断是否需要升级通知
        is_escalation = (
            (alert_level == "critical" and last_level != "critical") or
            consecutive >= ALERT_CONFIG["consecutive_count"]
        )
        
        # 构建消息
        lines = []
        lines.append(f"🚨 新增失败工单通知")
        lines.append(f"📅 检查时间：{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"📊 新增数量：{len(new_orders)} | 当前总数：{total_count}")
        lines.append(f"⚠️ 告警级别：{alert_level.upper()}")
        lines.append("")
        lines.append("=" * 40)
        lines.append("")
        
        # 显示新增工单（最多10条）
        for i, order in enumerate(new_orders[:10], 1):
            order_id = order.get("id", "N/A")
            order_type = order.get("order_type", {}).get("name", "N/A")
            creator = order.get("order_creator_name", "N/A")
            dept = order.get("order_creator_dep", "N/A")
            create_time = self._format_time(order.get("order_create_time"))
            
            lines.append(f"【{i}】#{order_id} | {order_type}")
            lines.append(f"     创建人：{creator} | 部门：{dept}")
            lines.append(f"     时间：{create_time}")
            lines.append("")
        
        if len(new_orders) > 10:
            lines.append(f"⚠️ 还有 {len(new_orders) - 10} 条新增工单...")
        
        content = "\n".join(lines)
        
        # 发送通知
        if is_escalation:
            print("🚨 触发升级告警！")
            self.feishu.send_urgent(content)
        else:
            self.feishu.send_text(content)
        
        # 记录已通知的工单
        self.state.add_notified_ids([str(o.get("id", "")) for o in new_orders])
        self.state.set_last_alert_level(alert_level)
    
    def send_daily_report(self, orders: List[Dict]):
        """发送日报"""
        if not self.feishu:
            return
        
        today = datetime.now().strftime("%Y-%m-%d")
        stats = self.state.state["daily_stats"].get(today, {"count": 0, "types": {}})
        
        lines = []
        lines.append(f"📊 失败工单日报")
        lines.append(f"📅 日期：{today}")
        lines.append(f"📈 新增失败工单：{stats['count']} 条")
        lines.append("")
        lines.append("工单类型分布：")
        
        for t, c in sorted(stats.get("types", {}).items(), key=lambda x: -x[1]):
            lines.append(f"  • {t}: {c}条")
        
        lines.append("")
        lines.append(f"当前待处理：{len(orders)} 条")
        
        content = "\n".join(lines)
        self.feishu.send_text(content)
        
        self.state.mark_daily_report_sent()
        print("✅ 日报已发送")
    
    def send_weekly_report(self, orders: List[Dict]):
        """发送周报"""
        if not self.feishu:
            return
        
        week = datetime.now().strftime("%Y-W%W")
        
        # 统计本周数据
        total_count = 0
        type_stats = {}
        
        for date, stats in self.state.state["daily_stats"].items():
            if date.startswith(week[:4]) and f"W{date.split('-')[1]}" in week:
                total_count += stats.get("count", 0)
                for t, c in stats.get("types", {}).items():
                    type_stats[t] = type_stats.get(t, 0) + c
        
        lines = []
        lines.append(f"📊 失败工单周报")
        lines.append(f"📅 周期：第 {week.split('-W')[1]} 周")
        lines.append(f"📈 本周新增：{total_count} 条")
        lines.append("")
        lines.append("工单类型 TOP5：")
        
        for t, c in sorted(type_stats.items(), key=lambda x: -x[1])[:5]:
            lines.append(f"  • {t}: {c}条")
        
        lines.append("")
        lines.append(f"当前待处理：{len(orders)} 条")
        
        content = "\n".join(lines)
        self.feishu.send_text(content)
        
        self.state.mark_weekly_report_sent()
        print("✅ 周报已发送")
    
    def run(self, check_reports: bool = True):
        """执行监控任务"""
        print("🎀 小可爱开始执行智能监控任务...")
        print(f"🔑 Token: {API_TOKEN[:10]}...")
        print(f"📱 飞书通知：{'已启用' if self.feishu else '未启用'}")
        print()
        
        # 获取所有失败工单
        all_orders = self.get_failed_orders()
        total_count = len(all_orders)
        
        print(f"📊 当前失败工单总数：{total_count}")
        
        # 获取新增工单
        new_orders = self.get_new_orders(all_orders)
        print(f"🆕 新增工单数：{len(new_orders)}")
        
        # 记录统计
        for order in new_orders:
            self.state.record_order_stat(order)
        
        # 更新连续失败计数
        if total_count > 0:
            self.state.increment_consecutive_failures()
        else:
            self.state.reset_consecutive_failures()
        
        # 发送增量通知
        if new_orders:
            self.send_incremental_notification(new_orders, total_count)
        else:
            print("✅ 无新增工单，跳过通知")
        
        # 检查是否需要发送报表
        if check_reports:
            now = datetime.now()
            
            # 日报（每天9点）
            if now.strftime("%H:%M") == REPORT_CONFIG["daily_report_time"]:
                if self.state.should_send_daily_report():
                    self.send_daily_report(all_orders)
            
            # 周报（周一10点）
            if now.weekday() == 0 and now.strftime("%H:%M") == REPORT_CONFIG["weekly_report_time"]:
                if self.state.should_send_weekly_report():
                    self.send_weekly_report(all_orders)
        
        # 保存状态
        self.state.state["last_check_time"] = datetime.now().isoformat()
        self.state.save()
        
        print("🎀 监控任务完成")
        return {
            "total": total_count,
            "new": len(new_orders),
            "alert_level": self.calculate_alert_level(total_count)
        }


# ============ 主函数 ============
if __name__ == "__main__":
    monitor = OrderMonitorPro()
    result = monitor.run(check_reports=True)
    
    # 输出结果
    print("\n" + "=" * 40)
    print(f"📊 本次检查汇总：")
    print(f"   失败工单总数：{result['total']}")
    print(f"   新增工单数：{result['new']}")
    print(f"   告警级别：{result['alert_level'].upper()}")
    print("=" * 40)
