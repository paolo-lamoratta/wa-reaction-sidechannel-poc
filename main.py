#!/usr/bin/env python3
"""
WhatsApp Reaction Timing Analyzer (Pure Async)
Measures timing between reaction send and delivery receipt (double tick).
"""

import asyncio
import time
import threading
from dataclasses import dataclass
from typing import Optional

import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt

from neonize.aioze.client import NewAClient
from neonize.aioze.events import ConnectedEv, ReceiptEv, event_global_loop
from neonize.proto.Neonize_pb2 import Message, Receipt, JID
from neonize.utils.jid import build_jid


@dataclass
class ReactionTiming:
    """Timing data for a single reaction"""
    reaction_id: str
    send_time: float
    double_tick_time: Optional[float] = None
    
    @property
    def delivery_time_ms(self) -> Optional[float]:
        if self.send_time and self.double_tick_time:
            return (self.double_tick_time - self.send_time) * 1000
        return None


class TimingTracker:
    """Tracks delivery times for reactions"""
    def __init__(self):
        self.pending: dict[str, ReactionTiming] = {}
        self.completed: list[ReactionTiming] = []
        self.early_receipts: dict[str, float] = {}  # Receipts arrived before registration
        self.expected_count: int = 0
        self.all_done = asyncio.Event()
        self.tracked_ids: set[str] = set()
    
    def register_send(self, message_id: str, send_time: float):
        """Registers a sent message - handles early receipts"""
        self.tracked_ids.add(message_id)
        
        if message_id in self.early_receipts:
            # Receipt already arrived!
            receipt_time = self.early_receipts.pop(message_id)
            timing = ReactionTiming(message_id, send_time, receipt_time)
            self.completed.append(timing)
            print(f"✓✓ #{len(self.completed)} Delivery: {timing.delivery_time_ms:.0f}ms (early)")
            self._check_done()
        else:
            self.pending[message_id] = ReactionTiming(message_id, send_time)
    
    def add_receipt(self, message_id: str):
        """Adds a receipt - might arrive before registration due to async nature"""
        receipt_time = time.time()
        
        if message_id in self.pending:
            timing = self.pending.pop(message_id)
            timing.double_tick_time = receipt_time
            self.completed.append(timing)
            print(f"✓✓ #{len(self.completed)} Delivery: {timing.delivery_time_ms:.0f}ms")
            self._check_done()
        elif message_id in self.tracked_ids:
            # Already completed
            pass
        else:
            # Receipt arrived before the send was registered
            self.early_receipts[message_id] = receipt_time
    
    def _check_done(self):
        if len(self.completed) >= self.expected_count:
            self.all_done.set()
    
    async def wait_for_completion(self, timeout: float) -> int:
        try:
            await asyncio.wait_for(self.all_done.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            pass
        return len(self.completed)
    
    def get_times_ms(self) -> list[float]:
        return [t.delivery_time_ms for t in self.completed if t.delivery_time_ms is not None]
    
    def print_stats(self):
        times = self.get_times_ms()
        if times:
            print(f"\n{'='*50}")
            print(f"FINAL STATISTICS")
            print(f"{'='*50}")
            print(f"Reactions sent: {self.expected_count}")
            print(f"Double ticks received: {len(times)}")
            print(f"Pending: {len(self.pending)}")
            print(f"\nDelivery Time (send → double tick):")
            print(f"  Average: {sum(times)/len(times):.0f}ms")
            print(f"  Min: {min(times):.0f}ms")
            print(f"  Max: {max(times):.0f}ms")
        else:
            print("\nNo timing data available")


# Globals
tracker = TimingTracker()
connected = threading.Event()


def parse_jid(input_str: str) -> JID:
    cleaned = ''.join(filter(str.isalnum, input_str.split('@')[0] if '@' in input_str else input_str))
    return build_jid(cleaned)


def generate_graph(tracker: TimingTracker):
    times = tracker.get_times_ms()
    if not times:
        print("No data available for plotting")
        return
    
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5))
    
    avg = sum(times) / len(times)
    
    ax1.bar(range(1, len(times) + 1), times, color='steelblue', alpha=0.7)
    ax1.axhline(y=avg, color='red', linestyle='--', label=f'Average: {avg:.0f}ms')
    ax1.set_xlabel('Reaction Index')
    ax1.set_ylabel('Delivery Time (ms)')
    ax1.set_title('Delivery Time per Reaction')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    
    ax2.hist(times, bins=min(20, len(times)), color='steelblue', alpha=0.7, edgecolor='black')
    ax2.axvline(x=avg, color='red', linestyle='--', label=f'Average: {avg:.0f}ms')
    ax2.set_xlabel('Delivery Time (ms)')
    ax2.set_ylabel('Frequency')
    ax2.set_title('Delivery Time Distribution')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    
    plt.tight_layout()
    filename = f'reaction_timing_{int(time.time())}.png'
    plt.savefig(filename, dpi=150, bbox_inches='tight')
    print(f"\n✓ Graph saved: {filename}")


# Event handlers
async def on_connected(nc: NewAClient, ev: ConnectedEv):
    print("\n✓ Connected to WhatsApp!")
    connected.set()


async def on_receipt(nc: NewAClient, ev: ReceiptEv):
    # INACTIVE or DELIVERED = double tick
    if ev.Type in (Receipt.INACTIVE, Receipt.DELIVERED):
        for msg_id in ev.MessageIDs:
            tracker.add_receipt(msg_id)


async def send_reaction(nc: NewAClient, chat_jid: JID, sender_jid: JID,
                        message_id: str, reaction: str) -> Optional[str]:
    """Sends a reaction and registers timing"""
    reaction_msg: Message = await nc.build_reaction(
        chat=chat_jid,
        sender=sender_jid,
        message_id=message_id,
        reaction=reaction
    )
    
    # Send time recorded immediately before send
    send_time = time.time()
    
    response = await nc.send_message(chat_jid, reaction_msg)
    
    if response and response.ID:
        tracker.register_send(response.ID, send_time)
        return response.ID
    return None


async def send_all_reactions(nc: NewAClient, chat_jid: JID, sender_jid: JID,
                              message_id: str, reaction: str, count: int, delay_ms: int):
    """Launches all reactions in parallel with delay between launches"""
    
    print(f"\n{'='*50}")
    print(f"SENDING {count} REACTIONS (delay: {delay_ms}ms)")
    print(f"{'='*50}")
    
    tracker.expected_count = count
    start = time.time()
    
    # Launch tasks
    tasks = []
    for i in range(count):
        task = asyncio.create_task(
            send_reaction(nc, chat_jid, sender_jid, message_id, reaction)
        )
        tasks.append(task)
        print(f"[{i+1}/{count}] Launched")
        
        if delay_ms > 0 and i < count - 1:
            await asyncio.sleep(delay_ms / 1000.0)
    
    launch_time = time.time() - start
    print(f"\n✓ Tasks launched in {launch_time:.2f}s")
    
    # Wait for completion
    results = await asyncio.gather(*tasks, return_exceptions=True)
    success = sum(1 for r in results if r and not isinstance(r, Exception))
    
    elapsed = time.time() - start
    print(f"✓ Completed in {elapsed:.2f}s ({success}/{count} ok)")


async def send_test_message(nc: NewAClient, jid: JID) -> Optional[str]:
    response = await nc.send_message(jid, "Test message 🧪")
    return response.ID if response else None


async def main():
    print("="*60)
    print("   WhatsApp Reaction Timing Analyzer")
    print("="*60)
    
    # Client and handler registration
    nc = NewAClient("reaction_timer.db")
    nc.event(ConnectedEv)(on_connected)
    nc.event(ReceiptEv)(on_receipt)
    
    print("\n📱 Connecting...")
    
    # Connect in background
    asyncio.create_task(nc.connect())
    
    try:
        await asyncio.wait_for(connected.wait(), timeout=120)
    except asyncio.TimeoutError:
        print("✗ Connection Timeout")
        return
    
    await asyncio.sleep(1)
    
    # Configuration
    print("\n" + "="*50)
    print("CONFIGURATION")
    print("="*50)
    
    target_input = input("Target phone number (with country code): ").strip()
    if not target_input:
        return
    target_jid = parse_jid(target_input)
    print(f"✓ Target: {target_jid.User}@{target_jid.Server}")
    
    # Mode
    print("\n1. Manual Message ID")
    print("2. Send test message")
    mode = input("Choice [1/2]: ").strip()
    
    if mode == "1":
        target_message_id = input("Message ID: ").strip()
    else:
        print("\n📤 Sending test message...")
        target_message_id = await send_test_message(nc, target_jid)
        if not target_message_id:
            print("✗ Error")
            return
        print(f"✓ Message ID: {target_message_id}")
        await asyncio.sleep(2)
    
    # Parameters
    try:
        count = int(input("Number of reactions [10]: ").strip() or "10")
    except ValueError:
        count = 10
    
    try:
        delay = int(input("Delay between sends ms [50]: ").strip() or "50")
    except ValueError:
        delay = 50
    
    try:
        timeout = float(input("Response timeout sec [30]: ").strip() or "30")
    except ValueError:
        timeout = 30.0
    
    reaction = input("Emoji [👍]: ").strip() or "👍"
    
    print(f"\n✓ Config: {count} reactions, {delay}ms delay, {timeout}s timeout")
    input("\nPress ENTER to start...")
    
    # Send reactions
    await send_all_reactions(nc, target_jid, target_jid, target_message_id, reaction, count, delay)
    
    # Wait for responses
    print(f"\n⏳ Waiting for responses (max {timeout}s)...")
    completed = await tracker.wait_for_completion(timeout)
    print(f"✓ Received {completed}/{count} double ticks")
    
    # Statistics and plot
    tracker.print_stats()
    
    if tracker.get_times_ms():
        print("\n📊 Generating graph...")
        generate_graph(tracker)
    
    print("\n✓ Analysis completed!")
    input("\nPress ENTER to exit...")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n✗ Interrupted")
