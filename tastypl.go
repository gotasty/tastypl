// Copyright (C) 2017  Go Tasty
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"encoding/csv"
	"flag"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/golang/glog"
	"github.com/shopspring/decimal"
)

type transaction struct {
	date        time.Time
	txType      string
	action      string
	symbol      string // OCC symbol
	instrument  string
	description string
	value       decimal.Decimal
	quantity    decimal.Decimal
	qtyOpen     decimal.Decimal // starts as quantity, decremented when closed
	avgPrice    decimal.Decimal
	commission  decimal.Decimal
	fees        decimal.Decimal
	multiplier  uint16
	underlying  string
	expDate     time.Time
	strike      decimal.Decimal
	option      bool // or non-option (such as future/equity) if false
	mtm         bool // mark-to-market daily settlement for futures
	call        bool // or put if false
	long        bool // or short if false
}

var (
	oneHundred  = decimal.New(100, 0)
	oneThousand = decimal.New(1000, 0)

	// Cutoff point for YTD stuff.
	ytdStart = time.Date(time.Now().Year(), 1, 1, 0, 0, 0, 0, time.Now().Location())
)

func (t *transaction) String() string {
	return t.description
}

// per contract fees, see:
// https://tastyworks.desk.com/customer/en/portal/articles/2696746-commissions-and-fees-breakdown
var (
	clearingFee = decimal.New(10, -2) // -0.10
	// Options Regulatory Fee (rounded to nearest cent)
	orFee = decimal.New(415, -4) // -0.0415
	// Trading Activity Fee (only for sales, rounded up)
	finraTAF = decimal.New(2, -3) // -0.002
	// SEC Regulatory Fee (on notional amount, only for sales, rounded up)
	secFee = decimal.New(231, -7) // -0.0000231

	// Commission paid on assignment
	assignmentFee = decimal.New(-5, 0) // -5.00
)

func roundUp(d decimal.Decimal) decimal.Decimal {
	return d.Mul(oneHundred).Ceil().Div(oneHundred)
}

// There is a rounding error in the fees reported by the desktop app and
// web-based account management interface.  Here we recompute the correct
// amount and fix the transaction.
// TODO: This computes higher fees than it should for some reason.
func (t *transaction) fixFees() {
	if t.txType != "Trade" {
		return
	}
	fees := clearingFee.Mul(t.quantity)
	fees = fees.Add(orFee.Mul(t.quantity).Round(2))
	if !t.long {
		fees = fees.Add(roundUp(finraTAF.Mul(t.quantity)))
		fees = fees.Add(roundUp(secFee.Mul(t.value)))
	}
	fees = fees.Neg()
	if !t.fees.Equal(fees) {
		glog.V(5).Infof("fee %s doesn't matched computed fee %s in %s", t.fees, fees, t)
		t.fees = fees
	}
}

func (t *transaction) sanityCheck() {
	if t.quantity.Sign() <= 0 {
		glog.Fatalf("quantity is negative in %s", t)
	}
	if !t.option {
		return // TODO: no other check yet for futures/equities
	} else if t.mtm {
		// Might not be true for options on futures (?)
		glog.Fatalf("options position can't be marked-to-market in %s", t)
	}
	if t.strike.LessThanOrEqual(decimal.Zero) {
		glog.Fatalf("strike price can't be less than or equal to zero in %s", t)
	}
	if t.date.After(t.expDate) {
		glog.Fatalf("transaction date %s happened after expiration %s in %s",
			t.date, t.expDate, t)
	}
	var cp byte
	if t.call {
		cp = 'C'
	} else {
		cp = 'P'
	}
	// Recompute OCC symbol and ensure what we have is the same
	// See https://www.theocc.com/components/docs/initiatives/symbology/symbology_initiative_v1_8.pdf
	symbol := fmt.Sprintf("%- 6s%s%c%08d", t.underlying, t.expDate.Format("060102"), cp,
		t.strike.Mul(oneThousand).IntPart())
	if symbol != t.symbol {
		glog.Fatalf("expected symbol %q but found %q in %s", symbol, t.symbol, t)
	}
}

func (t *transaction) ytd() bool {
	return t.date.After(ytdStart)
}

func loadCSV(path string) [][]string {
	f, err := os.Open(path)
	if err != nil {
		glog.Fatal(err)
	}
	defer f.Close()
	reader := csv.NewReader(f)
	records, err := reader.ReadAll()
	if err != nil {
		glog.Fatal(err)
	}
	return records
}

func parseDecimal(value string) decimal.Decimal {
	if value == "" {
		return decimal.Decimal{}
	}
	d, err := decimal.NewFromString(strings.Replace(value, ",", "", -1))
	if err != nil {
		glog.Fatal(err)
	}
	return d
}

const almostRFC3339 = "2006-01-02T15:04:05-0700"

type position struct {
	// Opening transaction(s) for this position
	opens []*transaction
}

type portfolio struct {
	ytd bool // Only track YTD transactions

	// All transactions.
	transactions []*transaction

	ach decimal.Decimal // sum of all ACH movements

	// Map of symbol to opening transaction(s)
	positions map[string]*position

	cash decimal.Decimal // Cash on hand

	rpl   decimal.Decimal // Realized P&L
	comms decimal.Decimal
	fees  decimal.Decimal
	intrs decimal.Decimal // interest
	// Misc. cash changes
	miscCash decimal.Decimal

	// Summary of realized P&L per underlying
	rplPerUnderlying map[string]decimal.Decimal

	numTrades uint16 // Number of trades executed

	// Number of times we got assigned.
	assigned uint16
	assTotal decimal.Decimal
}

func NewPortfolio(records [][]string, ytd bool) *portfolio {
	p := &portfolio{
		ytd:              ytd,
		positions:        make(map[string]*position),
		rplPerUnderlying: make(map[string]decimal.Decimal),
	}

	// The CSV is sorted from newest to oldest transaction, reverse it.
	for i := len(records)/2 - 1; i >= 0; i-- {
		opp := len(records) - 1 - i
		records[i], records[opp] = records[opp], records[i]
	}
	p.parseTransactions(records)

	var prevTime time.Time
	for _, tx := range p.transactions {
		if prevTime.After(tx.date) {
			glog.Fatalf("transaction log out of order at time %s (tx=%s)", tx.date, tx)
		}
		prevTime = tx.date
		p.handleTransaction(tx)
	}
	return p
}

func (p *portfolio) parseNonOptionTransaction(record []string) {
	switch record[1] {
	case "Money Movement":
		amount := parseDecimal(record[6])
		switch record[5] {
		case "INTEREST ON CREDIT BALANCE":
			p.intrs = p.intrs.Add(amount)
		case "ACH DEPOSIT", "ACH DISBURSEMENT":
			p.ach = p.ach.Add(amount)
		default:
			if strings.HasPrefix(record[5], "FROM ") {
				// Interest paid, e.g. "FROM 10/16 THRU 11/15 @ 8    %"
				p.intrs = p.intrs.Add(amount)
				return
			}
			glog.V(2).Infof("unhandled money movement: %#v", record)
			p.miscCash = p.miscCash.Add(amount)
		}
	default:
		glog.V(1).Infof("unhandled %#v", record)
	}
}

// parseTransactions parses raw transactions from the CSV and adds them to the
// transaction history.
func (p *portfolio) parseTransactions(records [][]string) {
	p.transactions = make([]*transaction, len(records))
	var j int
	ytd := p.ytd
	for i, rec := range records {
		date, err := time.Parse(almostRFC3339, rec[0])
		if err != nil {
			glog.Fatalf("record #%d, bad transaction date: %s", i, err)
		}
		if ytd && date.After(ytdStart) {
			// Reset running counts as we only want YTD numbers.
			p.miscCash = decimal.Zero
			p.intrs = decimal.Zero
			p.ach = decimal.Zero
			// Note: we don't reset p.cash as the only way to have the correct
			// amount of cash ultimately in the account is of course to take
			// into account (bad pun, sorry) the entire account history.
			ytd = false // So we don't reset again.
		}
		value := parseDecimal(rec[6])
		comm := parseDecimal(rec[9])
		if comm.GreaterThan(decimal.Zero) {
			glog.Fatalf("record #%d, positive commission amount %s", i, rec[9])
		}
		fees := parseDecimal(rec[10])
		if fees.GreaterThan(decimal.Zero) {
			glog.Fatalf("record #%d, positive fees amount %s", i, rec[10])
		}
		p.cash = p.cash.Add(value).Add(comm).Add(fees)
		var call bool
		var mtm bool
		option := true
		switch rec[15] {
		case "PUT":
		case "CALL":
			call = true
		case "":
			// Handle non-trade transactions.
			if rec[1] != "Trade" {
				// Detect daily mark-to-market of futures held overnight
				if rec[1] == "Money Movement" && rec[4] == "Future" &&
					strings.HasSuffix(rec[5], "Final settlement price") {
					mtm = true
				} else {
					p.parseNonOptionTransaction(rec)
					continue
				}
			}
			// else: fallthrough (this is a trade but a non-option transaction)
			option = false
		default:
			glog.Fatalf("record #%d, bad put/call type: %q", i, rec[15])
		}
		mult, err := strconv.ParseUint(rec[11], 10, 16)
		if option && err != nil {
			glog.Fatalf("record #%d, bad multiplier: %s", i, err)
		}
		expDate, err := time.Parse("1/02/06", rec[13])
		if option && err != nil {
			glog.Fatalf("record #%d, bad transaction date: %s", i, err)
		}
		long := strings.HasPrefix(rec[2], "BUY")
		// Pretend expiration is at 23:00 on the day of expiration so that
		// expDate > tx date for transactions on the day of expiration.
		if option {
			expDate = expDate.Add(23 * time.Hour)
		}
		qty := parseDecimal(rec[7])
		tx := &transaction{
			date:        date,
			txType:      rec[1],
			action:      rec[2],
			symbol:      rec[3],
			instrument:  rec[4],
			description: rec[5],
			value:       value,
			quantity:    qty,
			qtyOpen:     qty,
			avgPrice:    parseDecimal(rec[8]),
			commission:  comm,
			fees:        fees,
			multiplier:  uint16(mult),
			underlying:  rec[12],
			expDate:     expDate,
			strike:      parseDecimal(rec[14]),
			option:      option,
			mtm:         mtm,
			call:        call,
			long:        long,
		}
		if !strings.HasPrefix(tx.symbol, tx.underlying) {
			glog.Fatalf("invalid symbol %q for underlying %q in %s",
				tx.symbol, tx.underlying, tx)
		}
		// Pending clarification from TW support.
		//tx.fixFees()
		tx.sanityCheck()
		p.transactions[j] = tx
		j++
	}
	if ignored := len(records) - j; ignored != 0 {
		glog.V(3).Infof("Ignored %d non-option transactions", ignored)
	}
	p.transactions = p.transactions[:j]
}

func (p *portfolio) handleTransaction(tx *transaction) {
	count := !p.ytd || tx.ytd()
	if count {
		p.comms = p.comms.Add(tx.commission)
		p.fees = p.fees.Add(tx.fees)
	}
	switch tx.txType {
	case "Trade":
		p.numTrades++
		switch tx.action {
		case "SELL_TO_OPEN", "BUY_TO_OPEN":
			p.openPosition(tx)
		case "SELL_TO_CLOSE", "BUY_TO_CLOSE":
			p.closePosition(tx, count)
		default:
			if tx.instrument == "Future" {
				// TODO check what happens for equities
				if tx.value.Equal(decimal.Zero) { // Open
					p.openPosition(tx)
				} else { // Settle
					p.closePosition(tx, count)
				}
			} else {
				glog.Fatalf("Unhandled action type %q in %s %#v", tx.action, tx, tx)
			}
		}
	case "Receive Deliver":
		// We can't use closePosition() here because we don't know whether
		// we're closing a long or a short position.  So the best we can
		// do is just close all positions for this symbol.  In theory, if
		// we held both long and short positions for the same option, that
		// would be problematic, in practice however I don't believe that's
		// possible on TW.
		pos, ok := p.positions[tx.symbol]
		if !ok {
			glog.Fatalf("Couldn't find an opening transaction for %s", tx)
		}
		assigned := strings.HasSuffix(tx.description, "due to assignment")
		if assigned && count {
			p.assigned++
			p.fees = p.fees.Add(assignmentFee)
		}
		quantity := tx.quantity.Abs() // clone
		for _, open := range pos.opens {
			glog.V(3).Infof("position %s expired by %s", open, tx)
			quantity = quantity.Sub(open.quantity)
			if count {
				if assigned {
					cost := open.strike.Mul(decimal.New(int64(open.multiplier), 0))
					p.assTotal = p.assTotal.Sub(cost)
				} else {
					p.recordPL(open, open.value)
				}
			}
		}
		if !quantity.Equal(decimal.Zero) {
			glog.Fatalf("Left with %d position after handling %s", quantity, tx)
		}
		delete(p.positions, tx.symbol)
	case "Money Movement":
		// Handle daily mark-to-market settlement of futures
		if !tx.mtm {
			glog.Fatalf("Unhandled money movement transaction in %s", tx)
		}
		pos, ok := p.positions[tx.symbol]
		if !ok {
			glog.Fatalf("Couldn't find an opening transaction for %s", tx)
		}
		if glog.V(4) {
			descs := make([]string, len(pos.opens))
			for i, open := range pos.opens {
				descs[i] = open.String()
			}
			glog.Infof("%s -> %s ==> mark-to-market = %s",
				strings.Join(descs, ", "), tx, tx.value)
		}
		p.recordPL(tx, tx.value)
	default:
		glog.Fatalf("Unhandled transaction type %q in %s", tx.txType, tx)
	}
}

func (p *portfolio) recordPL(tx *transaction, amount decimal.Decimal) {
	p.rpl = p.rpl.Add(amount)
	var underlying string
	if tx.option {
		underlying = tx.underlying
	} else {
		underlying = tx.symbol
	}
	rpl, ok := p.rplPerUnderlying[underlying]
	if ok {
		amount = rpl.Add(amount)
	}
	p.rplPerUnderlying[underlying] = amount
}

func (p *portfolio) openPosition(tx *transaction) {
	pos, ok := p.positions[tx.symbol]
	if !ok {
		pos = new(position)
		p.positions[tx.symbol] = pos
	}
	pos.opens = append(pos.opens, tx)
}

func (p *portfolio) closePosition(tx *transaction, count bool) {
	// Find a matching, opposite transaction(s)
	pos, ok := p.positions[tx.symbol]
	if !ok {
		glog.Fatalf("Couldn't find an opening transaction for %s", tx)
	}
	remaining := tx.quantity.Abs() // Clone
	for _, open := range pos.opens {
		if open.long == tx.long { // Same direction, ignore.
			glog.V(4).Infof("ignore %s -- it's not closed by %s", open, tx)
			continue
		}
		closed := decimal.Min(remaining, open.qtyOpen)
		remaining = remaining.Sub(closed)
		// Close off this opening transaction.
		open.qtyOpen = open.qtyOpen.Sub(closed)
		rpl := open.avgPrice.Add(tx.avgPrice).Mul(closed)
		glog.V(4).Infof("%s -> %s [%s+%s] ==> realized P&L = %s",
			open, tx, open.avgPrice, tx.avgPrice, rpl)
		if count {
			p.recordPL(tx, rpl)
		}
		if remaining.Equal(decimal.Zero) {
			// We closed off enough opening transactions.
			glog.V(3).Infof("done handling %s after closing %s", tx, open)
			break
		} // else: need to close some more opening transactions.
		glog.V(3).Infof("not done handling %s after closing %s", tx, open)
	}
	if !remaining.Equal(decimal.Zero) {
		glog.Fatalf("couldn't close %s contracts in %s", remaining, tx)
	}
	// Purge closed positions.
	var stillOpen int
	for _, open := range pos.opens {
		if !open.qtyOpen.Equal(decimal.Zero) {
			stillOpen += 1
		}
	}
	if stillOpen == 0 {
		delete(p.positions, tx.symbol)
	} else {
		opens := make([]*transaction, stillOpen)
		var j int
		for _, open := range pos.opens {
			if !open.qtyOpen.Equal(decimal.Zero) {
				opens[j] = open
				j++
			}
		}
		pos.opens = opens
	}
}

func (p *portfolio) PrintPositions() {
	fmt.Println("----- Current portfolio -----")
	for symbol, pos := range p.positions {
		if len(pos.opens) == 1 {
			fmt.Println(symbol, pos.opens[0])
		} else {
			fmt.Println(symbol)
			for i, open := range pos.opens {
				fmt.Printf("    %d: %s\n", i, open)
			}
		}
	}
}

func (p *portfolio) PrintStats() {
	if p.ytd {
		fmt.Println("------- YTD statistics ------")
	} else {
		fmt.Println("----- Overall statistics ----")
	}
	const day = 24 * time.Hour
	numTrades := p.numTrades
	var beginTime time.Time
	if p.ytd {
		beginTime = ytdStart
		var n uint16
		for _, tx := range p.transactions {
			if tx.ytd() { // Once we find the first YTD transaction...
				numTrades -= n // ... discount all the ones before.
				break
			} else if tx.txType == "Trade" {
				n++
			}
		}
	} else {
		beginTime = p.transactions[0].date
	}
	duration := p.transactions[len(p.transactions)-1].date.Sub(beginTime).Round(day)
	days := int(duration / day)
	pct := func(amount decimal.Decimal) string {
		return amount.Mul(oneHundred).Div(p.rpl).StringFixed(2) + "%"
	}
	fmt.Printf("Number of transactions: %5d    (in %d days => %.1f/day avg)\n",
		numTrades, days, float32(numTrades)/float32(days))
	fmt.Printf("Realized P&L:           %8s\n", p.rpl.StringFixed(2))
	fmt.Printf("Commissions:            %8s (%s of P&L)\n", p.comms.StringFixed(2), pct(p.comms))
	fmt.Printf("Fees:                   %8s (%s of P&L)\n", p.fees.StringFixed(2), pct(p.fees))
	fmt.Printf("Interest:               %8s (%s of P&L)\n", p.intrs.StringFixed(2), pct(p.intrs))
	grosspl := p.rpl.Add(p.comms).Add(p.fees).Add(p.intrs)
	fmt.Printf("Gross P&L:              %8s (~%s/day avg, %s of P&L)\n",
		grosspl.StringFixed(2),
		grosspl.Div(decimal.New(int64(days), 0)).StringFixed(2), pct(grosspl))
	fmt.Printf("Stock assignments:      %8s (%d)\n", p.assTotal.StringFixed(2), p.assigned)
	fmt.Printf("Adjusted Gross P&L:     %8s\n", grosspl.Add(p.assTotal).StringFixed(2))
	fmt.Printf("Net ACH movements:      %8s\n", p.ach.StringFixed(2))
	var premium decimal.Decimal
	for _, pos := range p.positions {
		for _, open := range pos.opens {
			premium = premium.Add(open.value)
		}
	}
	fmt.Printf("Outstanding premium:    %8s\n", premium.StringFixed(2))
	fmt.Printf("Cash on hand:           %8s\n", p.cash.StringFixed(2))
	if !p.ytd {
		// Alternative method to compute cash on hand
		cash := grosspl.Add(premium).Add(p.ach).Add(p.assTotal).Add(p.miscCash)
		if !cash.Equal(p.cash) {
			fmt.Printf("-> Warning: estimated cash on hand should've been %s (difference: %s)\n",
				cash.StringFixed(2), cash.Sub(p.cash).StringFixed(2))
		}
	}
}

func (p *portfolio) PrintPL() {
	fmt.Println("---- Realized P&L detail ----")
	underlyings := make([]string, len(p.rplPerUnderlying))
	var i int
	for underlying := range p.rplPerUnderlying {
		underlyings[i] = underlying
		i++
	}
	sort.Sort(sort.StringSlice(underlyings))
	for _, underlying := range underlyings {
		fmt.Printf("%-5s%8s\n", underlying, p.rplPerUnderlying[underlying].StringFixed(2))
	}
}

func main() {
	input := flag.String("input", "", "input csv file containing tastyworks transactions")
	stats := flag.Bool("stats", true, "print overall statistics")
	ytd := flag.Bool("ytd", false, "limit output to YTD transactions")
	printPL := flag.Bool("printpl", false, "print realized P&L per underlying")
	positions := flag.Bool("positions", false, "print current positions")
	flag.Parse()
	if *input == "" {
		glog.Fatal("-input flag required")
	}
	records := loadCSV(*input)
	if len(records) < 3 { // need header + 2 transactions minimum
		glog.Fatal("not enough records, check CSV file")
	}
	if records[0][0] != "Date" { // Quick sanity check
		glog.Fatal("CSV seems malformed")
	}

	portfolio := NewPortfolio(records[1:], *ytd)
	if *stats {
		portfolio.PrintStats()
	}
	if *printPL {
		portfolio.PrintPL()
	}
	if *positions {
		portfolio.PrintPositions()
	}
}
